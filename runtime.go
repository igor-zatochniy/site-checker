package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

func NewConfiguredRepository(ctx context.Context, cfg Config, policy *NetworkPolicy, logger *slog.Logger) (MonitorRepository, func(), error) {
	if cfg.StorageType == "postgres" {
		pool, err := OpenPostgresPool(ctx, cfg.DatabaseURL)
		if err != nil {
			return nil, nil, err
		}
		if cfg.RunMigrations {
			if err := RunMigrations(ctx, pool); err != nil {
				pool.Close()
				return nil, nil, err
			}
			logger.Info("Database migrations applied")
		}
		return NewPostgresMonitorRepository(pool, policy), pool.Close, nil
	}

	return NewInMemoryMonitorRepository(policy), func() {}, nil
}

func NewConfiguredQueue(cfg Config) (JobQueue, error) {
	if cfg.QueueType == "rabbitmq" {
		return NewRabbitMQQueue(cfg)
	}
	return NewInMemoryQueue(cfg.QueueBufferSize, cfg.MaxJobAttempts), nil
}

func RunHTTPServer(
	ctx context.Context,
	cfg Config,
	metrics *Metrics,
	api *APIHandler,
	enableAPI bool,
	dependencies []ReadinessDependency,
	logger *slog.Logger,
	cancel context.CancelFunc,
) *http.Server {
	registrars := []func(*http.ServeMux){RegisterOpenAPI}
	if enableAPI && api != nil {
		registrars = append([]func(*http.ServeMux){api.Register}, registrars...)
	}

	server := NewObservabilityServerWithDependencies(cfg.HealthAddr, cfg, metrics, dependencies, registrars...)
	listener, err := net.Listen("tcp", cfg.HealthAddr)
	if err != nil {
		logger.Error("Failed to start API server", "addr", cfg.HealthAddr, "error", err)
		cancel()
		return nil
	}

	go func() {
		logger.Info("HTTP server started", "addr", cfg.HealthAddr, "api_enabled", enableAPI)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("API server stopped unexpectedly", "error", err)
			cancel()
		}
	}()

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn("API server shutdown timed out", "error", err)
		}
	}()
	return server
}

func RunQueueScheduler(ctx context.Context, service *MonitorService, queue JobQueue, cfg Config, logger *slog.Logger) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		enqueueDueMonitorJobs(ctx, service, queue, cfg.SchedulerBatchSize, cfg.CheckLeaseTimeout, logger)

		select {
		case <-ctx.Done():
			logger.Info("Scheduler stopped")
			return
		case <-ticker.C:
		}
	}
}

func enqueueDueMonitorJobs(ctx context.Context, service *MonitorService, queue JobQueue, limit int, leaseTimeout time.Duration, logger *slog.Logger) {
	now := time.Now().UTC()
	monitors, err := service.ClaimDue(ctx, limit, now, leaseTimeout)
	if err != nil {
		logger.Warn("Failed to claim due monitors", "error", err)
		return
	}

	for _, monitor := range monitors {
		job := CheckJobMessage{
			JobID:      NewCheckJobID(monitor.ID, monitor.NextCheckAt),
			MonitorID:  monitor.ID,
			Attempt:    1,
			EnqueuedAt: now,
		}
		if err := queue.Publish(ctx, job); err != nil {
			_ = service.CompleteWithoutRecord(ctx, monitor.ID)
			service.metrics.RecordSkipped()
			logger.Warn("Failed to publish monitor job", "monitor_id", monitor.ID, "error", err)
			continue
		}
		service.metrics.RecordScheduled()
	}
}

func RunQueueWorkers(ctx context.Context, service *MonitorService, queue JobQueue, workerCount int, logger *slog.Logger) error {
	deliveries, err := queue.Consume(ctx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	for workerID := 1; workerID <= workerCount; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runQueueWorker(ctx, workerID, service, deliveries, logger)
		}(workerID)
	}

	<-ctx.Done()
	wg.Wait()
	logger.Info("Workers stopped")
	return nil
}

func runQueueWorker(ctx context.Context, workerID int, service *MonitorService, deliveries <-chan QueueDelivery, logger *slog.Logger) {
	for {
		select {
		case <-ctx.Done():
			return
		case delivery, ok := <-deliveries:
			if !ok {
				return
			}
			handleQueueDelivery(ctx, workerID, service, delivery, logger)
		}
	}
}

func handleQueueDelivery(ctx context.Context, workerID int, service *MonitorService, delivery QueueDelivery, logger *slog.Logger) {
	monitor, err := service.Get(ctx, delivery.Job.MonitorID)
	if err != nil {
		if errors.Is(err, ErrMonitorNotFound) {
			_ = delivery.Ack(ctx)
			return
		}
		if nackErr := delivery.Nack(ctx, true); nackErr != nil {
			logger.Warn("Failed to nack job after monitor lookup error", "worker", workerID, "job_id", delivery.Job.JobID, "error", nackErr)
		}
		return
	}

	checkCtx, cancel := context.WithTimeout(ctx, time.Duration(monitor.TimeoutSeconds)*time.Second)
	result := service.checker.CheckMonitor(checkCtx, monitor)
	cancel()

	record := CheckRecordFromResult(result)
	record.JobID = delivery.Job.JobID
	if err := service.StoreCheckResult(ctx, record, result); err != nil {
		if nackErr := delivery.Nack(ctx, true); nackErr != nil {
			logger.Warn("Failed to nack job after result storage error", "worker", workerID, "job_id", delivery.Job.JobID, "error", nackErr)
		}
		return
	}

	if err := delivery.Ack(ctx); err != nil {
		logger.Warn("Failed to ack job", "worker", workerID, "job_id", delivery.Job.JobID, "error", err)
	}
}

func roleEnabled(role, target string) bool {
	return role == "all" || role == target
}

func BuildReadinessDependencies(cfg Config, repo MonitorRepository, queue JobQueue) []ReadinessDependency {
	dependencies := make([]ReadinessDependency, 0, 2)
	if cfg.StorageType == "postgres" && repo != nil {
		dependencies = append(dependencies, ReadinessDependency{
			Name:  "postgres",
			Check: repo.Ping,
		})
	}
	if cfg.QueueType == "rabbitmq" && queue != nil && (roleEnabled(cfg.AppRole, "scheduler") || roleEnabled(cfg.AppRole, "worker")) {
		dependencies = append(dependencies, ReadinessDependency{
			Name:  "rabbitmq",
			Check: queue.Ping,
		})
	}
	return dependencies
}
