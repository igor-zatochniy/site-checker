package sitechecker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"time"
)

const (
	leaseStateRetryInitialDelay = 50 * time.Millisecond
	leaseStateRetryMaxDelay     = time.Second
	checkJobRetryInitialDelay   = 250 * time.Millisecond
	checkJobRetryMaxDelay       = 30 * time.Second
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
) (*http.Server, <-chan struct{}) {
	done := make(chan struct{})
	registrars := []func(*http.ServeMux){RegisterOpenAPI}
	if enableAPI && api != nil {
		registrars = append([]func(*http.ServeMux){api.Register}, registrars...)
	}

	server := NewObservabilityServerWithDependencies(cfg.HealthAddr, cfg, metrics, dependencies, registrars...)
	listener, err := net.Listen("tcp", cfg.HealthAddr)
	if err != nil {
		logger.Error("Failed to start API server", "addr", cfg.HealthAddr, "error", err)
		close(done)
		cancel()
		return nil, done
	}
	server.Addr = listener.Addr().String()

	serveDone := make(chan struct{})
	go func() {
		defer close(serveDone)
		logger.Info("HTTP server started", "addr", server.Addr, "api_enabled", enableAPI)
		if err := server.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("API server stopped unexpectedly", "error", err)
			cancel()
		}
	}()

	go func() {
		defer close(done)
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := server.Shutdown(shutdownCtx); err != nil {
			logger.Warn("API server shutdown timed out", "error", err)
		}
		<-serveDone
	}()
	return server, done
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
	jobs, err := service.ClaimDueJobs(ctx, limit, now, leaseTimeout)
	if err != nil {
		logger.Warn("Failed to claim due check jobs", "error", err)
		return
	}

	for _, persistedJob := range jobs {
		message := CheckJobMessage{
			JobID:      persistedJob.ID,
			MonitorID:  persistedJob.MonitorID,
			Attempt:    persistedJob.Attempt + 1,
			EnqueuedAt: now,
		}
		if err := queue.Publish(ctx, message); err != nil {
			if releaseErr := service.ReleaseJobPublish(ctx, persistedJob.MonitorID, persistedJob.ID, "broker publish failed", time.Now().UTC()); releaseErr != nil {
				logger.Warn("Failed to release check job publish lease",
					"monitor_id", persistedJob.MonitorID,
					"job_id", persistedJob.ID,
					"error", releaseErr,
				)
			}
			service.metrics.RecordSkipped()
			logger.Warn("Failed to publish monitor job", "monitor_id", persistedJob.MonitorID, "job_id", persistedJob.ID, "error", err)
			continue
		}
		if err := service.MarkJobPublished(ctx, persistedJob.MonitorID, persistedJob.ID, time.Now().UTC()); err != nil {
			logger.Warn("Failed to persist published check job state",
				"monitor_id", persistedJob.MonitorID,
				"job_id", persistedJob.ID,
				"error", err,
			)
		}
		service.metrics.RecordScheduled()
	}
}

func RunQueueWorkers(ctx context.Context, service *MonitorService, queue JobQueue, workerCount int, leaseTimeout time.Duration, logger *slog.Logger) error {
	workerCtx, stopWorkers := context.WithCancel(ctx)
	defer stopWorkers()

	deliveries, consumerErrors, err := queue.Consume(workerCtx)
	if err != nil {
		return err
	}

	var wg sync.WaitGroup
	workerErrors := make(chan error, workerCount)
	for workerID := 1; workerID <= workerCount; workerID++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			if err := runQueueWorker(workerCtx, workerID, service, deliveries, leaseTimeout, logger); err != nil {
				select {
				case workerErrors <- err:
				default:
				}
			}
		}(workerID)
	}

	workersDone := make(chan struct{})
	go func() {
		wg.Wait()
		close(workersDone)
	}()

	select {
	case <-ctx.Done():
		stopWorkers()
		<-workersDone
		logger.Info("Workers stopped")
		return nil
	case err := <-consumerErrors:
		stopWorkers()
		<-workersDone
		if err == nil {
			err = ErrQueueConsumerClosed
		}
		return fmt.Errorf("queue consumer stopped: %w", err)
	case err := <-workerErrors:
		stopWorkers()
		<-workersDone
		return fmt.Errorf("queue worker failed: %w", err)
	case <-workersDone:
		if ctx.Err() == nil {
			return errors.New("all queue workers stopped unexpectedly")
		}
		logger.Info("Workers stopped")
		return nil
	}
}

func runQueueWorker(ctx context.Context, workerID int, service *MonitorService, deliveries <-chan QueueDelivery, leaseTimeout time.Duration, logger *slog.Logger) error {
	for {
		select {
		case <-ctx.Done():
			return nil
		case delivery, ok := <-deliveries:
			if !ok {
				return nil
			}
			if err := handleQueueDelivery(ctx, workerID, service, delivery, leaseTimeout, logger); err != nil {
				return err
			}
		}
	}
}

func handleQueueDelivery(ctx context.Context, workerID int, service *MonitorService, delivery QueueDelivery, leaseTimeout time.Duration, logger *slog.Logger) error {
	monitor, err := service.Get(ctx, delivery.Job.MonitorID)
	if err != nil {
		if errors.Is(err, ErrMonitorNotFound) {
			_ = delivery.Ack(ctx)
			return nil
		}
		return requeueInfrastructureFailure(ctx, delivery, err, workerID, "monitor lookup", logger)
	}

	lease, err := service.MarkProcessing(ctx, delivery.Job.MonitorID, delivery.Job.JobID, delivery.Job.Attempt, time.Now().UTC(), leaseTimeout)
	if err != nil {
		if errors.Is(err, ErrStaleJob) || errors.Is(err, ErrJobAlreadyProcessing) || errors.Is(err, ErrMonitorNotFound) {
			if ackErr := delivery.Ack(ctx); ackErr != nil {
				logger.Warn("Failed to ack inactive job", "worker", workerID, "job_id", delivery.Job.JobID, "error", ackErr)
			}
			return nil
		}
		return requeueInfrastructureFailure(ctx, delivery, err, workerID, "processing state transition", logger)
	}

	checkCtx, cancel := context.WithTimeout(ctx, time.Duration(monitor.TimeoutSeconds)*time.Second)
	result := service.checker.CheckMonitor(checkCtx, monitor)
	cancel()

	record := CheckRecordFromResult(result)
	record.JobID = delivery.Job.JobID
	if err := service.StoreCheckResult(ctx, record, result, lease); err != nil {
		if delivery.Retryable {
			if stateErr := retryJobStateTransition(ctx, func(ctx context.Context) error {
				now := time.Now().UTC()
				retryAt := now.Add(checkJobRetryDelay(delivery.Job.Attempt))
				return service.MarkJobFailed(ctx, lease, "result persistence failed", now, retryAt)
			}); stateErr != nil {
				if ackInactiveJob(ctx, delivery, stateErr, workerID, logger) {
					return nil
				}
				logger.Warn("Failed to persist retryable check job failure", "worker", workerID, "job_id", delivery.Job.JobID, "error", stateErr)
				return fmt.Errorf("persist retryable check job failure: %w", stateErr)
			}
			if ackErr := delivery.Ack(ctx); ackErr != nil {
				logger.Warn("Failed to ack job after persisting retry state", "worker", workerID, "job_id", delivery.Job.JobID, "error", ackErr)
				return fmt.Errorf("ack job after persisting retry state: %w", ackErr)
			}
		} else {
			now := time.Now().UTC()
			nextCheckAt := now.Add(time.Duration(monitor.IntervalSeconds) * time.Second)
			if stateErr := retryJobStateTransition(ctx, func(ctx context.Context) error {
				return service.FailProcessing(ctx, lease, "result persistence failed", now, nextCheckAt)
			}); stateErr != nil {
				if ackInactiveJob(ctx, delivery, stateErr, workerID, logger) {
					return nil
				}
				logger.Warn("Failed to persist dead check job state", "worker", workerID, "job_id", delivery.Job.JobID, "error", stateErr)
				return fmt.Errorf("persist dead check job state: %w", stateErr)
			}
			if nackErr := delivery.Nack(ctx, false); nackErr != nil {
				logger.Warn("Failed to dead-letter exhausted job", "worker", workerID, "job_id", delivery.Job.JobID, "error", nackErr)
				return fmt.Errorf("dead-letter exhausted job: %w", nackErr)
			}
		}
		return nil
	}

	if err := delivery.Ack(ctx); err != nil {
		logger.Warn("Failed to ack job", "worker", workerID, "job_id", delivery.Job.JobID, "error", err)
	}
	return nil
}

func requeueInfrastructureFailure(
	ctx context.Context,
	delivery QueueDelivery,
	cause error,
	workerID int,
	operation string,
	logger *slog.Logger,
) error {
	requeue := delivery.Requeue
	if requeue == nil {
		requeue = func(ctx context.Context) error {
			return delivery.Nack(ctx, true)
		}
	}
	if err := requeue(ctx); err != nil {
		logger.Warn("Failed to requeue job after infrastructure error",
			"worker", workerID,
			"job_id", delivery.Job.JobID,
			"operation", operation,
			"error", err,
		)
		return fmt.Errorf("%s failed and job requeue failed: %w", operation, errors.Join(cause, err))
	}
	return fmt.Errorf("%s failed: %w", operation, cause)
}

func checkJobRetryDelay(attempt int) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := checkJobRetryInitialDelay
	for current := 1; current < attempt && delay < checkJobRetryMaxDelay; current++ {
		if delay > checkJobRetryMaxDelay/2 {
			return checkJobRetryMaxDelay
		}
		delay *= 2
	}
	return min(delay, checkJobRetryMaxDelay)
}

func retryJobStateTransition(ctx context.Context, transition func(context.Context) error) error {
	delay := leaseStateRetryInitialDelay
	for {
		err := transition(ctx)
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrMonitorNotFound) || errors.Is(err, ErrStaleJob) {
			return err
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				select {
				case <-timer.C:
				default:
				}
			}
			return err
		case <-timer.C:
		}
		delay = min(delay*2, leaseStateRetryMaxDelay)
	}
}

func ackInactiveJob(ctx context.Context, delivery QueueDelivery, err error, workerID int, logger *slog.Logger) bool {
	if !errors.Is(err, ErrMonitorNotFound) && !errors.Is(err, ErrStaleJob) {
		return false
	}
	if ackErr := delivery.Ack(ctx); ackErr != nil {
		logger.Warn("Failed to ack inactive job", "worker", workerID, "job_id", delivery.Job.JobID, "error", ackErr)
	}
	return true
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
