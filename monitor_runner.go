package main

import (
	"context"
	"errors"
	"log/slog"
	"sync"
	"time"
)

// RunMonitorScheduler is the legacy in-memory monitor runner retained for
// regression tests and development reference. Production runtime is split into
// API, scheduler, and worker roles connected through JobQueue.
func RunMonitorScheduler(
	ctx context.Context,
	store *MonitorStore,
	checker *Checker,
	metrics *Metrics,
	workerCount int,
	logger *slog.Logger,
) {
	jobs := make(chan Monitor, workerCount)
	var wg sync.WaitGroup

	for i := 1; i <= workerCount; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runMonitorWorker(ctx, workerID, jobs, store, checker, metrics, logger)
		}(i)
	}

	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		enqueueDueMonitors(ctx, store, jobs, workerCount, logger)

		select {
		case <-ctx.Done():
			close(jobs)
			wg.Wait()
			logger.Info("Monitor scheduler stopped")
			return
		case <-ticker.C:
		}
	}
}

func enqueueDueMonitors(ctx context.Context, store *MonitorStore, jobs chan<- Monitor, limit int, logger *slog.Logger) {
	now := time.Now().UTC()
	for _, monitor := range store.ClaimDue(limit, now) {
		select {
		case jobs <- monitor:
		case <-ctx.Done():
			store.CompleteWithoutRecord(monitor.ID)
			return
		default:
			store.CompleteWithoutRecord(monitor.ID)
			logger.Warn("Monitor queue is full", "monitor_id", monitor.ID, "url", monitor.URL)
			return
		}
	}
}

func runMonitorWorker(
	ctx context.Context,
	workerID int,
	jobs <-chan Monitor,
	store *MonitorStore,
	checker *Checker,
	metrics *Metrics,
	logger *slog.Logger,
) {
	for {
		select {
		case <-ctx.Done():
			return
		case monitor, ok := <-jobs:
			if !ok {
				return
			}

			checkCtx, cancel := context.WithTimeout(ctx, time.Duration(monitor.TimeoutSeconds)*time.Second)
			result := checker.CheckMonitor(checkCtx, monitor)
			cancel()

			record := CheckRecordFromResult(result)
			if _, err := store.AddCheck(record); err != nil {
				if !errors.Is(err, ErrMonitorNotFound) {
					logger.Warn("Failed to store check result", "worker", workerID, "monitor_id", monitor.ID, "error", err)
				}
				continue
			}

			metrics.RecordResult(result)
		}
	}
}
