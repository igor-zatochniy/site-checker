package sitechecker

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
)

func Main(version, commit, buildDate string) error {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)
	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	return runApplication(signalCtx, version, commit, buildDate, logger)
}

func runApplication(parentCtx context.Context, version, commit, buildDate string, logger *slog.Logger) error {
	cfg, err := LoadConfig()
	if err != nil {
		return fmt.Errorf("invalid configuration: %w", err)
	}

	policy := NewNetworkPolicy(cfg)
	if cfg.AlertWebhookURL != "" {
		if err := policy.ValidateURL(cfg.AlertWebhookURL); err != nil {
			return fmt.Errorf("invalid alert webhook URL: %w", err)
		}
	}

	ctx, cancel := context.WithCancel(parentCtx)
	defer cancel()

	seedLinks := []string{}
	if roleEnabled(cfg.AppRole, "scheduler") {
		seedLinks, err = LoadSeedLinks(cfg)
		if err != nil {
			return fmt.Errorf("load seed links: %w", err)
		}
		if len(seedLinks) > 0 {
			if err := ValidateLinks(seedLinks, policy); err != nil {
				return fmt.Errorf("invalid seed link configuration: %w", err)
			}
		}
	}

	metrics := NewMetrics(version, commit, buildDate, 0)
	repo, closeRepo, err := NewConfiguredRepository(ctx, cfg, policy, logger)
	if err != nil {
		return fmt.Errorf("initialize repository: %w", err)
	}
	defer closeRepo()
	if len(seedLinks) > 0 {
		if err := SeedRepository(ctx, repo, seedLinks, cfg); err != nil {
			return fmt.Errorf("seed monitors: %w", err)
		}
		logger.Info("Seeded configured monitors", "count", len(seedLinks))
	} else if roleEnabled(cfg.AppRole, "scheduler") {
		logger.Info("No seed URLs configured")
	}

	checkClient := NewCheckHTTPClient(cfg, policy)
	checker := NewChecker(checkClient, cfg, metrics)
	alertPolicy := AlertPolicy{
		Enabled:          cfg.AlertWebhookURL != "",
		FailureThreshold: cfg.AlertFailureThreshold,
		Cooldown:         cfg.AlertCooldown,
	}
	service := NewMonitorService(repo, checker, metrics, alertPolicy, logger)
	service.updateTotalLinks(ctx)
	api := NewAPIHandler(service, cfg.APIKey, logger)
	var retentionRepo RetentionRepository
	if cfg.RetentionEnabled {
		var ok bool
		retentionRepo, ok = repo.(RetentionRepository)
		if !ok {
			return errors.New("configured repository does not support data retention")
		}
	}

	var (
		alertRepo   AlertOutboxRepository
		alertSender *AlertSender
	)
	if roleEnabled(cfg.AppRole, "alert-dispatcher") && cfg.AlertWebhookURL != "" {
		var ok bool
		alertRepo, ok = repo.(AlertOutboxRepository)
		if !ok {
			return errors.New("configured repository does not support persisted alert delivery")
		}
		alertClient := &http.Client{
			Transport: NewSecureTransport(cfg, policy),
			Timeout:   cfg.AlertDeliveryTimeout,
		}
		alertSender = NewAlertSender(cfg.AlertWebhookURL, cfg.UserAgent, alertClient)
	}

	var queue JobQueue
	if roleEnabled(cfg.AppRole, "scheduler") || roleEnabled(cfg.AppRole, "worker") {
		queue, err = NewConfiguredQueueWithTopologyLossHandler(cfg, queueTopologyLossHandler(service, logger))
		if err != nil {
			return fmt.Errorf("initialize queue: %w", err)
		}
		defer queue.Close()
	}

	var wg sync.WaitGroup
	fatalErrors := make(chan error, 1)
	reportFatal := func(err error) {
		if err == nil || ctx.Err() != nil {
			return
		}
		select {
		case fatalErrors <- err:
		default:
		}
		cancel()
	}
	if cfg.HealthAddr != "" {
		_, httpDone, err := RunHTTPServer(ctx, cfg, metrics, api, roleEnabled(cfg.AppRole, "api"), BuildReadinessDependencies(cfg, repo, queue), logger)
		if err != nil {
			return fmt.Errorf("start HTTP server: %w", err)
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := <-httpDone; err != nil {
				logger.Error("HTTP server stopped unexpectedly", "error", err)
				reportFatal(err)
			}
		}()
	}

	logger.Info("Site Checker started",
		"version", version,
		"commit", commit,
		"build_date", buildDate,
		"role", cfg.AppRole,
		"storage", cfg.StorageType,
		"queue", cfg.QueueType,
		"workers", cfg.WorkerCount,
		"interval", cfg.CheckInterval,
		"timeout", cfg.HTTPTimeout,
		"seed_links", len(seedLinks),
		"health_addr", cfg.HealthAddr,
		"pprof_enabled", cfg.EnablePprof,
		"alerts_enabled", alertPolicy.Enabled,
	)

	if queue != nil && roleEnabled(cfg.AppRole, "scheduler") {
		wg.Add(1)
		go func() {
			defer wg.Done()
			RunQueueScheduler(ctx, service, queue, cfg, logger)
		}()
	}
	if queue != nil && roleEnabled(cfg.AppRole, "worker") {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := runWorkerComponent(ctx, service, queue, cfg.WorkerCount, cfg.CheckLeaseTimeout, logger); err != nil {
				logger.Error("Workers stopped unexpectedly", "error", err)
				reportFatal(err)
			}
		}()
	}
	if alertRepo != nil && alertSender != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			RunAlertDispatcher(ctx, alertRepo, alertSender, cfg, metrics, logger)
		}()
	}
	if retentionRepo != nil {
		wg.Add(1)
		go func() {
			defer wg.Done()
			RunRetention(ctx, retentionRepo, cfg, logger)
		}()
	}

	var runtimeErr error
	select {
	case <-parentCtx.Done():
	case runtimeErr = <-fatalErrors:
	}
	cancel()
	wg.Wait()
	if runtimeErr != nil {
		return runtimeErr
	}
	logger.Info("Site Checker stopped gracefully")
	return nil
}

func runWorkerComponent(ctx context.Context, service *MonitorService, queue JobQueue, workerCount int, leaseTimeout time.Duration, logger *slog.Logger) error {
	err := RunQueueWorkers(ctx, service, queue, workerCount, leaseTimeout, logger)
	if err == nil || ctx.Err() != nil {
		return nil
	}
	return fmt.Errorf("queue workers stopped: %w", err)
}
