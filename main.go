package main

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

var (
	version   = "dev"
	commit    = "none"
	buildDate = "unknown"
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	slog.SetDefault(logger)

	cfg, err := LoadConfig()
	if err != nil {
		logger.Error("Invalid configuration", "error", err)
		os.Exit(1)
	}

	policy := NewNetworkPolicy(cfg)
	if cfg.AlertWebhookURL != "" {
		if err := policy.ValidateURL(cfg.AlertWebhookURL); err != nil {
			logger.Error("Invalid alert webhook URL", "error", err)
			os.Exit(1)
		}
	}

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()

	seedLinks := []string{}
	if roleEnabled(cfg.AppRole, "scheduler") {
		seedLinks, err = LoadSeedLinks(cfg)
		if err != nil {
			logger.Error("Failed to load seed links", "error", err)
			os.Exit(1)
		}
		if len(seedLinks) > 0 {
			if err := ValidateLinks(seedLinks, policy); err != nil {
				logger.Error("Invalid seed link configuration", "error", err)
				os.Exit(1)
			}
		}
	}

	metrics := NewMetrics(version, commit, buildDate, 0)
	repo, closeRepo, err := NewConfiguredRepository(ctx, cfg, policy, logger)
	if err != nil {
		logger.Error("Failed to initialize repository", "error", err)
		os.Exit(1)
	}
	defer closeRepo()
	if len(seedLinks) > 0 {
		if err := SeedRepository(ctx, repo, seedLinks, cfg); err != nil {
			logger.Error("Failed to seed monitors", "error", err)
			os.Exit(1)
		}
		logger.Info("Seeded configured monitors", "count", len(seedLinks))
	} else if roleEnabled(cfg.AppRole, "scheduler") {
		logger.Info("No seed URLs configured")
	}

	checkClient := &http.Client{
		Transport:     NewSecureTransport(cfg, policy),
		Timeout:       cfg.HTTPTimeout,
		CheckRedirect: policy.CheckRedirect,
	}
	alertClient := &http.Client{
		Transport:     NewSecureTransport(cfg, policy),
		Timeout:       cfg.HTTPTimeout,
		CheckRedirect: policy.CheckRedirect,
	}

	alerts := NewAlertManager(cfg.AlertWebhookURL, alertClient, metrics, logger, cfg.AlertFailureThreshold, cfg.AlertCooldown)
	checker := NewChecker(checkClient, cfg, metrics)
	service := NewMonitorService(repo, checker, metrics, alerts, logger)
	service.updateTotalLinks(ctx)
	api := NewAPIHandler(service, cfg.APIKey, logger)

	var queue JobQueue
	if roleEnabled(cfg.AppRole, "scheduler") || roleEnabled(cfg.AppRole, "worker") {
		queue, err = NewConfiguredQueue(cfg)
		if err != nil {
			logger.Error("Failed to initialize queue", "error", err)
			os.Exit(1)
		}
		defer queue.Close()
	}

	if cfg.HealthAddr != "" {
		RunHTTPServer(ctx, cfg, metrics, api, roleEnabled(cfg.AppRole, "api"), BuildReadinessDependencies(cfg, repo, queue), logger, cancel)
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
	)

	var wg sync.WaitGroup
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
			if err := RunQueueWorkers(ctx, service, queue, cfg.WorkerCount, cfg.CheckLeaseTimeout, logger); err != nil && ctx.Err() == nil {
				logger.Error("Workers stopped unexpectedly", "error", err)
				cancel()
			}
		}()
	}

	<-ctx.Done()
	wg.Wait()
	logger.Info("Site Checker stopped gracefully")
}
