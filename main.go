package main

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
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

	links, err := LoadLinks(cfg.URLsFile, DefaultLinks())
	if err != nil {
		logger.Error("Failed to load links", "error", err)
		os.Exit(1)
	}
	if err := ValidateLinks(links, policy); err != nil {
		logger.Error("Invalid link configuration", "error", err)
		os.Exit(1)
	}

	signalCtx, stopSignals := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stopSignals()
	ctx, cancel := context.WithCancel(signalCtx)
	defer cancel()

	metrics := NewMetrics(version, commit, buildDate, len(links))
	store, err := SeedMonitorStore(links, cfg, policy)
	if err != nil {
		logger.Error("Failed to initialize monitor store", "error", err)
		os.Exit(1)
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
	api := NewAPIHandler(store, checker, metrics, alerts, logger)

	var observabilityServer *http.Server
	if cfg.HealthAddr != "" {
		observabilityServer = NewObservabilityServer(cfg.HealthAddr, cfg, metrics, api.Register, RegisterOpenAPI)
		listener, err := net.Listen("tcp", cfg.HealthAddr)
		if err != nil {
			logger.Error("Failed to start observability server", "addr", cfg.HealthAddr, "error", err)
			os.Exit(1)
		}

		go func() {
			logger.Info("Observability server started", "addr", cfg.HealthAddr)
			if err := observabilityServer.Serve(listener); err != nil && !errors.Is(err, http.ErrServerClosed) {
				logger.Error("Observability server stopped unexpectedly", "error", err)
				cancel()
			}
		}()
	}

	logger.Info("Site Checker started",
		"version", version,
		"commit", commit,
		"build_date", buildDate,
		"workers", cfg.WorkerCount,
		"interval", cfg.CheckInterval,
		"timeout", cfg.HTTPTimeout,
		"links", len(links),
		"health_addr", cfg.HealthAddr,
		"pprof_enabled", cfg.EnablePprof,
	)

	RunMonitorScheduler(ctx, store, checker, metrics, alerts, cfg.WorkerCount, logger)

	if observabilityServer != nil {
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		if err := observabilityServer.Shutdown(shutdownCtx); err != nil {
			logger.Warn("Observability server shutdown timed out", "error", err)
		}
	}

	logger.Info("Site Checker stopped gracefully")
}
