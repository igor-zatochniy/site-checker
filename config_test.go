package main

import (
	"os"
	"testing"
	"time"
)

var configEnvKeys = []string{
	"WORKER_COUNT",
	"CHECK_INTERVAL",
	"HTTP_TIMEOUT",
	"HEALTH_ADDR",
	"MAX_REDIRECTS",
	"MAX_BODY_BYTES",
	"MAX_HEADER_BYTES",
	"ALLOW_PRIVATE_NETWORKS",
	"ALLOW_PROXY_ENV",
	"ALLOWED_PORTS",
	"URLS_FILE",
	"EXPECTED_STATUS",
	"ALERT_WEBHOOK_URL",
	"ALERT_FAILURE_THRESHOLD",
	"ALERT_COOLDOWN",
	"USER_AGENT",
	"ENABLE_PPROF",
	"READINESS_STALE_AFTER",
	"STARTUP_GRACE_PERIOD",
}

func cleanConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range configEnvKeys {
		oldValue, existed := os.LookupEnv(key)
		if err := os.Unsetenv(key); err != nil {
			t.Fatalf("unset %s: %v", key, err)
		}
		t.Cleanup(func() {
			if existed {
				_ = os.Setenv(key, oldValue)
			} else {
				_ = os.Unsetenv(key)
			}
		})
	}
}

func TestLoadConfigDefaults(t *testing.T) {
	cleanConfigEnv(t)

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}

	if cfg.WorkerCount != 10 {
		t.Fatalf("WorkerCount = %d, want 10", cfg.WorkerCount)
	}
	if cfg.CheckInterval != 5*time.Minute {
		t.Fatalf("CheckInterval = %s, want 5m", cfg.CheckInterval)
	}
	if _, ok := cfg.AllowedPorts[80]; !ok {
		t.Fatalf("port 80 is not allowed by default")
	}
	if _, ok := cfg.AllowedPorts[443]; !ok {
		t.Fatalf("port 443 is not allowed by default")
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("WORKER_COUNT", "0")
	t.Setenv("CHECK_INTERVAL", "5s")
	t.Setenv("ALLOWED_PORTS", "443,not-a-port")

	_, err := LoadConfig()
	if err == nil {
		t.Fatal("LoadConfig returned nil error for invalid values")
	}
}

func TestLoadConfigAcceptsOverrides(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("WORKER_COUNT", "4")
	t.Setenv("CHECK_INTERVAL", "45s")
	t.Setenv("HTTP_TIMEOUT", "2s")
	t.Setenv("EXPECTED_STATUS", "200-204,301")
	t.Setenv("ALLOWED_PORTS", "80,443,8443")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if cfg.WorkerCount != 4 {
		t.Fatalf("WorkerCount = %d, want 4", cfg.WorkerCount)
	}
	if !cfg.ExpectedStatus.Allows(301) {
		t.Fatalf("status policy does not allow 301")
	}
	if _, ok := cfg.AllowedPorts[8443]; !ok {
		t.Fatalf("port 8443 is not allowed")
	}
}
