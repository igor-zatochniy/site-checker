package main

import (
	"os"
	"testing"
	"time"
)

var configEnvKeys = []string{
	"APP_ENV",
	"APP_ROLE",
	"STORAGE_TYPE",
	"DATABASE_URL",
	"RUN_MIGRATIONS",
	"API_KEY",
	"QUEUE_TYPE",
	"RABBITMQ_URL",
	"RABBITMQ_CONNECT_TIMEOUT",
	"RABBITMQ_RECONNECT_INITIAL_BACKOFF",
	"RABBITMQ_RECONNECT_MAX_BACKOFF",
	"QUEUE_NAME",
	"DEAD_LETTER_QUEUE_NAME",
	"QUEUE_BUFFER_SIZE",
	"QUEUE_PREFETCH",
	"MAX_JOB_ATTEMPTS",
	"WORKER_COUNT",
	"SCHEDULER_BATCH_SIZE",
	"CHECK_INTERVAL",
	"HTTP_TIMEOUT",
	"CHECK_LEASE_TIMEOUT",
	"HEALTH_ADDR",
	"MAX_REDIRECTS",
	"MAX_BODY_BYTES",
	"MAX_HEADER_BYTES",
	"ALLOW_PRIVATE_NETWORKS",
	"ALLOW_PROXY_ENV",
	"ALLOWED_PORTS",
	"URLS_FILE",
	"SEED_URLS_FILE",
	"SEED_DEFAULT_LINKS",
	"EXPECTED_STATUS",
	"ALERT_WEBHOOK_URL",
	"ALERT_FAILURE_THRESHOLD",
	"ALERT_COOLDOWN",
	"ALERT_DISPATCH_INTERVAL",
	"ALERT_DISPATCH_BATCH_SIZE",
	"ALERT_LEASE_TIMEOUT",
	"ALERT_DELIVERY_TIMEOUT",
	"ALERT_MAX_ATTEMPTS",
	"ALERT_RETRY_INITIAL_BACKOFF",
	"ALERT_RETRY_MAX_BACKOFF",
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
	if cfg.AppRole != "all" {
		t.Fatalf("AppRole = %q, want all", cfg.AppRole)
	}
	if cfg.AppEnv != "production" {
		t.Fatalf("AppEnv = %q, want production", cfg.AppEnv)
	}
	if cfg.SeedDefaultLinks {
		t.Fatal("SeedDefaultLinks = true, want false")
	}
	if cfg.SeedURLsFile != "" {
		t.Fatalf("SeedURLsFile = %q, want empty", cfg.SeedURLsFile)
	}
	if cfg.StorageType != "memory" {
		t.Fatalf("StorageType = %q, want memory", cfg.StorageType)
	}
	if cfg.QueueType != "memory" {
		t.Fatalf("QueueType = %q, want memory", cfg.QueueType)
	}
	if cfg.RabbitMQReconnectInitial != time.Second || cfg.RabbitMQReconnectMax != 30*time.Second {
		t.Fatalf("RabbitMQ reconnect backoff = %s..%s, want 1s..30s", cfg.RabbitMQReconnectInitial, cfg.RabbitMQReconnectMax)
	}
	if cfg.CheckInterval != 5*time.Minute {
		t.Fatalf("CheckInterval = %s, want 5m", cfg.CheckInterval)
	}
	if cfg.CheckLeaseTimeout != 2*time.Minute {
		t.Fatalf("CheckLeaseTimeout = %s, want 2m", cfg.CheckLeaseTimeout)
	}
	if cfg.AlertMaxAttempts != 8 {
		t.Fatalf("AlertMaxAttempts = %d, want 8", cfg.AlertMaxAttempts)
	}
	if _, ok := cfg.AllowedPorts[80]; !ok {
		t.Fatalf("port 80 is not allowed by default")
	}
	if _, ok := cfg.AllowedPorts[443]; !ok {
		t.Fatalf("port 443 is not allowed by default")
	}
}

func TestLoadConfigAcceptsAlertDispatcherRole(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("APP_ROLE", "alert-dispatcher")
	t.Setenv("STORAGE_TYPE", "postgres")
	t.Setenv("DATABASE_URL", "postgres://user:pass@example.com:5432/site_checker")
	t.Setenv("ALERT_WEBHOOK_URL", "https://alerts.example.com/site-checker")
	t.Setenv("ALERT_MAX_ATTEMPTS", "5")
	t.Setenv("ALERT_RETRY_INITIAL_BACKOFF", "2s")
	t.Setenv("ALERT_RETRY_MAX_BACKOFF", "2m")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if cfg.AppRole != "alert-dispatcher" || cfg.AlertMaxAttempts != 5 {
		t.Fatalf("alert dispatcher config = role:%q attempts:%d", cfg.AppRole, cfg.AlertMaxAttempts)
	}
}

func TestLoadConfigRequiresPostgresForAlerts(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("ALERT_WEBHOOK_URL", "https://alerts.example.com/site-checker")

	if _, err := LoadConfig(); err == nil {
		t.Fatal("LoadConfig returned nil error for alerts with in-memory storage")
	}
}

func TestLoadConfigRejectsInvalidValues(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("WORKER_COUNT", "0")
	t.Setenv("CHECK_INTERVAL", "5s")
	t.Setenv("ALLOWED_PORTS", "443,not-a-port")
	t.Setenv("APP_ROLE", "invalid")
	t.Setenv("STORAGE_TYPE", "postgres")
	t.Setenv("QUEUE_TYPE", "rabbitmq")

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
	t.Setenv("CHECK_LEASE_TIMEOUT", "30s")
	t.Setenv("EXPECTED_STATUS", "200-204,301")
	t.Setenv("ALLOWED_PORTS", "80,443,8443")
	t.Setenv("APP_ROLE", "worker")
	t.Setenv("APP_ENV", "demo")
	t.Setenv("SEED_URLS_FILE", "seed.txt")
	t.Setenv("DATABASE_URL", "postgres://user:pass@example.com:5432/site_checker")
	t.Setenv("RABBITMQ_URL", "amqp://guest:guest@example.com:5672/")
	t.Setenv("QUEUE_PREFETCH", "7")
	t.Setenv("RABBITMQ_CONNECT_TIMEOUT", "3s")
	t.Setenv("RABBITMQ_RECONNECT_INITIAL_BACKOFF", "500ms")
	t.Setenv("RABBITMQ_RECONNECT_MAX_BACKOFF", "10s")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if cfg.WorkerCount != 4 {
		t.Fatalf("WorkerCount = %d, want 4", cfg.WorkerCount)
	}
	if cfg.AppRole != "worker" {
		t.Fatalf("AppRole = %q, want worker", cfg.AppRole)
	}
	if cfg.AppEnv != "demo" {
		t.Fatalf("AppEnv = %q, want demo", cfg.AppEnv)
	}
	if !cfg.SeedDefaultLinks {
		t.Fatal("SeedDefaultLinks = false, want true for APP_ENV=demo")
	}
	if cfg.SeedURLsFile != "seed.txt" {
		t.Fatalf("SeedURLsFile = %q, want seed.txt", cfg.SeedURLsFile)
	}
	if cfg.StorageType != "postgres" {
		t.Fatalf("StorageType = %q, want postgres", cfg.StorageType)
	}
	if cfg.QueueType != "rabbitmq" {
		t.Fatalf("QueueType = %q, want rabbitmq", cfg.QueueType)
	}
	if cfg.QueuePrefetch != 7 {
		t.Fatalf("QueuePrefetch = %d, want 7", cfg.QueuePrefetch)
	}
	if cfg.RabbitMQConnectTimeout != 3*time.Second || cfg.RabbitMQReconnectMax != 10*time.Second {
		t.Fatalf("RabbitMQ reconnect config = timeout:%s max:%s", cfg.RabbitMQConnectTimeout, cfg.RabbitMQReconnectMax)
	}
	if cfg.CheckLeaseTimeout != 30*time.Second {
		t.Fatalf("CheckLeaseTimeout = %s, want 30s", cfg.CheckLeaseTimeout)
	}
	if !cfg.ExpectedStatus.Allows(301) {
		t.Fatalf("status policy does not allow 301")
	}
	if _, ok := cfg.AllowedPorts[8443]; !ok {
		t.Fatalf("port 8443 is not allowed")
	}
}
