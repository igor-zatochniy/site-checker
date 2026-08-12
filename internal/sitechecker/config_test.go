package sitechecker

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
	"AUTH_DISABLED",
	"QUEUE_TYPE",
	"RABBITMQ_URL",
	"RABBITMQ_CONNECT_TIMEOUT",
	"RABBITMQ_PUBLISH_TIMEOUT",
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
	"RETENTION_ENABLED",
	"RETENTION_INTERVAL",
	"RETENTION_BATCH_SIZE",
	"CHECK_RESULTS_RETENTION",
	"CHECK_JOBS_RETENTION",
	"ALERT_OUTBOX_RETENTION",
	"RESOLVED_INCIDENT_RETENTION",
	"USER_AGENT",
	"ENABLE_PPROF",
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
	t.Setenv("API_KEY", "test-api-key-with-24-characters")
	t.Setenv("APP_ENV", "local")
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
	if cfg.AppEnv != "local" {
		t.Fatalf("AppEnv = %q, want local", cfg.AppEnv)
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
	if cfg.RabbitMQPublishTimeout != 10*time.Second {
		t.Fatalf("RabbitMQ publish timeout = %s, want 10s", cfg.RabbitMQPublishTimeout)
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
	if cfg.RetentionEnabled {
		t.Fatal("RetentionEnabled = true, want false")
	}
	if cfg.RetentionInterval != time.Minute || cfg.RetentionBatchSize != 10000 {
		t.Fatalf("retention schedule = interval:%s batch:%d", cfg.RetentionInterval, cfg.RetentionBatchSize)
	}
	if cfg.CheckResultsRetention != 90*24*time.Hour || cfg.CheckJobsRetention != 30*24*time.Hour ||
		cfg.AlertOutboxRetention != 30*24*time.Hour || cfg.ResolvedIncidentRetention != 365*24*time.Hour {
		t.Fatalf("retention defaults = results:%s jobs:%s outbox:%s incidents:%s",
			cfg.CheckResultsRetention, cfg.CheckJobsRetention, cfg.AlertOutboxRetention, cfg.ResolvedIncidentRetention)
	}
	if _, ok := cfg.AllowedPorts[80]; !ok {
		t.Fatalf("port 80 is not allowed by default")
	}
	if _, ok := cfg.AllowedPorts[443]; !ok {
		t.Fatalf("port 443 is not allowed by default")
	}
}

func TestLoadConfigRequiresAuthenticationForAPIEnabledRoles(t *testing.T) {
	for _, role := range []string{"all", "api"} {
		t.Run(role, func(t *testing.T) {
			cleanConfigEnv(t)
			t.Setenv("APP_ROLE", role)
			t.Setenv("API_KEY", "")

			if _, err := LoadConfig(); err == nil {
				t.Fatalf("LoadConfig returned nil error for unauthenticated production role %q", role)
			}
		})
	}
}

func TestLoadConfigAllowsExplicitlyDisabledAuthenticationOnlyOutsideProduction(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("APP_ENV", "local")
	t.Setenv("APP_ROLE", "api")
	t.Setenv("API_KEY", "")
	t.Setenv("AUTH_DISABLED", "true")
	t.Setenv("STORAGE_TYPE", "postgres")
	t.Setenv("DATABASE_URL", "postgres://user:pass@example.com:5432/site_checker")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if !cfg.AuthDisabled {
		t.Fatal("AuthDisabled = false, want true")
	}

	t.Setenv("APP_ENV", "production")
	if _, err := LoadConfig(); err == nil {
		t.Fatal("LoadConfig returned nil error for AUTH_DISABLED=true in production")
	}
}

func TestLoadConfigRejectsWeakAPIKey(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("APP_ROLE", "api")
	t.Setenv("API_KEY", "too-short")

	if _, err := LoadConfig(); err == nil {
		t.Fatal("LoadConfig returned nil error for a weak API key")
	}
}

func TestLoadConfigDoesNotRequireAPIKeyForWorkerRole(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("APP_ROLE", "worker")
	t.Setenv("API_KEY", "")
	t.Setenv("STORAGE_TYPE", "postgres")
	t.Setenv("DATABASE_URL", "postgres://user:pass@example.com:5432/site_checker")
	t.Setenv("QUEUE_TYPE", "rabbitmq")
	t.Setenv("RABBITMQ_URL", "amqp://user:pass@example.com:5672/")

	if _, err := LoadConfig(); err != nil {
		t.Fatalf("LoadConfig returned error for worker role: %v", err)
	}
}

func TestLoadConfigRejectsUnsafeProductionAndSplitBackends(t *testing.T) {
	tests := []struct {
		name string
		env  map[string]string
	}{
		{
			name: "production all with memory storage",
			env:  map[string]string{"APP_ENV": "production", "APP_ROLE": "all"},
		},
		{
			name: "split api with memory storage",
			env:  map[string]string{"APP_ROLE": "api"},
		},
		{
			name: "split scheduler with memory queue",
			env: map[string]string{
				"APP_ROLE":     "scheduler",
				"STORAGE_TYPE": "postgres",
				"DATABASE_URL": "postgres://user:pass@example.com:5432/site_checker",
			},
		},
		{
			name: "split worker with memory queue",
			env: map[string]string{
				"APP_ROLE":     "worker",
				"STORAGE_TYPE": "postgres",
				"DATABASE_URL": "postgres://user:pass@example.com:5432/site_checker",
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			cleanConfigEnv(t)
			for key, value := range test.env {
				t.Setenv(key, value)
			}
			if _, err := LoadConfig(); err == nil {
				t.Fatal("LoadConfig returned nil error for unsafe backend configuration")
			}
		})
	}
}

func TestLoadConfigAllowsSupportedLocalAndProductionBackends(t *testing.T) {
	t.Run("local all in memory", func(t *testing.T) {
		cleanConfigEnv(t)
		if _, err := LoadConfig(); err != nil {
			t.Fatalf("LoadConfig returned error: %v", err)
		}
	})

	t.Run("production split scheduler", func(t *testing.T) {
		cleanConfigEnv(t)
		t.Setenv("APP_ENV", "production")
		t.Setenv("APP_ROLE", "scheduler")
		t.Setenv("STORAGE_TYPE", "postgres")
		t.Setenv("DATABASE_URL", "postgres://user:pass@example.com:5432/site_checker")
		t.Setenv("QUEUE_TYPE", "rabbitmq")
		t.Setenv("RABBITMQ_URL", "amqp://user:pass@example.com:5672/")
		cfg, err := LoadConfig()
		if err != nil {
			t.Fatalf("LoadConfig returned error: %v", err)
		}
		if cfg.AppEnv != "production" || cfg.StorageType != "postgres" || cfg.QueueType != "rabbitmq" {
			t.Fatalf("config = env:%s storage:%s queue:%s", cfg.AppEnv, cfg.StorageType, cfg.QueueType)
		}
	})
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

func TestLoadConfigRejectsAlertDispatcherWithoutWebhook(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("APP_ROLE", "alert-dispatcher")
	t.Setenv("STORAGE_TYPE", "postgres")
	t.Setenv("DATABASE_URL", "postgres://user:pass@example.com:5432/site_checker")
	t.Setenv("ALERT_WEBHOOK_URL", "")

	if _, err := LoadConfig(); err == nil {
		t.Fatal("LoadConfig returned nil error for alert dispatcher without webhook URL")
	}
}

func TestLoadConfigRequiresPostgresForAlerts(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("ALERT_WEBHOOK_URL", "https://alerts.example.com/site-checker")

	if _, err := LoadConfig(); err == nil {
		t.Fatal("LoadConfig returned nil error for alerts with in-memory storage")
	}
}

func TestLoadConfigValidatesRetentionConfiguration(t *testing.T) {
	t.Run("requires PostgreSQL", func(t *testing.T) {
		cleanConfigEnv(t)
		t.Setenv("RETENTION_ENABLED", "true")
		if _, err := LoadConfig(); err == nil {
			t.Fatal("LoadConfig returned nil error for retention with in-memory storage")
		}
	})

	t.Run("requires scheduler owner", func(t *testing.T) {
		cleanConfigEnv(t)
		t.Setenv("APP_ROLE", "api")
		t.Setenv("STORAGE_TYPE", "postgres")
		t.Setenv("DATABASE_URL", "postgres://user:pass@example.com:5432/site_checker")
		t.Setenv("RETENTION_ENABLED", "true")
		if _, err := LoadConfig(); err == nil {
			t.Fatal("LoadConfig returned nil error for retention in API role")
		}
	})

	t.Run("accepts scheduler", func(t *testing.T) {
		cleanConfigEnv(t)
		t.Setenv("APP_ROLE", "scheduler")
		t.Setenv("STORAGE_TYPE", "postgres")
		t.Setenv("DATABASE_URL", "postgres://user:pass@example.com:5432/site_checker")
		t.Setenv("QUEUE_TYPE", "rabbitmq")
		t.Setenv("RABBITMQ_URL", "amqp://user:pass@example.com:5672/")
		t.Setenv("RETENTION_ENABLED", "true")
		cfg, err := LoadConfig()
		if err != nil {
			t.Fatalf("LoadConfig returned error: %v", err)
		}
		if !cfg.RetentionEnabled {
			t.Fatal("RetentionEnabled = false, want true")
		}
	})
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

func TestLoadConfigRejectsCheckLeaseBelowMaximumMonitorTimeoutMargin(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("HTTP_TIMEOUT", "5s")
	t.Setenv("CHECK_LEASE_TIMEOUT", "89s")

	if _, err := LoadConfig(); err == nil {
		t.Fatal("expected CHECK_LEASE_TIMEOUT below maximum monitor timeout margin to be rejected")
	}
}

func TestLoadConfigRejectsProxyWithoutPrivateNetworkTrust(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("ALLOW_PROXY_ENV", "true")

	if _, err := LoadConfig(); err == nil {
		t.Fatal("LoadConfig returned nil error for proxy use outside private-network trust mode")
	}
}

func TestLoadConfigAllowsProxyWithPrivateNetworkTrust(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("ALLOW_PROXY_ENV", "true")
	t.Setenv("ALLOW_PRIVATE_NETWORKS", "true")

	cfg, err := LoadConfig()
	if err != nil {
		t.Fatalf("LoadConfig returned error: %v", err)
	}
	if !cfg.AllowProxyEnv || !cfg.AllowPrivateNetworks {
		t.Fatalf("proxy trust config = proxy:%t private:%t, want both true", cfg.AllowProxyEnv, cfg.AllowPrivateNetworks)
	}
}

func TestLoadConfigAcceptsOverrides(t *testing.T) {
	cleanConfigEnv(t)
	t.Setenv("WORKER_COUNT", "4")
	t.Setenv("CHECK_INTERVAL", "45s")
	t.Setenv("HTTP_TIMEOUT", "2s")
	t.Setenv("CHECK_LEASE_TIMEOUT", "2m")
	t.Setenv("EXPECTED_STATUS", "200-204,301")
	t.Setenv("ALLOWED_PORTS", "80,443,8443")
	t.Setenv("APP_ROLE", "worker")
	t.Setenv("APP_ENV", "demo")
	t.Setenv("SEED_URLS_FILE", "seed.txt")
	t.Setenv("DATABASE_URL", "postgres://user:pass@example.com:5432/site_checker")
	t.Setenv("RABBITMQ_URL", "amqp://guest:guest@example.com:5672/")
	t.Setenv("QUEUE_PREFETCH", "7")
	t.Setenv("RABBITMQ_CONNECT_TIMEOUT", "3s")
	t.Setenv("RABBITMQ_PUBLISH_TIMEOUT", "7s")
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
	if cfg.RabbitMQPublishTimeout != 7*time.Second {
		t.Fatalf("RabbitMQ publish timeout = %s, want 7s", cfg.RabbitMQPublishTimeout)
	}
	if cfg.CheckLeaseTimeout != 2*time.Minute {
		t.Fatalf("CheckLeaseTimeout = %s, want 2m", cfg.CheckLeaseTimeout)
	}
	if !cfg.ExpectedStatus.Allows(301) {
		t.Fatalf("status policy does not allow 301")
	}
	if _, ok := cfg.AllowedPorts[8443]; !ok {
		t.Fatalf("port 8443 is not allowed")
	}
}
