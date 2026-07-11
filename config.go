package main

import (
	"errors"
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"
	"time"
)

const (
	defaultAppRole               = "all"
	defaultStorageType           = "memory"
	defaultQueueType             = "memory"
	defaultWorkerCount           = 10
	defaultSchedulerBatchSize    = 100
	defaultCheckInterval         = 5 * time.Minute
	defaultHTTPTimeout           = 5 * time.Second
	defaultCheckLeaseTimeout     = 2 * time.Minute
	defaultHealthAddr            = ":8080"
	defaultMaxRedirects          = 3
	defaultMaxBodyBytes          = 64 * 1024
	defaultMaxHeaderBytes        = 64 * 1024
	defaultExpectedStatus        = "200-399"
	defaultAllowedPorts          = "80,443"
	defaultAlertFailureThreshold = 3
	defaultAlertCooldown         = 10 * time.Minute
	defaultUserAgent             = "site-checker"
	defaultQueueBufferSize       = 1000
	defaultQueuePrefetch         = 10
	defaultMaxJobAttempts        = 3
	defaultQueueName             = "site_checker.checks"
	defaultDeadLetterQueueName   = "site_checker.checks.dead"
)

type Config struct {
	AppRole               string
	StorageType           string
	DatabaseURL           string
	RunMigrations         bool
	APIKey                string
	QueueType             string
	RabbitMQURL           string
	QueueName             string
	DeadLetterQueueName   string
	QueueBufferSize       int
	QueuePrefetch         int
	MaxJobAttempts        int
	WorkerCount           int
	SchedulerBatchSize    int
	CheckInterval         time.Duration
	HTTPTimeout           time.Duration
	CheckLeaseTimeout     time.Duration
	HealthAddr            string
	ReadinessStaleAfter   time.Duration
	StartupGracePeriod    time.Duration
	MaxRedirects          int
	MaxBodyBytes          int64
	MaxHeaderBytes        int64
	AllowPrivateNetworks  bool
	AllowProxyEnv         bool
	AllowedPorts          map[int]struct{}
	URLsFile              string
	ExpectedStatus        StatusPolicy
	AlertWebhookURL       string
	AlertFailureThreshold int
	AlertCooldown         time.Duration
	UserAgent             string
	EnablePprof           bool
}

func LoadConfig() (Config, error) {
	var errs []error
	cfg := Config{}

	cfg.AppRole = envEnum("APP_ROLE", defaultAppRole, []string{"all", "api", "scheduler", "worker"}, &errs)
	cfg.DatabaseURL = envString("DATABASE_URL", "")
	cfg.StorageType = envString("STORAGE_TYPE", "")
	if cfg.StorageType == "" {
		cfg.StorageType = defaultStorageType
		if cfg.DatabaseURL != "" {
			cfg.StorageType = "postgres"
		}
	}
	cfg.StorageType = validateEnum("STORAGE_TYPE", cfg.StorageType, []string{"memory", "postgres"}, &errs)
	cfg.RunMigrations = envBool("RUN_MIGRATIONS", true, &errs)
	cfg.APIKey = envString("API_KEY", "")
	cfg.RabbitMQURL = envString("RABBITMQ_URL", "")
	cfg.QueueType = envString("QUEUE_TYPE", "")
	if cfg.QueueType == "" {
		cfg.QueueType = defaultQueueType
		if cfg.RabbitMQURL != "" {
			cfg.QueueType = "rabbitmq"
		}
	}
	cfg.QueueType = validateEnum("QUEUE_TYPE", cfg.QueueType, []string{"memory", "rabbitmq"}, &errs)
	cfg.QueueName = envString("QUEUE_NAME", defaultQueueName)
	cfg.DeadLetterQueueName = envString("DEAD_LETTER_QUEUE_NAME", defaultDeadLetterQueueName)
	cfg.QueueBufferSize = envInt("QUEUE_BUFFER_SIZE", defaultQueueBufferSize, 1, 100000, &errs)
	cfg.QueuePrefetch = envInt("QUEUE_PREFETCH", defaultQueuePrefetch, 1, 1000, &errs)
	cfg.MaxJobAttempts = envInt("MAX_JOB_ATTEMPTS", defaultMaxJobAttempts, 1, 20, &errs)
	cfg.WorkerCount = envInt("WORKER_COUNT", defaultWorkerCount, 1, 100, &errs)
	cfg.SchedulerBatchSize = envInt("SCHEDULER_BATCH_SIZE", defaultSchedulerBatchSize, 1, 1000, &errs)
	cfg.CheckInterval = envDuration("CHECK_INTERVAL", defaultCheckInterval, 30*time.Second, 24*time.Hour, &errs)
	cfg.HTTPTimeout = envDuration("HTTP_TIMEOUT", defaultHTTPTimeout, time.Second, time.Minute, &errs)
	cfg.CheckLeaseTimeout = envDuration("CHECK_LEASE_TIMEOUT", defaultCheckLeaseTimeout, cfg.HTTPTimeout, 24*time.Hour, &errs)
	cfg.HealthAddr = envString("HEALTH_ADDR", defaultHealthAddr)
	cfg.MaxRedirects = envInt("MAX_REDIRECTS", defaultMaxRedirects, 0, 10, &errs)
	cfg.MaxBodyBytes = int64(envInt("MAX_BODY_BYTES", defaultMaxBodyBytes, 1024, 10*1024*1024, &errs))
	cfg.MaxHeaderBytes = int64(envInt("MAX_HEADER_BYTES", defaultMaxHeaderBytes, 1024, 1024*1024, &errs))
	cfg.AllowPrivateNetworks = envBool("ALLOW_PRIVATE_NETWORKS", false, &errs)
	cfg.AllowProxyEnv = envBool("ALLOW_PROXY_ENV", false, &errs)
	cfg.AllowedPorts = envPorts("ALLOWED_PORTS", defaultAllowedPorts, &errs)
	cfg.URLsFile = envString("URLS_FILE", "")
	cfg.AlertWebhookURL = envString("ALERT_WEBHOOK_URL", "")
	cfg.AlertFailureThreshold = envInt("ALERT_FAILURE_THRESHOLD", defaultAlertFailureThreshold, 1, 100, &errs)
	cfg.AlertCooldown = envDuration("ALERT_COOLDOWN", defaultAlertCooldown, 0, 24*time.Hour, &errs)
	cfg.UserAgent = envString("USER_AGENT", defaultUserAgent)
	cfg.EnablePprof = envBool("ENABLE_PPROF", false, &errs)

	cfg.ReadinessStaleAfter = envDuration("READINESS_STALE_AFTER", cfg.CheckInterval*3+cfg.HTTPTimeout, cfg.CheckInterval+cfg.HTTPTimeout, 7*24*time.Hour, &errs)
	cfg.StartupGracePeriod = envDuration("STARTUP_GRACE_PERIOD", cfg.CheckInterval+cfg.HTTPTimeout+30*time.Second, time.Second, time.Hour, &errs)

	statusPolicy, err := ParseStatusPolicy(envString("EXPECTED_STATUS", defaultExpectedStatus))
	if err != nil {
		errs = append(errs, fmt.Errorf("EXPECTED_STATUS: %w", err))
	}
	cfg.ExpectedStatus = statusPolicy

	if cfg.AlertWebhookURL != "" {
		if err := validateHTTPURLShape(cfg.AlertWebhookURL); err != nil {
			errs = append(errs, fmt.Errorf("ALERT_WEBHOOK_URL: %w", err))
		}
	}

	if cfg.UserAgent == "" {
		errs = append(errs, errors.New("USER_AGENT must not be empty"))
	}
	if cfg.StorageType == "postgres" && cfg.DatabaseURL == "" {
		errs = append(errs, errors.New("DATABASE_URL is required when STORAGE_TYPE=postgres"))
	}
	if cfg.QueueType == "rabbitmq" && cfg.RabbitMQURL == "" {
		errs = append(errs, errors.New("RABBITMQ_URL is required when QUEUE_TYPE=rabbitmq"))
	}
	if cfg.QueueName == "" {
		errs = append(errs, errors.New("QUEUE_NAME must not be empty"))
	}
	if cfg.DeadLetterQueueName == "" {
		errs = append(errs, errors.New("DEAD_LETTER_QUEUE_NAME must not be empty"))
	}

	if len(errs) > 0 {
		return Config{}, errors.Join(errs...)
	}
	return cfg, nil
}

func envString(key, defaultValue string) string {
	value, exists := os.LookupEnv(key)
	if !exists {
		return defaultValue
	}
	return strings.TrimSpace(value)
}

func envInt(key string, defaultValue, minValue, maxValue int, errs *[]error) int {
	raw, exists := os.LookupEnv(key)
	if !exists || strings.TrimSpace(raw) == "" {
		return defaultValue
	}

	value, err := strconv.Atoi(strings.TrimSpace(raw))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s must be an integer", key))
		return defaultValue
	}
	if value < minValue || value > maxValue {
		*errs = append(*errs, fmt.Errorf("%s must be between %d and %d", key, minValue, maxValue))
		return defaultValue
	}
	return value
}

func envDuration(key string, defaultValue, minValue, maxValue time.Duration, errs *[]error) time.Duration {
	raw, exists := os.LookupEnv(key)
	if !exists || strings.TrimSpace(raw) == "" {
		return defaultValue
	}

	value, err := time.ParseDuration(strings.TrimSpace(raw))
	if err != nil {
		*errs = append(*errs, fmt.Errorf("%s must be a Go duration like 30s, 5m, or 1h", key))
		return defaultValue
	}
	if value < minValue || value > maxValue {
		*errs = append(*errs, fmt.Errorf("%s must be between %s and %s", key, minValue, maxValue))
		return defaultValue
	}
	return value
}

func envBool(key string, defaultValue bool, errs *[]error) bool {
	raw, exists := os.LookupEnv(key)
	if !exists || strings.TrimSpace(raw) == "" {
		return defaultValue
	}

	switch strings.ToLower(strings.TrimSpace(raw)) {
	case "1", "true", "yes", "y", "on":
		return true
	case "0", "false", "no", "n", "off":
		return false
	default:
		*errs = append(*errs, fmt.Errorf("%s must be a boolean", key))
		return defaultValue
	}
}

func envEnum(key, defaultValue string, allowed []string, errs *[]error) string {
	return validateEnum(key, envString(key, defaultValue), allowed, errs)
}

func validateEnum(key, value string, allowed []string, errs *[]error) string {
	value = strings.ToLower(strings.TrimSpace(value))
	for _, candidate := range allowed {
		if value == candidate {
			return value
		}
	}
	*errs = append(*errs, fmt.Errorf("%s must be one of %s", key, strings.Join(allowed, ", ")))
	return allowed[0]
}

func envPorts(key, defaultValue string, errs *[]error) map[int]struct{} {
	raw := envString(key, defaultValue)
	ports := make(map[int]struct{})
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			*errs = append(*errs, fmt.Errorf("%s contains an empty port", key))
			continue
		}

		port, err := strconv.Atoi(part)
		if err != nil || port < 1 || port > 65535 {
			*errs = append(*errs, fmt.Errorf("%s contains invalid port %q", key, part))
			continue
		}
		ports[port] = struct{}{}
	}
	if len(ports) == 0 {
		*errs = append(*errs, fmt.Errorf("%s must contain at least one port", key))
	}
	return ports
}

func validateHTTPURLShape(raw string) error {
	parsed, err := url.Parse(raw)
	if err != nil {
		return err
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return fmt.Errorf("scheme must be http or https")
	}
	if parsed.Hostname() == "" {
		return fmt.Errorf("host is required")
	}
	if parsed.User != nil {
		return fmt.Errorf("userinfo is not allowed")
	}
	return nil
}
