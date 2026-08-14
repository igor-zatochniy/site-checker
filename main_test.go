package main

import (
	"net"
	"testing"
)

func TestRunReturnsNonZeroWhenHTTPListenerIsUnavailable(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()

	t.Setenv("APP_ENV", "local")
	t.Setenv("APP_ROLE", "all")
	t.Setenv("STORAGE_TYPE", "memory")
	t.Setenv("DATABASE_URL", "")
	t.Setenv("QUEUE_TYPE", "memory")
	t.Setenv("RABBITMQ_URL", "")
	t.Setenv("API_KEY", "test-api-key-with-24-characters")
	t.Setenv("AUTH_DISABLED", "false")
	t.Setenv("HEALTH_ADDR", listener.Addr().String())
	t.Setenv("ALERT_WEBHOOK_URL", "")
	t.Setenv("RETENTION_ENABLED", "false")
	t.Setenv("ALLOW_PROXY_ENV", "false")
	t.Setenv("ALLOW_PRIVATE_NETWORKS", "false")
	t.Setenv("ENABLE_PPROF", "false")

	if code := run(); code == 0 {
		t.Fatal("run returned exit code 0 after HTTP listener failure")
	}
}
