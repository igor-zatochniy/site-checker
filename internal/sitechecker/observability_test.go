package sitechecker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestObservabilityEndpoints(t *testing.T) {
	statusPolicy, err := ParseStatusPolicy("200-299")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		ExpectedStatus: statusPolicy,
	}
	metrics := NewMetrics("test", "commit", "date", 1)
	observabilityServer := NewObservabilityServer(":0", cfg, metrics)
	if observabilityServer.ReadTimeout <= 0 {
		t.Fatal("HTTP server ReadTimeout is not configured")
	}
	server := httptest.NewServer(observabilityServer.Handler)
	defer server.Close()

	for _, path := range []string{"/healthz", "/readyz", "/metrics"} {
		resp, err := http.Get(server.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("GET %s status = %d, want 200", path, resp.StatusCode)
		}
	}
}

func TestHTTPWriteTimeoutExceedsAPIRequestBudget(t *testing.T) {
	cfg := Config{DatabaseOperationTimeout: 15 * time.Second}
	server := NewObservabilityServer(":0", cfg, NewMetrics("test", "commit", "date", 0))
	if server.WriteTimeout <= cfg.DatabaseOperationTimeout {
		t.Fatalf("WriteTimeout = %s, must exceed API request budget %s", server.WriteTimeout, cfg.DatabaseOperationTimeout)
	}
	if margin := server.WriteTimeout - cfg.DatabaseOperationTimeout; margin < httpResponseWriteMargin {
		t.Fatalf("write margin = %s, want at least %s", margin, httpResponseWriteMargin)
	}
}

func TestReadinessForSplitRoleDoesNotRequireCompletedChecks(t *testing.T) {
	statusPolicy, err := ParseStatusPolicy("200-299")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		AppRole:        "api",
		ExpectedStatus: statusPolicy,
	}
	metrics := NewMetrics("test", "commit", "date", 1)
	server := httptest.NewServer(NewObservabilityServer(":0", cfg, metrics).Handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestReadinessForAllRoleDoesNotRequireMonitorsOrCompletedChecks(t *testing.T) {
	statusPolicy, err := ParseStatusPolicy("200-299")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{AppRole: "all", ExpectedStatus: statusPolicy}
	metrics := NewMetrics("test", "commit", "date", 0)
	server := httptest.NewServer(NewObservabilityServer(":0", cfg, metrics).Handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
}

func TestReadinessReportsDependencyFailure(t *testing.T) {
	statusPolicy, err := ParseStatusPolicy("200-299")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		AppRole:        "worker",
		ExpectedStatus: statusPolicy,
	}
	metrics := NewMetrics("test", "commit", "date", 1)
	server := httptest.NewServer(NewObservabilityServerWithDependencies(":0", cfg, metrics, []ReadinessDependency{
		{
			Name: "rabbitmq",
			Check: func(context.Context) error {
				return errors.New("connection refused")
			},
		},
	}).Handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/readyz")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", resp.StatusCode)
	}

	resp, err = http.Get(server.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(body), `site_checker_dependency_up{dependency="rabbitmq"} 0`) {
		t.Fatalf("metrics do not contain rabbitmq dependency down metric:\n%s", string(body))
	}
}
