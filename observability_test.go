package main

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
		StartupGracePeriod:  time.Minute,
		ReadinessStaleAfter: time.Minute,
		ExpectedStatus:      statusPolicy,
	}
	metrics := NewMetrics("test", "commit", "date", 1)
	server := httptest.NewServer(NewObservabilityServer(":0", cfg, metrics).Handler)
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

func TestReadinessForSplitRoleDoesNotRequireCompletedChecks(t *testing.T) {
	statusPolicy, err := ParseStatusPolicy("200-299")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		AppRole:             "api",
		StartupGracePeriod:  time.Nanosecond,
		ReadinessStaleAfter: time.Nanosecond,
		ExpectedStatus:      statusPolicy,
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

func TestReadinessReportsDependencyFailure(t *testing.T) {
	statusPolicy, err := ParseStatusPolicy("200-299")
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		AppRole:             "worker",
		StartupGracePeriod:  time.Minute,
		ReadinessStaleAfter: time.Minute,
		ExpectedStatus:      statusPolicy,
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
