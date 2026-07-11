package main

import (
	"net/http"
	"net/http/httptest"
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
