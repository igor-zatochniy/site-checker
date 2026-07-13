package main

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAPIManageMonitorLifecycle(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.WorkerCount = 1
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	cfg.CheckInterval = time.Minute
	policy := NewNetworkPolicy(cfg)
	store := NewMonitorStore(policy)
	metrics := NewMetrics("test", "commit", "date", 0)
	checker := NewChecker(http.DefaultClient, cfg, metrics)
	service := NewMonitorService(NewInMemoryMonitorRepositoryFromStore(store), checker, metrics, AlertPolicy{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	handler := NewAPIHandler(service, "", slog.New(slog.NewTextHandler(io.Discard, nil)))

	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	createBody := `{"url":"https://example.com","interval_seconds":60,"timeout_seconds":5,"expected_status":200}`
	resp := apiRequest(t, server.URL+"/api/v1/monitors", http.MethodPost, createBody)
	if resp.StatusCode != http.StatusCreated {
		t.Fatalf("create status = %d, want 201", resp.StatusCode)
	}

	var created Monitor
	decodeResponse(t, resp, &created)
	if created.ID == "" {
		t.Fatal("created monitor ID is empty")
	}

	resp = apiRequest(t, server.URL+"/api/v1/monitors/"+created.ID, http.MethodGet, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("get status = %d, want 200", resp.StatusCode)
	}

	patchBody := `{"enabled":false}`
	resp = apiRequest(t, server.URL+"/api/v1/monitors/"+created.ID, http.MethodPatch, patchBody)
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("patch status = %d, want 200", resp.StatusCode)
	}
	var updated Monitor
	decodeResponse(t, resp, &updated)
	if updated.Enabled {
		t.Fatal("monitor is still enabled after patch")
	}

	resp = apiRequest(t, server.URL+"/api/v1/monitors", http.MethodGet, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("list status = %d, want 200", resp.StatusCode)
	}

	resp = apiRequest(t, server.URL+"/api/v1/monitors/"+created.ID, http.MethodDelete, "")
	if resp.StatusCode != http.StatusNoContent {
		t.Fatalf("delete status = %d, want 204", resp.StatusCode)
	}
}

func TestAPIReturnsValidationError(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	policy := NewNetworkPolicy(cfg)
	store := NewMonitorStore(policy)
	metrics := NewMetrics("test", "commit", "date", 0)
	service := NewMonitorService(NewInMemoryMonitorRepositoryFromStore(store), NewChecker(http.DefaultClient, cfg, metrics), metrics, AlertPolicy{}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	handler := NewAPIHandler(service, "", slog.New(slog.NewTextHandler(io.Discard, nil)))

	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	body := `{"url":"http://127.0.0.1","interval_seconds":60,"timeout_seconds":5,"expected_status":200}`
	resp := apiRequest(t, server.URL+"/api/v1/monitors", http.MethodPost, body)
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", resp.StatusCode)
	}
}

func TestAPIRequiresAPIKeyWhenConfigured(t *testing.T) {
	cfg := testCheckerConfig(t)
	policy := NewNetworkPolicy(cfg)
	store := NewMonitorStore(policy)
	metrics := NewMetrics("test", "commit", "date", 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := NewMonitorService(NewInMemoryMonitorRepositoryFromStore(store), NewChecker(http.DefaultClient, cfg, metrics), metrics, AlertPolicy{}, logger)
	handler := NewAPIHandler(service, "secret", logger)

	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	resp := apiRequest(t, server.URL+"/api/v1/monitors", http.MethodGet, "")
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("status without key = %d, want 401", resp.StatusCode)
	}
	resp.Body.Close()

	req, err := http.NewRequest(http.MethodGet, server.URL+"/api/v1/monitors", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-API-Key", "secret")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status with key = %d, want 200", resp.StatusCode)
	}
}

func apiRequest(t *testing.T, url, method, body string) *http.Response {
	t.Helper()
	var reader io.Reader
	if body != "" {
		reader = bytes.NewBufferString(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	return resp
}

func decodeResponse(t *testing.T, resp *http.Response, target any) {
	t.Helper()
	defer resp.Body.Close()
	if err := json.NewDecoder(resp.Body).Decode(target); err != nil {
		t.Fatal(err)
	}
}
