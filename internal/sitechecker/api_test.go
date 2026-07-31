package sitechecker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
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
	defer resp.Body.Close()

	var apiErr map[string]any
	decodeResponse(t, resp, &apiErr)
	if msg, _ := apiErr["error"].(map[string]any)["message"].(string); strings.Contains(msg, "127.0.0.1") {
		t.Fatalf("validation message leaked internal detail: %q", msg)
	}
}

func TestAPIRunManualCheckReturnsAcceptedPersistedJob(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	policy := NewNetworkPolicy(cfg)
	store := NewMonitorStore(policy)
	metrics := NewMetrics("test", "commit", "date", 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := NewMonitorService(NewInMemoryMonitorRepositoryFromStore(store), nil, metrics, AlertPolicy{}, logger)
	handler := NewAPIHandler(service, "", logger)

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
	resp.Body.Close()

	resp = apiRequest(t, server.URL+"/api/v1/monitors/"+created.ID+"/check", http.MethodPost, "")
	if resp.StatusCode != http.StatusAccepted {
		t.Fatalf("manual check status = %d, want 202", resp.StatusCode)
	}
	var receipt ManualCheckJobReceipt
	decodeResponse(t, resp, &receipt)
	if receipt.JobID == "" || receipt.MonitorID != created.ID {
		t.Fatalf("manual job receipt = %+v", receipt)
	}
	if receipt.Kind != checkJobKindManual || receipt.Status != checkJobStatusScheduled || receipt.Deduplicated {
		t.Fatalf("manual job receipt = %+v, want new scheduled manual job", receipt)
	}
	job, err := store.CheckJob(receipt.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Kind != checkJobKindManual || job.Status != checkJobStatusScheduled {
		t.Fatalf("persisted manual job = %+v", job)
	}
	if _, total, err := store.ListChecks(created.ID, 0, 10); err != nil || total != 0 {
		t.Fatalf("checks before worker processing: total=%d err=%v", total, err)
	}
}

func TestAPIReturnsSafeInternalError(t *testing.T) {
	loggerBuf := &bytes.Buffer{}
	logger := slog.New(slog.NewTextHandler(loggerBuf, nil))
	service := NewMonitorService(failingMonitorRepository{err: errors.New("connection refused")}, nil, NewMetrics("test", "commit", "date", 0), AlertPolicy{}, logger)
	handler := NewAPIHandler(service, "", logger)

	mux := http.NewServeMux()
	handler.Register(mux)
	server := httptest.NewServer(mux)
	defer server.Close()

	resp := apiRequest(t, server.URL+"/api/v1/monitors", http.MethodGet, "")
	if resp.StatusCode != http.StatusInternalServerError && resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 500 or 503", resp.StatusCode)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "connection refused") {
		t.Fatalf("response leaked internal error: %s", body)
	}
	if !strings.Contains(loggerBuf.String(), "connection refused") {
		t.Fatalf("server log did not capture internal error: %s", loggerBuf.String())
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

type failingMonitorRepository struct {
	err error
}

func (r failingMonitorRepository) Ping(context.Context) error { return r.err }

func (r failingMonitorRepository) Count(context.Context) (int, error) { return 0, r.err }

func (r failingMonitorRepository) Create(context.Context, MonitorInput) (Monitor, error) {
	return Monitor{}, r.err
}

func (r failingMonitorRepository) List(context.Context, int, int) ([]Monitor, int, error) {
	return nil, 0, r.err
}

func (r failingMonitorRepository) Get(context.Context, string) (Monitor, error) {
	return Monitor{}, r.err
}

func (r failingMonitorRepository) Update(context.Context, string, MonitorPatch) (Monitor, error) {
	return Monitor{}, r.err
}

func (r failingMonitorRepository) Delete(context.Context, string) error { return r.err }

func (r failingMonitorRepository) CreateManualJob(context.Context, string, time.Time) (CheckJobRecord, bool, error) {
	return CheckJobRecord{}, false, r.err
}

func (r failingMonitorRepository) ClaimDueJobs(context.Context, int, time.Time, time.Duration) ([]CheckJobRecord, error) {
	return nil, r.err
}

func (r failingMonitorRepository) MarkJobPublished(context.Context, string, string, time.Time) error {
	return r.err
}

func (r failingMonitorRepository) ReleaseJobPublish(context.Context, string, string, string, time.Time) error {
	return r.err
}

func (r failingMonitorRepository) MarkJobProcessing(context.Context, string, string, int, time.Time, time.Duration) (ProcessingLease, error) {
	return ProcessingLease{}, r.err
}

func (r failingMonitorRepository) MarkJobFailed(context.Context, ProcessingLease, string, time.Time, time.Time) error {
	return r.err
}

func (r failingMonitorRepository) MarkJobDead(context.Context, ProcessingLease, string, time.Time, time.Time) error {
	return r.err
}

func (r failingMonitorRepository) AddCheck(context.Context, CheckRecord, ProcessingLease, AlertPolicy) (Monitor, error) {
	return Monitor{}, r.err
}

func (r failingMonitorRepository) ListChecks(context.Context, string, int, int) ([]CheckRecord, int, error) {
	return nil, 0, r.err
}

func (r failingMonitorRepository) Stats(context.Context, string) (MonitorStats, error) {
	return MonitorStats{}, r.err
}

func (r failingMonitorRepository) ListIncidents(context.Context, string, int, int) ([]Incident, int, error) {
	return nil, 0, r.err
}
