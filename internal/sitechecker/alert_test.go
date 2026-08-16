package sitechecker

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestAlertSenderSendsIdempotentWebhook(t *testing.T) {
	received := make(chan AlertPayload, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		if got := r.Header.Get("Idempotency-Key"); got != "incident-1:failure:3" {
			t.Errorf("Idempotency-Key = %q, want incident-1:failure:3", got)
		}
		if got := r.Header.Get("User-Agent"); got != "site-checker-test" {
			t.Errorf("User-Agent = %q, want site-checker-test", got)
		}
		var payload AlertPayload
		if err := json.NewDecoder(r.Body).Decode(&payload); err != nil {
			t.Errorf("decode payload: %v", err)
		}
		received <- payload
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	event := AlertOutboxEvent{
		IdempotencyKey: "incident-1:failure:3",
		Payload: AlertPayload{
			EventType:           alertEventIncidentFailure,
			IncidentID:          "incident-1",
			MonitorID:           "monitor-1",
			URL:                 "https://example.com",
			StatusCode:          http.StatusServiceUnavailable,
			Error:               "unexpected status code 503",
			ConsecutiveFailures: 3,
			CheckedAt:           time.Now().UTC(),
		},
	}
	sender := NewAlertSender(server.URL, "site-checker-test", server.Client())
	if err := sender.Send(t.Context(), event); err != nil {
		t.Fatal(err)
	}

	select {
	case payload := <-received:
		if payload.IncidentID != event.Payload.IncidentID || payload.ConsecutiveFailures != 3 {
			t.Fatalf("payload = %+v, want incident data", payload)
		}
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for alert webhook")
	}
}

func TestAlertSenderRejectsNonSuccessStatus(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary failure", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	sender := NewAlertSender(server.URL, "site-checker-test", server.Client())
	err := sender.Send(t.Context(), AlertOutboxEvent{IdempotencyKey: "event-1"})
	if err == nil {
		t.Fatal("Send returned nil error for non-success status")
	}
}

func TestAlertSenderRejectsRedirectWithoutChangingPOSTToGET(t *testing.T) {
	var redirectedRequests atomic.Int32
	redirectTarget := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		redirectedRequests.Add(1)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer redirectTarget.Close()

	redirectingWebhook := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("method = %s, want POST", r.Method)
		}
		http.Redirect(w, r, redirectTarget.URL, http.StatusFound)
	}))
	defer redirectingWebhook.Close()

	repo := &recordingAlertOutboxRepository{events: []AlertOutboxEvent{{
		ID:             "event-redirect",
		IdempotencyKey: "incident-redirect:failure:1",
		AttemptCount:   1,
		LeaseToken:     "lease-redirect",
	}}}
	cfg := Config{
		AlertDispatchBatchSize:   1,
		AlertLeaseTimeout:        time.Minute,
		AlertDeliveryTimeout:     time.Second,
		AlertMaxAttempts:         3,
		AlertRetryInitialBackoff: time.Second,
		AlertRetryMaxBackoff:     time.Minute,
	}
	metrics := NewMetrics("test", "commit", "date", 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sender := NewAlertSender(redirectingWebhook.URL, "site-checker-test", redirectingWebhook.Client())

	if _, err := dispatchAlertBatch(t.Context(), repo, sender, cfg, metrics, logger); err != nil {
		t.Fatal(err)
	}
	if repo.deliveredID != "" {
		t.Fatalf("redirected alert was marked delivered as %q", repo.deliveredID)
	}
	if repo.failedID != "event-redirect" || repo.failedDead {
		t.Fatalf("redirect failure = id:%q dead:%t, want retryable event-redirect", repo.failedID, repo.failedDead)
	}
	if redirectedRequests.Load() != 0 {
		t.Fatalf("redirect target received %d requests, want 0", redirectedRequests.Load())
	}
}

func TestDispatchAlertBatchPersistsRetry(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "temporary failure", http.StatusBadGateway)
	}))
	defer server.Close()

	repo := &recordingAlertOutboxRepository{
		events: []AlertOutboxEvent{{
			ID:             "event-1",
			IdempotencyKey: "incident-1:failure:3",
			AttemptCount:   2,
			LeaseToken:     "lease-1",
		}},
	}
	cfg := Config{
		AlertDispatchBatchSize:   10,
		AlertLeaseTimeout:        time.Minute,
		AlertDeliveryTimeout:     time.Second,
		AlertMaxAttempts:         3,
		AlertRetryInitialBackoff: time.Second,
		AlertRetryMaxBackoff:     time.Minute,
	}
	metrics := NewMetrics("test", "commit", "date", 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	dispatched, err := dispatchAlertBatch(t.Context(), repo, NewAlertSender(server.URL, "test", server.Client()), cfg, metrics, logger)
	if err != nil {
		t.Fatal(err)
	}
	if dispatched != 1 {
		t.Fatalf("dispatched = %d, want 1", dispatched)
	}
	if repo.failedID != "event-1" || repo.failedLease != "lease-1" || repo.failedDead {
		t.Fatalf("persisted failure = id:%q lease:%q dead:%v", repo.failedID, repo.failedLease, repo.failedDead)
	}
	if repo.failedRetryDelay != 2*time.Second {
		t.Fatalf("retry delay = %s, want 2s", repo.failedRetryDelay)
	}
	if repo.claimedMaxAttempts != cfg.AlertMaxAttempts {
		t.Fatalf("claimed max attempts = %d, want %d", repo.claimedMaxAttempts, cfg.AlertMaxAttempts)
	}
}

func TestDispatchAlertBatchMarksExhaustedEventDead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "permanent failure", http.StatusBadGateway)
	}))
	defer server.Close()

	repo := &recordingAlertOutboxRepository{
		events: []AlertOutboxEvent{{
			ID:             "event-dead",
			IdempotencyKey: "incident-1:failure:4",
			AttemptCount:   3,
			LeaseToken:     "lease-dead",
		}},
	}
	cfg := Config{
		AlertDispatchBatchSize:   10,
		AlertLeaseTimeout:        time.Minute,
		AlertDeliveryTimeout:     time.Second,
		AlertMaxAttempts:         3,
		AlertRetryInitialBackoff: time.Second,
		AlertRetryMaxBackoff:     time.Minute,
	}
	metrics := NewMetrics("test", "commit", "date", 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if _, err := dispatchAlertBatch(t.Context(), repo, NewAlertSender(server.URL, "test", server.Client()), cfg, metrics, logger); err != nil {
		t.Fatal(err)
	}
	if !repo.failedDead {
		t.Fatal("exhausted alert was not marked dead")
	}
	if metrics.Snapshot().AlertsDeadTotal != 1 {
		t.Fatal("dead alert metric was not recorded")
	}
}

func TestDispatchAlertMarksPermanentClientErrorDead(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "invalid credentials", http.StatusUnauthorized)
	}))
	defer server.Close()

	repo := &recordingAlertOutboxRepository{events: []AlertOutboxEvent{{
		ID:             "event-permanent",
		IdempotencyKey: "incident-1:failure:1",
		AttemptCount:   1,
		LeaseToken:     "lease-permanent",
	}}}
	cfg := Config{
		AlertDispatchBatchSize:   10,
		AlertLeaseTimeout:        time.Minute,
		AlertDeliveryTimeout:     time.Second,
		AlertMaxAttempts:         8,
		AlertRetryInitialBackoff: time.Second,
		AlertRetryMaxBackoff:     time.Minute,
	}
	metrics := NewMetrics("test", "commit", "date", 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if _, err := dispatchAlertBatch(t.Context(), repo, NewAlertSender(server.URL, "test", server.Client()), cfg, metrics, logger); err != nil {
		t.Fatal(err)
	}
	if !repo.failedDead {
		t.Fatal("permanent webhook response was not marked dead")
	}
}

func TestDispatchAlertHonorsRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "120")
		http.Error(w, "rate limited", http.StatusTooManyRequests)
	}))
	defer server.Close()

	repo := &recordingAlertOutboxRepository{events: []AlertOutboxEvent{{
		ID:             "event-rate-limited",
		IdempotencyKey: "incident-1:failure:2",
		AttemptCount:   1,
		LeaseToken:     "lease-rate-limited",
	}}}
	cfg := Config{
		AlertDispatchBatchSize:   10,
		AlertLeaseTimeout:        time.Minute,
		AlertDeliveryTimeout:     time.Second,
		AlertMaxAttempts:         8,
		AlertRetryInitialBackoff: time.Second,
		AlertRetryMaxBackoff:     time.Minute,
	}
	metrics := NewMetrics("test", "commit", "date", 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	if _, err := dispatchAlertBatch(t.Context(), repo, NewAlertSender(server.URL, "test", server.Client()), cfg, metrics, logger); err != nil {
		t.Fatal(err)
	}
	if repo.failedDead {
		t.Fatal("rate-limited webhook response was marked dead")
	}
	if repo.failedRetryDelay != 2*time.Minute {
		t.Fatalf("retry delay = %s, want Retry-After 2m", repo.failedRetryDelay)
	}
}

func TestAlertRetryDelayIsBounded(t *testing.T) {
	if got := alertRetryDelay(1, time.Second, 10*time.Second); got != time.Second {
		t.Fatalf("attempt 1 delay = %s, want 1s", got)
	}
	if got := alertRetryDelay(10, time.Second, 10*time.Second); got != 10*time.Second {
		t.Fatalf("attempt 10 delay = %s, want 10s", got)
	}
}

func TestParseRetryAfterIsBounded(t *testing.T) {
	if got := parseRetryAfter("999999999999999999", ""); got != maxAlertRetryAfter {
		t.Fatalf("large Retry-After = %s, want %s", got, maxAlertRetryAfter)
	}
}

func TestParseRetryAfterHTTPDateUsesServerDate(t *testing.T) {
	serverTime := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	retryAt := serverTime.Add(2 * time.Minute)
	if got := parseRetryAfter(retryAt.Format(http.TimeFormat), serverTime.Format(http.TimeFormat)); got != 2*time.Minute {
		t.Fatalf("HTTP-date Retry-After = %s, want 2m", got)
	}
	if got := parseRetryAfter(retryAt.Format(http.TimeFormat), ""); got != 0 {
		t.Fatalf("HTTP-date Retry-After without server Date = %s, want exponential fallback", got)
	}
}

type recordingAlertOutboxRepository struct {
	events             []AlertOutboxEvent
	deliveredID        string
	failedID           string
	failedLease        string
	failedRetryDelay   time.Duration
	failedDead         bool
	claimedMaxAttempts int
}

func (r *recordingAlertOutboxRepository) ClaimAlerts(_ context.Context, _ int, _ time.Duration, maxAttempts int) ([]AlertOutboxEvent, error) {
	r.claimedMaxAttempts = maxAttempts
	return r.events, nil
}

func (r *recordingAlertOutboxRepository) MarkAlertDelivered(_ context.Context, id, _ string) error {
	r.deliveredID = id
	return nil
}

func (r *recordingAlertOutboxRepository) MarkAlertFailed(_ context.Context, id, leaseToken, _ string, retryDelay time.Duration, dead bool) error {
	r.failedID = id
	r.failedLease = leaseToken
	r.failedRetryDelay = retryDelay
	r.failedDead = dead
	return nil
}
