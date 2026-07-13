package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
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
	if repo.failedAvailableAt.Before(time.Now().Add(500 * time.Millisecond)) {
		t.Fatalf("retry available_at = %s, want future backoff", repo.failedAvailableAt)
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

func TestAlertRetryDelayIsBounded(t *testing.T) {
	if got := alertRetryDelay(1, time.Second, 10*time.Second); got != time.Second {
		t.Fatalf("attempt 1 delay = %s, want 1s", got)
	}
	if got := alertRetryDelay(10, time.Second, 10*time.Second); got != 10*time.Second {
		t.Fatalf("attempt 10 delay = %s, want 10s", got)
	}
}

type recordingAlertOutboxRepository struct {
	events            []AlertOutboxEvent
	failedID          string
	failedLease       string
	failedAvailableAt time.Time
	failedDead        bool
}

func (r *recordingAlertOutboxRepository) ClaimAlerts(context.Context, int, time.Time, time.Duration) ([]AlertOutboxEvent, error) {
	return r.events, nil
}

func (r *recordingAlertOutboxRepository) MarkAlertDelivered(context.Context, string, string, time.Time) error {
	return errors.New("unexpected delivery")
}

func (r *recordingAlertOutboxRepository) MarkAlertFailed(_ context.Context, id, leaseToken, _ string, availableAt time.Time, dead bool) error {
	r.failedID = id
	r.failedLease = leaseToken
	r.failedAvailableAt = availableAt
	r.failedDead = dead
	return nil
}
