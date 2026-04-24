package main

import (
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAlertManagerSendsAfterThreshold(t *testing.T) {
	received := make(chan struct{}, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Fatalf("method = %s, want POST", r.Method)
		}
		received <- struct{}{}
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	metrics := NewMetrics("test", "commit", "date", 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	alerts := NewAlertManager(server.URL, server.Client(), metrics, logger, 1, time.Hour)
	result := CheckResult{
		URL:       "https://example.com",
		Healthy:   false,
		Error:     "boom",
		CheckedAt: time.Now(),
	}
	metrics.RecordResult(result)
	alerts.Handle(t.Context(), result)

	select {
	case <-received:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("timed out waiting for alert webhook")
	}
}
