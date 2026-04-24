package main

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

type AlertManager struct {
	webhookURL string
	client     *http.Client
	metrics    *Metrics
	logger     *slog.Logger
	threshold  int
	cooldown   time.Duration
	mu         sync.Mutex
	lastSent   map[string]time.Time
}

type AlertPayload struct {
	URL                 string    `json:"url"`
	StatusCode          int       `json:"status_code"`
	Error               string    `json:"error,omitempty"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	CheckedAt           time.Time `json:"checked_at"`
}

func NewAlertManager(webhookURL string, client *http.Client, metrics *Metrics, logger *slog.Logger, threshold int, cooldown time.Duration) *AlertManager {
	return &AlertManager{
		webhookURL: webhookURL,
		client:     client,
		metrics:    metrics,
		logger:     logger,
		threshold:  threshold,
		cooldown:   cooldown,
		lastSent:   make(map[string]time.Time),
	}
}

func (a *AlertManager) Handle(_ context.Context, result CheckResult) {
	if a.webhookURL == "" || result.Healthy {
		return
	}

	failures := a.metrics.ConsecutiveFailures(result.URL)
	if failures < a.threshold || !a.shouldSend(result.URL, result.CheckedAt) {
		return
	}

	payload := AlertPayload{
		URL:                 result.URL,
		StatusCode:          result.StatusCode,
		Error:               result.Error,
		ConsecutiveFailures: failures,
		CheckedAt:           result.CheckedAt.UTC(),
	}

	go a.send(payload)
}

func (a *AlertManager) shouldSend(url string, checkedAt time.Time) bool {
	a.mu.Lock()
	defer a.mu.Unlock()

	if lastSent, exists := a.lastSent[url]; exists && checkedAt.Sub(lastSent) < a.cooldown {
		return false
	}
	a.lastSent[url] = checkedAt
	return true
}

func (a *AlertManager) send(payload AlertPayload) {
	body, err := json.Marshal(payload)
	if err != nil {
		a.logger.Error("Failed to encode alert payload", "url", payload.URL, "error", err)
		return
	}

	timeout := a.client.Timeout
	if timeout <= 0 {
		timeout = 5 * time.Second
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, a.webhookURL, bytes.NewReader(body))
	if err != nil {
		a.logger.Error("Failed to create alert request", "url", payload.URL, "error", err)
		return
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", defaultUserAgent)

	resp, err := a.client.Do(req)
	if err != nil {
		a.logger.Error("Failed to send alert", "url", payload.URL, "error", err)
		return
	}
	defer resp.Body.Close()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		a.logger.Warn("Alert webhook returned non-success status", "url", payload.URL, "status", resp.StatusCode)
	}
}
