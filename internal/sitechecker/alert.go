package sitechecker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sync"
	"time"
)

var ErrStaleAlertLease = errors.New("alert outbox lease is no longer active")

const alertEventIncidentFailure = "incident.failure"

type AlertPolicy struct {
	Enabled          bool
	FailureThreshold int
	Cooldown         time.Duration
}

type AlertPayload struct {
	EventType           string    `json:"event_type"`
	IncidentID          string    `json:"incident_id"`
	MonitorID           string    `json:"monitor_id"`
	URL                 string    `json:"url"`
	StatusCode          int       `json:"status_code"`
	Error               string    `json:"error,omitempty"`
	ConsecutiveFailures int       `json:"consecutive_failures"`
	CheckedAt           time.Time `json:"checked_at"`
}

type AlertOutboxEvent struct {
	ID             string
	IdempotencyKey string
	IncidentID     string
	MonitorID      string
	Payload        AlertPayload
	AttemptCount   int
	LeaseToken     string
}

type AlertOutboxRepository interface {
	ClaimAlerts(ctx context.Context, limit int, leaseTimeout time.Duration, maxAttempts int) ([]AlertOutboxEvent, error)
	MarkAlertDelivered(ctx context.Context, id, leaseToken string) error
	MarkAlertFailed(ctx context.Context, id, leaseToken, lastError string, retryDelay time.Duration, dead bool) error
}

type AlertSender struct {
	webhookURL string
	userAgent  string
	client     *http.Client
}

func NewAlertSender(webhookURL, userAgent string, client *http.Client) *AlertSender {
	return &AlertSender{
		webhookURL: webhookURL,
		userAgent:  userAgent,
		client:     client,
	}
}

func (s *AlertSender) Send(ctx context.Context, event AlertOutboxEvent) error {
	body, err := json.Marshal(event.Payload)
	if err != nil {
		return fmt.Errorf("encode alert payload: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.webhookURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("create alert request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("User-Agent", s.userAgent)
	req.Header.Set("Idempotency-Key", event.IdempotencyKey)

	resp, err := s.client.Do(req)
	if err != nil {
		return fmt.Errorf("send alert: %w", err)
	}
	defer resp.Body.Close()
	_, _ = io.CopyN(io.Discard, resp.Body, 4*1024)

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return fmt.Errorf("alert webhook returned status %d", resp.StatusCode)
	}
	return nil
}

func RunAlertDispatcher(ctx context.Context, repo AlertOutboxRepository, sender *AlertSender, cfg Config, metrics *Metrics, logger *slog.Logger) {
	ticker := time.NewTicker(cfg.AlertDispatchInterval)
	defer ticker.Stop()

	for {
		dispatched, err := dispatchAlertBatch(ctx, repo, sender, cfg, metrics, logger)
		if err != nil && ctx.Err() == nil {
			logger.Warn("Failed to dispatch alert batch", "error", err)
		}
		if ctx.Err() != nil {
			logger.Info("Alert dispatcher stopped")
			return
		}
		if dispatched == cfg.AlertDispatchBatchSize {
			continue
		}

		select {
		case <-ctx.Done():
			logger.Info("Alert dispatcher stopped")
			return
		case <-ticker.C:
		}
	}
}

func dispatchAlertBatch(ctx context.Context, repo AlertOutboxRepository, sender *AlertSender, cfg Config, metrics *Metrics, logger *slog.Logger) (int, error) {
	events, err := repo.ClaimAlerts(ctx, cfg.AlertDispatchBatchSize, cfg.AlertLeaseTimeout, cfg.AlertMaxAttempts)
	if err != nil {
		return 0, err
	}

	var wg sync.WaitGroup
	for _, event := range events {
		wg.Add(1)
		go func(event AlertOutboxEvent) {
			defer wg.Done()
			dispatchAlert(ctx, repo, sender, event, cfg, metrics, logger)
		}(event)
	}
	wg.Wait()
	if ctx.Err() != nil {
		return len(events), ctx.Err()
	}
	return len(events), nil
}

func dispatchAlert(ctx context.Context, repo AlertOutboxRepository, sender *AlertSender, event AlertOutboxEvent, cfg Config, metrics *Metrics, logger *slog.Logger) {
	deliveryCtx, cancel := context.WithTimeout(ctx, cfg.AlertDeliveryTimeout)
	deliveryErr := sender.Send(deliveryCtx, event)
	cancel()
	if deliveryErr == nil {
		if err := repo.MarkAlertDelivered(ctx, event.ID, event.LeaseToken); err != nil {
			if !errors.Is(err, ErrStaleAlertLease) {
				logger.Warn("Failed to mark alert as delivered", "event_id", event.ID, "error", err)
			}
			return
		}
		metrics.RecordAlertDelivered()
		return
	}

	if ctx.Err() != nil {
		return
	}
	dead := event.AttemptCount >= cfg.AlertMaxAttempts
	retryDelay := alertRetryDelay(event.AttemptCount, cfg.AlertRetryInitialBackoff, cfg.AlertRetryMaxBackoff)
	if err := repo.MarkAlertFailed(ctx, event.ID, event.LeaseToken, deliveryErr.Error(), retryDelay, dead); err != nil {
		if !errors.Is(err, ErrStaleAlertLease) {
			logger.Warn("Failed to persist alert delivery failure", "event_id", event.ID, "error", err)
		}
		return
	}
	metrics.RecordAlertFailure(dead)
	logger.Warn("Alert delivery failed", "event_id", event.ID, "attempt", event.AttemptCount, "dead", dead, "error", deliveryErr)
}

func alertRetryDelay(attempt int, initial, maximum time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := initial
	for i := 1; i < attempt && delay < maximum; i++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}
