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
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

var ErrStaleAlertLease = errors.New("alert outbox lease is no longer active")

const (
	alertEventIncidentFailure = "incident.failure"
	maxAlertRetryAfter        = 24 * time.Hour
)

type AlertDeliveryError struct {
	StatusCode int
	Permanent  bool
	RetryAfter time.Duration
}

func (e *AlertDeliveryError) Error() string {
	return fmt.Sprintf("alert webhook returned status %d", e.StatusCode)
}

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
	if client == nil {
		client = http.DefaultClient
	}
	alertClient := *client
	alertClient.CheckRedirect = func(*http.Request, []*http.Request) error {
		return http.ErrUseLastResponse
	}
	return &AlertSender{
		webhookURL: webhookURL,
		userAgent:  userAgent,
		client:     &alertClient,
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
		return fmt.Errorf("send alert: %w", alertTransportCause(err))
	}
	defer resp.Body.Close()
	_, _ = io.CopyN(io.Discard, resp.Body, 4*1024)

	if resp.StatusCode < http.StatusOK || resp.StatusCode >= http.StatusMultipleChoices {
		return &AlertDeliveryError{
			StatusCode: resp.StatusCode,
			Permanent:  isPermanentAlertStatus(resp.StatusCode),
			RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), resp.Header.Get("Date")),
		}
	}
	return nil
}

func alertTransportCause(err error) error {
	var urlErr *url.Error
	if errors.As(err, &urlErr) && urlErr.Err != nil {
		return urlErr.Err
	}
	return err
}

func isPermanentAlertStatus(statusCode int) bool {
	if statusCode < http.StatusBadRequest || statusCode >= http.StatusInternalServerError {
		return false
	}
	switch statusCode {
	case http.StatusRequestTimeout, http.StatusTooEarly, http.StatusTooManyRequests:
		return false
	default:
		return true
	}
}

func parseRetryAfter(value, responseDate string) time.Duration {
	value = strings.TrimSpace(value)
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil {
		if seconds <= 0 {
			return 0
		}
		if seconds >= int64(maxAlertRetryAfter/time.Second) {
			return maxAlertRetryAfter
		}
		return time.Duration(seconds) * time.Second
	}
	retryAt, err := http.ParseTime(value)
	if err != nil {
		return 0
	}
	serverTime, err := http.ParseTime(responseDate)
	if err != nil || !retryAt.After(serverTime) {
		return 0
	}
	delay := retryAt.Sub(serverTime)
	if delay > maxAlertRetryAfter {
		return maxAlertRetryAfter
	}
	return delay
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
	var responseError *AlertDeliveryError
	permanent := errors.As(deliveryErr, &responseError) && responseError.Permanent
	dead := permanent || event.AttemptCount >= cfg.AlertMaxAttempts
	retryDelay := alertRetryDelay(event.AttemptCount, cfg.AlertRetryInitialBackoff, cfg.AlertRetryMaxBackoff)
	if responseError != nil && responseError.RetryAfter > retryDelay {
		retryDelay = responseError.RetryAfter
	}
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
