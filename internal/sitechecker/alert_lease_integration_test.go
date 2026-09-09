//go:build integration

package sitechecker

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestPostgresAlertFinalizationMargin(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	container, err := postgres.Run(ctx, integrationPostgresImage,
		postgres.WithDatabase("site_checker"), postgres.WithUsername("site_checker"),
		postgres.WithPassword("site_checker"), postgres.BasicWaitStrategies())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := testcontainers.TerminateContainer(container); err != nil {
			t.Logf("terminate PostgreSQL: %v", err)
		}
	})
	pool, err := OpenPostgresPool(ctx, container.MustConnectionString(ctx, "sslmode=disable"))
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	verifyPostgresAlertFinalizationMargin(t, ctx, NewPostgresMonitorRepository(pool, NewNetworkPolicy(cfg)))
}

func verifyPostgresAlertFinalizationMargin(t *testing.T, parent context.Context, repo *PostgresMonitorRepository) {
	t.Helper()
	ctx, cancel := context.WithTimeout(parent, 15*time.Second)
	defer cancel()
	monitor, err := repo.Create(ctx, MonitorInput{
		URL: "https://alert-margin.example.com", IntervalSeconds: 60, TimeoutSeconds: 5, ExpectedStatus: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := repo.Delete(ctx, monitor.ID); err != nil {
			t.Error(err)
		}
	}()
	processPostgresManualCheck(t, ctx, repo, monitor.ID, CheckRecord{
		ID: newID("chk"), MonitorID: monitor.ID, StatusCode: 500, CheckedAt: time.Now().UTC(),
	}, AlertPolicy{Enabled: true, FailureThreshold: 1, Cooldown: time.Hour})

	cfg := Config{
		DatabaseOperationTimeout: time.Second,
		AlertDeliveryTimeout:     time.Second,
		AlertDispatchBatchSize:   1,
		AlertMaxAttempts:         3,
		AlertRetryInitialBackoff: time.Hour,
		AlertRetryMaxBackoff:     time.Hour,
	}
	cfg.AlertLeaseTimeout = minimumAlertLeaseTimeout(cfg.AlertDeliveryTimeout, cfg.DatabaseOperationTimeout)
	alertRepo := NewPostgresMonitorRepositoryWithTimeout(repo.pool, repo.policy, cfg.DatabaseOperationTimeout)
	events, err := alertRepo.ClaimAlerts(ctx, 1, cfg.AlertLeaseTimeout, cfg.AlertMaxAttempts)
	if err != nil || len(events) != 1 || events[0].MonitorID != monitor.ID {
		t.Fatalf("claim alert: events=%+v error=%v", events, err)
	}
	event := events[0]

	var requests atomic.Int32
	stopServer := make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, r.Body)
		requests.Add(1)
		select {
		case <-r.Context().Done():
		case <-stopServer:
		}
	}))
	defer func() {
		close(stopServer)
		server.Close()
	}()
	sender := NewAlertSender(server.URL, "test", server.Client())
	finalizing := &pausedAlertFinalization{
		AlertOutboxRepository: alertRepo,
		started:               make(chan struct{}),
		resume:                make(chan struct{}),
	}
	var once sync.Once
	resume := func() { once.Do(func() { close(finalizing.resume) }) }
	defer resume()
	done := make(chan struct{})
	go func() {
		defer close(done)
		dispatchAlert(ctx, finalizing, sender, event, cfg, NewMetrics("test", "test", "test", 0), discardLogger())
	}()
	select {
	case <-finalizing.started:
	case <-ctx.Done():
		t.Fatal("delivery did not reach database finalization")
	}
	if requests.Load() != 1 {
		t.Fatalf("webhook requests=%d, want one timed-out delivery", requests.Load())
	}
	var remainingMicros int64
	if err := repo.pool.QueryRow(ctx, `SELECT (extract(epoch FROM (locked_until-clock_timestamp()))*1000000)::bigint
		FROM alert_outbox WHERE id=$1`, event.ID).Scan(&remainingMicros); err != nil {
		t.Fatal(err)
	}
	remaining := time.Duration(remainingMicros) * time.Microsecond
	if remaining <= cfg.DatabaseOperationTimeout {
		t.Fatalf("remaining lease=%s, want time for database finalization", remaining)
	}
	// A competing dispatcher must not reclaim this event after HTTP timeout
	// while the first dispatcher is still finalizing its database state.
	concurrent, err := alertRepo.ClaimAlerts(ctx, 100, cfg.AlertLeaseTimeout, cfg.AlertMaxAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if len(concurrent) != 0 {
		t.Fatal("competing dispatcher reclaimed an event during finalization")
	}
	resume()
	select {
	case <-done:
	case <-ctx.Done():
		t.Fatal("dispatcher did not finish")
	}
	var status string
	var attempts int
	if err := repo.pool.QueryRow(ctx, `SELECT status, attempt_count FROM alert_outbox WHERE id=$1`, event.ID).
		Scan(&status, &attempts); err != nil {
		t.Fatal(err)
	}
	if status != "pending" || attempts != 1 {
		t.Fatalf("finalized event: status=%s attempts=%d", status, attempts)
	}
	if _, err := repo.pool.Exec(ctx, `UPDATE alert_outbox SET available_at = clock_timestamp() - interval '1 second'
		WHERE id=$1`, event.ID); err != nil {
		t.Fatal(err)
	}
	retry, err := alertRepo.ClaimAlerts(ctx, 1, cfg.AlertLeaseTimeout, cfg.AlertMaxAttempts)
	if err != nil || len(retry) != 1 || retry[0].ID != event.ID || retry[0].AttemptCount != 2 ||
		retry[0].IdempotencyKey != event.IdempotencyKey || retry[0].LeaseToken == event.LeaseToken {
		t.Fatalf("retry lost event identity or lease fencing: %+v error=%v", retry, err)
	}
	if err := alertRepo.MarkAlertDelivered(ctx, event.ID, event.LeaseToken); !errors.Is(err, ErrStaleAlertLease) {
		t.Fatalf("old dispatcher finalization=%v, want ErrStaleAlertLease", err)
	}
	if err := alertRepo.MarkAlertDelivered(ctx, event.ID, retry[0].LeaseToken); err != nil {
		t.Fatal(err)
	}
}

type pausedAlertFinalization struct {
	AlertOutboxRepository
	started chan struct{}
	resume  chan struct{}
}

func (r *pausedAlertFinalization) MarkAlertFailed(ctx context.Context, id, token, lastError string, delay time.Duration, dead bool) error {
	close(r.started)
	select {
	case <-r.resume:
		return r.AlertOutboxRepository.MarkAlertFailed(ctx, id, token, lastError, delay, dead)
	case <-ctx.Done():
		return ctx.Err()
	}
}
