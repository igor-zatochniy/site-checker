//go:build integration

package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestPostgresMonitorRepositoryLifecycle(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("site_checker"),
		postgres.WithUsername("site_checker"),
		postgres.WithPassword("site_checker"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	}()

	databaseURL := postgresContainer.MustConnectionString(ctx, "sslmode=disable")
	pool, err := OpenPostgresPool(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}

	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	policy := NewNetworkPolicy(cfg)
	repo := NewPostgresMonitorRepository(pool, policy)

	monitor, err := repo.Create(ctx, MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	claimed, err := repo.ClaimDue(ctx, 10, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != monitor.ID {
		t.Fatalf("claimed = %+v, want created monitor", claimed)
	}
	jobID := NewCheckJobID(monitor.ID, monitor.NextCheckAt)
	if err := repo.MarkProcessing(ctx, monitor.ID, jobID, now.Add(10*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}

	secondClaim, err := repo.ClaimDue(ctx, 10, now.Add(30*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondClaim) != 0 {
		t.Fatalf("second claim len = %d, want 0 before lease timeout", len(secondClaim))
	}
	if err := repo.MarkProcessing(ctx, monitor.ID, jobID, now.Add(30*time.Second), time.Minute); !errors.Is(err, ErrJobAlreadyProcessing) {
		t.Fatalf("second MarkProcessing error = %v, want ErrJobAlreadyProcessing", err)
	}

	reclaimed, err := repo.ClaimDue(ctx, 10, now.Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 || reclaimed[0].ID != monitor.ID {
		t.Fatalf("reclaimed = %+v, want stale pending monitor", reclaimed)
	}
	if err := repo.MarkProcessing(ctx, monitor.ID, jobID, now.Add(2*time.Minute), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkQueued(ctx, monitor.ID, jobID, now.Add(2*time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkProcessing(ctx, monitor.ID, jobID, now.Add(2*time.Minute+2*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}

	record := CheckRecord{
		ID:         newID("chk"),
		JobID:      jobID,
		MonitorID:  monitor.ID,
		StatusCode: 500,
		LatencyMS:  42,
		Error:      "unexpected status code 500",
		Success:    false,
		CheckedAt:  time.Now().UTC(),
	}
	alertPolicy := AlertPolicy{Enabled: true, FailureThreshold: 1, Cooldown: time.Hour}
	if _, err := repo.AddCheck(ctx, record, alertPolicy); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AddCheck(ctx, record, alertPolicy); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("duplicate AddCheck error = %v, want ErrDuplicateJob", err)
	}

	alertNow := time.Now().UTC()
	events, err := repo.ClaimAlerts(ctx, 10, alertNow, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("claimed alerts = %d, want 1", len(events))
	}
	event := events[0]
	if event.AttemptCount != 1 || event.Payload.IncidentID == "" || event.Payload.ConsecutiveFailures != 1 {
		t.Fatalf("alert event = %+v, want first incident failure", event)
	}
	events, err = repo.ClaimAlerts(ctx, 10, alertNow.Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].AttemptCount != 2 {
		t.Fatalf("reclaimed alerts = %+v, want stale lease attempt 2", events)
	}
	if err := repo.MarkAlertDelivered(ctx, event.ID, event.LeaseToken, alertNow); !errors.Is(err, ErrStaleAlertLease) {
		t.Fatalf("expired lease delivery error = %v, want ErrStaleAlertLease", err)
	}
	event = events[0]
	retryAt := alertNow.Add(3 * time.Minute)
	if err := repo.MarkAlertFailed(ctx, event.ID, event.LeaseToken, "temporary failure", retryAt, false); err != nil {
		t.Fatal(err)
	}
	events, err = repo.ClaimAlerts(ctx, 10, alertNow.Add(150*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("alerts before retry = %d, want 0", len(events))
	}
	events, err = repo.ClaimAlerts(ctx, 10, retryAt.Add(time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].AttemptCount != 3 {
		t.Fatalf("retried alerts = %+v, want attempt 3", events)
	}
	if err := repo.MarkAlertDelivered(ctx, events[0].ID, events[0].LeaseToken, retryAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}

	checks, total, err := repo.ListChecks(ctx, monitor.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(checks) != 1 {
		t.Fatalf("checks total=%d len=%d, want 1", total, len(checks))
	}

	incidents, total, err := repo.ListIncidents(ctx, incidentStatusOpen, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 1 || len(incidents) != 1 {
		t.Fatalf("open incidents total=%d len=%d, want 1", total, len(incidents))
	}

	record.ID = newID("chk")
	record.JobID = ""
	record.CheckedAt = time.Now().UTC()
	if _, err := repo.AddCheck(ctx, record, alertPolicy); err != nil {
		t.Fatal(err)
	}
	events, err = repo.ClaimAlerts(ctx, 10, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("alerts during cooldown = %d, want 0", len(events))
	}

	record.ID = newID("chk")
	record.StatusCode = 200
	record.Error = ""
	record.Success = true
	record.CheckedAt = time.Now().UTC()
	if _, err := repo.AddCheck(ctx, record, alertPolicy); err != nil {
		t.Fatal(err)
	}

	stats, err := repo.Stats(ctx, monitor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChecksTotal != 3 || stats.SuccessfulChecks != 1 || stats.FailedChecks != 2 {
		t.Fatalf("stats = %+v, want one success and two failures", stats)
	}

	incidents, total, err = repo.ListIncidents(ctx, incidentStatusOpen, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 || len(incidents) != 0 {
		t.Fatalf("open incidents total=%d len=%d, want 0 after recovery", total, len(incidents))
	}
}
