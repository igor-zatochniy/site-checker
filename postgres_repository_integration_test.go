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

	secondClaim, err := repo.ClaimDue(ctx, 10, now.Add(30*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondClaim) != 0 {
		t.Fatalf("second claim len = %d, want 0 before lease timeout", len(secondClaim))
	}

	reclaimed, err := repo.ClaimDue(ctx, 10, now.Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 || reclaimed[0].ID != monitor.ID {
		t.Fatalf("reclaimed = %+v, want stale pending monitor", reclaimed)
	}

	record := CheckRecord{
		ID:         newID("chk"),
		JobID:      NewCheckJobID(monitor.ID, monitor.NextCheckAt),
		MonitorID:  monitor.ID,
		StatusCode: 500,
		LatencyMS:  42,
		Error:      "unexpected status code 500",
		Success:    false,
		CheckedAt:  time.Now().UTC(),
	}
	if _, err := repo.AddCheck(ctx, record); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.AddCheck(ctx, record); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("duplicate AddCheck error = %v, want ErrDuplicateJob", err)
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
	record.JobID = NewCheckJobID(monitor.ID, time.Now().UTC())
	record.StatusCode = 200
	record.Error = ""
	record.Success = true
	record.CheckedAt = time.Now().UTC()
	if _, err := repo.AddCheck(ctx, record); err != nil {
		t.Fatal(err)
	}

	stats, err := repo.Stats(ctx, monitor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChecksTotal != 2 || stats.SuccessfulChecks != 1 || stats.FailedChecks != 1 {
		t.Fatalf("stats = %+v, want one success and one failure", stats)
	}

	incidents, total, err = repo.ListIncidents(ctx, incidentStatusOpen, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 || len(incidents) != 0 {
		t.Fatalf("open incidents total=%d len=%d, want 0 after recovery", total, len(incidents))
	}
}
