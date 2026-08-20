//go:build integration

package sitechecker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/moby/moby/client"
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

	claimed, err := repo.ClaimDueJobs(ctx, 10, time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].MonitorID != monitor.ID {
		t.Fatalf("claimed = %+v, want created monitor job", claimed)
	}
	jobID := claimed[0].ID
	if claimed[0].Status != checkJobStatusScheduled || claimed[0].Attempt != 0 {
		t.Fatalf("initial check job = %+v, want scheduled attempt 0", claimed[0])
	}
	if err := repo.MarkJobPublished(ctx, monitor.ID, jobID); err != nil {
		t.Fatal(err)
	}
	staleLease, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	secondClaim, err := repo.ClaimDueJobs(ctx, 10, time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondClaim) != 0 {
		t.Fatalf("second claim len = %d, want 0 before lease timeout", len(secondClaim))
	}
	if _, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 1, time.Minute); !errors.Is(err, ErrJobAlreadyProcessing) {
		t.Fatalf("second MarkJobProcessing error = %v, want ErrJobAlreadyProcessing", err)
	}

	if _, err := pool.Exec(ctx, `UPDATE check_jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = $1`, jobID); err != nil {
		t.Fatal(err)
	}
	reclaimed, err := repo.ClaimDueJobs(ctx, 10, time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 || reclaimed[0].ID != jobID || reclaimed[0].Status != checkJobStatusScheduled || reclaimed[0].Attempt != 1 {
		t.Fatalf("reclaimed = %+v, want persisted stale job", reclaimed)
	}
	if _, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 1, time.Minute); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("old delivery error = %v, want ErrStaleJob", err)
	}
	if err := repo.MarkJobPublished(ctx, monitor.ID, jobID); err != nil {
		t.Fatal(err)
	}
	retryLease, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkJobFailed(ctx, staleLease, "stale worker failure", 0); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("stale worker MarkJobFailed error = %v, want ErrStaleJob", err)
	}
	if err := repo.MarkJobFailed(ctx, retryLease, "temporary storage failure", 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	beforeRetry, err := repo.ClaimDueJobs(ctx, 10, time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeRetry) != 0 {
		t.Fatalf("jobs before retry = %+v, want none", beforeRetry)
	}
	time.Sleep(150 * time.Millisecond)
	retryJobs, err := repo.ClaimDueJobs(ctx, 10, time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if len(retryJobs) != 1 || retryJobs[0].ID != jobID || retryJobs[0].Attempt != 2 {
		t.Fatalf("retry jobs = %+v, want attempt 2 job", retryJobs)
	}
	if err := repo.MarkJobPublished(ctx, monitor.ID, jobID); err != nil {
		t.Fatal(err)
	}
	finalLease, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 3, time.Minute)
	if err != nil {
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
	if _, err := repo.AddCheck(ctx, record, finalLease, alertPolicy); err != nil {
		t.Fatal(err)
	}
	completedJob, err := repo.GetCheckJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if completedJob.Status != checkJobStatusCompleted || completedJob.Attempt != 3 || completedJob.CompletedAt.IsZero() {
		t.Fatalf("completed job = %+v", completedJob)
	}
	if _, err := repo.AddCheck(ctx, record, finalLease, alertPolicy); !errors.Is(err, ErrDuplicateJob) {
		t.Fatalf("duplicate AddCheck error = %v, want ErrDuplicateJob", err)
	}

	events, err := repo.ClaimAlerts(ctx, 10, time.Minute, 3)
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
	if _, err := pool.Exec(ctx, `UPDATE alert_outbox SET locked_until = clock_timestamp() - interval '1 second' WHERE id = $1`, event.ID); err != nil {
		t.Fatal(err)
	}
	events, err = repo.ClaimAlerts(ctx, 10, time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].AttemptCount != 2 {
		t.Fatalf("reclaimed alerts = %+v, want stale lease attempt 2", events)
	}
	if err := repo.MarkAlertDelivered(ctx, event.ID, event.LeaseToken); !errors.Is(err, ErrStaleAlertLease) {
		t.Fatalf("expired lease delivery error = %v, want ErrStaleAlertLease", err)
	}
	event = events[0]
	if err := repo.MarkAlertFailed(ctx, event.ID, event.LeaseToken, "temporary failure", 100*time.Millisecond, false); err != nil {
		t.Fatal(err)
	}
	events, err = repo.ClaimAlerts(ctx, 10, time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("alerts before retry = %d, want 0", len(events))
	}
	time.Sleep(150 * time.Millisecond)
	events, err = repo.ClaimAlerts(ctx, 10, time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].AttemptCount != 3 {
		t.Fatalf("retried alerts = %+v, want attempt 3", events)
	}
	if err := repo.MarkAlertDelivered(ctx, events[0].ID, events[0].LeaseToken); err != nil {
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
	record.CheckedAt = time.Now().UTC()
	processPostgresManualCheck(t, ctx, repo, monitor.ID, record, alertPolicy)
	events, err = repo.ClaimAlerts(ctx, 10, time.Minute, 3)
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
	processPostgresManualCheck(t, ctx, repo, monitor.ID, record, alertPolicy)

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

	manualLeaseMonitor, err := repo.Create(ctx, MonitorInput{
		URL:             "https://manual-check.example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	manualNow := time.Now().UTC()
	manualUpdated := processPostgresManualCheck(t, ctx, repo, manualLeaseMonitor.ID, CheckRecord{
		ID:         newID("chk"),
		MonitorID:  manualLeaseMonitor.ID,
		StatusCode: 200,
		LatencyMS:  20,
		Success:    true,
		CheckedAt:  manualNow.Add(20 * time.Second),
	}, AlertPolicy{})
	if !manualUpdated.NextCheckAt.Equal(manualLeaseMonitor.NextCheckAt) {
		t.Fatalf("manual check moved next_check_at to %s, want %s", manualUpdated.NextCheckAt, manualLeaseMonitor.NextCheckAt)
	}

	manualClaimed, err := repo.ClaimDueJobs(ctx, 10, time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	manualScheduledJobID := findClaimedJobID(manualClaimed, manualLeaseMonitor.ID)
	if manualScheduledJobID == "" {
		t.Fatalf("manual monitor periodic job was not claimed: %+v", manualClaimed)
	}
	if err := repo.MarkJobPublished(ctx, manualLeaseMonitor.ID, manualScheduledJobID); err != nil {
		t.Fatal(err)
	}
	manualScheduledLease, err := repo.MarkJobProcessing(ctx, manualLeaseMonitor.ID, manualScheduledJobID, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, err = repo.AddCheck(ctx, CheckRecord{
		ID:         newID("chk"),
		JobID:      manualScheduledJobID,
		MonitorID:  manualLeaseMonitor.ID,
		StatusCode: 200,
		LatencyMS:  25,
		Success:    true,
		CheckedAt:  manualNow.Add(40 * time.Second),
	}, manualScheduledLease, AlertPolicy{})
	if err != nil {
		t.Fatalf("scheduled result after manual check error = %v", err)
	}
	manualChecks, manualTotal, err := repo.ListChecks(ctx, manualLeaseMonitor.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if manualTotal != 2 || len(manualChecks) != 2 {
		t.Fatalf("manual monitor checks total=%d len=%d, want 2", manualTotal, len(manualChecks))
	}

	failProcessingMonitor, err := repo.Create(ctx, MonitorInput{
		URL:             "https://fail-processing.example.com",
		IntervalSeconds: 300,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	failClaimed, err := repo.ClaimDueJobs(ctx, 10, time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	foundFailProcessingMonitor := false
	failJobID := ""
	for _, claimed := range failClaimed {
		if claimed.MonitorID == failProcessingMonitor.ID {
			foundFailProcessingMonitor = true
			failJobID = claimed.ID
			break
		}
	}
	if !foundFailProcessingMonitor {
		t.Fatalf("fail-processing monitor was not claimed: %+v", failClaimed)
	}
	if err := repo.MarkJobPublished(ctx, failProcessingMonitor.ID, failJobID); err != nil {
		t.Fatal(err)
	}
	failLease, err := repo.MarkJobProcessing(ctx, failProcessingMonitor.ID, failJobID, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	dbBeforeDead, err := databaseTime(ctx, pool)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkJobDead(ctx, failLease, "result persistence failed"); err != nil {
		t.Fatal(err)
	}
	afterFailProcessing, err := repo.Get(ctx, failProcessingMonitor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if afterFailProcessing.UpdatedAt.Before(dbBeforeDead) || afterFailProcessing.UpdatedAt.After(time.Now().UTC().Add(time.Second)) {
		t.Fatalf("updated_at = %s, want database current time after %s", afterFailProcessing.UpdatedAt, dbBeforeDead)
	}
	wantNextCheckAt := afterFailProcessing.UpdatedAt.Add(5 * time.Minute)
	if !afterFailProcessing.NextCheckAt.Equal(wantNextCheckAt) {
		t.Fatalf("next_check_at = %s, want %s", afterFailProcessing.NextCheckAt, wantNextCheckAt)
	}
	deadJob, err := repo.GetCheckJob(ctx, failJobID)
	if err != nil {
		t.Fatal(err)
	}
	if deadJob.Status != checkJobStatusDead || deadJob.CompletedAt.IsZero() {
		t.Fatalf("dead job = %+v", deadJob)
	}

	attemptLimitMonitor, err := repo.Create(ctx, MonitorInput{
		URL:             "https://attempt-limit.example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	attemptJobs, err := repo.ClaimDueJobs(ctx, 100, time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	attemptJobID := findClaimedJobID(attemptJobs, attemptLimitMonitor.ID)
	if attemptJobID == "" {
		t.Fatalf("attempt-limit monitor was not claimed: %+v", attemptJobs)
	}
	if err := repo.MarkJobPublished(ctx, attemptLimitMonitor.ID, attemptJobID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.MarkJobProcessing(ctx, attemptLimitMonitor.ID, attemptJobID, 3, time.Minute); err != nil {
		t.Fatal(err)
	}

	if _, err := pool.Exec(ctx, `UPDATE check_jobs SET lease_expires_at = clock_timestamp() - interval '1 second' WHERE id = $1`, attemptJobID); err != nil {
		t.Fatal(err)
	}
	recoveredJobs, err := repo.ClaimDueJobs(ctx, 100, time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredJobID := findClaimedJobID(recoveredJobs, attemptLimitMonitor.ID); recoveredJobID != "" {
		t.Fatalf("recovered final attempt as job %s, want no attempt 4", recoveredJobID)
	}
	terminalJob, err := repo.GetCheckJob(ctx, attemptJobID)
	if err != nil {
		t.Fatal(err)
	}
	if terminalJob.Status != checkJobStatusDead || terminalJob.Attempt != 3 || terminalJob.CompletedAt.IsZero() {
		t.Fatalf("terminal job = %+v, want dead attempt 3", terminalJob)
	}
	var activeJobs int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM check_jobs
		WHERE monitor_id = $1
			AND status IN ('scheduled', 'queued', 'processing', 'failed')
	`, attemptLimitMonitor.ID).Scan(&activeJobs); err != nil {
		t.Fatal(err)
	}
	if activeJobs != 0 {
		t.Fatalf("active jobs after terminal recovery = %d, want 0", activeJobs)
	}
	updatedAttemptMonitor, err := repo.Get(ctx, attemptLimitMonitor.ID)
	if err != nil {
		t.Fatal(err)
	}
	wantAttemptNextCheckAt := updatedAttemptMonitor.UpdatedAt.Add(time.Minute)
	if !updatedAttemptMonitor.NextCheckAt.Equal(wantAttemptNextCheckAt) {
		t.Fatalf("next_check_at = %s, want %s", updatedAttemptMonitor.NextCheckAt, wantAttemptNextCheckAt)
	}
	if _, err := pool.Exec(ctx, `UPDATE monitors SET next_check_at = clock_timestamp() - interval '1 second' WHERE id = $1`, attemptLimitMonitor.ID); err != nil {
		t.Fatal(err)
	}
	nextPeriodicJobs, err := repo.ClaimDueJobs(ctx, 100, time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	nextPeriodicJobID := findClaimedJobID(nextPeriodicJobs, attemptLimitMonitor.ID)
	if nextPeriodicJobID == "" || nextPeriodicJobID == attemptJobID {
		t.Fatalf("next periodic jobs = %+v, want a new job", nextPeriodicJobs)
	}
	nextPeriodicJob, err := repo.GetCheckJob(ctx, nextPeriodicJobID)
	if err != nil {
		t.Fatal(err)
	}
	if nextPeriodicJob.Attempt != 0 {
		t.Fatalf("next periodic attempt = %d, want 0", nextPeriodicJob.Attempt)
	}

	verifyPostgresConfigurationUpdateInvalidatesActiveJob(t, ctx, repo)
	verifyPostgresUpdateResetsIncidentAndReschedulesInterval(t, ctx, repo)
	verifyPostgresRetentionDeletesOnlyExpiredTerminalData(t, ctx, repo)
	verifyPostgresUsesAuthoritativeLifecycleClock(t, ctx, repo)
	verifyPostgresUsesRecordedTimeForHistoryAndRetention(t, ctx, repo)
	verifyPostgresRecoversKilledWorkerLease(t, ctx, repo)
	verifyPostgresAlertAttemptLimitIsAtomic(t, ctx, repo)
	verifyPostgresRecoversAfterDatabaseInterruption(t, ctx, repo, postgresContainer)
}

func verifyPostgresUsesAuthoritativeLifecycleClock(
	t *testing.T,
	ctx context.Context,
	repo *PostgresMonitorRepository,
) {
	t.Helper()
	monitor, err := repo.Create(ctx, MonitorInput{
		URL:             "https://database-clock.example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := repo.ClaimDueJobs(ctx, 100, 90*time.Second, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	jobID := findClaimedJobID(jobs, monitor.ID)
	if jobID == "" {
		t.Fatalf("database-clock monitor was not claimed: %+v", jobs)
	}
	if err := repo.MarkJobPublished(ctx, monitor.ID, jobID); err != nil {
		t.Fatal(err)
	}
	dbBeforeProcessing, err := databaseTime(ctx, repo.pool)
	if err != nil {
		t.Fatal(err)
	}
	lease, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 1, 90*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	dbAfterProcessing, err := databaseTime(ctx, repo.pool)
	if err != nil {
		t.Fatal(err)
	}
	job, err := repo.GetCheckJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.ProcessingStartedAt.Before(dbBeforeProcessing) || job.ProcessingStartedAt.After(dbAfterProcessing) {
		t.Fatalf("processing_started_at = %s, want PostgreSQL time in [%s, %s]", job.ProcessingStartedAt, dbBeforeProcessing, dbAfterProcessing)
	}
	if got := job.LeaseExpiresAt.Sub(job.ProcessingStartedAt); got != 90*time.Second {
		t.Fatalf("processing lease duration = %s, want 90s", got)
	}
	if reclaimed, err := repo.ClaimDueJobs(ctx, 100, 90*time.Second, defaultMaxJobAttempts); err != nil {
		t.Fatal(err)
	} else if reclaimedID := findClaimedJobID(reclaimed, monitor.ID); reclaimedID != "" {
		t.Fatalf("live database-clock lease was reclaimed as %s", reclaimedID)
	}

	observationTime := time.Now().UTC().Add(-time.Hour)
	updated, err := repo.AddCheck(ctx, CheckRecord{
		ID:         newID("chk"),
		JobID:      jobID,
		MonitorID:  monitor.ID,
		StatusCode: 500,
		LatencyMS:  5,
		Error:      "unexpected status code 500",
		Success:    false,
		CheckedAt:  observationTime,
	}, lease, AlertPolicy{Enabled: true, FailureThreshold: 1, Cooldown: time.Hour})
	if err != nil {
		t.Fatal(err)
	}
	if updated.LastCheckedAt.Equal(observationTime.Truncate(time.Microsecond)) || updated.LastCheckedAt.Before(dbAfterProcessing) {
		t.Fatalf("last_checked_at = %s, want PostgreSQL recording time after %s", updated.LastCheckedAt, dbAfterProcessing)
	}
	if want := updated.UpdatedAt.Add(time.Minute); !updated.NextCheckAt.Equal(want) {
		t.Fatalf("next_check_at = %s, want PostgreSQL lifecycle time %s", updated.NextCheckAt, want)
	}
	if updated.NextCheckAt.Before(time.Now().UTC().Add(30 * time.Second)) {
		t.Fatalf("skewed checked_at moved next_check_at into the past: %s", updated.NextCheckAt)
	}

	processPostgresManualCheck(t, ctx, repo, monitor.ID, CheckRecord{
		ID:         newID("chk"),
		MonitorID:  monitor.ID,
		StatusCode: 500,
		LatencyMS:  5,
		Error:      "unexpected status code 500",
		Success:    false,
		CheckedAt:  time.Now().UTC().Add(time.Hour),
	}, AlertPolicy{Enabled: true, FailureThreshold: 1, Cooldown: time.Hour})
	var alertCount int
	if err := repo.pool.QueryRow(ctx, `SELECT count(*) FROM alert_outbox WHERE monitor_id = $1`, monitor.ID).Scan(&alertCount); err != nil {
		t.Fatal(err)
	}
	if alertCount != 1 {
		t.Fatalf("alert outbox rows under skewed observation times = %d, want cooldown to keep 1", alertCount)
	}
	var firstFailureAt, lastFailureAt time.Time
	if err := repo.pool.QueryRow(ctx, `
		SELECT first_failure_at, last_failure_at
		FROM incidents
		WHERE monitor_id = $1 AND status = 'open'
	`, monitor.ID).Scan(&firstFailureAt, &lastFailureAt); err != nil {
		t.Fatal(err)
	}
	dbNow, err := databaseTime(ctx, repo.pool)
	if err != nil {
		t.Fatal(err)
	}
	if firstFailureAt.Before(dbBeforeProcessing) || lastFailureAt.After(dbNow) {
		t.Fatalf("incident lifecycle timestamps are outside PostgreSQL time window: first=%s last=%s db=[%s,%s]", firstFailureAt, lastFailureAt, dbBeforeProcessing, dbNow)
	}
}

func verifyPostgresUsesRecordedTimeForHistoryAndRetention(
	t *testing.T,
	ctx context.Context,
	repo *PostgresMonitorRepository,
) {
	t.Helper()
	monitor, err := repo.Create(ctx, MonitorInput{
		URL:             "https://recorded-order.example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	dbBefore, err := databaseTime(ctx, repo.pool)
	if err != nil {
		t.Fatal(err)
	}
	futureObservation := dbBefore.Add(10 * time.Minute)
	processPostgresManualCheck(t, ctx, repo, monitor.ID, CheckRecord{
		ID:         "recorded_order_success",
		MonitorID:  monitor.ID,
		StatusCode: 200,
		LatencyMS:  5,
		Success:    true,
		CheckedAt:  futureObservation,
	}, AlertPolicy{})
	failureObservation := dbBefore
	updated := processPostgresManualCheck(t, ctx, repo, monitor.ID, CheckRecord{
		ID:         "recorded_order_failure",
		MonitorID:  monitor.ID,
		StatusCode: 500,
		LatencyMS:  5,
		Error:      "unexpected status code 500",
		Success:    false,
		CheckedAt:  failureObservation,
	}, AlertPolicy{})
	dbAfter, err := databaseTime(ctx, repo.pool)
	if err != nil {
		t.Fatal(err)
	}

	checks, total, err := repo.ListChecks(ctx, monitor.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 || len(checks) != 2 || checks[0].ID != "recorded_order_failure" {
		t.Fatalf("recorded-time history = %+v total=%d, want latest failure first", checks, total)
	}
	if !checks[0].CheckedAt.Equal(failureObservation.Truncate(time.Microsecond)) ||
		!checks[1].CheckedAt.Equal(futureObservation.Truncate(time.Microsecond)) {
		t.Fatalf("public observation timestamps changed: %+v", checks)
	}
	stats, err := repo.Stats(ctx, monitor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ConsecutiveFailure != 1 || stats.LastStatusCode != 500 {
		t.Fatalf("stats after skewed observations = %+v, want one latest failure", stats)
	}
	if updated.LastCheckedAt.Before(dbBefore) || updated.LastCheckedAt.After(dbAfter) {
		t.Fatalf("last_checked_at = %s, want PostgreSQL time in [%s, %s]", updated.LastCheckedAt, dbBefore, dbAfter)
	}

	retentionMonitor, err := repo.Create(ctx, MonitorInput{
		URL:             "https://recorded-retention.example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	oldObservationID := "recorded_retention_fresh"
	processPostgresManualCheck(t, ctx, repo, retentionMonitor.ID, CheckRecord{
		ID:         oldObservationID,
		MonitorID:  retentionMonitor.ID,
		StatusCode: 200,
		LatencyMS:  5,
		Success:    true,
		CheckedAt:  dbBefore.Add(-100 * 24 * time.Hour),
	}, AlertPolicy{})
	result, err := repo.DeleteExpiredData(ctx, time.Time{}, RetentionPolicy{
		CheckResults:     90 * 24 * time.Hour,
		CheckJobs:        10 * 365 * 24 * time.Hour,
		AlertOutbox:      10 * 365 * 24 * time.Hour,
		ResolvedIncident: 10 * 365 * 24 * time.Hour,
		BatchSize:        100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CheckResults != 0 {
		t.Fatalf("retention deleted %d fresh results with skewed observation time", result.CheckResults)
	}
	assertPostgresRecordExists(t, ctx, repo, "SELECT EXISTS (SELECT 1 FROM check_results WHERE id = $1)", oldObservationID, true)
}

func verifyPostgresAlertAttemptLimitIsAtomic(
	t *testing.T,
	ctx context.Context,
	repo *PostgresMonitorRepository,
) {
	t.Helper()
	var eventID string
	if err := repo.pool.QueryRow(ctx, `
		SELECT id
		FROM alert_outbox
		WHERE status = 'pending'
		ORDER BY created_at DESC
		LIMIT 1
	`).Scan(&eventID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.pool.Exec(ctx, `
		UPDATE alert_outbox
		SET status = 'processing',
			attempt_count = 1,
			lease_token = 'expired-max-attempt',
			locked_until = clock_timestamp() - interval '1 second'
		WHERE id = $1
	`, eventID); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.pool.Exec(ctx, `
		UPDATE alert_outbox
		SET available_at = clock_timestamp() + interval '1 hour'
		WHERE id <> $1 AND status = 'pending'
	`, eventID); err != nil {
		t.Fatal(err)
	}
	events, err := repo.ClaimAlerts(ctx, 100, time.Minute, 1)
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 0 {
		t.Fatalf("claimed alerts after exhausted lease recovery = %+v, want none", events)
	}
	var status string
	var attempts int
	var leaseCleared bool
	if err := repo.pool.QueryRow(ctx, `
		SELECT status, attempt_count, lease_token IS NULL
		FROM alert_outbox
		WHERE id = $1
	`, eventID).Scan(&status, &attempts, &leaseCleared); err != nil {
		t.Fatal(err)
	}
	if status != "dead" || attempts != 1 || !leaseCleared {
		t.Fatalf("exhausted alert = status:%s attempts:%d lease_cleared:%t, want dead/1/true", status, attempts, leaseCleared)
	}
}

func verifyPostgresRecoversKilledWorkerLease(
	t *testing.T,
	ctx context.Context,
	repo *PostgresMonitorRepository,
) {
	t.Helper()
	monitor, err := repo.Create(ctx, MonitorInput{
		URL:             "https://killed-worker.example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := repo.ClaimDueJobs(ctx, 100, time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	jobID := findClaimedJobID(jobs, monitor.ID)
	if jobID == "" {
		t.Fatalf("killed-worker monitor was not claimed: %+v", jobs)
	}
	if err := repo.MarkJobPublished(ctx, monitor.ID, jobID); err != nil {
		t.Fatal(err)
	}
	firstLease, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.pool.Exec(ctx, `
		UPDATE check_jobs
		SET lease_expires_at = clock_timestamp() - interval '1 second'
		WHERE id = $1 AND lease_token = $2
	`, jobID, firstLease.LeaseToken); err != nil {
		t.Fatal(err)
	}
	recovered, err := repo.ClaimDueJobs(ctx, 100, time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if recoveredID := findClaimedJobID(recovered, monitor.ID); recoveredID != jobID {
		t.Fatalf("recovered worker job = %q, want %q", recoveredID, jobID)
	}
	if err := repo.MarkJobPublished(ctx, monitor.ID, jobID); err != nil {
		t.Fatal(err)
	}
	secondLease, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 2, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if secondLease.LeaseToken == firstLease.LeaseToken {
		t.Fatal("recovered worker reused stale fencing token")
	}
	if _, err := repo.AddCheck(ctx, CheckRecord{
		ID:         newID("chk"),
		JobID:      jobID,
		MonitorID:  monitor.ID,
		StatusCode: 200,
		LatencyMS:  5,
		Success:    true,
		CheckedAt:  time.Now().UTC(),
	}, secondLease, AlertPolicy{}); err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkJobFailed(ctx, firstLease, "late killed worker", 0); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("stale killed worker transition error = %v, want ErrStaleJob", err)
	}
	var resultCount int
	if err := repo.pool.QueryRow(ctx, `SELECT count(*) FROM check_results WHERE job_id = $1`, jobID).Scan(&resultCount); err != nil {
		t.Fatal(err)
	}
	if resultCount != 1 {
		t.Fatalf("results after worker recovery = %d, want 1", resultCount)
	}
}

func verifyPostgresRecoversAfterDatabaseInterruption(
	t *testing.T,
	ctx context.Context,
	repo *PostgresMonitorRepository,
	container testcontainers.Container,
) {
	t.Helper()
	before, err := repo.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	provider, err := testcontainers.NewDockerProvider()
	if err != nil {
		t.Fatal(err)
	}
	defer provider.Close()
	if _, err := provider.Client().ContainerPause(ctx, container.GetContainerID(), client.ContainerPauseOptions{}); err != nil {
		t.Fatal(err)
	}
	pingCtx, pingCancel := context.WithTimeout(ctx, 2*time.Second)
	pingErr := repo.Ping(pingCtx)
	pingCancel()
	if pingErr == nil {
		t.Fatal("PostgreSQL ping succeeded while database container was paused")
	}
	if _, err := provider.Client().ContainerUnpause(ctx, container.GetContainerID(), client.ContainerUnpauseOptions{}); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(30 * time.Second)
	for {
		pingCtx, pingCancel = context.WithTimeout(ctx, time.Second)
		err = repo.Ping(pingCtx)
		pingCancel()
		if err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("PostgreSQL did not recover after interruption: %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	after, err := repo.Count(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if after != before {
		t.Fatalf("monitor count after PostgreSQL interruption = %d, want %d", after, before)
	}
}

func verifyPostgresConfigurationUpdateInvalidatesActiveJob(
	t *testing.T,
	ctx context.Context,
	repo *PostgresMonitorRepository,
) {
	t.Helper()
	monitor, err := repo.Create(ctx, MonitorInput{
		URL:             "https://configuration-a.example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs, err := repo.ClaimDueJobs(ctx, 100, time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	jobID := findClaimedJobID(jobs, monitor.ID)
	if jobID == "" {
		t.Fatalf("no job claimed for monitor %s", monitor.ID)
	}
	if err := repo.MarkJobPublished(ctx, monitor.ID, jobID); err != nil {
		t.Fatal(err)
	}
	lease, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	newURL := "https://configuration-b.example.com"
	updated, err := repo.Update(ctx, monitor.ID, MonitorPatch{URL: &newURL})
	if err != nil {
		t.Fatal(err)
	}
	if updated.URL != newURL || updated.LastStatusCode != 0 || !updated.LastCheckedAt.IsZero() {
		t.Fatalf("updated monitor = %+v", updated)
	}

	_, err = repo.AddCheck(ctx, CheckRecord{
		ID:         "check_stale_configuration",
		JobID:      jobID,
		MonitorID:  monitor.ID,
		StatusCode: 500,
		Error:      "old target failed",
		Success:    false,
		CheckedAt:  time.Now().UTC(),
	}, lease, AlertPolicy{Enabled: true, FailureThreshold: 1})
	if !errors.Is(err, ErrStaleJob) {
		t.Fatalf("AddCheck error = %v, want ErrStaleJob", err)
	}
	checks, total, err := repo.ListChecks(ctx, monitor.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 || len(checks) != 0 {
		t.Fatalf("checks = %+v total=%d, want none", checks, total)
	}
	job, err := repo.GetCheckJob(ctx, jobID)
	if err != nil {
		t.Fatal(err)
	}
	if job.Status != checkJobStatusDead || job.LastError != "monitor configuration changed" || job.LeaseToken != "" {
		t.Fatalf("invalidated job = %+v", job)
	}
	incidents, _, err := repo.ListIncidents(ctx, "", 0, 100)
	if err != nil {
		t.Fatal(err)
	}
	for _, incident := range incidents {
		if incident.MonitorID == monitor.ID {
			t.Fatalf("stale result created incident %+v", incident)
		}
	}
}

func verifyPostgresUpdateResetsIncidentAndReschedulesInterval(
	t *testing.T,
	ctx context.Context,
	repo *PostgresMonitorRepository,
) {
	t.Helper()
	monitor, err := repo.Create(ctx, MonitorInput{
		URL:             "https://semantic-a.example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	alertPolicy := AlertPolicy{Enabled: true, FailureThreshold: 3, Cooldown: time.Hour}
	for index := range 2 {
		processPostgresManualCheck(t, ctx, repo, monitor.ID, CheckRecord{
			ID:         fmt.Sprintf("semantic_failure_%d", index),
			MonitorID:  monitor.ID,
			StatusCode: 500,
			LatencyMS:  5,
			Error:      "unexpected status code 500",
			Success:    false,
			CheckedAt:  time.Now().UTC(),
		}, alertPolicy)
	}
	var oldIncidentID string
	var oldFailureCount int
	if err := repo.pool.QueryRow(ctx, `
		SELECT id, failure_count
		FROM incidents
		WHERE monitor_id = $1 AND status = 'open'
	`, monitor.ID).Scan(&oldIncidentID, &oldFailureCount); err != nil {
		t.Fatal(err)
	}
	if oldFailureCount != 2 {
		t.Fatalf("old incident failure_count = %d, want 2", oldFailureCount)
	}

	newURL := "https://semantic-b.example.com"
	if _, err := repo.Update(ctx, monitor.ID, MonitorPatch{URL: &newURL}); err != nil {
		t.Fatal(err)
	}
	var openCount int
	if err := repo.pool.QueryRow(ctx, `SELECT count(*) FROM incidents WHERE monitor_id = $1 AND status = 'open'`, monitor.ID).Scan(&openCount); err != nil {
		t.Fatal(err)
	}
	if openCount != 0 {
		t.Fatalf("open incidents after semantic update = %d, want 0", openCount)
	}
	var oldStatus string
	var oldResolvedAt time.Time
	if err := repo.pool.QueryRow(ctx, `SELECT status, resolved_at FROM incidents WHERE id = $1`, oldIncidentID).Scan(&oldStatus, &oldResolvedAt); err != nil {
		t.Fatal(err)
	}
	if oldStatus != incidentStatusResolved || oldResolvedAt.IsZero() {
		t.Fatalf("old incident = status:%s resolved_at:%s, want resolved", oldStatus, oldResolvedAt)
	}
	stats, err := repo.Stats(ctx, monitor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ConsecutiveFailure != 0 || !stats.LastCheckedAt.IsZero() || stats.LastStatusCode != 0 {
		t.Fatalf("stats after semantic update = %+v, want reset current state", stats)
	}

	processPostgresManualCheck(t, ctx, repo, monitor.ID, CheckRecord{
		ID:         "semantic_new_target_failure",
		MonitorID:  monitor.ID,
		StatusCode: 500,
		LatencyMS:  5,
		Error:      "unexpected status code 500",
		Success:    false,
		CheckedAt:  time.Now().UTC(),
	}, alertPolicy)
	var newIncidentID string
	var newFailureCount int
	if err := repo.pool.QueryRow(ctx, `
		SELECT id, failure_count
		FROM incidents
		WHERE monitor_id = $1 AND status = 'open'
	`, monitor.ID).Scan(&newIncidentID, &newFailureCount); err != nil {
		t.Fatal(err)
	}
	if newIncidentID == oldIncidentID || newFailureCount != 1 {
		t.Fatalf("new incident = id:%s failures:%d, old=%s", newIncidentID, newFailureCount, oldIncidentID)
	}
	var alertCount int
	if err := repo.pool.QueryRow(ctx, `SELECT count(*) FROM alert_outbox WHERE monitor_id = $1`, monitor.ID).Scan(&alertCount); err != nil {
		t.Fatal(err)
	}
	if alertCount != 0 {
		t.Fatalf("alerts after first new-target failure = %d, want 0", alertCount)
	}

	shorter, err := repo.Create(ctx, MonitorInput{
		URL:             "https://interval-shorter.example.com",
		IntervalSeconds: 86400,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := repo.pool.Exec(ctx, `
		UPDATE monitors
		SET last_checked_at = clock_timestamp() - interval '31 seconds',
			next_check_at = clock_timestamp() + interval '24 hours'
		WHERE id = $1
	`, shorter.ID); err != nil {
		t.Fatal(err)
	}
	shortInterval := 30
	shorter, err = repo.Update(ctx, shorter.ID, MonitorPatch{IntervalSeconds: &shortInterval})
	if err != nil {
		t.Fatal(err)
	}
	dbNow, err := databaseTime(ctx, repo.pool)
	if err != nil {
		t.Fatal(err)
	}
	if shorter.NextCheckAt.After(dbNow) {
		t.Fatalf("shortened interval next_check_at = %s, want due by %s", shorter.NextCheckAt, dbNow)
	}
	claimed, err := repo.ClaimDueJobs(ctx, 100, time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if findClaimedJobID(claimed, shorter.ID) == "" {
		t.Fatalf("shortened interval monitor was not claimed: %+v", claimed)
	}

	longer, err := repo.Create(ctx, MonitorInput{
		URL:             "https://interval-longer.example.com",
		IntervalSeconds: 30,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	var lastCheckedAt time.Time
	if err := repo.pool.QueryRow(ctx, `
		UPDATE monitors
		SET last_checked_at = clock_timestamp(),
			next_check_at = clock_timestamp() + interval '30 seconds'
		WHERE id = $1
		RETURNING last_checked_at
	`, longer.ID).Scan(&lastCheckedAt); err != nil {
		t.Fatal(err)
	}
	longInterval := 86400
	longer, err = repo.Update(ctx, longer.ID, MonitorPatch{IntervalSeconds: &longInterval})
	if err != nil {
		t.Fatal(err)
	}
	if want := lastCheckedAt.Add(24 * time.Hour); !longer.NextCheckAt.Equal(want) {
		t.Fatalf("lengthened interval next_check_at = %s, want %s", longer.NextCheckAt, want)
	}
}

func verifyPostgresRetentionDeletesOnlyExpiredTerminalData(
	t *testing.T,
	ctx context.Context,
	repo *PostgresMonitorRepository,
) {
	t.Helper()
	monitor, err := repo.Create(ctx, MonitorInput{
		URL:             "https://retention.example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	old := now.Add(-48 * time.Hour)

	if _, err := repo.pool.Exec(ctx, `
		INSERT INTO check_jobs (
			id, monitor_id, scheduled_for, status, attempt, available_at,
			completed_at, last_error, created_at, updated_at
		)
		VALUES
			('ret_job_old', $1, $2, 'completed', 1, $2, $2, '', $2, $2),
			('ret_job_fresh', $1, $3, 'completed', 1, $3, $3, '', $3, $3),
			('ret_job_active', $1, $2, 'scheduled', 0, $2, NULL, '', $2, $2)
	`, monitor.ID, old, now); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.pool.Exec(ctx, `
		INSERT INTO check_results (
			id, job_id, monitor_id, status_code, latency_ms, error, success, checked_at, recorded_at
		)
		VALUES
			('ret_result_old', 'ret_job_old', $1, 200, 10, '', true, $2, $2),
			('ret_result_fresh', 'ret_job_fresh', $1, 200, 10, '', true, $3, $3)
	`, monitor.ID, old, now); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.pool.Exec(ctx, `
		INSERT INTO incidents (
			id, monitor_id, status, failure_count, first_failure_at, last_failure_at,
			resolved_at, last_error, created_at, updated_at
		)
		VALUES
			('ret_incident_old', $1, 'resolved', 1, $2, $2, $2, 'old', $2, $2),
			('ret_incident_active_outbox', $1, 'resolved', 1, $2, $2, $2, 'old', $2, $2),
			('ret_incident_fresh', $1, 'resolved', 1, $3, $3, $3, 'fresh', $3, $3)
	`, monitor.ID, old, now); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.pool.Exec(ctx, `
		INSERT INTO alert_outbox (
			id, idempotency_key, incident_id, monitor_id, event_type, payload,
			status, available_at, delivered_at, created_at, updated_at
		)
		VALUES
			('ret_alert_old', 'ret-alert-old', 'ret_incident_old', $1, 'incident.failure', '{}'::jsonb,
				'delivered', $2, $2, $2, $2),
			('ret_alert_active', 'ret-alert-active', 'ret_incident_active_outbox', $1, 'incident.failure', '{}'::jsonb,
				'pending', $2, NULL, $2, $2),
			('ret_alert_fresh', 'ret-alert-fresh', 'ret_incident_fresh', $1, 'incident.failure', '{}'::jsonb,
				'delivered', $3, $3, $3, $3)
	`, monitor.ID, old, now); err != nil {
		t.Fatal(err)
	}

	result, err := repo.DeleteExpiredData(ctx, now, RetentionPolicy{
		CheckResults:     24 * time.Hour,
		CheckJobs:        24 * time.Hour,
		AlertOutbox:      24 * time.Hour,
		ResolvedIncident: 24 * time.Hour,
		BatchSize:        100,
	})
	if err != nil {
		t.Fatal(err)
	}
	if result != (RetentionResult{CheckResults: 1, CheckJobs: 1, AlertOutbox: 1, ResolvedIncidents: 1}) {
		t.Fatalf("retention result = %+v", result)
	}

	assertPostgresRecordExists(t, ctx, repo, "SELECT EXISTS (SELECT 1 FROM check_results WHERE id = $1)", "ret_result_old", false)
	assertPostgresRecordExists(t, ctx, repo, "SELECT EXISTS (SELECT 1 FROM check_results WHERE id = $1)", "ret_result_fresh", true)
	assertPostgresRecordExists(t, ctx, repo, "SELECT EXISTS (SELECT 1 FROM check_jobs WHERE id = $1)", "ret_job_old", false)
	assertPostgresRecordExists(t, ctx, repo, "SELECT EXISTS (SELECT 1 FROM check_jobs WHERE id = $1)", "ret_job_fresh", true)
	assertPostgresRecordExists(t, ctx, repo, "SELECT EXISTS (SELECT 1 FROM check_jobs WHERE id = $1)", "ret_job_active", true)
	assertPostgresRecordExists(t, ctx, repo, "SELECT EXISTS (SELECT 1 FROM alert_outbox WHERE id = $1)", "ret_alert_old", false)
	assertPostgresRecordExists(t, ctx, repo, "SELECT EXISTS (SELECT 1 FROM alert_outbox WHERE id = $1)", "ret_alert_active", true)
	assertPostgresRecordExists(t, ctx, repo, "SELECT EXISTS (SELECT 1 FROM alert_outbox WHERE id = $1)", "ret_alert_fresh", true)
	assertPostgresRecordExists(t, ctx, repo, "SELECT EXISTS (SELECT 1 FROM incidents WHERE id = $1)", "ret_incident_old", false)
	assertPostgresRecordExists(t, ctx, repo, "SELECT EXISTS (SELECT 1 FROM incidents WHERE id = $1)", "ret_incident_active_outbox", true)
	assertPostgresRecordExists(t, ctx, repo, "SELECT EXISTS (SELECT 1 FROM incidents WHERE id = $1)", "ret_incident_fresh", true)
}

func assertPostgresRecordExists(
	t *testing.T,
	ctx context.Context,
	repo *PostgresMonitorRepository,
	query string,
	id string,
	want bool,
) {
	t.Helper()
	var exists bool
	if err := repo.pool.QueryRow(ctx, query, id).Scan(&exists); err != nil {
		t.Fatal(err)
	}
	if exists != want {
		t.Fatalf("record %s exists = %t, want %t", id, exists, want)
	}
}

func processPostgresManualCheck(
	t *testing.T,
	ctx context.Context,
	repo *PostgresMonitorRepository,
	monitorID string,
	record CheckRecord,
	alertPolicy AlertPolicy,
) Monitor {
	t.Helper()
	job, created, err := repo.CreateManualJob(ctx, monitorID)
	if err != nil {
		t.Fatal(err)
	}
	if !created || job.Kind != checkJobKindManual {
		t.Fatalf("manual job = %+v created=%t", job, created)
	}
	if err := repo.MarkJobPublished(ctx, monitorID, job.ID); err != nil {
		t.Fatal(err)
	}
	lease, err := repo.MarkJobProcessing(ctx, monitorID, job.ID, 1, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	record.JobID = job.ID
	monitor, err := repo.AddCheck(ctx, record, lease, alertPolicy)
	if err != nil {
		t.Fatal(err)
	}
	return monitor
}

func findClaimedJobID(jobs []CheckJobRecord, monitorID string) string {
	for _, job := range jobs {
		if job.MonitorID == monitorID {
			return job.ID
		}
	}
	return ""
}

func TestPostgresOperationAndMigrationTimeouts(t *testing.T) {
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
	repo := NewPostgresMonitorRepositoryWithTimeout(pool, NewNetworkPolicy(cfg), 100*time.Millisecond)
	monitor, err := repo.Create(ctx, MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	lockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := lockTx.Exec(ctx, "SELECT id FROM monitors WHERE id = $1 FOR UPDATE", monitor.ID); err != nil {
		_ = lockTx.Rollback(ctx)
		t.Fatal(err)
	}
	started := time.Now()
	_, err = repo.Update(context.Background(), monitor.ID, MonitorPatch{})
	if !errors.Is(err, context.DeadlineExceeded) {
		_ = lockTx.Rollback(ctx)
		t.Fatalf("locked update error = %v, want context deadline exceeded", err)
	}
	if elapsed := time.Since(started); elapsed > time.Second {
		_ = lockTx.Rollback(ctx)
		t.Fatalf("locked update returned after %s, want bounded operation timeout", elapsed)
	}
	if err := lockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.Update(ctx, monitor.ID, MonitorPatch{}); err != nil {
		t.Fatalf("update after lock release failed: %v", err)
	}

	migrationLockTx, err := pool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := migrationLockTx.Exec(ctx, "SELECT pg_advisory_xact_lock(hashtext('site_checker_migrations'))"); err != nil {
		_ = migrationLockTx.Rollback(ctx)
		t.Fatal(err)
	}
	err = RunMigrationsWithTimeout(context.Background(), pool, 100*time.Millisecond)
	if !errors.Is(err, context.DeadlineExceeded) {
		_ = migrationLockTx.Rollback(ctx)
		t.Fatalf("migration lock error = %v, want context deadline exceeded", err)
	}
	if err := migrationLockTx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	if err := RunMigrationsWithTimeout(ctx, pool, time.Second); err != nil {
		t.Fatalf("migration after lock release failed: %v", err)
	}
}
