//go:build integration

package sitechecker

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
	claimed, err := repo.ClaimDueJobs(ctx, 10, now, time.Minute, defaultMaxJobAttempts)
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
	if err := repo.MarkJobPublished(ctx, monitor.ID, jobID, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	staleLease, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 1, now.Add(10*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	secondClaim, err := repo.ClaimDueJobs(ctx, 10, now.Add(30*time.Second), time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondClaim) != 0 {
		t.Fatalf("second claim len = %d, want 0 before lease timeout", len(secondClaim))
	}
	if _, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 1, now.Add(30*time.Second), time.Minute); !errors.Is(err, ErrJobAlreadyProcessing) {
		t.Fatalf("second MarkJobProcessing error = %v, want ErrJobAlreadyProcessing", err)
	}

	reclaimed, err := repo.ClaimDueJobs(ctx, 10, now.Add(2*time.Minute), time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if len(reclaimed) != 1 || reclaimed[0].ID != jobID || reclaimed[0].Status != checkJobStatusScheduled || reclaimed[0].Attempt != 1 {
		t.Fatalf("reclaimed = %+v, want persisted stale job", reclaimed)
	}
	if _, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 1, now.Add(2*time.Minute+time.Second), time.Minute); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("old delivery error = %v, want ErrStaleJob", err)
	}
	if err := repo.MarkJobPublished(ctx, monitor.ID, jobID, now.Add(2*time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}
	retryLease, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 2, now.Add(2*time.Minute+2*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkJobFailed(ctx, staleLease, "stale worker failure", now, now); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("stale worker MarkJobFailed error = %v, want ErrStaleJob", err)
	}
	jobRetryAt := now.Add(2*time.Minute + 10*time.Second)
	if err := repo.MarkJobFailed(ctx, retryLease, "temporary storage failure", now.Add(2*time.Minute+3*time.Second), jobRetryAt); err != nil {
		t.Fatal(err)
	}
	beforeRetry, err := repo.ClaimDueJobs(ctx, 10, jobRetryAt.Add(-time.Millisecond), time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if len(beforeRetry) != 0 {
		t.Fatalf("jobs before retry = %+v, want none", beforeRetry)
	}
	retryJobs, err := repo.ClaimDueJobs(ctx, 10, jobRetryAt, time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	if len(retryJobs) != 1 || retryJobs[0].ID != jobID || retryJobs[0].Attempt != 2 {
		t.Fatalf("retry jobs = %+v, want attempt 2 job", retryJobs)
	}
	if err := repo.MarkJobPublished(ctx, monitor.ID, jobID, jobRetryAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	finalLease, err := repo.MarkJobProcessing(ctx, monitor.ID, jobID, 3, jobRetryAt.Add(2*time.Second), time.Minute)
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
	record.CheckedAt = time.Now().UTC()
	processPostgresManualCheck(t, ctx, repo, monitor.ID, record, alertPolicy)
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

	manualClaimed, err := repo.ClaimDueJobs(ctx, 10, manualNow.Add(30*time.Second), time.Minute, defaultMaxJobAttempts)
	if err != nil {
		t.Fatal(err)
	}
	manualScheduledJobID := findClaimedJobID(manualClaimed, manualLeaseMonitor.ID)
	if manualScheduledJobID == "" {
		t.Fatalf("manual monitor periodic job was not claimed: %+v", manualClaimed)
	}
	if err := repo.MarkJobPublished(ctx, manualLeaseMonitor.ID, manualScheduledJobID, manualNow.Add(31*time.Second)); err != nil {
		t.Fatal(err)
	}
	manualScheduledLease, err := repo.MarkJobProcessing(ctx, manualLeaseMonitor.ID, manualScheduledJobID, 1, manualNow.Add(32*time.Second), time.Minute)
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
	failNow := time.Now().UTC()
	failClaimed, err := repo.ClaimDueJobs(ctx, 10, failNow, time.Minute, defaultMaxJobAttempts)
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
	if err := repo.MarkJobPublished(ctx, failProcessingMonitor.ID, failJobID, failNow.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	failLease, err := repo.MarkJobProcessing(ctx, failProcessingMonitor.ID, failJobID, 1, failNow.Add(10*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	updatedAt := failNow.Add(20 * time.Second)
	nextCheckAt := updatedAt.Add(5 * time.Minute)
	if err := repo.MarkJobDead(ctx, failLease, "result persistence failed", updatedAt, nextCheckAt); err != nil {
		t.Fatal(err)
	}
	afterFailProcessing, err := repo.Get(ctx, failProcessingMonitor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !afterFailProcessing.UpdatedAt.Equal(updatedAt.Truncate(time.Microsecond)) {
		t.Fatalf("updated_at = %s, want %s", afterFailProcessing.UpdatedAt, updatedAt)
	}
	if !afterFailProcessing.NextCheckAt.Equal(nextCheckAt.Truncate(time.Microsecond)) {
		t.Fatalf("next_check_at = %s, want %s", afterFailProcessing.NextCheckAt, nextCheckAt)
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
	attemptNow := time.Now().UTC()
	attemptJobs, err := repo.ClaimDueJobs(ctx, 100, attemptNow, time.Minute, 3)
	if err != nil {
		t.Fatal(err)
	}
	attemptJobID := findClaimedJobID(attemptJobs, attemptLimitMonitor.ID)
	if attemptJobID == "" {
		t.Fatalf("attempt-limit monitor was not claimed: %+v", attemptJobs)
	}
	if err := repo.MarkJobPublished(ctx, attemptLimitMonitor.ID, attemptJobID, attemptNow); err != nil {
		t.Fatal(err)
	}
	if _, err := repo.MarkJobProcessing(ctx, attemptLimitMonitor.ID, attemptJobID, 3, attemptNow, time.Minute); err != nil {
		t.Fatal(err)
	}

	recoveredAt := attemptNow.Add(2 * time.Minute)
	recoveredJobs, err := repo.ClaimDueJobs(ctx, 100, recoveredAt, time.Minute, 3)
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
	wantNextCheckAt := recoveredAt.Add(time.Minute)
	if !updatedAttemptMonitor.NextCheckAt.Equal(wantNextCheckAt.Truncate(time.Microsecond)) {
		t.Fatalf("next_check_at = %s, want %s", updatedAttemptMonitor.NextCheckAt, wantNextCheckAt)
	}
	nextPeriodicJobs, err := repo.ClaimDueJobs(ctx, 100, wantNextCheckAt, time.Minute, 3)
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
	now := record.CheckedAt.Add(-time.Second)
	job, created, err := repo.CreateManualJob(ctx, monitorID, now)
	if err != nil {
		t.Fatal(err)
	}
	if !created || job.Kind != checkJobKindManual {
		t.Fatalf("manual job = %+v created=%t", job, created)
	}
	if err := repo.MarkJobPublished(ctx, monitorID, job.ID, now); err != nil {
		t.Fatal(err)
	}
	lease, err := repo.MarkJobProcessing(ctx, monitorID, job.ID, 1, now, time.Minute)
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
