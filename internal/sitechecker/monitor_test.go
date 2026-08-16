package sitechecker

import (
	"errors"
	"fmt"
	"testing"
	"time"
)

func TestMonitorStoreClaimDueAvoidsDuplicates(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	first := store.ClaimDueJobs(10, time.Now().UTC(), time.Minute)
	if len(first) != 1 {
		t.Fatalf("first claim len = %d, want 1", len(first))
	}
	second := store.ClaimDueJobs(10, time.Now().UTC(), time.Minute)
	if len(second) != 0 {
		t.Fatalf("second claim len = %d, want 0", len(second))
	}

	if err := store.MarkJobPublished(monitor.ID, first[0].ID, time.Now().UTC()); err != nil {
		t.Fatal(err)
	}
	lease, err := store.MarkJobProcessing(monitor.ID, first[0].ID, 1, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	_, err = store.AddCheck(CheckRecord{
		ID:        "check_1",
		JobID:     first[0].ID,
		MonitorID: monitor.ID,
		Success:   true,
		CheckedAt: time.Now().UTC(),
	}, lease)
	if err != nil {
		t.Fatal(err)
	}
}

func TestMonitorStoreRejectsResultAfterCheckConfigurationChanges(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	jobs := store.ClaimDueJobs(1, now, time.Minute)
	if len(jobs) != 1 {
		t.Fatalf("claimed jobs = %d, want 1", len(jobs))
	}
	if err := store.MarkJobPublished(monitor.ID, jobs[0].ID, now); err != nil {
		t.Fatal(err)
	}
	lease, err := store.MarkJobProcessing(monitor.ID, jobs[0].ID, 1, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	newURL := "https://example.org"
	updated, err := store.Update(monitor.ID, MonitorPatch{URL: &newURL})
	if err != nil {
		t.Fatal(err)
	}
	if updated.URL != newURL || updated.NextCheckAt.Before(now) {
		t.Fatalf("updated monitor = %+v", updated)
	}

	_, err = store.AddCheck(CheckRecord{
		ID:         "check_stale_configuration",
		JobID:      jobs[0].ID,
		MonitorID:  monitor.ID,
		StatusCode: 500,
		Success:    false,
		CheckedAt:  time.Now().UTC(),
	}, lease)
	if !errors.Is(err, ErrStaleJob) {
		t.Fatalf("AddCheck error = %v, want ErrStaleJob", err)
	}
	checks, total, err := store.ListChecks(monitor.ID, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if total != 0 || len(checks) != 0 {
		t.Fatalf("checks = %+v total=%d, want none", checks, total)
	}
}

func TestMonitorStoreResolvesIncidentWhenCheckSemanticsChange(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))
	monitor, err := store.Create(MonitorInput{
		URL:             "https://a.example",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	for index := range 2 {
		addStoreManualCheck(t, store, monitor.ID, CheckRecord{
			ID:         fmt.Sprintf("semantic_failure_%d", index),
			MonitorID:  monitor.ID,
			StatusCode: 500,
			Error:      "unexpected status code 500",
			Success:    false,
			CheckedAt:  time.Now().UTC(),
		})
	}
	open, total := store.ListIncidents(incidentStatusOpen, 0, 10)
	if total != 1 || len(open) != 1 || open[0].FailureCount != 2 {
		t.Fatalf("open incidents before update = %+v total=%d", open, total)
	}
	oldIncidentID := open[0].ID

	newURL := "https://b.example"
	if _, err := store.Update(monitor.ID, MonitorPatch{URL: &newURL}); err != nil {
		t.Fatal(err)
	}
	if open, total = store.ListIncidents(incidentStatusOpen, 0, 10); total != 0 || len(open) != 0 {
		t.Fatalf("open incidents after semantic update = %+v total=%d, want none", open, total)
	}
	stats, err := store.Stats(monitor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ConsecutiveFailure != 0 {
		t.Fatalf("consecutive failures after semantic update = %d, want 0", stats.ConsecutiveFailure)
	}

	addStoreManualCheck(t, store, monitor.ID, CheckRecord{
		ID:         "new_semantic_failure",
		MonitorID:  monitor.ID,
		StatusCode: 500,
		Error:      "unexpected status code 500",
		Success:    false,
		CheckedAt:  time.Now().UTC(),
	})
	open, total = store.ListIncidents(incidentStatusOpen, 0, 10)
	if total != 1 || len(open) != 1 || open[0].ID == oldIncidentID || open[0].FailureCount != 1 {
		t.Fatalf("open incident after new-target failure = %+v total=%d", open, total)
	}
}

func TestMonitorStoreRecalculatesScheduleWhenIntervalChanges(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))
	monitor, err := store.Create(MonitorInput{
		URL:             "https://interval-shorter.example",
		IntervalSeconds: 86400,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	store.mu.Lock()
	stored := store.byID[monitor.ID]
	stored.LastCheckedAt = now.Add(-time.Minute)
	stored.NextCheckAt = now.Add(24 * time.Hour)
	store.byID[monitor.ID] = stored
	store.mu.Unlock()

	shortInterval := 30
	updated, err := store.Update(monitor.ID, MonitorPatch{IntervalSeconds: &shortInterval})
	if err != nil {
		t.Fatal(err)
	}
	if updated.NextCheckAt.After(time.Now().UTC().Add(time.Second)) {
		t.Fatalf("shortened interval next_check_at = %s, want due now", updated.NextCheckAt)
	}
	jobs := store.ClaimDueJobs(10, time.Now().UTC().Add(time.Second), time.Minute)
	found := false
	for _, job := range jobs {
		if job.MonitorID == monitor.ID {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("shortened interval did not produce a due job: %+v", jobs)
	}

	longMonitor, err := store.Create(MonitorInput{
		URL:             "https://interval-longer.example",
		IntervalSeconds: 30,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}
	lastCheckedAt := time.Now().UTC()
	store.mu.Lock()
	stored = store.byID[longMonitor.ID]
	stored.LastCheckedAt = lastCheckedAt
	stored.NextCheckAt = lastCheckedAt.Add(30 * time.Second)
	store.byID[longMonitor.ID] = stored
	store.mu.Unlock()

	longInterval := 86400
	updated, err = store.Update(longMonitor.ID, MonitorPatch{IntervalSeconds: &longInterval})
	if err != nil {
		t.Fatal(err)
	}
	if want := lastCheckedAt.Add(24 * time.Hour); !updated.NextCheckAt.Equal(want) {
		t.Fatalf("lengthened interval next_check_at = %s, want %s", updated.NextCheckAt, want)
	}
}

func addStoreManualCheck(t *testing.T, store *MonitorStore, monitorID string, record CheckRecord) {
	t.Helper()
	now := time.Now().UTC()
	job, created, err := store.CreateManualJob(monitorID, now)
	if err != nil {
		t.Fatal(err)
	}
	if !created {
		t.Fatal("manual job was unexpectedly deduplicated")
	}
	if err := store.MarkJobPublished(monitorID, job.ID, now); err != nil {
		t.Fatal(err)
	}
	lease, err := store.MarkJobProcessing(monitorID, job.ID, 1, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	record.JobID = job.ID
	if _, err := store.AddCheck(record, lease); err != nil {
		t.Fatal(err)
	}
}

func TestMonitorStoreReclaimsStaleScheduledJob(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	first := store.ClaimDueJobs(10, now, time.Minute)
	if len(first) != 1 || first[0].MonitorID != monitor.ID {
		t.Fatalf("first claim = %+v, want monitor job %s", first, monitor.ID)
	}

	second := store.ClaimDueJobs(10, now.Add(30*time.Second), time.Minute)
	if len(second) != 0 {
		t.Fatalf("second claim len = %d, want 0 before lease timeout", len(second))
	}

	third := store.ClaimDueJobs(10, now.Add(2*time.Minute), time.Minute)
	if len(third) != 1 || third[0].ID != first[0].ID {
		t.Fatalf("third claim = %+v, want reclaimed job %s", third, first[0].ID)
	}
}

func TestMonitorStoreRecoversQueuedJobOnlyAfterTopologyLoss(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	leaseTimeout := time.Minute
	claimed := store.ClaimDueJobs(1, now, leaseTimeout)
	if len(claimed) != 1 {
		t.Fatalf("claimed = %+v, want one job", claimed)
	}
	if err := store.MarkJobPublished(monitor.ID, claimed[0].ID, now); err != nil {
		t.Fatal(err)
	}

	if replayed := store.ClaimDueJobs(1, now.Add(5*leaseTimeout), leaseTimeout); len(replayed) != 0 {
		t.Fatalf("time-based queued replay = %+v, want none", replayed)
	}
	recoveryTime := now.Add(5 * leaseTimeout)
	if recovered := store.RecoverQueuedJobsAfterTopologyLoss(recoveryTime); recovered != 1 {
		t.Fatalf("recovered queued jobs = %d, want 1", recovered)
	}
	reclaimed := store.ClaimDueJobs(1, recoveryTime, leaseTimeout)
	if len(reclaimed) != 1 || reclaimed[0].ID != claimed[0].ID {
		t.Fatalf("reclaimed = %+v, want queued job %s", reclaimed, claimed[0].ID)
	}
	if reclaimed[0].LastError != "rabbitmq queue topology was lost" {
		t.Fatalf("last error = %q, want RabbitMQ topology loss", reclaimed[0].LastError)
	}
}

func TestMonitorStoreAllowsOnlyOneWorkerToProcessActiveJob(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	claimed := store.ClaimDueJobs(10, now, time.Minute)
	if len(claimed) != 1 || claimed[0].MonitorID != monitor.ID {
		t.Fatalf("claimed = %+v, want monitor job %s", claimed, monitor.ID)
	}
	jobID := claimed[0].ID

	if err := store.MarkJobPublished(monitor.ID, jobID, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkJobProcessing(monitor.ID, jobID, 1, now.Add(10*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkJobProcessing(monitor.ID, jobID, 1, now.Add(20*time.Second), time.Minute); !errors.Is(err, ErrJobAlreadyProcessing) {
		t.Fatalf("second MarkJobProcessing error = %v, want ErrJobAlreadyProcessing", err)
	}

	reclaimed := store.ClaimDueJobs(10, now.Add(2*time.Minute), time.Minute)
	if len(reclaimed) != 1 || reclaimed[0].ID != jobID {
		t.Fatalf("reclaimed = %+v, want stale processing job %s", reclaimed, jobID)
	}
	if err := store.MarkJobPublished(monitor.ID, jobID, now.Add(2*time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkJobProcessing(monitor.ID, jobID, 2, now.Add(2*time.Minute+2*time.Second), time.Minute); err != nil {
		t.Fatalf("reclaimed MarkJobProcessing error = %v", err)
	}
}

func TestMonitorStorePersistsCheckJobLifecycle(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	jobs := store.ClaimDueJobs(1, now, time.Minute)
	if len(jobs) != 1 {
		t.Fatalf("claimed jobs = %d, want 1", len(jobs))
	}
	job := jobs[0]
	if job.Status != checkJobStatusScheduled || job.Attempt != 0 {
		t.Fatalf("claimed job = %+v, want scheduled attempt 0", job)
	}

	if err := store.MarkJobPublished(monitor.ID, job.ID, now.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	firstLease, err := store.MarkJobProcessing(monitor.ID, job.ID, 1, now.Add(2*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	retryAt := now.Add(5 * time.Second)
	if err := store.MarkJobFailed(firstLease, "temporary storage failure", now.Add(3*time.Second), retryAt); err != nil {
		t.Fatal(err)
	}
	if early := store.ClaimDueJobs(1, retryAt.Add(-time.Millisecond), time.Minute); len(early) != 0 {
		t.Fatalf("jobs before retry = %+v, want none", early)
	}

	retryJobs := store.ClaimDueJobs(1, retryAt, time.Minute)
	if len(retryJobs) != 1 || retryJobs[0].Status != checkJobStatusScheduled || retryJobs[0].Attempt != 1 {
		t.Fatalf("retry jobs = %+v, want scheduled attempt 1", retryJobs)
	}
	if err := store.MarkJobPublished(monitor.ID, job.ID, retryAt.Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkJobProcessing(monitor.ID, job.ID, 1, retryAt.Add(2*time.Second), time.Minute); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("old attempt error = %v, want ErrStaleJob", err)
	}
	secondLease, err := store.MarkJobProcessing(monitor.ID, job.ID, 2, retryAt.Add(2*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	record := CheckRecord{
		ID:         newID("check"),
		JobID:      job.ID,
		MonitorID:  monitor.ID,
		StatusCode: 200,
		LatencyMS:  12,
		Success:    true,
		CheckedAt:  retryAt.Add(3 * time.Second),
	}
	if _, err := store.AddCheck(record, secondLease); err != nil {
		t.Fatal(err)
	}
	completed, err := store.CheckJob(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if completed.Status != checkJobStatusCompleted || completed.Attempt != 2 || completed.CompletedAt.IsZero() {
		t.Fatalf("completed job = %+v", completed)
	}
}

func TestMonitorStoreRejectsStaleWorkerLease(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	jobs := store.ClaimDueJobs(1, now, time.Minute)
	if len(jobs) != 1 {
		t.Fatalf("claimed jobs = %d, want 1", len(jobs))
	}
	if err := store.MarkJobPublished(monitor.ID, jobs[0].ID, now); err != nil {
		t.Fatal(err)
	}
	staleLease, err := store.MarkJobProcessing(monitor.ID, jobs[0].ID, 1, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	reclaimed := store.ClaimDueJobs(1, now.Add(2*time.Minute), time.Minute)
	if len(reclaimed) != 1 || reclaimed[0].ID != jobs[0].ID {
		t.Fatalf("reclaimed jobs = %+v", reclaimed)
	}
	if err := store.MarkJobPublished(monitor.ID, jobs[0].ID, now.Add(2*time.Minute)); err != nil {
		t.Fatal(err)
	}
	currentLease, err := store.MarkJobProcessing(monitor.ID, jobs[0].ID, 2, now.Add(2*time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if currentLease.LeaseToken == staleLease.LeaseToken {
		t.Fatal("processing lease token was reused")
	}

	record := CheckRecord{
		ID:         newID("check"),
		JobID:      jobs[0].ID,
		MonitorID:  monitor.ID,
		StatusCode: 200,
		Success:    true,
		CheckedAt:  now.Add(2*time.Minute + time.Second),
	}
	if _, err := store.AddCheck(record, staleLease); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("stale worker AddCheck error = %v, want ErrStaleJob", err)
	}
	if err := store.MarkJobFailed(staleLease, "stale failure", now, now); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("stale worker MarkJobFailed error = %v, want ErrStaleJob", err)
	}
	if err := store.MarkJobDead(staleLease, "stale dead-letter", now, now); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("stale worker MarkJobDead error = %v, want ErrStaleJob", err)
	}
	if _, err := store.AddCheck(record, currentLease); err != nil {
		t.Fatalf("current worker AddCheck error = %v", err)
	}
}

func TestMonitorStoreTerminatesExpiredFinalAttempt(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://attempt-limit.example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	jobs := store.ClaimDueJobsWithMaxAttempts(1, now, time.Minute, 3)
	if len(jobs) != 1 {
		t.Fatalf("claimed jobs = %+v, want one", jobs)
	}
	jobID := jobs[0].ID
	if err := store.MarkJobPublished(monitor.ID, jobID, now); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkJobProcessing(monitor.ID, jobID, 3, now, time.Minute); err != nil {
		t.Fatal(err)
	}

	recoveredAt := now.Add(2 * time.Minute)
	if recovered := store.ClaimDueJobsWithMaxAttempts(1, recoveredAt, time.Minute, 3); len(recovered) != 0 {
		t.Fatalf("recovered jobs = %+v, want no attempt 4", recovered)
	}
	dead, err := store.CheckJob(jobID)
	if err != nil {
		t.Fatal(err)
	}
	if dead.Status != checkJobStatusDead || dead.Attempt != 3 || dead.CompletedAt.IsZero() {
		t.Fatalf("terminal job = %+v, want dead attempt 3", dead)
	}

	updated, err := store.Get(monitor.ID)
	if err != nil {
		t.Fatal(err)
	}
	wantNextCheckAt := recoveredAt.Add(time.Minute)
	if !updated.NextCheckAt.Equal(wantNextCheckAt) {
		t.Fatalf("next_check_at = %s, want %s", updated.NextCheckAt, wantNextCheckAt)
	}
	next := store.ClaimDueJobsWithMaxAttempts(1, wantNextCheckAt, time.Minute, 3)
	if len(next) != 1 || next[0].ID == jobID || next[0].Attempt != 0 {
		t.Fatalf("next periodic jobs = %+v, want a new attempt 0 job", next)
	}
}

func TestMonitorStoreManualCheckUsesPipelineAndPreservesPeriodicSchedule(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	claimed := store.ClaimDueJobs(10, now, time.Minute)
	if len(claimed) != 1 || claimed[0].MonitorID != monitor.ID {
		t.Fatalf("claimed = %+v, want monitor job %s", claimed, monitor.ID)
	}
	jobID := claimed[0].ID
	if err := store.MarkJobPublished(monitor.ID, jobID, now.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	scheduledLease, err := store.MarkJobProcessing(monitor.ID, jobID, 1, now.Add(10*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	deduplicated, created, err := store.CreateManualJob(monitor.ID, now.Add(20*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if created || deduplicated.ID != jobID {
		t.Fatalf("manual request during active job = %+v created=%t, want existing job %s", deduplicated, created, jobID)
	}

	updated, err := store.AddCheck(CheckRecord{
		ID:         newID("check"),
		JobID:      jobID,
		MonitorID:  monitor.ID,
		StatusCode: 200,
		LatencyMS:  25,
		Success:    true,
		CheckedAt:  now.Add(20 * time.Second),
	}, scheduledLease)
	if err != nil {
		t.Fatal(err)
	}
	periodicNextCheckAt := updated.NextCheckAt

	manualJob, created, err := store.CreateManualJob(monitor.ID, now.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	if !created || manualJob.Kind != checkJobKindManual {
		t.Fatalf("manual job = %+v created=%t", manualJob, created)
	}
	if err := store.MarkJobPublished(monitor.ID, manualJob.ID, now.Add(31*time.Second)); err != nil {
		t.Fatal(err)
	}
	manualLease, err := store.MarkJobProcessing(monitor.ID, manualJob.ID, 1, now.Add(32*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	updated, err = store.AddCheck(CheckRecord{
		ID:         newID("check"),
		JobID:      manualJob.ID,
		MonitorID:  monitor.ID,
		StatusCode: 200,
		LatencyMS:  30,
		Success:    true,
		CheckedAt:  now.Add(40 * time.Second),
	}, manualLease)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.NextCheckAt.Equal(periodicNextCheckAt) {
		t.Fatalf("manual check moved next_check_at to %s, want %s", updated.NextCheckAt, periodicNextCheckAt)
	}
}

func TestMonitorStoreFailProcessingUsesCurrentUpdatedAt(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 300,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	claimedAt := time.Now().UTC()
	claimed := store.ClaimDueJobs(10, claimedAt, time.Minute)
	if len(claimed) != 1 || claimed[0].MonitorID != monitor.ID {
		t.Fatalf("claimed = %+v, want monitor job %s", claimed, monitor.ID)
	}
	jobID := claimed[0].ID
	if err := store.MarkJobPublished(monitor.ID, jobID, claimedAt.Add(5*time.Second)); err != nil {
		t.Fatal(err)
	}
	lease, err := store.MarkJobProcessing(monitor.ID, jobID, 1, claimedAt.Add(10*time.Second), time.Minute)
	if err != nil {
		t.Fatal(err)
	}

	updatedAt := claimedAt.Add(20 * time.Second)
	nextCheckAt := updatedAt.Add(5 * time.Minute)
	if err := store.MarkJobDead(lease, "result persistence failed", updatedAt, nextCheckAt); err != nil {
		t.Fatal(err)
	}

	got, err := store.Get(monitor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !got.UpdatedAt.Equal(updatedAt) {
		t.Fatalf("updated_at = %s, want %s", got.UpdatedAt, updatedAt)
	}
	if !got.NextCheckAt.Equal(nextCheckAt) {
		t.Fatalf("next_check_at = %s, want %s", got.NextCheckAt, nextCheckAt)
	}
}

func TestMonitorStoreBoundsProcessedJobIDs(t *testing.T) {
	store := NewMonitorStore(nil)
	store.mu.Lock()
	for i := range maxProcessedJobIDs + 5 {
		store.rememberProcessedJobLocked(fmt.Sprintf("job_%d", i))
	}
	store.mu.Unlock()

	store.mu.RLock()
	if got := len(store.processedJobs); got != maxProcessedJobIDs {
		t.Fatalf("processedJobs len = %d, want %d", got, maxProcessedJobIDs)
	}
	if _, exists := store.processedJobs["job_0"]; exists {
		t.Fatal("oldest processed job was not evicted")
	}
	latest := fmt.Sprintf("job_%d", maxProcessedJobIDs+4)
	if _, exists := store.processedJobs[latest]; !exists {
		store.mu.RUnlock()
		t.Fatalf("latest processed job %q was evicted", latest)
	}
	store.mu.RUnlock()
}

func TestMonitorStoreBoundsTerminalCheckJobs(t *testing.T) {
	store := NewMonitorStore(nil)
	now := time.Now().UTC()

	store.mu.Lock()
	for index := 0; index < maxRetainedCheckJobs+5; index++ {
		jobID := fmt.Sprintf("job_%d", index)
		store.checkJobs[jobID] = CheckJobRecord{
			ID:          jobID,
			Status:      checkJobStatusCompleted,
			CompletedAt: now,
		}
		store.rememberTerminalJobLocked(jobID)
	}
	if got := len(store.checkJobs); got != maxRetainedCheckJobs {
		store.mu.Unlock()
		t.Fatalf("terminal check jobs = %d, want %d", got, maxRetainedCheckJobs)
	}
	_, oldestExists := store.checkJobs["job_0"]
	_, latestExists := store.checkJobs[fmt.Sprintf("job_%d", maxRetainedCheckJobs+4)]
	store.mu.Unlock()

	if oldestExists {
		t.Fatal("oldest terminal check job was not evicted")
	}
	if !latestExists {
		t.Fatal("latest terminal check job was evicted")
	}
}

func TestMonitorStoreStats(t *testing.T) {
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	store := NewMonitorStore(NewNetworkPolicy(cfg))

	monitor, err := store.Create(MonitorInput{
		URL:             "https://example.com",
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	now := time.Now().UTC()
	for _, record := range []CheckRecord{
		{ID: "check_1", MonitorID: monitor.ID, Success: true, LatencyMS: 100, CheckedAt: now},
		{ID: "check_2", MonitorID: monitor.ID, Success: false, LatencyMS: 200, CheckedAt: now.Add(time.Second)},
	} {
		job, _, err := store.CreateManualJob(monitor.ID, record.CheckedAt)
		if err != nil {
			t.Fatal(err)
		}
		if err := store.MarkJobPublished(monitor.ID, job.ID, record.CheckedAt); err != nil {
			t.Fatal(err)
		}
		lease, err := store.MarkJobProcessing(monitor.ID, job.ID, 1, record.CheckedAt, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		record.JobID = job.ID
		if _, err := store.AddCheck(record, lease); err != nil {
			t.Fatal(err)
		}
	}

	stats, err := store.Stats(monitor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if stats.ChecksTotal != 2 {
		t.Fatalf("ChecksTotal = %d, want 2", stats.ChecksTotal)
	}
	if stats.UptimePercent != 50 {
		t.Fatalf("UptimePercent = %f, want 50", stats.UptimePercent)
	}
	if stats.AverageLatencyMS != 150 {
		t.Fatalf("AverageLatencyMS = %f, want 150", stats.AverageLatencyMS)
	}
}
