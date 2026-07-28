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
	if err := store.MarkJobProcessing(monitor.ID, first[0].ID, 1, time.Now().UTC(), time.Minute); err != nil {
		t.Fatal(err)
	}
	_, err = store.AddCheck(CheckRecord{
		ID:        "check_1",
		JobID:     first[0].ID,
		MonitorID: monitor.ID,
		Success:   true,
		CheckedAt: time.Now().UTC(),
	})
	if err != nil {
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

func TestMonitorStoreReclaimsStaleQueuedJob(t *testing.T) {
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

	if early := store.ClaimDueJobs(1, now.Add(leaseTimeout-time.Millisecond), leaseTimeout); len(early) != 0 {
		t.Fatalf("early queued reclaim = %+v, want none", early)
	}
	reclaimed := store.ClaimDueJobs(1, now.Add(leaseTimeout), leaseTimeout)
	if len(reclaimed) != 1 || reclaimed[0].ID != claimed[0].ID {
		t.Fatalf("reclaimed = %+v, want queued job %s", reclaimed, claimed[0].ID)
	}
	if reclaimed[0].LastError != "queued delivery lease expired" {
		t.Fatalf("last error = %q, want queued delivery lease expiry", reclaimed[0].LastError)
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
	if err := store.MarkJobProcessing(monitor.ID, jobID, 1, now.Add(10*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkJobProcessing(monitor.ID, jobID, 1, now.Add(20*time.Second), time.Minute); !errors.Is(err, ErrJobAlreadyProcessing) {
		t.Fatalf("second MarkJobProcessing error = %v, want ErrJobAlreadyProcessing", err)
	}

	reclaimed := store.ClaimDueJobs(10, now.Add(2*time.Minute), time.Minute)
	if len(reclaimed) != 1 || reclaimed[0].ID != jobID {
		t.Fatalf("reclaimed = %+v, want stale processing job %s", reclaimed, jobID)
	}
	if err := store.MarkJobPublished(monitor.ID, jobID, now.Add(2*time.Minute+time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkJobProcessing(monitor.ID, jobID, 2, now.Add(2*time.Minute+2*time.Second), time.Minute); err != nil {
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
	if err := store.MarkJobProcessing(monitor.ID, job.ID, 1, now.Add(2*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}

	retryAt := now.Add(5 * time.Second)
	if err := store.MarkJobFailed(monitor.ID, job.ID, "temporary storage failure", now.Add(3*time.Second), retryAt); err != nil {
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
	if err := store.MarkJobProcessing(monitor.ID, job.ID, 1, retryAt.Add(2*time.Second), time.Minute); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("old attempt error = %v, want ErrStaleJob", err)
	}
	if err := store.MarkJobProcessing(monitor.ID, job.ID, 2, retryAt.Add(2*time.Second), time.Minute); err != nil {
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
	if _, err := store.AddCheck(record); err != nil {
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

func TestMonitorStoreManualCheckDoesNotClearScheduledLease(t *testing.T) {
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
	if err := store.MarkJobProcessing(monitor.ID, jobID, 1, now.Add(10*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}

	updated, err := store.AddManualCheck(CheckRecord{
		ID:         newID("check"),
		JobID:      newID("manual"),
		MonitorID:  monitor.ID,
		StatusCode: 200,
		LatencyMS:  25,
		Success:    true,
		CheckedAt:  now.Add(20 * time.Second),
	})
	if err != nil {
		t.Fatal(err)
	}
	if !updated.NextCheckAt.Equal(monitor.NextCheckAt) {
		t.Fatalf("manual check moved next_check_at to %s, want %s", updated.NextCheckAt, monitor.NextCheckAt)
	}
	if err := store.MarkJobProcessing(monitor.ID, jobID, 1, now.Add(30*time.Second), time.Minute); !errors.Is(err, ErrJobAlreadyProcessing) {
		t.Fatalf("manual check cleared scheduled lease, MarkJobProcessing error = %v", err)
	}

	_, err = store.AddCheck(CheckRecord{
		ID:         newID("check"),
		JobID:      jobID,
		MonitorID:  monitor.ID,
		StatusCode: 200,
		LatencyMS:  30,
		Success:    true,
		CheckedAt:  now.Add(40 * time.Second),
	})
	if err != nil {
		t.Fatalf("scheduled result after manual check error = %v", err)
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
	if err := store.MarkJobProcessing(monitor.ID, jobID, 1, claimedAt.Add(10*time.Second), time.Minute); err != nil {
		t.Fatal(err)
	}

	updatedAt := claimedAt.Add(20 * time.Second)
	nextCheckAt := updatedAt.Add(5 * time.Minute)
	if err := store.MarkJobDead(monitor.ID, jobID, "result persistence failed", updatedAt, nextCheckAt); err != nil {
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
	for i := range maxProcessedJobIDs + 5 {
		_, err := store.AddManualCheck(CheckRecord{
			ID:         fmt.Sprintf("check_%d", i),
			JobID:      fmt.Sprintf("job_%d", i),
			MonitorID:  monitor.ID,
			StatusCode: 200,
			Success:    true,
			CheckedAt:  now.Add(time.Duration(i) * time.Second),
		})
		if err != nil {
			t.Fatal(err)
		}
	}

	store.mu.RLock()
	defer store.mu.RUnlock()
	if got := len(store.processedJobs); got != maxProcessedJobIDs {
		t.Fatalf("processedJobs len = %d, want %d", got, maxProcessedJobIDs)
	}
	if _, exists := store.processedJobs["job_0"]; exists {
		t.Fatal("oldest processed job was not evicted")
	}
	latest := fmt.Sprintf("job_%d", maxProcessedJobIDs+4)
	if _, exists := store.processedJobs[latest]; !exists {
		t.Fatalf("latest processed job %q was evicted", latest)
	}
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
		if _, err := store.AddManualCheck(record); err != nil {
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
