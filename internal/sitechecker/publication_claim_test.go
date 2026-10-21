package sitechecker

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestTopologyRecoveryRevokesPublicationClaims(t *testing.T) {
	for _, marked := range []bool{false, true} {
		name := "recovery_before_publication_mark"
		if marked {
			name = "publication_mark_before_recovery"
		}
		t.Run(name, func(t *testing.T) {
			store, monitor, now := publicationTestStore(t)
			job := store.ClaimDueJobs(1, now, time.Minute)[0]
			if marked {
				if err := store.MarkJobPublished(job.PublicationClaim(), now); err != nil {
					t.Fatal(err)
				}
			}
			if recovered := store.RecoverQueuedJobsAfterTopologyLoss(now); recovered != 1 {
				t.Fatalf("recovered jobs = %d, want 1", recovered)
			}
			for _, callback := range []func() error{
				func() error { return store.MarkJobPublished(job.PublicationClaim(), now) },
				func() error { return store.ReleaseJobPublish(job.PublicationClaim(), "late callback", now) },
			} {
				if err := callback(); !errors.Is(err, ErrStaleJob) {
					t.Fatalf("publication callback after topology recovery = %v, want ErrStaleJob", err)
				}
			}
			recovered, err := store.CheckJob(job.ID)
			if err != nil || recovered.Status != checkJobStatusFailed || recovered.PublicationToken != "" || !recovered.LeaseExpiresAt.IsZero() {
				t.Fatalf("recovered job = %+v, error = %v", recovered, err)
			}
			republished := store.ClaimDueJobs(1, now, time.Minute)[0]
			if republished.ID != job.ID || republished.PublicationToken == job.PublicationToken {
				t.Fatalf("republication did not rotate ownership: %+v", republished)
			}
			if err := store.MarkJobPublished(republished.PublicationClaim(), now); err != nil {
				t.Fatal(err)
			}
			lease, err := store.MarkJobProcessing(monitor.ID, job.ID, 1, now, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.AddCheck(CheckRecord{
				ID: newID("check"), JobID: job.ID, MonitorID: monitor.ID,
				StatusCode: 200, Success: true, CheckedAt: now,
			}, lease); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestTopologyRecoveryPreservesProcessingLease(t *testing.T) {
	store, monitor, now := publicationTestStore(t)
	job := store.ClaimDueJobs(1, now, time.Minute)[0]
	lease, err := store.MarkJobProcessing(monitor.ID, job.ID, 1, now, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	before, err := store.CheckJob(job.ID)
	if err != nil {
		t.Fatal(err)
	}
	if recovered := store.RecoverQueuedJobsAfterTopologyLoss(now); recovered != 0 {
		t.Fatalf("recovered processing jobs = %d, want 0", recovered)
	}
	after, err := store.CheckJob(job.ID)
	if err != nil || !reflect.DeepEqual(before, after) || after.LeaseToken != lease.LeaseToken {
		t.Fatalf("topology recovery changed processing lease: before=%+v after=%+v error=%v", before, after, err)
	}
}

func publicationTestStore(t *testing.T) (*MonitorStore, Monitor, time.Time) {
	t.Helper()
	store := NewMonitorStore(NewNetworkPolicy(testCheckerConfig(t)))
	monitor, err := store.Create(MonitorInput{
		URL: "https://example.com", IntervalSeconds: 300,
		TimeoutSeconds: 5, ExpectedStatus: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	return store, monitor, time.Now().UTC()
}

func TestPublicationClaimRejectsStaleSchedulerDuringRetry(t *testing.T) {
	store, monitor, now := publicationTestStore(t)
	first := store.ClaimDueJobs(1, now, time.Minute)[0]
	reclaimedAt := now.Add(time.Minute)
	second := store.ClaimDueJobs(1, reclaimedAt, time.Minute)[0]
	if first.Attempt != second.Attempt || first.PublicationToken == second.PublicationToken {
		t.Fatal("reclaim must rotate the token even before the first processing attempt")
	}
	if err := store.MarkJobPublished(first.PublicationClaim(), reclaimedAt); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("stale same-attempt publication = %v", err)
	}
	if err := store.MarkJobPublished(second.PublicationClaim(), reclaimedAt); err != nil {
		t.Fatal(err)
	}
	lease, err := store.MarkJobProcessing(monitor.ID, first.ID, 1, reclaimedAt, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkJobFailed(lease, "temporary database failure", reclaimedAt, reclaimedAt); err != nil {
		t.Fatal(err)
	}
	retry := store.ClaimDueJobs(1, reclaimedAt, time.Minute)[0]
	if retry.Attempt != 1 || retry.PublicationToken == second.PublicationToken {
		t.Fatalf("invalid retry claim: %+v", retry)
	}
	for _, stale := range []CheckJobRecord{first, second} {
		if err := store.MarkJobPublished(stale.PublicationClaim(), reclaimedAt); !errors.Is(err, ErrStaleJob) {
			t.Fatalf("stale retry publication = %v", err)
		}
		if err := store.ReleaseJobPublish(stale.PublicationClaim(), "old publish failed", reclaimedAt); !errors.Is(err, ErrStaleJob) {
			t.Fatalf("stale retry release = %v", err)
		}
	}
	if err := store.ReleaseJobPublish(retry.PublicationClaim(), "current publish failed", reclaimedAt); err != nil {
		t.Fatal(err)
	}
	republished := store.ClaimDueJobs(1, reclaimedAt, time.Minute)[0]
	if err := store.MarkJobPublished(republished.PublicationClaim(), reclaimedAt); err != nil {
		t.Fatal(err)
	}
	if _, err := store.MarkJobProcessing(monitor.ID, first.ID, 1, reclaimedAt, time.Minute); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("old broker message = %v", err)
	}
	lease, err = store.MarkJobProcessing(monitor.ID, first.ID, 2, reclaimedAt, time.Minute)
	if err != nil {
		t.Fatalf("current retry cannot run: %v", err)
	}
	if _, err := store.AddCheck(CheckRecord{
		ID: newID("check"), JobID: first.ID, MonitorID: monitor.ID,
		StatusCode: 200, Success: true, CheckedAt: reclaimedAt,
	}, lease); err != nil {
		t.Fatal(err)
	}
	job, err := store.CheckJob(first.ID)
	if err != nil || job.Status != checkJobStatusCompleted {
		t.Fatalf("job did not recover: %+v, %v", job, err)
	}
}

func TestPublicationClaimExpiresAndAllowsFastWorkerProgress(t *testing.T) {
	store, monitor, now := publicationTestStore(t)
	job := store.ClaimDueJobs(1, now, time.Minute)[0]
	for _, finish := range []func() error{
		func() error { return store.MarkJobPublished(job.PublicationClaim(), now.Add(time.Minute)) },
		func() error {
			return store.ReleaseJobPublish(job.PublicationClaim(), "late failure", now.Add(time.Minute))
		},
	} {
		if err := finish(); !errors.Is(err, ErrStaleJob) {
			t.Fatalf("expired publication claim = %v", err)
		}
	}
	current := store.ClaimDueJobs(1, now.Add(time.Minute), time.Minute)[0]
	lease, err := store.MarkJobProcessing(monitor.ID, job.ID, 1, now.Add(time.Minute), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.MarkJobPublished(current.PublicationClaim(), now.Add(time.Minute)); err != nil {
		t.Fatalf("confirmation after worker started: %v", err)
	}
	if err := store.ReleaseJobPublish(current.PublicationClaim(), "ambiguous confirmation", now.Add(time.Minute)); err != nil {
		t.Fatalf("release after worker started: %v", err)
	}
	got, err := store.CheckJob(job.ID)
	if err != nil || got.Status != checkJobStatusProcessing || got.LeaseToken != lease.LeaseToken {
		t.Fatalf("publication callback changed processing lease: %+v, %v", got, err)
	}
}

func TestExpiredFinalProcessingAttemptRequiresSchedulerRecovery(t *testing.T) {
	store, monitor, now := publicationTestStore(t)
	job := store.ClaimDueJobs(1, now, time.Minute)[0]
	for attempt := 1; attempt <= 3; attempt++ {
		if err := store.MarkJobPublished(job.PublicationClaim(), now); err != nil {
			t.Fatal(err)
		}
		lease, err := store.MarkJobProcessing(monitor.ID, job.ID, attempt, now, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if attempt < 3 {
			if err := store.MarkJobFailed(lease, "transient failure", now, now); err != nil {
				t.Fatal(err)
			}
			job = store.ClaimDueJobs(1, now, time.Minute)[0]
			continue
		}
		expiredAt := now.Add(time.Minute)
		for _, redelivery := range []int{3, 4} {
			if _, err := store.MarkJobProcessing(monitor.ID, job.ID, redelivery, expiredAt, time.Minute); !errors.Is(err, ErrJobAlreadyProcessing) {
				t.Fatalf("expired attempt %d was reclaimed directly: %v", redelivery, err)
			}
		}
		got, err := store.CheckJob(job.ID)
		if err != nil || got.Attempt != 3 || got.LeaseToken != lease.LeaseToken {
			t.Fatalf("redelivery changed the final lease: %+v, %v", got, err)
		}
		if claimed := store.ClaimDueJobsWithMaxAttempts(1, expiredAt, time.Minute, 3); len(claimed) != 0 {
			t.Fatalf("exhausted job was published again: %+v", claimed)
		}
		got, err = store.CheckJob(job.ID)
		if err != nil || got.Status != checkJobStatusDead {
			t.Fatalf("exhausted job not terminal: %+v, %v", got, err)
		}
		monitor, err = store.Get(monitor.ID)
		if err != nil || !monitor.NextCheckAt.Equal(expiredAt.Add(300*time.Second)) {
			t.Fatalf("next periodic check incorrect: %+v, %v", monitor, err)
		}
		if next := store.ClaimDueJobsWithMaxAttempts(1, monitor.NextCheckAt, time.Minute, 3); len(next) != 1 || next[0].ID == job.ID {
			t.Fatalf("active constraint was not released: %+v", next)
		}
	}
}
