package sitechecker

import (
	"errors"
	"reflect"
	"testing"
	"time"
)

func TestMonitorStoreExpiredProcessingLease(t *testing.T) {
	for _, operation := range []string{"complete", "retry", "dead", "retry_with_old_timestamp", "dead_with_old_timestamp"} {
		t.Run(operation, func(t *testing.T) {
			cfg := testCheckerConfig(t)
			cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
			store := NewMonitorStore(NewNetworkPolicy(cfg))
			monitor, err := store.Create(MonitorInput{
				URL: "https://example.com", IntervalSeconds: 60,
				TimeoutSeconds: 5, ExpectedStatus: 200,
			})
			if err != nil {
				t.Fatal(err)
			}
			now := time.Now().UTC()
			jobs := store.ClaimDueJobs(1, now, time.Minute)
			if len(jobs) != 1 {
				t.Fatalf("claim: %+v", jobs)
			}
			lease, err := store.MarkJobProcessing(monitor.ID, jobs[0].ID, 1, now, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			store.mu.Lock()
			expired := store.checkJobs[lease.JobID]
			expired.LeaseExpiresAt = now
			store.checkJobs[lease.JobID] = expired
			store.mu.Unlock()
			record := CheckRecord{ID: newID("chk"), JobID: lease.JobID, MonitorID: monitor.ID,
				StatusCode: 500, CheckedAt: now.Add(-time.Second)}
			switch operation {
			case "complete":
				_, err = store.AddCheck(record, lease)
			case "retry":
				err = store.MarkJobFailed(lease, "temporary failure", now, now)
			case "dead":
				err = store.MarkJobDead(lease, "terminal failure", now, now.Add(time.Minute))
			case "retry_with_old_timestamp":
				err = store.MarkJobFailed(lease, "temporary failure", now.Add(-time.Second), now)
			case "dead_with_old_timestamp":
				err = store.MarkJobDead(lease, "terminal failure", now.Add(-time.Second), now.Add(time.Minute))
			}
			if !errors.Is(err, ErrStaleJob) {
				t.Fatalf("expired owner %s: %v, want ErrStaleJob", operation, err)
			}
			job, err := store.CheckJob(lease.JobID)
			if err != nil || !reflect.DeepEqual(job, expired) {
				t.Fatalf("expired job changed: %+v, error=%v", job, err)
			}
			unchanged, err := store.Get(monitor.ID)
			if err != nil || !reflect.DeepEqual(unchanged, monitor) {
				t.Fatalf("monitor changed: %+v, error=%v", unchanged, err)
			}
			if len(store.checks[monitor.ID]) != 0 || len(store.processedJobs) != 0 || len(store.incidents) != 0 {
				t.Fatal("expired worker persisted check or incident state")
			}
			recovered := store.ClaimDueJobs(1, now, time.Minute)
			if len(recovered) != 1 || recovered[0].ID != lease.JobID {
				t.Fatalf("expired job not recoverable: %+v", recovered)
			}
			nextLease, err := store.MarkJobProcessing(monitor.ID, lease.JobID, 2, now, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := store.AddCheck(record, nextLease); err != nil {
				t.Fatalf("current lease failed: %v", err)
			}
		})
	}
}
