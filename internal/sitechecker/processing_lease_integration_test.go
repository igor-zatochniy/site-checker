//go:build integration

package sitechecker

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestPostgresExpiredProcessingLease(t *testing.T) {
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
	repo := NewPostgresMonitorRepository(pool, NewNetworkPolicy(cfg))
	policy := AlertPolicy{Enabled: true, FailureThreshold: 1, Cooldown: time.Hour}

	for _, operation := range []string{"complete", "retry", "dead", "complete_during_recovery", "expires_during_completion"} {
		t.Run(operation, func(t *testing.T) {
			monitor, err := repo.Create(ctx, MonitorInput{
				URL: "https://example.com/" + operation, IntervalSeconds: 60,
				TimeoutSeconds: 5, ExpectedStatus: 200,
			})
			if err != nil {
				t.Fatal(err)
			}
			defer func() {
				if err := repo.Delete(ctx, monitor.ID); err != nil {
					t.Error(err)
				}
			}()
			jobs, err := repo.ClaimDueJobs(ctx, 1, time.Minute, 3)
			if err != nil || len(jobs) != 1 || jobs[0].MonitorID != monitor.ID {
				t.Fatalf("claim: jobs=%+v error=%v", jobs, err)
			}
			job := jobs[0]
			lease, err := repo.MarkJobProcessing(ctx, monitor.ID, job.ID, 1, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			expireLease := func() {
				t.Helper()
				if _, err := pool.Exec(ctx, `UPDATE check_jobs
					SET lease_expires_at = clock_timestamp() - interval '1 second'
					WHERE id = $1 AND lease_token = $2`, job.ID, lease.LeaseToken); err != nil {
					t.Fatal(err)
				}
			}
			if operation == "expires_during_completion" {
				// Force expiry inside persistence without timing-dependent sleeps.
				if _, err := pool.Exec(ctx, `
					CREATE FUNCTION expire_job_during_alert() RETURNS trigger LANGUAGE plpgsql AS $$
					BEGIN
						UPDATE check_jobs SET lease_expires_at = clock_timestamp() - interval '1 second'
						WHERE monitor_id = NEW.monitor_id AND status = 'processing';
						RETURN NEW;
					END $$;
					CREATE TRIGGER expire_job_during_alert BEFORE INSERT ON alert_outbox
					FOR EACH ROW EXECUTE FUNCTION expire_job_during_alert();`); err != nil {
					t.Fatal(err)
				}
				defer func() {
					if _, err := pool.Exec(ctx, `DROP TRIGGER IF EXISTS expire_job_during_alert ON alert_outbox;
						DROP FUNCTION IF EXISTS expire_job_during_alert()`); err != nil {
						t.Error(err)
					}
				}()
			} else {
				expireLease()
			}
			expired, err := repo.GetCheckJob(ctx, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			record := CheckRecord{ID: newID("chk"), JobID: job.ID, MonitorID: monitor.ID,
				StatusCode: 500, Error: "target unavailable", CheckedAt: time.Now().UTC()}

			var recovered []CheckJobRecord
			switch operation {
			case "retry":
				err = repo.MarkJobFailed(ctx, lease, "temporary failure", 0)
			case "dead":
				err = repo.MarkJobDead(ctx, lease, "terminal failure")
			case "complete_during_recovery":
				start := make(chan struct{})
				result := make(chan error, 1)
				go func() {
					<-start
					_, err := repo.AddCheck(ctx, record, lease, policy)
					result <- err
				}()
				close(start)
				var recoveryErr error
				recovered, recoveryErr = repo.ClaimDueJobs(ctx, 1, time.Minute, 3)
				err = <-result
				if recoveryErr != nil {
					t.Fatal(recoveryErr)
				}
			default:
				_, err = repo.AddCheck(ctx, record, lease, policy)
			}
			if !errors.Is(err, ErrStaleJob) {
				t.Fatalf("expired owner %s: error=%v, want ErrStaleJob before reclaim", operation, err)
			}
			unchanged, err := repo.Get(ctx, monitor.ID)
			if err != nil || !reflect.DeepEqual(unchanged, monitor) {
				t.Fatalf("expired owner changed monitor: %+v, error=%v", unchanged, err)
			}
			var checks, incidents, alerts int
			if err := pool.QueryRow(ctx, `SELECT
				(SELECT count(*) FROM check_results WHERE monitor_id=$1),
				(SELECT count(*) FROM incidents WHERE monitor_id=$1),
				(SELECT count(*) FROM alert_outbox WHERE monitor_id=$1)`, monitor.ID).
				Scan(&checks, &incidents, &alerts); err != nil {
				t.Fatal(err)
			}
			if checks != 0 || incidents != 0 || alerts != 0 {
				t.Fatalf("expired owner persisted checks=%d incidents=%d alerts=%d", checks, incidents, alerts)
			}
			if operation != "complete_during_recovery" {
				unchangedJob, err := repo.GetCheckJob(ctx, job.ID)
				if err != nil || !reflect.DeepEqual(unchangedJob, expired) {
					t.Fatalf("expired owner changed job: %+v, error=%v", unchangedJob, err)
				}
				if operation == "expires_during_completion" {
					if _, err := pool.Exec(ctx, `DROP TRIGGER expire_job_during_alert ON alert_outbox`); err != nil {
						t.Fatal(err)
					}
					expireLease()
				}
				recovered, err = repo.ClaimDueJobs(ctx, 1, time.Minute, 3)
				if err != nil {
					t.Fatal(err)
				}
			}
			if len(recovered) != 1 || recovered[0].ID != job.ID || recovered[0].Attempt != 1 {
				t.Fatalf("recovery=%+v, want original job with attempt 1", recovered)
			}
			nextLease, err := repo.MarkJobProcessing(ctx, monitor.ID, job.ID, 2, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			if _, err := repo.AddCheck(ctx, record, nextLease, policy); err != nil {
				t.Fatalf("new lease must complete recovered job: %v", err)
			}
			if _, err := repo.AddCheck(ctx, record, nextLease, policy); !errors.Is(err, ErrDuplicateJob) {
				t.Fatalf("completed job duplicate: %v", err)
			}
		})
	}
}
