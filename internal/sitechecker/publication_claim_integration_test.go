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

func TestPostgresPublicationClaimOwnership(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	t.Cleanup(cancel)
	repo := publicationTestRepository(t, ctx)

	t.Run("stale_scheduler_cannot_complete_or_release_retry_claim", func(t *testing.T) {
		monitor := publicationTestMonitor(t, ctx, repo, "stale-scheduler")
		first := publicationTestClaim(t, ctx, repo, monitor.ID)
		expirePublicationTestLease(t, ctx, repo, first.ID)
		second := publicationTestClaim(t, ctx, repo, monitor.ID)
		if second.Attempt != first.Attempt || second.PublicationToken == first.PublicationToken {
			t.Fatalf("reclaimed publication = %+v, want same attempt and a new token", second)
		}
		assertStalePublicationCallbacks(t, ctx, repo, first.PublicationClaim())
		if err := repo.MarkJobPublished(ctx, second.PublicationClaim()); err != nil {
			t.Fatal(err)
		}
		lease, err := repo.MarkJobProcessing(ctx, monitor.ID, first.ID, 1, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if err := repo.MarkJobFailed(ctx, lease, "temporary result storage failure", 0); err != nil {
			t.Fatal(err)
		}
		retry := publicationTestClaim(t, ctx, repo, monitor.ID)
		if retry.Attempt != 1 || retry.PublicationToken == second.PublicationToken {
			t.Fatalf("retry publication = %+v, want attempt 1 and a new token", retry)
		}
		assertStalePublicationCallbacks(t, ctx, repo, first.PublicationClaim())
		assertStalePublicationCallbacks(t, ctx, repo, second.PublicationClaim())
		if err := repo.ReleaseJobPublish(ctx, retry.PublicationClaim(), "retry publish failed"); err != nil {
			t.Fatalf("current claim release failed: %v", err)
		}
		republished := publicationTestClaim(t, ctx, repo, monitor.ID)
		if republished.Attempt != 1 || republished.PublicationToken == retry.PublicationToken {
			t.Fatalf("republished claim = %+v, want attempt 1 and a new token", republished)
		}
		if err := repo.MarkJobPublished(ctx, republished.PublicationClaim()); err != nil {
			t.Fatal(err)
		}
		if _, err := repo.MarkJobProcessing(ctx, monitor.ID, first.ID, 1, time.Minute); !errors.Is(err, ErrStaleJob) {
			t.Fatalf("old delivery processing error = %v, want ErrStaleJob", err)
		}
		currentLease, err := repo.MarkJobProcessing(ctx, monitor.ID, first.ID, 2, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		completePublicationTestJob(t, ctx, repo, currentLease)
		job, err := repo.GetCheckJob(ctx, first.ID)
		if err != nil {
			t.Fatal(err)
		}
		if job.Status != checkJobStatusCompleted || job.Attempt != 2 {
			t.Fatalf("recovered job = %+v, want completed attempt 2", job)
		}
	})

	t.Run("expired_publication_owner_cannot_change_unreclaimed_job", func(t *testing.T) {
		monitor := publicationTestMonitor(t, ctx, repo, "expired-publication")
		claim := publicationTestClaim(t, ctx, repo, monitor.ID)
		expirePublicationTestLease(t, ctx, repo, claim.ID)
		assertStalePublicationCallbacks(t, ctx, repo, claim.PublicationClaim())
		reclaimed := publicationTestClaim(t, ctx, repo, monitor.ID)
		if reclaimed.PublicationToken == claim.PublicationToken {
			t.Fatal("recovery reused an expired publication token")
		}
	})

	for _, operation := range []string{"confirm", "release"} {
		t.Run("publication_"+operation+"_waits_past_lease_expiry", func(t *testing.T) {
			monitor := publicationTestMonitor(t, ctx, repo, "publication-lock-"+operation)
			job := publicationTestClaim(t, ctx, repo, monitor.ID)
			var expiresAt time.Time
			if err := repo.pool.QueryRow(ctx, `UPDATE check_jobs
				SET lease_expires_at=clock_timestamp()+interval '3 seconds'
				WHERE id=$1 RETURNING lease_expires_at`, job.ID).Scan(&expiresAt); err != nil {
				t.Fatal(err)
			}
			before, err := repo.GetCheckJob(ctx, job.ID)
			if err != nil {
				t.Fatal(err)
			}
			lockTx, err := repo.pool.Begin(ctx)
			if err != nil {
				t.Fatal(err)
			}
			defer func() { _ = lockTx.Rollback(ctx) }()
			var blockerPID int
			if err := lockTx.QueryRow(ctx, `SELECT pg_backend_pid() FROM check_jobs WHERE id=$1 FOR UPDATE`, job.ID).Scan(&blockerPID); err != nil {
				t.Fatal(err)
			}
			callbackCtx, callbackCancel := context.WithTimeout(ctx, 15*time.Second)
			defer callbackCancel()
			result := make(chan error, 1)
			go func() {
				if operation == "confirm" {
					result <- repo.MarkJobPublished(callbackCtx, job.PublicationClaim())
					return
				}
				result <- repo.ReleaseJobPublish(callbackCtx, job.PublicationClaim(), "publish failed while waiting")
			}()
			e2eEventually(t, 2*time.Second, func() (bool, error) {
				var blocked bool
				err := repo.pool.QueryRow(ctx, `SELECT EXISTS (
					SELECT 1 FROM pg_stat_activity
					WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid))
				)`, blockerPID).Scan(&blocked)
				return blocked, err
			})
			dbNow, err := databaseTime(ctx, repo.pool)
			if err != nil {
				t.Fatal(err)
			}
			if !dbNow.Before(expiresAt) {
				t.Fatalf("callback did not block before lease expiry: now=%s expires_at=%s", dbNow, expiresAt)
			}
			if _, err := repo.pool.Exec(ctx, `SELECT pg_sleep(
				GREATEST(EXTRACT(EPOCH FROM ($1::timestamptz-clock_timestamp())), 0)+0.01
			)`, expiresAt); err != nil {
				t.Fatal(err)
			}
			if err := lockTx.Rollback(ctx); err != nil {
				t.Fatal(err)
			}
			select {
			case err := <-result:
				if !errors.Is(err, ErrStaleJob) {
					t.Fatalf("publication callback after lock crossed expiry error = %v, want ErrStaleJob", err)
				}
			case <-callbackCtx.Done():
				t.Fatal("publication callback did not finish after releasing row lock")
			}
			after, err := repo.GetCheckJob(ctx, job.ID)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatalf("expired publication changed job after lock wait: before=%+v after=%+v error=%v", before, after, err)
			}
		})
	}

	t.Run("fast_worker_progress_survives_delayed_publication_callbacks", func(t *testing.T) {
		monitor := publicationTestMonitor(t, ctx, repo, "fast-worker")
		job := publicationTestClaim(t, ctx, repo, monitor.ID)
		lease, err := repo.MarkJobProcessing(ctx, monitor.ID, job.ID, 1, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		assertCurrentPublicationCallbacksPreserveJob(t, ctx, repo, job.PublicationClaim())
		completePublicationTestJob(t, ctx, repo, lease)
		assertCurrentPublicationCallbacksPreserveJob(t, ctx, repo, job.PublicationClaim())
	})
}

func TestPostgresExpiredFinalProcessingAttemptCannotBeReacquired(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	repo := publicationTestRepository(t, ctx)
	monitor := publicationTestMonitor(t, ctx, repo, "final-processing-attempt")
	job := publicationTestClaim(t, ctx, repo, monitor.ID)
	var lease ProcessingLease
	for attempt := 1; attempt <= 3; attempt++ {
		if err := repo.MarkJobPublished(ctx, job.PublicationClaim()); err != nil {
			t.Fatal(err)
		}
		var err error
		lease, err = repo.MarkJobProcessing(ctx, monitor.ID, job.ID, attempt, time.Minute)
		if err != nil {
			t.Fatal(err)
		}
		if attempt < 3 {
			if err := repo.MarkJobFailed(ctx, lease, "temporary result storage failure", 0); err != nil {
				t.Fatal(err)
			}
			job = publicationTestClaim(t, ctx, repo, monitor.ID)
		}
	}
	expirePublicationTestLease(t, ctx, repo, job.ID)
	expired, err := repo.GetCheckJob(ctx, job.ID)
	if err != nil {
		t.Fatal(err)
	}
	for _, attempt := range []int{3, 4} {
		if _, err := repo.MarkJobProcessing(ctx, monitor.ID, job.ID, attempt, time.Minute); !errors.Is(err, ErrJobAlreadyProcessing) {
			t.Fatalf("expired processing attempt %d error = %v, want ErrJobAlreadyProcessing", attempt, err)
		}
		unchanged, err := repo.GetCheckJob(ctx, job.ID)
		if err != nil || !reflect.DeepEqual(unchanged, expired) || unchanged.LeaseToken != lease.LeaseToken {
			t.Fatalf("expired attempt was changed: job=%+v error=%v", unchanged, err)
		}
	}
	claimed, err := repo.ClaimDueJobs(ctx, 10, time.Minute, 3)
	if err != nil || len(claimed) != 0 {
		t.Fatalf("terminal recovery claimed jobs=%+v error=%v, want none", claimed, err)
	}
	terminal, err := repo.GetCheckJob(ctx, job.ID)
	if err != nil || terminal.Status != checkJobStatusDead || terminal.Attempt != 3 || terminal.CompletedAt.IsZero() {
		t.Fatalf("terminal job=%+v error=%v, want dead attempt 3", terminal, err)
	}
	var active int
	if err := repo.pool.QueryRow(ctx, `SELECT count(*) FROM check_jobs
		WHERE monitor_id=$1 AND status IN ('scheduled', 'queued', 'processing', 'failed')`, monitor.ID).Scan(&active); err != nil {
		t.Fatal(err)
	}
	if active != 0 {
		t.Fatalf("active jobs after terminal recovery = %d, want 0", active)
	}
	updated, err := repo.Get(ctx, monitor.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !updated.NextCheckAt.Equal(updated.UpdatedAt.Add(time.Minute)) {
		t.Fatalf("next periodic time = %s, want %s", updated.NextCheckAt, updated.UpdatedAt.Add(time.Minute))
	}
	if _, err := repo.pool.Exec(ctx, `UPDATE monitors SET next_check_at=clock_timestamp()-interval '1 second' WHERE id=$1`, monitor.ID); err != nil {
		t.Fatal(err)
	}
	next := publicationTestClaim(t, ctx, repo, monitor.ID)
	if next.ID == job.ID || next.Attempt != 0 {
		t.Fatalf("next periodic job = %+v, want a new job with no processing attempts", next)
	}
}

func publicationTestRepository(t *testing.T, ctx context.Context, customizers ...testcontainers.ContainerCustomizer) *PostgresMonitorRepository {
	t.Helper()
	options := []testcontainers.ContainerCustomizer{
		postgres.WithDatabase("site_checker"), postgres.WithUsername("site_checker"),
		postgres.WithPassword("site_checker"), postgres.BasicWaitStrategies(),
	}
	container, err := postgres.Run(ctx, integrationPostgresImage, append(options, customizers...)...)
	if container != nil {
		t.Cleanup(func() {
			if err := testcontainers.TerminateContainer(container); err != nil {
				t.Logf("terminate PostgreSQL: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	pool, err := OpenPostgresPool(ctx, container.MustConnectionString(ctx, "sslmode=disable"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(pool.Close)
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}
	cfg := testCheckerConfig(t)
	cfg.AllowedPorts = map[int]struct{}{80: {}, 443: {}}
	return NewPostgresMonitorRepository(pool, NewNetworkPolicy(cfg))
}

func publicationTestMonitor(t *testing.T, ctx context.Context, repo *PostgresMonitorRepository, name string) Monitor {
	t.Helper()
	monitor, err := repo.Create(ctx, MonitorInput{
		URL: "https://example.com/" + name, IntervalSeconds: 60,
		TimeoutSeconds: 5, ExpectedStatus: 200,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := repo.Delete(ctx, monitor.ID); err != nil {
			t.Error(err)
		}
	})
	return monitor
}

func publicationTestClaim(t *testing.T, ctx context.Context, repo *PostgresMonitorRepository, monitorID string) CheckJobRecord {
	t.Helper()
	jobs, err := repo.ClaimDueJobs(ctx, 10, time.Minute, 3)
	if err != nil || len(jobs) != 1 || jobs[0].MonitorID != monitorID || jobs[0].PublicationToken == "" {
		t.Fatalf("publication claim: jobs=%+v error=%v, want one owned claim for monitor %s", jobs, err, monitorID)
	}
	return jobs[0]
}

func expirePublicationTestLease(t *testing.T, ctx context.Context, repo *PostgresMonitorRepository, jobID string) {
	t.Helper()
	if _, err := repo.pool.Exec(ctx, `UPDATE check_jobs SET lease_expires_at=clock_timestamp()-interval '1 second' WHERE id=$1`, jobID); err != nil {
		t.Fatal(err)
	}
}

func assertStalePublicationCallbacks(t *testing.T, ctx context.Context, repo *PostgresMonitorRepository, claim PublicationClaim) {
	t.Helper()
	before, err := repo.GetCheckJob(ctx, claim.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkJobPublished(ctx, claim); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("stale publication confirmation error = %v, want ErrStaleJob", err)
	}
	if err := repo.ReleaseJobPublish(ctx, claim, "stale scheduler publish failure"); !errors.Is(err, ErrStaleJob) {
		t.Fatalf("stale publication release error = %v, want ErrStaleJob", err)
	}
	after, err := repo.GetCheckJob(ctx, claim.JobID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("stale publication changed job: before=%+v after=%+v error=%v", before, after, err)
	}
}

func assertCurrentPublicationCallbacksPreserveJob(t *testing.T, ctx context.Context, repo *PostgresMonitorRepository, claim PublicationClaim) {
	t.Helper()
	before, err := repo.GetCheckJob(ctx, claim.JobID)
	if err != nil {
		t.Fatal(err)
	}
	if err := repo.MarkJobPublished(ctx, claim); err != nil {
		t.Fatalf("delayed publication confirmation error = %v", err)
	}
	if err := repo.ReleaseJobPublish(ctx, claim, "ambiguous publish outcome"); err != nil {
		t.Fatalf("delayed publication release error = %v", err)
	}
	after, err := repo.GetCheckJob(ctx, claim.JobID)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("delayed publication changed worker progress: before=%+v after=%+v error=%v", before, after, err)
	}
}

func completePublicationTestJob(t *testing.T, ctx context.Context, repo *PostgresMonitorRepository, lease ProcessingLease) {
	t.Helper()
	if _, err := repo.AddCheck(ctx, CheckRecord{
		ID: newID("chk"), JobID: lease.JobID, MonitorID: lease.MonitorID,
		StatusCode: 200, Success: true, CheckedAt: time.Now().UTC(),
	}, lease, AlertPolicy{}); err != nil {
		t.Fatal(err)
	}
	checks, total, err := repo.ListChecks(ctx, lease.MonitorID, 0, 10)
	if err != nil || total != 1 || len(checks) != 1 {
		t.Fatalf("completed check history: checks=%+v total=%d error=%v", checks, total, err)
	}
}
