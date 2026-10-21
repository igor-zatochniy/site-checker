//go:build integration

package sitechecker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestRabbitMQTopologyLossFencesPublicationClaims(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Minute)
	t.Cleanup(cancel)
	repo := publicationTestRepository(t, ctx, testcontainers.WithWaitStrategyAndDeadline(3*time.Minute,
		wait.ForLog("database system is ready to accept connections").WithOccurrence(2).WithStartupTimeout(3*time.Minute),
		wait.ForListeningPort("5432/tcp").WithStartupTimeout(3*time.Minute),
	))
	rabbit, err := testcontainers.Run(ctx, integrationRabbitMQImage,
		testcontainers.WithEnv(map[string]string{
			"RABBITMQ_DEFAULT_USER":               "site_checker",
			"RABBITMQ_DEFAULT_PASS":               "site_checker",
			"RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS": "+S 2:2 +SDcpu 1 +SDio 1",
		}),
		testcontainers.WithExposedPorts("5672/tcp"),
		testcontainers.WithWaitStrategyAndDeadline(3*time.Minute,
			wait.ForLog("Server startup complete").WithStartupTimeout(3*time.Minute),
			wait.ForListeningPort("5672/tcp"),
		),
	)
	if rabbit != nil {
		t.Cleanup(func() {
			if err := testcontainers.TerminateContainer(rabbit); err != nil {
				t.Logf("terminate RabbitMQ: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := rabbit.PortEndpoint(ctx, "5672/tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	var recoveredJobs atomic.Int64
	handler := func(recoveryCtx context.Context) error {
		n, err := repo.RecoverQueuedJobsAfterTopologyLoss(recoveryCtx)
		recoveredJobs.Add(n)
		return err
	}
	cfg := Config{
		RabbitMQURL:         fmt.Sprintf("amqp://site_checker:site_checker@%s/", endpoint),
		QueueName:           "site_checker.publication-topology.checks",
		DeadLetterQueueName: "site_checker.publication-topology.checks.dead",
		QueuePrefetch:       1, MaxJobAttempts: 3,
	}
	publisher, err := NewRabbitMQQueueWithTopologyLossHandler(cfg, handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = publisher.Close() })
	reconciler, err := NewRabbitMQQueueWithTopologyLossHandler(cfg, handler)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = reconciler.Close() })

	for _, marked := range []bool{false, true} {
		name := "recovery_before_publication_mark"
		if marked {
			name = "publication_mark_before_recovery"
		}
		t.Run(name, func(t *testing.T) {
			processingMonitor := publicationTestMonitor(t, ctx, repo, name+"-processing")
			processingJob := publicationTestClaim(t, ctx, repo, processingMonitor.ID)
			processingLease, err := repo.MarkJobProcessing(ctx, processingMonitor.ID, processingJob.ID, 1, 5*time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			processingBefore, err := repo.GetCheckJob(ctx, processingJob.ID)
			if err != nil {
				t.Fatal(err)
			}

			monitor := publicationTestMonitor(t, ctx, repo, name)
			job := publicationTestClaim(t, ctx, repo, monitor.ID)
			message := CheckJobMessage{
				JobID: job.ID, MonitorID: monitor.ID, Attempt: job.Attempt + 1,
				EnqueuedAt: time.Now().UTC(),
			}
			if err := publisher.Publish(ctx, message); err != nil {
				t.Fatal(err)
			}
			if depth, err := rabbitQueueDepth(publisher.url, publisher.queueName); err != nil || depth != 1 {
				t.Fatalf("confirmed publication depth=%d error=%v, want 1", depth, err)
			}
			// The broker confirm has arrived; the scheduler may still be paused
			// before its separate database publication callback.
			if marked {
				if err := repo.MarkJobPublished(ctx, job.PublicationClaim()); err != nil {
					t.Fatal(err)
				}
			}
			recoveredJobs.Store(0)
			if err := deleteRabbitQueue(publisher.url, publisher.queueName); err != nil {
				t.Fatal(err)
			}
			if err := reconciler.Ping(ctx); err != nil {
				t.Fatalf("topology recovery: %v", err)
			}
			if recoveredJobs.Load() != 1 {
				t.Fatalf("recovered jobs=%d, want 1", recoveredJobs.Load())
			}
			if depth, err := rabbitQueueDepth(publisher.url, publisher.queueName); err != nil || depth != 0 {
				t.Fatalf("recreated queue depth=%d error=%v, want 0", depth, err)
			}
			assertStalePublicationCallbacks(t, ctx, repo, job.PublicationClaim())
			recovered, err := repo.GetCheckJob(ctx, job.ID)
			if err != nil || recovered.Status != checkJobStatusFailed || recovered.PublicationToken != "" || !recovered.LeaseExpiresAt.IsZero() {
				t.Fatalf("recovered job=%+v error=%v", recovered, err)
			}
			processingAfter, err := repo.GetCheckJob(ctx, processingJob.ID)
			if err != nil || !reflect.DeepEqual(processingBefore, processingAfter) {
				t.Fatalf("processing lease changed: before=%+v after=%+v error=%v", processingBefore, processingAfter, err)
			}

			retry := publicationTestClaim(t, ctx, repo, monitor.ID)
			if retry.ID != job.ID || retry.PublicationToken == job.PublicationToken {
				t.Fatalf("republication did not rotate ownership: %+v", retry)
			}
			if err := publisher.Publish(ctx, message); err != nil {
				t.Fatal(err)
			}
			if err := repo.MarkJobPublished(ctx, retry.PublicationClaim()); err != nil {
				t.Fatal(err)
			}
			consumeCtx, stopConsumer := context.WithCancel(ctx)
			defer stopConsumer()
			deliveries, _, err := publisher.Consume(consumeCtx)
			if err != nil {
				t.Fatal(err)
			}
			delivery := receiveRabbitDelivery(t, deliveries)
			lease, err := repo.MarkJobProcessing(ctx, delivery.Job.MonitorID, delivery.Job.JobID, delivery.Job.Attempt, time.Minute)
			if err != nil {
				t.Fatal(err)
			}
			completePublicationTestJob(t, ctx, repo, lease)
			if err := delivery.Ack(ctx); err != nil {
				t.Fatal(err)
			}
			if err := publisher.Publish(ctx, message); err != nil {
				t.Fatal(err)
			}
			duplicate := receiveRabbitDelivery(t, deliveries)
			if _, err := repo.MarkJobProcessing(ctx, duplicate.Job.MonitorID, duplicate.Job.JobID, duplicate.Job.Attempt, time.Minute); !errors.Is(err, ErrStaleJob) {
				t.Fatalf("duplicate processing error=%v, want ErrStaleJob", err)
			}
			if err := duplicate.Ack(ctx); err != nil {
				t.Fatal(err)
			}
			checks, total, err := repo.ListChecks(ctx, monitor.ID, 0, 10)
			if err != nil || total != 1 || len(checks) != 1 {
				t.Fatalf("recovered result count=%d checks=%d error=%v, want 1", total, len(checks), err)
			}
			terminal, err := repo.GetCheckJob(ctx, job.ID)
			if err != nil || terminal.Status != checkJobStatusCompleted {
				t.Fatalf("recovered job=%+v error=%v, want completed", terminal, err)
			}
			completePublicationTestJob(t, ctx, repo, processingLease)
			stopConsumer()
			select {
			case _, ok := <-deliveries:
				if ok {
					t.Fatal("unexpected delivery while stopping test consumer")
				}
			case <-ctx.Done():
				t.Fatal("test consumer did not stop")
			}
		})
	}
}
