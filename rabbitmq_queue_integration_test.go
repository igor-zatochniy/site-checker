//go:build integration

package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestRabbitMQQueueRetriesAndDeadLetters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()

	rabbit, err := testcontainers.Run(ctx,
		"rabbitmq:4-management-alpine",
		testcontainers.WithEnv(map[string]string{
			"RABBITMQ_DEFAULT_USER": "site_checker",
			"RABBITMQ_DEFAULT_PASS": "site_checker",
		}),
		testcontainers.WithExposedPorts("5672/tcp"),
		testcontainers.WithWaitStrategy(
			wait.ForLog("Server startup complete").WithStartupTimeout(2*time.Minute),
			wait.ForListeningPort("5672/tcp"),
		),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(rabbit); err != nil {
			t.Logf("failed to terminate rabbitmq container: %v", err)
		}
	}()

	endpoint, err := rabbit.PortEndpoint(ctx, "5672/tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewRabbitMQQueue(Config{
		RabbitMQURL:         fmt.Sprintf("amqp://site_checker:site_checker@%s/", endpoint),
		QueueName:           "site_checker.integration.checks",
		DeadLetterQueueName: "site_checker.integration.checks.dead",
		QueuePrefetch:       1,
		MaxJobAttempts:      2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	deliveries, _, err := queue.Consume(ctx)
	if err != nil {
		t.Fatal(err)
	}

	job := CheckJobMessage{
		JobID:      "job_integration_retry",
		MonitorID:  "mon_integration",
		Attempt:    1,
		EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(ctx, job); err != nil {
		t.Fatal(err)
	}

	first := receiveRabbitDelivery(t, deliveries)
	if first.Job.Attempt != 1 {
		t.Fatalf("first attempt = %d, want 1", first.Job.Attempt)
	}
	if err := first.Nack(ctx, true); err != nil {
		t.Fatal(err)
	}

	second := receiveRabbitDelivery(t, deliveries)
	if second.Job.Attempt != 2 {
		t.Fatalf("second attempt = %d, want 2", second.Job.Attempt)
	}
	if err := second.Nack(ctx, true); err != nil {
		t.Fatal(err)
	}

	deadLetter := receiveRabbitDeadLetter(t, ctx, queue)
	if deadLetter.JobID != job.JobID {
		t.Fatalf("dead-letter job_id = %q, want %q", deadLetter.JobID, job.JobID)
	}

	queue.mu.RLock()
	connection := queue.conn
	queue.mu.RUnlock()
	if connection == nil {
		t.Fatal("RabbitMQ connection is nil before reconnect test")
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close RabbitMQ connection: %v", err)
	}

	reconnectCtx, reconnectCancel := context.WithTimeout(ctx, 30*time.Second)
	defer reconnectCancel()
	recoveredJob := CheckJobMessage{
		JobID:      "job_integration_reconnect",
		MonitorID:  "mon_integration",
		Attempt:    1,
		EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(reconnectCtx, recoveredJob); err != nil {
		t.Fatalf("publish after connection loss: %v", err)
	}
	recovered := receiveRabbitDelivery(t, deliveries)
	if recovered.Job.JobID != recoveredJob.JobID {
		t.Fatalf("recovered job_id = %q, want %q", recovered.Job.JobID, recoveredJob.JobID)
	}
	if err := recovered.Ack(reconnectCtx); err != nil {
		t.Fatalf("ack after reconnect: %v", err)
	}
	if err := queue.Ping(reconnectCtx); err != nil {
		t.Fatalf("ping after reconnect: %v", err)
	}
}

func receiveRabbitDelivery(t *testing.T, deliveries <-chan QueueDelivery) QueueDelivery {
	t.Helper()
	select {
	case delivery := <-deliveries:
		return delivery
	case <-time.After(20 * time.Second):
		t.Fatal("timed out waiting for RabbitMQ delivery")
		return QueueDelivery{}
	}
}

func receiveRabbitDeadLetter(t *testing.T, ctx context.Context, queue *RabbitMQQueue) CheckJobMessage {
	t.Helper()
	deadline := time.Now().Add(20 * time.Second)
	for time.Now().Before(deadline) {
		if err := queue.ensureConnected(ctx); err != nil {
			t.Fatal(err)
		}
		queue.publishMu.Lock()
		channel := queue.publishChannel()
		if channel == nil {
			queue.publishMu.Unlock()
			continue
		}
		delivery, ok, err := channel.Get(queue.dlqName, true)
		queue.publishMu.Unlock()
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			job, err := decodeJobMessage(delivery.Body)
			if err != nil {
				t.Fatal(err)
			}
			return job
		}
		select {
		case <-ctx.Done():
			t.Fatal(ctx.Err())
		case <-time.After(250 * time.Millisecond):
		}
	}
	t.Fatal("timed out waiting for RabbitMQ dead-letter job")
	return CheckJobMessage{}
}
