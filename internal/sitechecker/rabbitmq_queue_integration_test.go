//go:build integration

package sitechecker

import (
	"context"
	"fmt"
	"io"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
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

	execRabbitMQControl(t, ctx, rabbit, "stop_app")
	downCtx, downCancel := context.WithTimeout(ctx, 2*time.Second)
	downErr := queue.Ping(downCtx)
	downCancel()
	if downErr == nil {
		t.Fatal("RabbitMQ ping succeeded while broker application was stopped")
	}
	execRabbitMQControl(t, ctx, rabbit, "start_app")
	brokerRestartCtx, brokerRestartCancel := context.WithTimeout(ctx, 45*time.Second)
	defer brokerRestartCancel()
	restartedJob := CheckJobMessage{
		JobID:      "job_integration_broker_restart",
		MonitorID:  "mon_integration",
		Attempt:    1,
		EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(brokerRestartCtx, restartedJob); err != nil {
		t.Fatalf("publish after RabbitMQ restart: %v", err)
	}
	restarted := receiveRabbitDelivery(t, deliveries)
	if restarted.Job.JobID != restartedJob.JobID {
		t.Fatalf("job after RabbitMQ restart = %q, want %q", restarted.Job.JobID, restartedJob.JobID)
	}
	if err := restarted.Ack(brokerRestartCtx); err != nil {
		t.Fatalf("ack after RabbitMQ restart: %v", err)
	}

	queue.mu.RLock()
	connectionBeforeDelete := queue.conn
	queue.mu.RUnlock()
	if connectionBeforeDelete == nil || connectionBeforeDelete.IsClosed() {
		t.Fatal("RabbitMQ connection is not available before topology recovery test")
	}
	adminConnection, err := amqp.Dial(queue.url)
	if err != nil {
		t.Fatal(err)
	}
	adminChannel, err := adminConnection.Channel()
	if err != nil {
		_ = adminConnection.Close()
		t.Fatal(err)
	}
	if _, err := adminChannel.QueueDelete(queue.dlqName, false, false, false); err != nil {
		t.Fatal(err)
	}
	if _, err := adminChannel.QueueDelete(queue.queueName, false, false, false); err != nil {
		t.Fatal(err)
	}
	_ = adminChannel.Close()
	_ = adminConnection.Close()

	topologyCtx, topologyCancel := context.WithTimeout(ctx, 30*time.Second)
	defer topologyCancel()
	topologyJob := CheckJobMessage{
		JobID:      "job_integration_topology_recovery",
		MonitorID:  "mon_integration",
		Attempt:    2,
		EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(topologyCtx, topologyJob); err != nil {
		t.Fatalf("publish after queue deletion: %v", err)
	}
	restored := receiveRabbitDelivery(t, deliveries)
	if restored.Job.JobID != topologyJob.JobID {
		t.Fatalf("job after topology recovery = %q, want %q", restored.Job.JobID, topologyJob.JobID)
	}
	if err := restored.Nack(topologyCtx, false); err != nil {
		t.Fatalf("dead-letter after topology recovery: %v", err)
	}
	restoredDeadLetter := receiveRabbitDeadLetter(t, topologyCtx, queue)
	if restoredDeadLetter.JobID != topologyJob.JobID {
		t.Fatalf("dead-letter after topology recovery = %q, want %q", restoredDeadLetter.JobID, topologyJob.JobID)
	}
	if err := queue.Ping(topologyCtx); err != nil {
		t.Fatalf("ping after topology recovery: %v", err)
	}
	queue.mu.RLock()
	connectionAfterDelete := queue.conn
	queue.mu.RUnlock()
	if connectionAfterDelete != connectionBeforeDelete || connectionAfterDelete.IsClosed() {
		t.Fatal("topology recovery unnecessarily replaced the live RabbitMQ connection")
	}
}

func execRabbitMQControl(t *testing.T, ctx context.Context, container testcontainers.Container, command string) {
	t.Helper()
	exitCode, output, err := container.Exec(ctx, []string{"rabbitmqctl", command})
	if err != nil {
		t.Fatalf("rabbitmqctl %s: %v", command, err)
	}
	body, readErr := io.ReadAll(output)
	if readErr != nil {
		t.Fatalf("read rabbitmqctl %s output: %v", command, readErr)
	}
	if exitCode != 0 {
		t.Fatalf("rabbitmqctl %s exit code %d: %s", command, exitCode, body)
	}
}

func TestRabbitMQQueueRepairsUnroutablePublish(t *testing.T) {
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
		QueueName:           "site_checker.integration.unroutable.checks",
		DeadLetterQueueName: "site_checker.integration.unroutable.checks.dead",
		QueuePrefetch:       1,
		MaxJobAttempts:      2,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	queue.queueName = "site_checker.integration.unroutable.missing"
	job := CheckJobMessage{
		JobID:      "job_integration_unroutable",
		MonitorID:  "mon_integration",
		Attempt:    1,
		EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(ctx, job); err != nil {
		t.Fatalf("publish did not repair missing expected queue: %v", err)
	}
	deliveries, _, err := queue.Consume(ctx)
	if err != nil {
		t.Fatal(err)
	}
	delivery := receiveRabbitDelivery(t, deliveries)
	if delivery.Job.JobID != job.JobID {
		t.Fatalf("repaired queue job_id = %q, want %q", delivery.Job.JobID, job.JobID)
	}
	if err := delivery.Ack(ctx); err != nil {
		t.Fatal(err)
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
		channel := queue.publishSession().channel
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
