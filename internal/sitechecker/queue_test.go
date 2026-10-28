package sitechecker

import (
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRabbitMQDoesNotStartOperationWithCanceledContext(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	queue := &RabbitMQQueue{}
	var called atomic.Bool
	err := queue.runRabbitMQOperation(ctx, nil, func() error {
		called.Store(true)
		return nil
	})
	if !errors.Is(err, context.Canceled) || called.Load() {
		t.Fatalf("operation error = %v, started = %t", err, called.Load())
	}
	if err := queue.Ping(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Ping error = %v, want context.Canceled", err)
	}
}

func TestInMemoryQueueDeduplicatesPublishedJobs(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	queue := NewInMemoryQueue(10, 3)
	defer queue.Close()

	job := CheckJobMessage{
		JobID:      "job_same",
		MonitorID:  "mon_1",
		Attempt:    1,
		EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(ctx, job); err != nil {
		t.Fatal(err)
	}
	if err := queue.Publish(ctx, job); err != nil {
		t.Fatal(err)
	}

	deliveries, _, err := queue.Consume(ctx)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case delivery := <-deliveries:
		if delivery.Job.JobID != job.JobID {
			t.Fatalf("job_id = %q, want %q", delivery.Job.JobID, job.JobID)
		}
		if err := delivery.Ack(ctx); err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for delivery")
	}
	if got := queueSeenLen(queue); got != 0 {
		t.Fatalf("seen len after ack = %d, want 0", got)
	}

	select {
	case delivery := <-deliveries:
		t.Fatalf("unexpected duplicate delivery: %+v", delivery.Job)
	case <-time.After(50 * time.Millisecond):
	}
}

func TestDialRabbitMQRedactsMalformedURL(t *testing.T) {
	secret := "SUPER_SECRET_PASSWORD"
	rawURL := "amqp://user:" + secret + "@rabbit/%zz"

	_, err := dialRabbitMQ(context.Background(), rawURL, time.Second, time.Second)
	if err == nil {
		t.Fatal("dialRabbitMQ returned nil error for malformed URL")
	}
	if strings.Contains(err.Error(), secret) || strings.Contains(err.Error(), rawURL) {
		t.Fatalf("dialRabbitMQ error leaked credentials or raw URL: %v", err)
	}
}

func TestInMemoryQueueRetriesAndThenDeadLetters(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	queue := NewInMemoryQueue(10, 2)
	defer queue.Close()

	job := CheckJobMessage{
		JobID:      "job_retry",
		MonitorID:  "mon_1",
		Attempt:    1,
		EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(ctx, job); err != nil {
		t.Fatal(err)
	}

	deliveries, _, err := queue.Consume(ctx)
	if err != nil {
		t.Fatal(err)
	}

	first := receiveDelivery(t, deliveries)
	if err := first.Nack(ctx, true); err != nil {
		t.Fatal(err)
	}
	if got := queueSeenLen(queue); got != 1 {
		t.Fatalf("seen len after retry nack = %d, want 1", got)
	}

	second := receiveDelivery(t, deliveries)
	if second.Job.Attempt != 2 {
		t.Fatalf("attempt = %d, want 2", second.Job.Attempt)
	}
	if err := second.Nack(ctx, true); err != nil {
		t.Fatal(err)
	}

	select {
	case dead := <-queue.deadLetters:
		if dead.JobID != job.JobID {
			t.Fatalf("dead-letter job_id = %q, want %q", dead.JobID, job.JobID)
		}
		if got := queueSeenLen(queue); got != 0 {
			t.Fatalf("seen len after dead-letter = %d, want 0", got)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for dead-letter job")
	}
}

func TestInMemoryQueueInfrastructureRequeuePreservesAttempt(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	queue := NewInMemoryQueue(10, 3)
	defer queue.Close()

	job := CheckJobMessage{
		JobID:      "job_infrastructure_requeue",
		MonitorID:  "mon_1",
		Attempt:    2,
		EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(ctx, job); err != nil {
		t.Fatal(err)
	}

	deliveries, _, err := queue.Consume(ctx)
	if err != nil {
		t.Fatal(err)
	}

	first := receiveDelivery(t, deliveries)
	if err := first.Requeue(ctx); err != nil {
		t.Fatal(err)
	}

	second := receiveDelivery(t, deliveries)
	if second.Job.Attempt != job.Attempt {
		t.Fatalf("attempt after infrastructure requeue = %d, want %d", second.Job.Attempt, job.Attempt)
	}
	if err := second.Ack(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestInMemoryQueueEnqueueRejectsClosedQueue(t *testing.T) {
	queue := NewInMemoryQueue(1, 2)
	if err := queue.Close(); err != nil {
		t.Fatal(err)
	}

	err := queue.enqueue(context.Background(), CheckJobMessage{JobID: "job_after_close"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("enqueue after close error = %v, want context canceled", err)
	}
}

func TestInMemoryQueueCloseConcurrentWithQueueOperations(t *testing.T) {
	const iterations = 200

	for iteration := range iterations {
		t.Run(fmt.Sprintf("iteration_%d", iteration), func(t *testing.T) {
			queue := NewInMemoryQueue(4, 3)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			seed := CheckJobMessage{
				JobID:      fmt.Sprintf("job_close_race_%d", iteration),
				MonitorID:  "mon_close_race",
				Attempt:    1,
				EnqueuedAt: time.Now().UTC(),
			}
			if err := queue.Publish(ctx, seed); err != nil {
				t.Fatal(err)
			}
			deliveries, _, err := queue.Consume(ctx)
			if err != nil {
				t.Fatal(err)
			}
			requeueDelivery := receiveDelivery(t, deliveries)

			retry := seed
			retry.JobID += "_retry"
			if err := queue.Publish(ctx, retry); err != nil {
				t.Fatal(err)
			}
			nackDelivery := receiveDelivery(t, deliveries)

			start := make(chan struct{})
			results := make(chan error, 3)
			go func() {
				<-start
				job := seed
				job.JobID += "_publish"
				results <- queue.Publish(ctx, job)
			}()
			go func() {
				<-start
				results <- requeueDelivery.Requeue(ctx)
			}()
			go func() {
				<-start
				results <- nackDelivery.Nack(ctx, true)
			}()

			close(start)
			if err := queue.Close(); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				assertInMemoryQueueShutdownError(t, <-results)
			}
		})
	}
}

func assertInMemoryQueueShutdownError(t *testing.T, err error) {
	t.Helper()
	if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, ErrQueueFull) {
		return
	}
	t.Fatalf("queue operation during close error = %v, want nil, context canceled, or queue full", err)
}

func receiveDelivery(t *testing.T, deliveries <-chan QueueDelivery) QueueDelivery {
	t.Helper()
	select {
	case delivery := <-deliveries:
		return delivery
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for delivery")
		return QueueDelivery{}
	}
}

func queueSeenLen(queue *InMemoryQueue) int {
	queue.mu.Lock()
	defer queue.mu.Unlock()
	return len(queue.seen)
}

func TestRabbitMQReconnectDelayIsBounded(t *testing.T) {
	initial := time.Second
	maximum := 30 * time.Second

	tests := []struct {
		attempt int
		want    time.Duration
	}{
		{attempt: 0, want: time.Second},
		{attempt: 1, want: time.Second},
		{attempt: 2, want: 2 * time.Second},
		{attempt: 5, want: 16 * time.Second},
		{attempt: 6, want: 30 * time.Second},
		{attempt: 100, want: 30 * time.Second},
	}

	for _, test := range tests {
		if base := rabbitMQReconnectBaseDelay(test.attempt, initial, maximum); base != test.want {
			t.Errorf("attempt %d: base delay = %s, want %s", test.attempt, base, test.want)
		}
		for range 100 {
			got := rabbitMQReconnectDelay(test.attempt, initial, maximum)
			if got < test.want/2 || got > test.want {
				t.Errorf("attempt %d: jittered delay = %s, want range [%s, %s]", test.attempt, got, test.want/2, test.want)
			}
		}
	}
}

func TestRabbitMQDeadlineConnBoundsEstablishedWrites(t *testing.T) {
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()

	conn := &rabbitMQDeadlineConn{Conn: client, writeTimeout: 50 * time.Millisecond}
	if err := conn.SetDeadline(time.Now().Add(time.Second)); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}

	startedAt := time.Now()
	_, err := conn.Write([]byte("blocked publish"))
	assertNetworkTimeout(t, err)
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("blocked write returned after %s, want bounded by write timeout", elapsed)
	}
}

func TestRabbitMQDeadlineConnPreservesHandshakeDeadline(t *testing.T) {
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()

	conn := &rabbitMQDeadlineConn{Conn: client, writeTimeout: time.Second}
	if err := conn.SetDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	startedAt := time.Now()
	_, err := conn.Write([]byte("blocked handshake"))
	assertNetworkTimeout(t, err)
	if elapsed := time.Since(startedAt); elapsed > 500*time.Millisecond {
		t.Fatalf("blocked handshake write returned after %s, want original handshake deadline", elapsed)
	}
}

func TestRabbitMQDeadlineConnPreservesEarlierEstablishedDeadline(t *testing.T) {
	client, peer := net.Pipe()
	defer client.Close()
	defer peer.Close()

	conn := &rabbitMQDeadlineConn{Conn: client, writeTimeout: 750 * time.Millisecond}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		t.Fatal(err)
	}
	if err := conn.SetDeadline(time.Now().Add(50 * time.Millisecond)); err != nil {
		t.Fatal(err)
	}

	startedAt := time.Now()
	_, err := conn.Write([]byte("blocked close"))
	assertNetworkTimeout(t, err)
	if elapsed := time.Since(startedAt); elapsed > 300*time.Millisecond {
		t.Fatalf("blocked established write returned after %s, want explicit deadline to take precedence", elapsed)
	}
}

func assertNetworkTimeout(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		t.Fatal("blocked write returned nil error")
	}
	var networkError net.Error
	if !errors.As(err, &networkError) || !networkError.Timeout() {
		t.Fatalf("write error = %v, want network timeout", err)
	}
}
