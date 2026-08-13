package sitechecker

import (
	"context"
	"errors"
	"net"
	"testing"
	"time"
)

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
