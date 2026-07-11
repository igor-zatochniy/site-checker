package main

import (
	"context"
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

	deliveries, err := queue.Consume(ctx)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case delivery := <-deliveries:
		if delivery.Job.JobID != job.JobID {
			t.Fatalf("job_id = %q, want %q", delivery.Job.JobID, job.JobID)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for delivery")
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

	deliveries, err := queue.Consume(ctx)
	if err != nil {
		t.Fatal(err)
	}

	first := receiveDelivery(t, deliveries)
	if err := first.Nack(ctx, true); err != nil {
		t.Fatal(err)
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
	case <-ctx.Done():
		t.Fatal("timed out waiting for dead-letter job")
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
