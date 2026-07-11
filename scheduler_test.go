package main

import (
	"context"
	"io"
	"log/slog"
	"testing"
	"time"
)

func TestSchedulerDoesNotDuplicatePendingJobs(t *testing.T) {
	jobs := make(chan CheckJob, 1)
	results := make(chan CheckResult, 2)
	metrics := NewMetrics("test", "test", "test", 2)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	scheduler := NewScheduler(
		[]string{"https://a.example", "https://b.example"},
		jobs,
		results,
		10*time.Millisecond,
		metrics,
		logger,
		nil,
	)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go scheduler.Run(ctx)

	first := receiveJob(t, jobs)
	second := receiveJob(t, jobs)
	if first.URL == second.URL {
		t.Fatalf("scheduler duplicated pending URL %q", first.URL)
	}

	select {
	case duplicate := <-jobs:
		t.Fatalf("scheduler enqueued third job while both URLs are pending: %+v", duplicate)
	case <-time.After(40 * time.Millisecond):
	}
}

func TestSchedulerReschedulesAfterResultAndInterval(t *testing.T) {
	jobs := make(chan CheckJob, 1)
	results := make(chan CheckResult, 1)
	metrics := NewMetrics("test", "test", "test", 1)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	scheduler := NewScheduler(
		[]string{"https://a.example"},
		jobs,
		results,
		10*time.Millisecond,
		metrics,
		logger,
		nil,
	)

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	go scheduler.Run(ctx)

	first := receiveJob(t, jobs)
	results <- CheckResult{URL: first.URL, Healthy: true, CheckedAt: time.Now()}
	second := receiveJob(t, jobs)
	if second.URL != first.URL {
		t.Fatalf("rescheduled URL = %q, want %q", second.URL, first.URL)
	}
	if second.Sequence == first.Sequence {
		t.Fatalf("sequence was not advanced")
	}
}

func receiveJob(t *testing.T, jobs <-chan CheckJob) CheckJob {
	t.Helper()
	select {
	case job := <-jobs:
		return job
	case <-time.After(200 * time.Millisecond):
		t.Fatal("timed out waiting for job")
		return CheckJob{}
	}
}
