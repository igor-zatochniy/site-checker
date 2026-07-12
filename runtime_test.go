package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"
)

type stoppedConsumerQueue struct {
	deliveries chan QueueDelivery
	errs       chan error
	err        error
}

func newStoppedConsumerQueue(err error) *stoppedConsumerQueue {
	return &stoppedConsumerQueue{
		deliveries: make(chan QueueDelivery),
		errs:       make(chan error),
		err:        err,
	}
}

func (q *stoppedConsumerQueue) Ping(context.Context) error {
	return nil
}

func (q *stoppedConsumerQueue) Publish(context.Context, CheckJobMessage) error {
	return nil
}

func (q *stoppedConsumerQueue) Consume(context.Context) (<-chan QueueDelivery, <-chan error, error) {
	go func() {
		q.errs <- q.err
		close(q.deliveries)
	}()
	return q.deliveries, q.errs, nil
}

func (q *stoppedConsumerQueue) Close() error {
	return nil
}

func TestRunQueueWorkersReturnsErrorWhenConsumerStops(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	queueErr := errors.New("rabbitmq channel closed")
	queue := newStoppedConsumerQueue(queueErr)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	err := RunQueueWorkers(ctx, nil, queue, 2, time.Minute, logger)
	if err == nil {
		t.Fatal("expected worker lifecycle error")
	}
	if !errors.Is(err, queueErr) {
		t.Fatalf("error does not wrap queue error: %v", err)
	}
	if !strings.Contains(err.Error(), "queue consumer stopped") {
		t.Fatalf("error = %q, want queue consumer stopped context", err.Error())
	}
}
