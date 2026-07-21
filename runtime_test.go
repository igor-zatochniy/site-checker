package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"sync"
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

func TestRunQueueWorkersRetriesAfterMarkQueued(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	}))
	defer target.Close()

	parsedURL, err := url.Parse(target.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, port, err := net.SplitHostPort(parsedURL.Host)
	if err != nil {
		t.Fatal(err)
	}
	portNum, err := strconv.Atoi(port)
	if err != nil {
		t.Fatal(err)
	}

	cfg := testCheckerConfig(t)
	cfg.AllowPrivateNetworks = true
	cfg.AllowedPorts = map[int]struct{}{portNum: {}}
	cfg.MaxJobAttempts = 2
	policy := NewNetworkPolicy(cfg)
	store := NewMonitorStore(policy)
	metrics := NewMetrics("test", "commit", "date", 0)
	checker := NewChecker(target.Client(), cfg, metrics)
	baseRepo := NewInMemoryMonitorRepositoryFromStore(store)
	repo := &retryOnceRepository{
		InMemoryMonitorRepository: baseRepo,
		failNextAddCheck:          true,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := NewMonitorService(repo, checker, metrics, AlertPolicy{}, logger)
	queue := NewInMemoryQueue(10, cfg.MaxJobAttempts)
	defer queue.Close()

	monitor, err := service.Create(ctx, MonitorInput{
		URL:             target.URL,
		IntervalSeconds: 60,
		TimeoutSeconds:  5,
		ExpectedStatus:  200,
	})
	if err != nil {
		t.Fatal(err)
	}

	claimed, err := service.ClaimDue(ctx, 1, time.Now().UTC(), time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if len(claimed) != 1 || claimed[0].ID != monitor.ID {
		t.Fatalf("claimed = %+v, want monitor %s", claimed, monitor.ID)
	}

	job := CheckJobMessage{
		JobID:      NewCheckJobID(monitor.ID, monitor.NextCheckAt),
		MonitorID:  monitor.ID,
		Attempt:    1,
		EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(ctx, job); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- RunQueueWorkers(ctx, service, queue, 2, time.Minute, logger)
	}()

	deadline := time.NewTimer(4 * time.Second)
	defer deadline.Stop()
	for {
		if repo.addCheckAttempts() >= 2 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("add check attempts = %d, want retry to run", repo.addCheckAttempts())
		case <-time.After(25 * time.Millisecond):
		}
	}

	for {
		checks, total, err := store.ListChecks(monitor.ID, 0, 10)
		if err != nil {
			t.Fatal(err)
		}
		if total == 1 && len(checks) == 1 {
			break
		}
		select {
		case <-deadline.C:
			t.Fatalf("check results did not persist after retry")
		case <-time.After(25 * time.Millisecond):
		}
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatalf("worker run returned error: %v", err)
	}
}

type retryOnceRepository struct {
	*InMemoryMonitorRepository
	mu               sync.Mutex
	failNextAddCheck bool
	addCheckCount    int
}

func (r *retryOnceRepository) AddCheck(ctx context.Context, record CheckRecord, alertPolicy AlertPolicy) (Monitor, error) {
	r.mu.Lock()
	r.addCheckCount++
	fail := r.failNextAddCheck
	if fail {
		r.failNextAddCheck = false
	}
	r.mu.Unlock()
	if fail {
		return Monitor{}, errors.New("transient storage failure")
	}
	return r.InMemoryMonitorRepository.AddCheck(ctx, record, alertPolicy)
}

func (r *retryOnceRepository) addCheckAttempts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.addCheckCount
}
