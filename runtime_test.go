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

func TestRunHTTPServerDoneWaitsForShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := testCheckerConfig(t)
	cfg.HealthAddr = "127.0.0.1:0"
	metrics := NewMetrics("test", "commit", "date", 0)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	checkStarted := make(chan struct{})
	releaseCheck := make(chan struct{})
	var startedOnce sync.Once

	server, done := RunHTTPServer(ctx, cfg, metrics, nil, false, []ReadinessDependency{
		{
			Name: "slow",
			Check: func(context.Context) error {
				startedOnce.Do(func() { close(checkStarted) })
				<-releaseCheck
				return nil
			},
		},
	}, logger, cancel)
	if server == nil {
		t.Fatal("HTTP server did not start")
	}

	requestDone := make(chan error, 1)
	client := &http.Client{
		Transport: &http.Transport{Proxy: nil},
		Timeout:   2 * time.Second,
	}
	go func() {
		resp, err := client.Get("http://" + server.Addr + "/readyz")
		if err != nil {
			requestDone <- err
			return
		}
		defer resp.Body.Close()
		_, _ = io.Copy(io.Discard, resp.Body)
		requestDone <- nil
	}()

	select {
	case <-checkStarted:
	case <-time.After(time.Second):
		t.Fatal("readiness request did not start")
	}

	cancel()
	select {
	case <-done:
		t.Fatal("HTTP server reported done before active request completed")
	case <-time.After(100 * time.Millisecond):
	}

	close(releaseCheck)
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("HTTP server did not finish shutdown after active request completed")
	}
	if err := <-requestDone; err != nil {
		t.Fatalf("readiness request error: %v", err)
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

func TestHandleQueueDeliveryDoesNotRetryBeforeLeaseStateChanges(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
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
	policy := NewNetworkPolicy(cfg)
	store := NewMonitorStore(policy)
	metrics := NewMetrics("test", "commit", "date", 0)
	checker := NewChecker(target.Client(), cfg, metrics)
	baseRepo := NewInMemoryMonitorRepositoryFromStore(store)
	repo := &failingLeaseTransitionRepository{
		InMemoryMonitorRepository: baseRepo,
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service := NewMonitorService(repo, checker, metrics, AlertPolicy{}, logger)

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
	var (
		deliveryMu sync.Mutex
		ackCalls   int
		nackCalls  int
	)
	delivery := QueueDelivery{
		Job:       job,
		Retryable: true,
		Ack: func(context.Context) error {
			deliveryMu.Lock()
			defer deliveryMu.Unlock()
			ackCalls++
			return nil
		},
		Nack: func(context.Context, bool) error {
			deliveryMu.Lock()
			defer deliveryMu.Unlock()
			nackCalls++
			return nil
		},
	}

	err = handleQueueDelivery(ctx, 1, service, delivery, time.Minute, logger)
	if err == nil {
		t.Fatal("expected lease transition error")
	}
	if !strings.Contains(err.Error(), "release processing lease before retry") {
		t.Fatalf("error = %q, want lease release context", err.Error())
	}
	if repo.addCheckAttempts() == 0 {
		t.Fatal("AddCheck was not attempted")
	}
	if repo.markQueuedAttempts() == 0 {
		t.Fatal("MarkQueued was not attempted")
	}

	deliveryMu.Lock()
	defer deliveryMu.Unlock()
	if ackCalls != 0 || nackCalls != 0 {
		t.Fatalf("ackCalls=%d nackCalls=%d, want no ack/nack before lease transition succeeds", ackCalls, nackCalls)
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

type failingLeaseTransitionRepository struct {
	*InMemoryMonitorRepository
	mu              sync.Mutex
	addCheckCount   int
	markQueuedCount int
}

func (r *failingLeaseTransitionRepository) AddCheck(context.Context, CheckRecord, AlertPolicy) (Monitor, error) {
	r.mu.Lock()
	r.addCheckCount++
	r.mu.Unlock()
	return Monitor{}, errors.New("transient storage failure")
}

func (r *failingLeaseTransitionRepository) MarkQueued(context.Context, string, string, time.Time) error {
	r.mu.Lock()
	r.markQueuedCount++
	r.mu.Unlock()
	return errors.New("transient lease failure")
}

func (r *failingLeaseTransitionRepository) addCheckAttempts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.addCheckCount
}

func (r *failingLeaseTransitionRepository) markQueuedAttempts() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.markQueuedCount
}
