//go:build integration

package sitechecker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestPersistedCheckPipelineEndToEnd(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	postgresContainer, err := postgres.Run(ctx,
		"postgres:16-alpine",
		postgres.WithDatabase("site_checker"),
		postgres.WithUsername("site_checker"),
		postgres.WithPassword("site_checker"),
		postgres.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := testcontainers.TerminateContainer(postgresContainer); err != nil {
			t.Logf("failed to terminate postgres container: %v", err)
		}
	}()

	rabbitContainer, err := testcontainers.Run(ctx,
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
		if err := testcontainers.TerminateContainer(rabbitContainer); err != nil {
			t.Logf("failed to terminate rabbitmq container: %v", err)
		}
	}()

	databaseURL := postgresContainer.MustConnectionString(ctx, "sslmode=disable")
	pool, err := OpenPostgresPool(ctx, databaseURL)
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()
	if err := RunMigrations(ctx, pool); err != nil {
		t.Fatal(err)
	}

	rabbitEndpoint, err := rabbitContainer.PortEndpoint(ctx, "5672/tcp", "")
	if err != nil {
		t.Fatal(err)
	}

	var targetRequests atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		targetRequests.Add(1)
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = w.Write([]byte("temporarily unavailable"))
	}))
	defer target.Close()
	targetURL, err := url.Parse(target.URL)
	if err != nil {
		t.Fatal(err)
	}
	targetPort, err := strconv.Atoi(targetURL.Port())
	if err != nil {
		t.Fatal(err)
	}

	statusPolicy, err := ParseStatusPolicy("200-299")
	if err != nil {
		t.Fatal(err)
	}
	queueSuffix := strconv.FormatInt(time.Now().UnixNano(), 10)
	cfg := Config{
		AppEnv:                   "local",
		AppRole:                  "all",
		StorageType:              "postgres",
		DatabaseURL:              databaseURL,
		QueueType:                "rabbitmq",
		RabbitMQURL:              fmt.Sprintf("amqp://site_checker:site_checker@%s/", rabbitEndpoint),
		RabbitMQConnectTimeout:   5 * time.Second,
		RabbitMQReconnectInitial: 100 * time.Millisecond,
		RabbitMQReconnectMax:     time.Second,
		QueueName:                "site_checker.e2e." + queueSuffix + ".checks",
		DeadLetterQueueName:      "site_checker.e2e." + queueSuffix + ".checks.dead",
		QueuePrefetch:            1,
		MaxJobAttempts:           3,
		WorkerCount:              1,
		SchedulerBatchSize:       10,
		CheckLeaseTimeout:        2 * time.Second,
		MaxRedirects:             1,
		MaxBodyBytes:             64 * 1024,
		MaxHeaderBytes:           64 * 1024,
		AllowPrivateNetworks:     true,
		AllowedPorts:             map[int]struct{}{targetPort: {}},
		ExpectedStatus:           statusPolicy,
		UserAgent:                "site-checker-e2e",
	}
	policy := NewNetworkPolicy(cfg)
	repo := NewPostgresMonitorRepository(pool, policy)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics := NewMetrics("e2e", "e2e", "e2e", 0)
	checker := NewChecker(NewCheckHTTPClient(cfg, policy), cfg, metrics)
	service := NewMonitorService(repo, checker, metrics, AlertPolicy{
		Enabled:          true,
		FailureThreshold: 1,
		Cooldown:         time.Hour,
	}, logger)
	queue, err := NewRabbitMQQueueWithTopologyLossHandler(cfg, queueTopologyLossHandler(service, logger))
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	const apiKey = "e2e-api-key-with-24-characters"
	apiHandler := NewAPIHandler(service, apiKey, logger)
	apiMux := http.NewServeMux()
	apiHandler.Register(apiMux)
	apiServer := httptest.NewServer(apiMux)
	defer apiServer.Close()

	var created Monitor
	e2eAPIRequest(t, apiServer.Client(), http.MethodPost, apiServer.URL+"/api/v1/monitors", apiKey,
		fmt.Sprintf(`{"url":%q,"interval_seconds":60,"timeout_seconds":5,"expected_status":200}`, target.URL),
		http.StatusCreated, &created)
	if created.ID == "" {
		t.Fatal("created monitor ID is empty")
	}
	if _, err := pool.Exec(ctx, `
		UPDATE monitors
		SET next_check_at = now() + interval '1 hour'
		WHERE id = $1
	`, created.ID); err != nil {
		t.Fatal(err)
	}

	var receipt ManualCheckJobReceipt
	e2eAPIRequest(t, apiServer.Client(), http.MethodPost, apiServer.URL+"/api/v1/monitors/"+created.ID+"/check", apiKey,
		"", http.StatusAccepted, &receipt)
	if receipt.JobID == "" || receipt.MonitorID != created.ID {
		t.Fatalf("manual check receipt = %+v", receipt)
	}

	runCtx, stopRuntime := context.WithCancel(ctx)
	schedulerDone := make(chan struct{})
	go func() {
		defer close(schedulerDone)
		RunQueueScheduler(runCtx, service, queue, cfg, logger)
	}()
	workerDone := make(chan error, 1)
	workerStarted := false
	defer func() {
		stopRuntime()
		select {
		case <-schedulerDone:
		case <-time.After(5 * time.Second):
			t.Error("scheduler did not stop")
		}
		if workerStarted {
			select {
			case err := <-workerDone:
				if err != nil {
					t.Errorf("worker stopped with error: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Error("worker did not stop")
			}
		}
	}()

	var firstPublishedAt time.Time
	e2eEventually(t, 30*time.Second, func() (bool, error) {
		job, err := repo.GetCheckJob(ctx, receipt.JobID)
		if err != nil {
			return false, err
		}
		depth, err := rabbitQueueDepth(queue.url, queue.queueName)
		if err != nil {
			return false, err
		}
		if job.Status == checkJobStatusQueued && depth == 1 {
			firstPublishedAt = job.PublishedAt
			return true, nil
		}
		return false, nil
	})

	// A confirmed queued job must not be replayed merely because no worker is
	// available. Its RabbitMQ message remains the single delivery authority.
	time.Sleep(3*cfg.CheckLeaseTimeout + 1500*time.Millisecond)
	queuedJob, err := repo.GetCheckJob(ctx, receipt.JobID)
	if err != nil {
		t.Fatal(err)
	}
	depth, err := rabbitQueueDepth(queue.url, queue.queueName)
	if err != nil {
		t.Fatal(err)
	}
	if queuedJob.Status != checkJobStatusQueued || depth != 1 || targetRequests.Load() != 0 {
		t.Fatalf("idle queue state = status=%s depth=%d target_requests=%d, want queued/1/0",
			queuedJob.Status, depth, targetRequests.Load())
	}
	var activeJobs int
	if err := pool.QueryRow(ctx, `
		SELECT count(*)
		FROM check_jobs
		WHERE monitor_id = $1
			AND status IN ('scheduled', 'queued', 'processing', 'failed')
	`, created.ID).Scan(&activeJobs); err != nil {
		t.Fatal(err)
	}
	if activeJobs != 1 {
		t.Fatalf("active jobs = %d, want 1", activeJobs)
	}

	if err := deleteRabbitQueue(queue.url, queue.queueName); err != nil {
		t.Fatal(err)
	}
	e2eEventually(t, 30*time.Second, func() (bool, error) {
		job, err := repo.GetCheckJob(ctx, receipt.JobID)
		if err != nil {
			return false, err
		}
		depth, err := rabbitQueueDepth(queue.url, queue.queueName)
		if err != nil {
			return false, err
		}
		return job.Status == checkJobStatusQueued &&
			job.PublishedAt.After(firstPublishedAt) && depth == 1, nil
	})

	workerStarted = true
	go func() {
		workerDone <- RunQueueWorkers(runCtx, service, queue, 1, cfg.CheckLeaseTimeout, logger)
	}()

	e2eEventually(t, 30*time.Second, func() (bool, error) {
		job, err := repo.GetCheckJob(ctx, receipt.JobID)
		if err != nil {
			return false, err
		}
		return job.Status == checkJobStatusCompleted, nil
	})

	var resultCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM check_results WHERE job_id = $1", receipt.JobID).Scan(&resultCount); err != nil {
		t.Fatal(err)
	}
	if resultCount != 1 {
		t.Fatalf("check result count = %d, want 1", resultCount)
	}

	var history struct {
		Items []CheckRecord `json:"items"`
		Total int           `json:"total"`
	}
	e2eAPIRequest(t, apiServer.Client(), http.MethodGet,
		apiServer.URL+"/api/v1/monitors/"+created.ID+"/checks?limit=10", apiKey, "", http.StatusOK, &history)
	if history.Total != 1 || len(history.Items) != 1 || history.Items[0].JobID != receipt.JobID {
		t.Fatalf("check history = %+v, want one persisted result", history)
	}

	incidents, incidentTotal, err := repo.ListIncidents(ctx, incidentStatusOpen, 0, 10)
	if err != nil {
		t.Fatal(err)
	}
	if incidentTotal != 1 || len(incidents) != 1 || incidents[0].MonitorID != created.ID {
		t.Fatalf("open incidents = %+v total=%d, want one", incidents, incidentTotal)
	}
	var outboxCount int
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM alert_outbox WHERE monitor_id = $1", created.ID).Scan(&outboxCount); err != nil {
		t.Fatal(err)
	}
	if outboxCount != 1 {
		t.Fatalf("alert outbox count = %d, want 1", outboxCount)
	}

	if err := queue.Publish(ctx, CheckJobMessage{
		JobID:      receipt.JobID,
		MonitorID:  created.ID,
		Attempt:    1,
		EnqueuedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	time.Sleep(1500 * time.Millisecond)
	if err := pool.QueryRow(ctx, "SELECT count(*) FROM check_results WHERE job_id = $1", receipt.JobID).Scan(&resultCount); err != nil {
		t.Fatal(err)
	}
	if resultCount != 1 || targetRequests.Load() != 1 {
		t.Fatalf("duplicate delivery produced results=%d target_requests=%d, want 1 and 1", resultCount, targetRequests.Load())
	}
}

func rabbitQueueDepth(rawURL, queueName string) (int, error) {
	connection, err := amqp.Dial(rawURL)
	if err != nil {
		return 0, err
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		return 0, err
	}
	defer channel.Close()
	queue, err := channel.QueueInspect(queueName)
	if err != nil {
		if isRabbitMQNotFound(err) {
			return -1, nil
		}
		return 0, err
	}
	return queue.Messages, nil
}

func deleteRabbitQueue(rawURL, queueName string) error {
	connection, err := amqp.Dial(rawURL)
	if err != nil {
		return err
	}
	defer connection.Close()
	channel, err := connection.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()
	_, err = channel.QueueDelete(queueName, false, false, false)
	return err
}

func e2eAPIRequest(
	t *testing.T,
	client *http.Client,
	method, endpoint, apiKey, body string,
	wantStatus int,
	destination any,
) {
	t.Helper()
	req, err := http.NewRequest(method, endpoint, bytes.NewBufferString(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("X-API-Key", apiKey)
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != wantStatus {
		responseBody, _ := io.ReadAll(resp.Body)
		t.Fatalf("%s %s status = %d, want %d: %s", method, endpoint, resp.StatusCode, wantStatus, responseBody)
	}
	if destination != nil {
		if err := json.NewDecoder(resp.Body).Decode(destination); err != nil {
			t.Fatal(err)
		}
	}
}

func e2eEventually(t *testing.T, timeout time.Duration, condition func() (bool, error)) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		ok, err := condition()
		if err != nil {
			t.Fatal(err)
		}
		if ok {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("condition was not met within %s", timeout)
}
