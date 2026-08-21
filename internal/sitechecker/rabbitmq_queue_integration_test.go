//go:build integration

package sitechecker

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
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
	queueConfig := Config{
		RabbitMQURL:         fmt.Sprintf("amqp://site_checker:site_checker@%s/", endpoint),
		QueueName:           "site_checker.integration.checks",
		DeadLetterQueueName: "site_checker.integration.checks.dead",
		QueuePrefetch:       1,
		MaxJobAttempts:      2,
	}
	queue, err := NewRabbitMQQueue(queueConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()

	var topologyRecoveries atomic.Int32
	recoveryHandler := func(context.Context) error {
		topologyRecoveries.Add(1)
		return nil
	}
	if err := queue.SetTopologyLossHandler(ctx, recoveryHandler); err != nil {
		t.Fatal(err)
	}
	topologyRecoveries.Store(0)
	secondQueue, err := NewRabbitMQQueueWithTopologyLossHandler(queueConfig, recoveryHandler)
	if err != nil {
		t.Fatal(err)
	}
	defer secondQueue.Close()
	mismatchedConfig := queueConfig
	mismatchedConfig.DeadLetterQueueName += ".changed"
	mismatchedQueue, mismatchErr := NewRabbitMQQueue(mismatchedConfig)
	if mismatchErr == nil {
		_ = mismatchedQueue.Close()
		t.Fatal("RabbitMQ queue accepted incompatible dead-letter arguments")
	}

	if err := deleteRabbitQueue(queue.url, queue.queueName); err != nil {
		t.Fatal(err)
	}
	var recoveryWaitGroup sync.WaitGroup
	recoveryErrors := make(chan error, 2)
	for _, candidate := range []*RabbitMQQueue{queue, secondQueue} {
		recoveryWaitGroup.Add(1)
		go func(candidate *RabbitMQQueue) {
			defer recoveryWaitGroup.Done()
			recoveryErrors <- candidate.Ping(ctx)
		}(candidate)
	}
	recoveryWaitGroup.Wait()
	close(recoveryErrors)
	for err := range recoveryErrors {
		if err != nil {
			t.Fatalf("concurrent topology recovery: %v", err)
		}
	}
	if got := topologyRecoveries.Load(); got != 1 {
		t.Fatalf("topology recovery handlers = %d, want exactly 1", got)
	}

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

	if err := unbindRabbitDeadLetterQueue(queue.url, queue.dlqName); err != nil {
		t.Fatalf("remove only RabbitMQ dead-letter binding: %v", err)
	}
	if err := queue.Ping(brokerRestartCtx); err != nil {
		t.Fatalf("repair isolated dead-letter binding: %v", err)
	}
	bindingJob := CheckJobMessage{
		JobID:      "job_integration_repair_isolated_binding",
		MonitorID:  "mon_integration",
		Attempt:    2,
		EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(brokerRestartCtx, bindingJob); err != nil {
		t.Fatalf("publish after isolated dead-letter binding recovery: %v", err)
	}
	bindingDelivery := receiveRabbitDelivery(t, deliveries)
	if err := bindingDelivery.Nack(brokerRestartCtx, false); err != nil {
		t.Fatalf("terminal nack after isolated dead-letter binding recovery: %v", err)
	}
	bindingDeadLetter := receiveRabbitDeadLetter(t, brokerRestartCtx, queue)
	if bindingDeadLetter.JobID != bindingJob.JobID {
		t.Fatalf("dead-letter after isolated binding recovery = %q, want %q", bindingDeadLetter.JobID, bindingJob.JobID)
	}

	if err := deleteRabbitQueue(queue.url, queue.dlqName); err != nil {
		t.Fatalf("delete only RabbitMQ dead-letter queue: %v", err)
	}
	terminalJob := CheckJobMessage{
		JobID:      "job_integration_repair_isolated_dlq",
		MonitorID:  "mon_integration",
		Attempt:    2,
		EnqueuedAt: time.Now().UTC(),
	}
	if err := queue.Publish(brokerRestartCtx, terminalJob); err != nil {
		t.Fatalf("publish before isolated dead-letter recovery: %v", err)
	}
	terminalDelivery := receiveRabbitDelivery(t, deliveries)
	if err := terminalDelivery.Nack(brokerRestartCtx, false); err != nil {
		t.Fatalf("terminal nack after isolated dead-letter deletion: %v", err)
	}
	repairedDeadLetter := receiveRabbitDeadLetter(t, brokerRestartCtx, queue)
	if repairedDeadLetter.JobID != terminalJob.JobID {
		t.Fatalf("dead-letter after isolated recovery = %q, want %q", repairedDeadLetter.JobID, terminalJob.JobID)
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

	testRabbitMQHalfOpenTeardown(t, ctx, endpoint)
}

func testRabbitMQHalfOpenTeardown(t *testing.T, ctx context.Context, endpoint string) {
	t.Helper()
	proxy := newHalfOpenRabbitMQProxy(t, endpoint)
	defer proxy.Close()

	publishTimeout := 250 * time.Millisecond
	config := Config{
		RabbitMQURL:              fmt.Sprintf("amqp://site_checker:site_checker@%s/", proxy.Addr()),
		QueueName:                "site_checker.integration.half_open.publish",
		DeadLetterQueueName:      "site_checker.integration.half_open.publish.dead",
		QueuePrefetch:            1,
		MaxJobAttempts:           2,
		RabbitMQConnectTimeout:   2 * time.Second,
		RabbitMQPublishTimeout:   publishTimeout,
		RabbitMQReconnectInitial: 50 * time.Millisecond,
		RabbitMQReconnectMax:     100 * time.Millisecond,
	}
	publishQueue, err := NewRabbitMQQueue(config)
	if err != nil {
		t.Fatal(err)
	}
	defer publishQueue.Close()

	shutdownConfig := config
	shutdownConfig.QueueName = "site_checker.integration.half_open.shutdown"
	shutdownConfig.DeadLetterQueueName = "site_checker.integration.half_open.shutdown.dead"
	shutdownQueue, err := NewRabbitMQQueue(shutdownConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer shutdownQueue.Close()
	closeConfig := config
	closeConfig.QueueName = "site_checker.integration.half_open.close"
	closeConfig.DeadLetterQueueName = "site_checker.integration.half_open.close.dead"
	closeQueue, err := NewRabbitMQQueue(closeConfig)
	if err != nil {
		t.Fatal(err)
	}
	defer closeQueue.Close()

	consumeCtx, cancelConsume := context.WithCancel(ctx)
	deliveries, _, err := shutdownQueue.Consume(consumeCtx)
	if err != nil {
		cancelConsume()
		t.Fatal(err)
	}
	warmup := CheckJobMessage{JobID: "job_half_open_warmup", MonitorID: "mon_half_open", Attempt: 1, EnqueuedAt: time.Now().UTC()}
	if err := shutdownQueue.Publish(ctx, warmup); err != nil {
		cancelConsume()
		t.Fatal(err)
	}
	if err := receiveRabbitDelivery(t, deliveries).Ack(ctx); err != nil {
		cancelConsume()
		t.Fatal(err)
	}

	proxy.DropBrokerResponses()
	startedAt := time.Now()
	publishErr := publishQueue.Publish(ctx, CheckJobMessage{
		JobID:      "job_half_open_publish",
		MonitorID:  "mon_half_open",
		Attempt:    1,
		EnqueuedAt: time.Now().UTC(),
	})
	if publishErr == nil {
		t.Fatal("publish through half-open proxy returned nil error")
	}
	if elapsed := time.Since(startedAt); elapsed > 2*time.Second {
		t.Fatalf("half-open publish returned after %s, want bounded by publish timeout %s", elapsed, publishTimeout)
	}
	startedAt = time.Now()
	_ = closeQueue.Close()
	if elapsed := time.Since(startedAt); elapsed > 2*time.Second {
		t.Fatalf("half-open connection close returned after %s, want bounded by publish timeout %s", elapsed, publishTimeout)
	}

	cancelConsume()
	startedAt = time.Now()
	_ = shutdownQueue.Close()
	if elapsed := time.Since(startedAt); elapsed > 2*time.Second {
		t.Fatalf("half-open queue close returned after %s, want bounded by publish timeout %s", elapsed, publishTimeout)
	}
	select {
	case _, open := <-deliveries:
		if open {
			t.Fatal("RabbitMQ consumer produced a delivery while shutting down")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("RabbitMQ consumer did not stop after bounded queue close")
	}
}

type halfOpenRabbitMQProxy struct {
	listener    net.Listener
	target      string
	drop        atomic.Bool
	acceptDone  chan struct{}
	mu          sync.Mutex
	connections map[net.Conn]struct{}
	workers     sync.WaitGroup
}

func newHalfOpenRabbitMQProxy(t *testing.T, target string) *halfOpenRabbitMQProxy {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	proxy := &halfOpenRabbitMQProxy{
		listener:    listener,
		target:      target,
		acceptDone:  make(chan struct{}),
		connections: make(map[net.Conn]struct{}),
	}
	go proxy.accept()
	return proxy
}

func (p *halfOpenRabbitMQProxy) Addr() string {
	return p.listener.Addr().String()
}

func (p *halfOpenRabbitMQProxy) DropBrokerResponses() {
	p.drop.Store(true)
}

func (p *halfOpenRabbitMQProxy) Close() {
	_ = p.listener.Close()
	<-p.acceptDone
	p.mu.Lock()
	connections := make([]net.Conn, 0, len(p.connections))
	for connection := range p.connections {
		connections = append(connections, connection)
	}
	p.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
	p.workers.Wait()
}

func (p *halfOpenRabbitMQProxy) accept() {
	defer close(p.acceptDone)
	for {
		client, err := p.listener.Accept()
		if err != nil {
			return
		}
		upstream, err := net.DialTimeout("tcp", p.target, 2*time.Second)
		if err != nil {
			_ = client.Close()
			continue
		}
		p.track(client, upstream)
		p.workers.Add(1)
		go p.forward(client, upstream)
	}
}

func (p *halfOpenRabbitMQProxy) forward(client, upstream net.Conn) {
	defer p.workers.Done()
	var closeOnce sync.Once
	closeConnections := func() {
		closeOnce.Do(func() {
			_ = client.Close()
			_ = upstream.Close()
		})
	}
	clientDone := make(chan struct{})
	go func() {
		_, _ = io.Copy(upstream, client)
		closeConnections()
		close(clientDone)
	}()

	buffer := make([]byte, 32*1024)
	for {
		read, err := upstream.Read(buffer)
		if read > 0 && !p.drop.Load() {
			if writeErr := writeAll(client, buffer[:read]); writeErr != nil {
				break
			}
		}
		if err != nil {
			break
		}
	}
	closeConnections()
	<-clientDone
	p.untrack(client, upstream)
}

func writeAll(connection net.Conn, payload []byte) error {
	for len(payload) > 0 {
		written, err := connection.Write(payload)
		if err != nil {
			return err
		}
		payload = payload[written:]
	}
	return nil
}

func (p *halfOpenRabbitMQProxy) track(connections ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, connection := range connections {
		p.connections[connection] = struct{}{}
	}
}

func (p *halfOpenRabbitMQProxy) untrack(connections ...net.Conn) {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, connection := range connections {
		delete(p.connections, connection)
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

func unbindRabbitDeadLetterQueue(rawURL, queueName string) error {
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
	return channel.QueueUnbind(queueName, queueName, "site_checker.dlx", nil)
}
