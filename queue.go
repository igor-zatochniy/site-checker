package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrQueueFull           = errors.New("job queue is full")
	ErrQueueConsumerClosed = errors.New("queue consumer closed unexpectedly")
)

type CheckJobMessage struct {
	JobID      string    `json:"job_id"`
	MonitorID  string    `json:"monitor_id"`
	Attempt    int       `json:"attempt"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

type QueueDelivery struct {
	Job  CheckJobMessage
	Ack  func(ctx context.Context) error
	Nack func(ctx context.Context, requeue bool) error
}

type JobQueue interface {
	Ping(ctx context.Context) error
	Publish(ctx context.Context, job CheckJobMessage) error
	Consume(ctx context.Context) (<-chan QueueDelivery, <-chan error, error)
	Close() error
}

type InMemoryQueue struct {
	jobs        chan CheckJobMessage
	deadLetters chan CheckJobMessage
	maxAttempts int

	mu     sync.Mutex
	seen   map[string]struct{}
	closed bool
}

func NewInMemoryQueue(bufferSize, maxAttempts int) *InMemoryQueue {
	return &InMemoryQueue{
		jobs:        make(chan CheckJobMessage, bufferSize),
		deadLetters: make(chan CheckJobMessage, bufferSize),
		maxAttempts: maxAttempts,
		seen:        make(map[string]struct{}),
	}
}

func (q *InMemoryQueue) Ping(context.Context) error {
	return nil
}

func (q *InMemoryQueue) Publish(ctx context.Context, job CheckJobMessage) error {
	if job.JobID == "" {
		job.JobID = NewCheckJobID(job.MonitorID, job.EnqueuedAt)
	}
	if job.Attempt == 0 {
		job.Attempt = 1
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return context.Canceled
	}
	if _, exists := q.seen[job.JobID]; exists {
		q.mu.Unlock()
		return nil
	}
	q.seen[job.JobID] = struct{}{}
	q.mu.Unlock()

	if err := q.enqueue(ctx, job); err != nil {
		q.mu.Lock()
		delete(q.seen, job.JobID)
		q.mu.Unlock()
		return err
	}
	return nil
}

func (q *InMemoryQueue) Consume(ctx context.Context) (<-chan QueueDelivery, <-chan error, error) {
	deliveries := make(chan QueueDelivery)
	consumerErrors := make(chan error, 1)
	go func() {
		defer close(deliveries)
		for {
			select {
			case <-ctx.Done():
				return
			case job, ok := <-q.jobs:
				if !ok {
					if ctx.Err() == nil {
						consumerErrors <- ErrQueueConsumerClosed
					}
					return
				}
				delivery := QueueDelivery{
					Job: job,
				}
				delivery.Ack = func(context.Context) error {
					q.forget(job.JobID)
					return nil
				}
				delivery.Nack = func(ctx context.Context, requeue bool) error {
					if requeue && job.Attempt < q.maxAttempts {
						next := job
						next.Attempt++
						if err := q.enqueue(ctx, next); err != nil {
							q.forget(job.JobID)
							return err
						}
						return nil
					}
					defer q.forget(job.JobID)
					select {
					case q.deadLetters <- job:
					default:
					}
					return nil
				}
				select {
				case <-ctx.Done():
					return
				case deliveries <- delivery:
				}
			}
		}
	}()
	return deliveries, consumerErrors, nil
}

func (q *InMemoryQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	if !q.closed {
		q.closed = true
		close(q.jobs)
	}
	return nil
}

func (q *InMemoryQueue) forget(jobID string) {
	q.mu.Lock()
	defer q.mu.Unlock()
	delete(q.seen, jobID)
}

func (q *InMemoryQueue) enqueue(ctx context.Context, job CheckJobMessage) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case q.jobs <- job:
		return nil
	default:
		return ErrQueueFull
	}
}

type RabbitMQQueue struct {
	url              string
	queueName        string
	dlqName          string
	prefetch         int
	maxAttempts      int
	connectTimeout   time.Duration
	reconnectInitial time.Duration
	reconnectMax     time.Duration
	dial             rabbitMQDialFunc

	mu            sync.RWMutex
	publishMu     sync.Mutex
	reconnectGate chan struct{}
	conn          *amqp.Connection
	channel       *amqp.Channel
	closed        bool
	done          chan struct{}
}

type rabbitMQDialFunc func(ctx context.Context, rawURL string, timeout time.Duration) (*amqp.Connection, error)

func NewRabbitMQQueue(cfg Config) (*RabbitMQQueue, error) {
	queue := &RabbitMQQueue{
		url:              cfg.RabbitMQURL,
		queueName:        cfg.QueueName,
		dlqName:          cfg.DeadLetterQueueName,
		prefetch:         cfg.QueuePrefetch,
		maxAttempts:      cfg.MaxJobAttempts,
		connectTimeout:   durationOrDefault(cfg.RabbitMQConnectTimeout, defaultRabbitMQConnectTimeout),
		reconnectInitial: durationOrDefault(cfg.RabbitMQReconnectInitial, defaultRabbitMQReconnectInitial),
		reconnectMax:     durationOrDefault(cfg.RabbitMQReconnectMax, defaultRabbitMQReconnectMax),
		dial:             dialRabbitMQ,
		reconnectGate:    make(chan struct{}, 1),
		done:             make(chan struct{}),
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), queue.connectTimeout)
	defer cancel()
	if err := queue.connectOnce(connectCtx); err != nil {
		return nil, err
	}
	return queue, nil
}

func dialRabbitMQ(ctx context.Context, rawURL string, timeout time.Duration) (*amqp.Connection, error) {
	dialer := &net.Dialer{Timeout: timeout}
	return amqp.DialConfig(rawURL, amqp.Config{
		Dial: func(network, address string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			deadline := time.Now().Add(timeout)
			if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
				deadline = ctxDeadline
			}
			if err := conn.SetDeadline(deadline); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return conn, nil
		},
	})
}

func (q *RabbitMQQueue) Ping(ctx context.Context) error {
	if err := q.ensureConnected(ctx); err != nil {
		return err
	}
	conn := q.connection()
	if conn == nil {
		return ErrQueueConsumerClosed
	}
	channel, err := conn.Channel()
	if err != nil {
		q.invalidateConnection(conn)
		return err
	}
	defer channel.Close()
	_, err = channel.QueueDeclarePassive(q.queueName, true, false, false, false, nil)
	return err
}

func (q *RabbitMQQueue) declareTopology(channel *amqp.Channel) error {
	if err := channel.ExchangeDeclare(
		"site_checker.dlx",
		"direct",
		true,
		false,
		false,
		false,
		nil,
	); err != nil {
		return err
	}

	if _, err := channel.QueueDeclare(q.dlqName, true, false, false, false, nil); err != nil {
		return err
	}
	if err := channel.QueueBind(q.dlqName, q.dlqName, "site_checker.dlx", false, nil); err != nil {
		return err
	}

	_, err := channel.QueueDeclare(q.queueName, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    "site_checker.dlx",
		"x-dead-letter-routing-key": q.dlqName,
	})
	return err
}

func (q *RabbitMQQueue) Publish(ctx context.Context, job CheckJobMessage) error {
	if job.JobID == "" {
		job.JobID = NewCheckJobID(job.MonitorID, job.EnqueuedAt)
	}
	if job.Attempt == 0 {
		job.Attempt = 1
	}
	if job.EnqueuedAt.IsZero() {
		job.EnqueuedAt = time.Now().UTC()
	}

	body, err := json.Marshal(job)
	if err != nil {
		return err
	}

	for attempt := 1; ; attempt++ {
		if err := q.ensureConnected(ctx); err != nil {
			return err
		}
		q.publishMu.Lock()
		channel := q.publishChannel()
		if channel == nil {
			q.publishMu.Unlock()
			continue
		}
		err := channel.PublishWithContext(ctx, "", q.queueName, false, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    job.JobID,
			Timestamp:    job.EnqueuedAt,
			Body:         body,
		})
		q.publishMu.Unlock()
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		q.invalidatePublishChannel(channel)
		delay := rabbitMQReconnectDelay(attempt, q.reconnectInitial, q.reconnectMax)
		slog.Warn("RabbitMQ publish failed; reconnecting", "attempt", attempt, "retry_in", delay, "error", err)
		if err := q.waitForReconnect(ctx, delay); err != nil {
			return err
		}
	}
}

func (q *RabbitMQQueue) Consume(ctx context.Context) (<-chan QueueDelivery, <-chan error, error) {
	if q.isClosed() {
		return nil, nil, context.Canceled
	}

	deliveries := make(chan QueueDelivery)
	consumerErrors := make(chan error, 1)
	go func() {
		defer close(deliveries)
		for attempt := 1; ; attempt++ {
			startedAt := time.Now()
			err := q.consumeSession(ctx, deliveries)
			if ctx.Err() != nil || q.isClosed() {
				return
			}
			if time.Since(startedAt) >= q.reconnectMax {
				attempt = 1
			}
			delay := rabbitMQReconnectDelay(attempt, q.reconnectInitial, q.reconnectMax)
			slog.Warn("RabbitMQ consumer interrupted; reconnecting", "retry_in", delay, "error", err)
			if err := q.waitForReconnect(ctx, delay); err != nil {
				return
			}
		}
	}()
	return deliveries, consumerErrors, nil
}

func (q *RabbitMQQueue) consumeSession(ctx context.Context, deliveries chan<- QueueDelivery) error {
	if err := q.ensureConnected(ctx); err != nil {
		return err
	}
	conn := q.connection()
	if conn == nil {
		return ErrQueueConsumerClosed
	}
	channel, err := conn.Channel()
	if err != nil {
		q.invalidateConnection(conn)
		return err
	}
	defer channel.Close()
	if err := channel.Qos(q.prefetch, 0, false); err != nil {
		return err
	}
	rawDeliveries, err := channel.Consume(q.queueName, "", false, false, false, false, nil)
	if err != nil {
		return err
	}
	channelClosed := channel.NotifyClose(make(chan *amqp.Error, 1))

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.done:
			return context.Canceled
		case closeErr, ok := <-channelClosed:
			if ok && closeErr != nil {
				return fmt.Errorf("rabbitmq consumer channel closed: %w", closeErr)
			}
			return ErrQueueConsumerClosed
		case delivery, ok := <-rawDeliveries:
			if !ok {
				return ErrQueueConsumerClosed
			}
			msg := delivery
			job, err := decodeJobMessage(msg.Body)
			if err != nil {
				_ = msg.Nack(false, false)
				continue
			}

			queueDelivery := QueueDelivery{Job: job}
			queueDelivery.Ack = func(context.Context) error {
				return msg.Ack(false)
			}
			queueDelivery.Nack = func(ctx context.Context, requeue bool) error {
				if requeue && job.Attempt < q.maxAttempts {
					next := job
					next.Attempt++
					next.EnqueuedAt = time.Now().UTC()
					if err := q.Publish(ctx, next); err != nil {
						return errors.Join(err, msg.Nack(false, true))
					}
					return msg.Ack(false)
				}
				return msg.Nack(false, false)
			}

			select {
			case <-ctx.Done():
				_ = msg.Nack(false, true)
				return ctx.Err()
			case <-q.done:
				_ = msg.Nack(false, true)
				return context.Canceled
			case deliveries <- queueDelivery:
			}
		}
	}
}

func (q *RabbitMQQueue) ensureConnected(ctx context.Context) error {
	for attempt := 1; ; attempt++ {
		if q.isConnected() {
			return nil
		}
		if q.isClosed() {
			return context.Canceled
		}

		connectCtx, cancel := context.WithTimeout(ctx, q.connectTimeout)
		err := q.connectOnce(connectCtx)
		cancel()
		if err == nil {
			if attempt > 1 {
				slog.Info("RabbitMQ connection restored", "attempt", attempt)
			}
			return nil
		}
		if ctx.Err() != nil {
			return ctx.Err()
		}
		delay := rabbitMQReconnectDelay(attempt, q.reconnectInitial, q.reconnectMax)
		slog.Warn("RabbitMQ reconnect failed", "attempt", attempt, "retry_in", delay, "error", err)
		if err := q.waitForReconnect(ctx, delay); err != nil {
			return err
		}
	}
}

func (q *RabbitMQQueue) connectOnce(ctx context.Context) error {
	if err := q.acquireReconnect(ctx); err != nil {
		return err
	}
	defer q.releaseReconnect()
	if q.isConnected() {
		return nil
	}
	if q.isClosed() {
		return context.Canceled
	}

	conn, err := q.dial(ctx, q.url, q.connectTimeout)
	if err != nil {
		return err
	}
	channel, err := conn.Channel()
	if err != nil {
		_ = conn.Close()
		return err
	}
	if err := q.declareTopology(channel); err != nil {
		_ = channel.Close()
		_ = conn.Close()
		return err
	}

	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		_ = channel.Close()
		_ = conn.Close()
		return context.Canceled
	}
	oldConn := q.conn
	q.conn = conn
	q.channel = channel
	q.mu.Unlock()
	if oldConn != nil && oldConn != conn {
		_ = oldConn.Close()
	}
	return nil
}

func (q *RabbitMQQueue) acquireReconnect(ctx context.Context) error {
	select {
	case q.reconnectGate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-q.done:
		return context.Canceled
	}
}

func (q *RabbitMQQueue) releaseReconnect() {
	<-q.reconnectGate
}

func (q *RabbitMQQueue) waitForReconnect(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-q.done:
		return context.Canceled
	case <-timer.C:
		return nil
	}
}

func (q *RabbitMQQueue) isConnected() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return !q.closed && q.conn != nil && !q.conn.IsClosed() && q.channel != nil && !q.channel.IsClosed()
}

func (q *RabbitMQQueue) isClosed() bool {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.closed
}

func (q *RabbitMQQueue) connection() *amqp.Connection {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.conn
}

func (q *RabbitMQQueue) publishChannel() *amqp.Channel {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.channel
}

func (q *RabbitMQQueue) invalidatePublishChannel(channel *amqp.Channel) {
	q.mu.Lock()
	if q.channel != channel {
		q.mu.Unlock()
		return
	}
	conn := q.conn
	q.conn = nil
	q.channel = nil
	q.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

func (q *RabbitMQQueue) invalidateConnection(conn *amqp.Connection) {
	q.mu.Lock()
	if q.conn != conn {
		q.mu.Unlock()
		return
	}
	q.conn = nil
	q.channel = nil
	q.mu.Unlock()
	_ = conn.Close()
}

func (q *RabbitMQQueue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	q.closed = true
	close(q.done)
	conn := q.conn
	q.conn = nil
	q.channel = nil
	q.mu.Unlock()
	if conn == nil || conn.IsClosed() {
		return nil
	}
	return conn.Close()
}

func rabbitMQReconnectDelay(attempt int, initial, maximum time.Duration) time.Duration {
	if attempt < 1 {
		attempt = 1
	}
	delay := initial
	for i := 1; i < attempt && delay < maximum; i++ {
		if delay > maximum/2 {
			return maximum
		}
		delay *= 2
	}
	if delay > maximum {
		return maximum
	}
	return delay
}

func durationOrDefault(value, fallback time.Duration) time.Duration {
	if value <= 0 {
		return fallback
	}
	return value
}

func decodeJobMessage(body []byte) (CheckJobMessage, error) {
	var job CheckJobMessage
	if err := json.Unmarshal(body, &job); err != nil {
		return CheckJobMessage{}, err
	}
	if job.MonitorID == "" {
		return CheckJobMessage{}, fmt.Errorf("monitor_id is required")
	}
	if job.JobID == "" {
		job.JobID = NewCheckJobID(job.MonitorID, job.EnqueuedAt)
	}
	if job.Attempt == 0 {
		job.Attempt = 1
	}
	return job, nil
}

func NewCheckJobID(monitorID string, nextCheckAt time.Time) string {
	if nextCheckAt.IsZero() {
		nextCheckAt = time.Now().UTC()
	}
	return fmt.Sprintf("job_%s_%d", monitorID, nextCheckAt.UTC().UnixNano())
}
