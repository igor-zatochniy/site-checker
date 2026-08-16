package sitechecker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net"
	"sync"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

var (
	ErrQueueFull           = errors.New("job queue is full")
	ErrQueueConsumerClosed = errors.New("queue consumer closed unexpectedly")
	ErrRabbitMQReturned    = errors.New("rabbitmq returned unroutable message")
	ErrRabbitMQNack        = errors.New("rabbitmq publish was negatively acknowledged")
)

type CheckJobMessage struct {
	JobID      string    `json:"job_id"`
	MonitorID  string    `json:"monitor_id"`
	Attempt    int       `json:"attempt"`
	EnqueuedAt time.Time `json:"enqueued_at"`
}

type QueueDelivery struct {
	Job       CheckJobMessage
	Retryable bool
	Ack       func(ctx context.Context) error
	Nack      func(ctx context.Context, requeue bool) error
	Requeue   func(ctx context.Context) error
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
					Job:       job,
					Retryable: job.Attempt < q.maxAttempts,
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
				delivery.Requeue = func(ctx context.Context) error {
					if err := q.enqueue(ctx, job); err != nil {
						q.forget(job.JobID)
						return err
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
	publishTimeout   time.Duration
	reconnectInitial time.Duration
	reconnectMax     time.Duration
	dial             rabbitMQDialFunc

	mu            sync.RWMutex
	publishMu     sync.Mutex
	reconnectGate chan struct{}
	conn          *amqp.Connection
	channel       *amqp.Channel
	confirms      <-chan amqp.Confirmation
	returns       <-chan amqp.Return
	publishClosed <-chan *amqp.Error
	closed        bool
	done          chan struct{}

	topologyMu                  sync.Mutex
	topologyRecoveryMu          sync.Mutex
	topologyLossGeneration      uint64
	recoveredTopologyGeneration uint64
	topologyLossHandler         func(context.Context) error
}

type rabbitMQDialFunc func(ctx context.Context, rawURL string, connectTimeout, writeTimeout time.Duration) (*amqp.Connection, error)

type rabbitMQDeadlineConn struct {
	net.Conn
	writeTimeout      time.Duration
	mu                sync.RWMutex
	handshakeDeadline time.Time
	established       bool
}

type rabbitMQPublishSession struct {
	channel  *amqp.Channel
	confirms <-chan amqp.Confirmation
	returns  <-chan amqp.Return
	closed   <-chan *amqp.Error
}

func NewRabbitMQQueue(cfg Config) (*RabbitMQQueue, error) {
	return newRabbitMQQueue(cfg, nil)
}

func NewRabbitMQQueueWithTopologyLossHandler(cfg Config, handler func(context.Context) error) (*RabbitMQQueue, error) {
	if handler == nil {
		return nil, errors.New("rabbitmq topology loss handler must not be nil")
	}
	return newRabbitMQQueue(cfg, handler)
}

func newRabbitMQQueue(cfg Config, handler func(context.Context) error) (*RabbitMQQueue, error) {
	queue := &RabbitMQQueue{
		url:                 cfg.RabbitMQURL,
		queueName:           cfg.QueueName,
		dlqName:             cfg.DeadLetterQueueName,
		prefetch:            cfg.QueuePrefetch,
		maxAttempts:         cfg.MaxJobAttempts,
		connectTimeout:      durationOrDefault(cfg.RabbitMQConnectTimeout, defaultRabbitMQConnectTimeout),
		publishTimeout:      durationOrDefault(cfg.RabbitMQPublishTimeout, defaultRabbitMQPublishTimeout),
		reconnectInitial:    durationOrDefault(cfg.RabbitMQReconnectInitial, defaultRabbitMQReconnectInitial),
		reconnectMax:        durationOrDefault(cfg.RabbitMQReconnectMax, defaultRabbitMQReconnectMax),
		dial:                dialRabbitMQ,
		reconnectGate:       make(chan struct{}, 1),
		done:                make(chan struct{}),
		topologyLossHandler: handler,
	}
	connectCtx, cancel := context.WithTimeout(context.Background(), queue.connectTimeout)
	defer cancel()
	if err := queue.connectOnce(connectCtx); err != nil {
		return nil, err
	}
	return queue, nil
}

func (q *RabbitMQQueue) SetTopologyLossHandler(ctx context.Context, handler func(context.Context) error) error {
	if handler == nil {
		return errors.New("rabbitmq topology loss handler must not be nil")
	}
	q.topologyMu.Lock()
	q.topologyLossHandler = handler
	q.topologyMu.Unlock()
	return q.recoverDetectedTopologyLoss(ctx)
}

func (q *RabbitMQQueue) recordTopologyLoss(ctx context.Context) error {
	q.topologyMu.Lock()
	q.topologyLossGeneration++
	q.topologyMu.Unlock()
	return q.recoverDetectedTopologyLoss(ctx)
}

func (q *RabbitMQQueue) recoverDetectedTopologyLoss(ctx context.Context) error {
	q.topologyRecoveryMu.Lock()
	defer q.topologyRecoveryMu.Unlock()

	for {
		q.topologyMu.Lock()
		generation := q.topologyLossGeneration
		if generation <= q.recoveredTopologyGeneration {
			q.topologyMu.Unlock()
			return nil
		}
		handler := q.topologyLossHandler
		q.topologyMu.Unlock()
		if handler == nil {
			return nil
		}
		if err := handler(ctx); err != nil {
			return err
		}

		q.topologyMu.Lock()
		if generation > q.recoveredTopologyGeneration {
			q.recoveredTopologyGeneration = generation
		}
		q.topologyMu.Unlock()
	}
}

func dialRabbitMQ(ctx context.Context, rawURL string, connectTimeout, writeTimeout time.Duration) (*amqp.Connection, error) {
	dialer := &net.Dialer{Timeout: connectTimeout}
	return amqp.DialConfig(rawURL, amqp.Config{
		Dial: func(network, address string) (net.Conn, error) {
			conn, err := dialer.DialContext(ctx, network, address)
			if err != nil {
				return nil, err
			}
			deadlineConn := &rabbitMQDeadlineConn{Conn: conn, writeTimeout: writeTimeout}
			deadline := time.Now().Add(connectTimeout)
			if ctxDeadline, ok := ctx.Deadline(); ok && ctxDeadline.Before(deadline) {
				deadline = ctxDeadline
			}
			if err := deadlineConn.SetDeadline(deadline); err != nil {
				_ = conn.Close()
				return nil, err
			}
			return deadlineConn, nil
		},
	})
}

func (c *rabbitMQDeadlineConn) SetDeadline(deadline time.Time) error {
	c.mu.Lock()
	if deadline.IsZero() {
		c.established = true
		c.handshakeDeadline = time.Time{}
	} else if !c.established {
		c.handshakeDeadline = deadline
	}
	c.mu.Unlock()
	return c.Conn.SetDeadline(deadline)
}

func (c *rabbitMQDeadlineConn) Write(payload []byte) (int, error) {
	c.mu.RLock()
	established := c.established
	handshakeDeadline := c.handshakeDeadline
	c.mu.RUnlock()

	deadline := handshakeDeadline
	if established {
		deadline = time.Now().Add(c.writeTimeout)
	}
	if !deadline.IsZero() {
		if err := c.Conn.SetWriteDeadline(deadline); err != nil {
			return 0, err
		}
	}
	return c.Conn.Write(payload)
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
	if _, err = channel.QueueDeclarePassive(q.queueName, true, false, false, false, nil); err != nil {
		passiveErr := err
		if err := q.repairTopology(ctx); err != nil {
			return errors.Join(passiveErr, fmt.Errorf("repair rabbitmq topology: %w", err))
		}
		return nil
	}
	if err = channel.ExchangeDeclarePassive("site_checker.dlx", "direct", true, false, false, false, nil); err != nil {
		passiveErr := err
		if err := q.repairTopology(ctx); err != nil {
			return errors.Join(passiveErr, fmt.Errorf("repair rabbitmq dead-letter exchange: %w", err))
		}
		return nil
	}
	if _, err = channel.QueueDeclarePassive(q.dlqName, true, false, false, false, nil); err != nil {
		passiveErr := err
		if err := q.repairTopology(ctx); err != nil {
			return errors.Join(passiveErr, fmt.Errorf("repair rabbitmq dead-letter queue: %w", err))
		}
		return nil
	}
	return q.recoverDetectedTopologyLoss(ctx)
}

func (q *RabbitMQQueue) declareTopology(channel *amqp.Channel) error {
	if err := q.declareDeadLetterTopology(channel); err != nil {
		return err
	}
	_, err := channel.QueueDeclare(q.queueName, true, false, false, false, amqp.Table{
		"x-dead-letter-exchange":    "site_checker.dlx",
		"x-dead-letter-routing-key": q.dlqName,
	})
	return err
}

func (q *RabbitMQQueue) declareDeadLetterTopology(channel *amqp.Channel) error {
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
	return nil
}

func (q *RabbitMQQueue) repairTopology(ctx context.Context) error {
	if err := q.acquireReconnect(ctx); err != nil {
		return err
	}
	defer q.releaseReconnect()
	if q.isClosed() {
		return context.Canceled
	}

	conn := q.connection()
	if conn == nil || conn.IsClosed() {
		return ErrQueueConsumerClosed
	}
	if err := q.repairTopologyOnConnection(ctx, conn); err != nil {
		if conn.IsClosed() {
			q.invalidateConnection(conn)
		}
		return err
	}
	return nil
}

func (q *RabbitMQQueue) repairTopologyOnConnection(ctx context.Context, conn *amqp.Connection) error {
	channel, err := conn.Channel()
	if err != nil {
		return err
	}
	if _, err := channel.QueueDeclarePassive(q.queueName, true, false, false, false, nil); err != nil {
		if !isRabbitMQNotFound(err) {
			_ = channel.Close()
			return err
		}
		_ = channel.Close()
	} else {
		defer channel.Close()
		return q.declareDeadLetterTopology(channel)
	}

	releaseLock, err := q.acquireTopologyRecoveryLock(ctx)
	if err != nil {
		return err
	}
	defer releaseLock()

	channel, err = conn.Channel()
	if err != nil {
		return err
	}
	if _, err := channel.QueueDeclarePassive(q.queueName, true, false, false, false, nil); err == nil {
		defer channel.Close()
		return q.declareDeadLetterTopology(channel)
	} else if !isRabbitMQNotFound(err) {
		_ = channel.Close()
		return err
	}
	_ = channel.Close()
	channel, err = conn.Channel()
	if err != nil {
		return err
	}
	defer channel.Close()

	q.topologyMu.Lock()
	hasHandler := q.topologyLossHandler != nil
	q.topologyMu.Unlock()
	if hasHandler {
		if err := q.recordTopologyLoss(ctx); err != nil {
			return fmt.Errorf("recover queued jobs before restoring rabbitmq topology: %w", err)
		}
	}
	if err := q.declareTopology(channel); err != nil {
		return err
	}
	if !hasHandler {
		if err := q.recordTopologyLoss(ctx); err != nil {
			return fmt.Errorf("record rabbitmq topology loss: %w", err)
		}
	}
	return nil
}

func (q *RabbitMQQueue) acquireTopologyRecoveryLock(ctx context.Context) (func(), error) {
	lockName := q.queueName + ".topology-recovery-lock"
	for {
		connection, err := q.dial(ctx, q.url, q.connectTimeout, q.publishTimeout)
		if err != nil {
			return nil, err
		}
		channel, err := connection.Channel()
		if err != nil {
			_ = connection.Close()
			return nil, err
		}
		_, err = channel.QueueDeclare(lockName, false, true, true, false, nil)
		if err == nil {
			return func() {
				_ = channel.Close()
				_ = connection.Close()
			}, nil
		}
		_ = channel.Close()
		_ = connection.Close()
		var rabbitErr *amqp.Error
		if !errors.As(err, &rabbitErr) || rabbitErr.Code != 405 {
			return nil, err
		}
		if err := q.waitForReconnect(ctx, 50*time.Millisecond); err != nil {
			return nil, err
		}
	}
}

func isRabbitMQNotFound(err error) bool {
	var rabbitErr *amqp.Error
	return errors.As(err, &rabbitErr) && rabbitErr.Code == 404
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
	publishCtx, cancel := context.WithTimeout(ctx, q.publishTimeout)
	defer cancel()
	topologyRepaired := false

	for attempt := 1; ; attempt++ {
		if err := q.ensureConnected(publishCtx); err != nil {
			return err
		}
		if err := q.recoverDetectedTopologyLoss(publishCtx); err != nil {
			return fmt.Errorf("recover queued jobs before publish: %w", err)
		}
		q.publishMu.Lock()
		session := q.publishSession()
		if session.channel == nil {
			q.publishMu.Unlock()
			continue
		}
		err := session.channel.PublishWithContext(publishCtx, "", q.queueName, true, false, amqp.Publishing{
			ContentType:  "application/json",
			DeliveryMode: amqp.Persistent,
			MessageId:    job.JobID,
			Timestamp:    job.EnqueuedAt,
			Body:         body,
		})
		if err == nil {
			err = q.waitForPublishConfirmation(publishCtx, session, job.JobID)
		}
		q.publishMu.Unlock()
		if err == nil {
			return nil
		}
		if errors.Is(err, ErrRabbitMQReturned) {
			if topologyRepaired {
				return err
			}
			if repairErr := q.repairTopology(publishCtx); repairErr == nil {
				topologyRepaired = true
				slog.Warn("RabbitMQ topology restored after unroutable publish", "job_id", job.JobID)
				continue
			} else {
				err = errors.Join(err, fmt.Errorf("repair rabbitmq topology: %w", repairErr))
			}
		}
		q.invalidatePublishChannel(session.channel)
		if publishCtx.Err() != nil {
			return publishCtx.Err()
		}
		delay := rabbitMQReconnectDelay(attempt, q.reconnectInitial, q.reconnectMax)
		slog.Warn("RabbitMQ publish failed; reconnecting", "attempt", attempt, "retry_in", delay, "error", err)
		if err := q.waitForReconnect(publishCtx, delay); err != nil {
			return err
		}
	}
}

func (q *RabbitMQQueue) waitForPublishConfirmation(ctx context.Context, session rabbitMQPublishSession, jobID string) error {
	confirms := session.confirms
	returns := session.returns
	closed := session.closed
	if session.channel == nil || confirms == nil || closed == nil {
		return ErrQueueConsumerClosed
	}
	var returned *amqp.Return

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-q.done:
			return context.Canceled
		case ret, ok := <-returns:
			if !ok {
				returns = nil
				continue
			}
			returned = &ret
		case closeErr, ok := <-closed:
			if ok && closeErr != nil {
				return fmt.Errorf("rabbitmq publish channel closed before confirmation: %w", closeErr)
			}
			return ErrQueueConsumerClosed
		case confirm, ok := <-confirms:
			if !ok {
				return ErrQueueConsumerClosed
			}
			if returned == nil && returns != nil {
				select {
				case ret, ok := <-returns:
					if ok {
						returned = &ret
					} else {
						returns = nil
					}
				default:
				}
			}
			if !confirm.Ack {
				if returned != nil {
					return fmt.Errorf("%w: job_id=%s routing_key=%s reply_code=%d reply_text=%s: %w",
						ErrRabbitMQReturned, jobID, returned.RoutingKey, returned.ReplyCode, returned.ReplyText, ErrRabbitMQNack)
				}
				return fmt.Errorf("%w: job_id=%s delivery_tag=%d", ErrRabbitMQNack, jobID, confirm.DeliveryTag)
			}
			if returned != nil {
				return fmt.Errorf("%w: job_id=%s routing_key=%s reply_code=%d reply_text=%s",
					ErrRabbitMQReturned, jobID, returned.RoutingKey, returned.ReplyCode, returned.ReplyText)
			}
			return nil
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
	channel, err := q.openTopologyChannel(ctx, conn)
	if err != nil {
		if conn.IsClosed() {
			q.invalidateConnection(conn)
		}
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
	consumerCanceled := channel.NotifyCancel(make(chan string, 1))

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
		case consumerTag, ok := <-consumerCanceled:
			if !ok {
				return ErrQueueConsumerClosed
			}
			if err := q.repairTopology(ctx); err != nil {
				return fmt.Errorf("recover topology after rabbitmq consumer %q was canceled: %w", consumerTag, err)
			}
			return fmt.Errorf("rabbitmq consumer %q was canceled", consumerTag)
		case delivery, ok := <-rawDeliveries:
			if !ok {
				return ErrQueueConsumerClosed
			}
			msg := delivery
			job, err := decodeJobMessage(msg.Body)
			if err != nil {
				_ = q.deadLetter(ctx, msg)
				continue
			}

			queueDelivery := QueueDelivery{
				Job:       job,
				Retryable: job.Attempt < q.maxAttempts,
			}
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
				return q.deadLetter(ctx, msg)
			}
			queueDelivery.Requeue = func(context.Context) error {
				return msg.Nack(false, true)
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

func (q *RabbitMQQueue) deadLetter(ctx context.Context, delivery amqp.Delivery) error {
	if err := q.Ping(ctx); err != nil {
		return errors.Join(fmt.Errorf("ensure rabbitmq dead-letter topology: %w", err), delivery.Nack(false, true))
	}
	return delivery.Nack(false, false)
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

	conn, err := q.dial(ctx, q.url, q.connectTimeout, q.publishTimeout)
	if err != nil {
		return err
	}
	channel, err := q.openTopologyChannel(ctx, conn)
	if err != nil {
		_ = conn.Close()
		return err
	}
	confirms := channel.NotifyPublish(make(chan amqp.Confirmation, 1))
	returns := channel.NotifyReturn(make(chan amqp.Return, 1))
	publishClosed := channel.NotifyClose(make(chan *amqp.Error, 1))
	if err := channel.Confirm(false); err != nil {
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
	q.confirms = confirms
	q.returns = returns
	q.publishClosed = publishClosed
	q.mu.Unlock()
	if oldConn != nil && oldConn != conn {
		_ = oldConn.Close()
	}
	return nil
}

func (q *RabbitMQQueue) openTopologyChannel(ctx context.Context, conn *amqp.Connection) (*amqp.Channel, error) {
	if err := q.repairTopologyOnConnection(ctx, conn); err != nil {
		return nil, err
	}
	return conn.Channel()
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

func (q *RabbitMQQueue) publishSession() rabbitMQPublishSession {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return rabbitMQPublishSession{
		channel:  q.channel,
		confirms: q.confirms,
		returns:  q.returns,
		closed:   q.publishClosed,
	}
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
	q.confirms = nil
	q.returns = nil
	q.publishClosed = nil
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
	q.confirms = nil
	q.returns = nil
	q.publishClosed = nil
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
	q.confirms = nil
	q.returns = nil
	q.publishClosed = nil
	q.mu.Unlock()
	if conn == nil || conn.IsClosed() {
		return nil
	}
	return conn.Close()
}

func rabbitMQReconnectDelay(attempt int, initial, maximum time.Duration) time.Duration {
	base := rabbitMQReconnectBaseDelay(attempt, initial, maximum)
	if base <= time.Nanosecond {
		return base
	}
	half := base / 2
	return half + time.Duration(rand.Int64N(int64(base-half)+1))
}

func rabbitMQReconnectBaseDelay(attempt int, initial, maximum time.Duration) time.Duration {
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
