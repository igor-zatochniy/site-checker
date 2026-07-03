package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
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
	conn        *amqp.Connection
	channel     *amqp.Channel
	queueName   string
	dlqName     string
	maxAttempts int
	mu          sync.Mutex
}

func NewRabbitMQQueue(cfg Config) (*RabbitMQQueue, error) {
	conn, err := amqp.Dial(cfg.RabbitMQURL)
	if err != nil {
		return nil, err
	}

	channel, err := conn.Channel()
	if err != nil {
		conn.Close()
		return nil, err
	}
	if err := channel.Qos(cfg.QueuePrefetch, 0, false); err != nil {
		channel.Close()
		conn.Close()
		return nil, err
	}

	queue := &RabbitMQQueue{
		conn:        conn,
		channel:     channel,
		queueName:   cfg.QueueName,
		dlqName:     cfg.DeadLetterQueueName,
		maxAttempts: cfg.MaxJobAttempts,
	}
	if err := queue.declareTopology(); err != nil {
		queue.Close()
		return nil, err
	}
	return queue, nil
}

func (q *RabbitMQQueue) Ping(context.Context) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	_, err := q.channel.QueueDeclarePassive(q.queueName, true, false, false, false, nil)
	return err
}

func (q *RabbitMQQueue) declareTopology() error {
	if err := q.channel.ExchangeDeclare(
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

	if _, err := q.channel.QueueDeclare(q.dlqName, true, false, false, false, nil); err != nil {
		return err
	}
	if err := q.channel.QueueBind(q.dlqName, q.dlqName, "site_checker.dlx", false, nil); err != nil {
		return err
	}

	_, err := q.channel.QueueDeclare(q.queueName, true, false, false, false, amqp.Table{
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

	q.mu.Lock()
	defer q.mu.Unlock()
	return q.channel.PublishWithContext(ctx, "", q.queueName, false, false, amqp.Publishing{
		ContentType:  "application/json",
		DeliveryMode: amqp.Persistent,
		MessageId:    job.JobID,
		Timestamp:    job.EnqueuedAt,
		Body:         body,
	})
}

func (q *RabbitMQQueue) Consume(ctx context.Context) (<-chan QueueDelivery, <-chan error, error) {
	q.mu.Lock()
	channelClosed := q.channel.NotifyClose(make(chan *amqp.Error, 1))
	rawDeliveries, err := q.channel.Consume(q.queueName, "", false, false, false, false, nil)
	q.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}

	deliveries := make(chan QueueDelivery)
	consumerErrors := make(chan error, 1)
	reportConsumerError := func(err error) {
		if ctx.Err() != nil {
			return
		}
		select {
		case consumerErrors <- err:
		default:
		}
	}

	go func() {
		defer close(deliveries)
		for {
			select {
			case <-ctx.Done():
				return
			case closeErr := <-channelClosed:
				if closeErr != nil {
					reportConsumerError(fmt.Errorf("rabbitmq channel closed: %w", closeErr))
					return
				}
				reportConsumerError(ErrQueueConsumerClosed)
				return
			case delivery, ok := <-rawDeliveries:
				if !ok {
					reportConsumerError(ErrQueueConsumerClosed)
					return
				}
				msg := delivery
				job, err := decodeJobMessage(msg.Body)
				if err != nil {
					q.nackMessage(msg, false)
					continue
				}

				queueDelivery := QueueDelivery{Job: job}
				queueDelivery.Ack = func(context.Context) error {
					return q.ackMessage(msg)
				}
				queueDelivery.Nack = func(ctx context.Context, requeue bool) error {
					if requeue && job.Attempt < q.maxAttempts {
						next := job
						next.Attempt++
						next.EnqueuedAt = time.Now().UTC()
						if err := q.Publish(ctx, next); err != nil {
							return q.nackMessage(msg, true)
						}
						return q.ackMessage(msg)
					}
					return q.nackMessage(msg, false)
				}

				select {
				case <-ctx.Done():
					q.nackMessage(msg, true)
					return
				case deliveries <- queueDelivery:
				}
			}
		}
	}()
	return deliveries, consumerErrors, nil
}

func (q *RabbitMQQueue) Close() error {
	q.mu.Lock()
	defer q.mu.Unlock()
	var err error
	if q.channel != nil {
		err = q.channel.Close()
	}
	if q.conn != nil {
		if closeErr := q.conn.Close(); err == nil {
			err = closeErr
		}
	}
	return err
}

func (q *RabbitMQQueue) ackMessage(delivery amqp.Delivery) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return delivery.Ack(false)
}

func (q *RabbitMQQueue) nackMessage(delivery amqp.Delivery, requeue bool) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return delivery.Nack(false, requeue)
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
