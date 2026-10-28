//go:build integration

package sitechecker

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

func TestReadinessPreservesHealthyRabbitMQConnection(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 4*time.Minute)
	defer cancel()
	rabbit, err := testcontainers.Run(ctx, integrationRabbitMQImage,
		testcontainers.WithEnv(map[string]string{
			"RABBITMQ_DEFAULT_USER":               "site_checker",
			"RABBITMQ_DEFAULT_PASS":               "site_checker",
			"RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS": "+S 2:2 +SDcpu 1 +SDio 1",
		}),
		testcontainers.WithExposedPorts("5672/tcp"),
		testcontainers.WithWaitStrategyAndDeadline(3*time.Minute,
			wait.ForLog("Server startup complete").WithStartupTimeout(3*time.Minute),
			wait.ForListeningPort("5672/tcp").WithStartupTimeout(3*time.Minute)),
	)
	if rabbit != nil {
		t.Cleanup(func() {
			if err := testcontainers.TerminateContainer(rabbit); err != nil {
				t.Logf("terminate readiness fixture: %v", err)
			}
		})
	}
	if err != nil {
		t.Fatal(err)
	}
	endpoint, err := rabbit.PortEndpoint(ctx, "5672/tcp", "")
	if err != nil {
		t.Fatal(err)
	}
	queue, err := NewRabbitMQQueue(Config{
		RabbitMQURL: fmt.Sprintf("amqp://site_checker:site_checker@%s/", endpoint),
		QueueName:   "site_checker.readiness", DeadLetterQueueName: "site_checker.readiness.dead",
		QueuePrefetch: 1, MaxJobAttempts: 3,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer queue.Close()
	if err := queue.Ping(ctx); err != nil {
		t.Fatal(err)
	}
	connection := queue.connection()
	failures := checkReadinessDependencies(ctx, NewMetrics("test", "commit", "date", 0), []ReadinessDependency{
		{Name: "postgres", Check: func(ctx context.Context) error {
			<-ctx.Done()
			return ctx.Err()
		}},
		{Name: "rabbitmq", Check: queue.Ping},
	})
	if len(failures) != 1 || failures["postgres"] == "" {
		t.Fatalf("readiness failures = %v, want only timed-out postgres", failures)
	}
	if connection.IsClosed() || queue.connection() != connection {
		t.Fatal("PostgreSQL probe timeout closed the healthy RabbitMQ connection")
	}
	canceled, stop := context.WithCancel(ctx)
	stop()
	if err := queue.Ping(canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled ping error = %v", err)
	}
	if connection.IsClosed() || queue.connection() != connection {
		t.Fatal("already-canceled ping closed the healthy RabbitMQ connection")
	}
	if err := queue.Ping(ctx); err != nil {
		t.Fatalf("connection was not usable after canceled health probes: %v", err)
	}
}
