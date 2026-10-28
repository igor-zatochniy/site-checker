package sitechecker

import (
	"context"
	"encoding/json"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	amqp "github.com/rabbitmq/amqp091-go"
	"gopkg.in/yaml.v3"
)

func TestDockerComposeUsesExplicitConnectionURLs(t *testing.T) {
	var compose struct {
		Services map[string]struct{ Environment map[string]string }
	}
	if err := yaml.Unmarshal([]byte(readTestFile(t, filepath.Join("..", "..", "docker-compose.yml"))), &compose); err != nil {
		t.Fatal(err)
	}
	for _, role := range []string{"api", "scheduler", "worker", "alert-dispatcher"} {
		env := compose.Services[role].Environment
		if env["DATABASE_URL"] != "${DATABASE_URL:?Set DATABASE_URL with percent-encoded credentials}" {
			t.Fatalf("%s must use an explicit PostgreSQL URL", role)
		}
		if role != "alert-dispatcher" && env["RABBITMQ_URL"] != "${RABBITMQ_URL:?Set RABBITMQ_URL with percent-encoded credentials}" {
			t.Fatalf("%s must use an explicit RabbitMQ URL", role)
		}
	}
}

func TestDockerComposePreservesCredentials(t *testing.T) {
	if os.Getenv("COMPOSE_RENDER_TEST") != "1" {
		t.Skip("set COMPOSE_RENDER_TEST=1 to validate rendered connection URLs with Docker Compose")
	}
	for _, password := range []string{"fixture/only#pass", "fixture%?@:$= and'quotes"} {
		t.Run(password, func(t *testing.T) {
			// Кодуємо всі reserved-символи, зокрема $, який Compose екранує під час export.
			credentials := "fixture_user:" + strings.ReplaceAll(url.QueryEscape(password), "+", "%20")
			postgresURL := "postgres://" + credentials + "@postgres:5432/fixture_db?sslmode=disable"
			rabbitURL := "amqp://" + credentials + "@rabbitmq:5672/"
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			cmd := exec.CommandContext(ctx, "docker", "compose", "--env-file", ".env.example", "-f", "docker-compose.yml", "--profile", "alerts", "config", "--format", "json")
			cmd.Dir = filepath.Join("..", "..")
			cmd.Env = append(os.Environ(),
				"POSTGRES_USER=fixture_user", "POSTGRES_DB=fixture_db", "POSTGRES_PASSWORD="+password,
				"RABBITMQ_USER=fixture_user", "RABBITMQ_PASSWORD="+password,
				"DATABASE_URL="+postgresURL, "RABBITMQ_URL="+rabbitURL,
				"APP_ENV=local", "AUTH_DISABLED=false", "API_KEY="+strings.Repeat("x", 24),
				"ALERT_WEBHOOK_URL=", "SEED_DEFAULT_LINKS=false", "SEED_URLS_FILE=",
			)
			output, err := cmd.Output()
			if err != nil {
				t.Fatalf("render fixture Compose configuration: %v", err)
			}
			var rendered struct {
				Services map[string]struct{ Environment map[string]string }
			}
			if err := json.Unmarshal(output, &rendered); err != nil {
				t.Fatal(err)
			}
			for _, role := range []string{"api", "scheduler", "worker", "alert-dispatcher"} {
				env := rendered.Services[role].Environment
				postgres, err := pgxpool.ParseConfig(env["DATABASE_URL"])
				if err != nil || postgres.ConnConfig.Password != password || postgres.ConnConfig.User != "fixture_user" {
					t.Fatalf("%s PostgreSQL credentials did not survive rendering", role)
				}
				if role != "alert-dispatcher" {
					rabbit, err := amqp.ParseURI(env["RABBITMQ_URL"])
					if err != nil || rabbit.Password != password || rabbit.Username != "fixture_user" {
						t.Fatalf("%s RabbitMQ credentials did not survive rendering", role)
					}
				}
			}
		})
	}
}
