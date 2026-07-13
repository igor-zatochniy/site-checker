# Site Checker

[![CI](https://github.com/igor-zatochniy/site-checker/actions/workflows/ci.yml/badge.svg)](https://github.com/igor-zatochniy/site-checker/actions/workflows/ci.yml)

Site Checker is a backend monitoring platform written in Go. It manages website monitors through a REST API, schedules checks, processes jobs with workers, stores history in PostgreSQL, publishes check jobs through RabbitMQ, exposes Prometheus-style metrics, and protects outbound HTTP checks from SSRF.

## Technology

- Go 1.26, `net/http`, `log/slog`, goroutines, channels, and `context.Context`.
- PostgreSQL with `pgxpool`, embedded SQL migrations, monitor history, incidents, and a transactional alert outbox.
- REST API with repository/service/handler separation and API key authentication.
- OpenAPI contract in [`api/openapi.yaml`](api/openapi.yaml).
- JobQueue abstraction with in-memory and RabbitMQ implementations.
- RabbitMQ durable queue, dead-letter queue, ack/nack, retry, idempotent `job_id`, prefetch, and bounded in-memory backpressure.
- Prometheus-compatible metrics on `/metrics`, health probes on `/healthz` and dependency-aware `/readyz`, optional pprof.
- Docker multi-stage build with Alpine runtime.
- GitHub Actions for tests, race detector, `go vet`, Staticcheck, govulncheck, Trivy scans, SBOM generation, integration tests, and container build.
- Persisted webhook alerting with transactional decisions, cross-replica cooldown, lease recovery, retry, and idempotency keys.
- Kubernetes manifests for API, Scheduler, Worker, Alert Dispatcher, PostgreSQL, RabbitMQ, NetworkPolicy, and optional KEDA queue-based worker scaling.

## Architecture

The same binary can run as separate roles:

```text
APP_ROLE=api        REST API, OpenAPI, health, readiness, metrics
APP_ROLE=scheduler  claims due monitors and publishes check jobs
APP_ROLE=worker     consumes jobs and stores check results
APP_ROLE=alert-dispatcher  delivers persisted webhook alerts
APP_ROLE=all        local all-in-one mode
```

Storage and queue backends are configurable:

```text
STORAGE_TYPE=memory|postgres
QUEUE_TYPE=memory|rabbitmq
```

Without `DATABASE_URL` or `RABBITMQ_URL`, the service uses in-memory backends for local development. Production and Kubernetes use PostgreSQL and RabbitMQ.

```mermaid
flowchart LR
  Client["Client"] --> API["API role x2"]
  API --> PG[("PostgreSQL")]
  Scheduler["Scheduler role x1"] --> PG
  Scheduler --> Rabbit[("RabbitMQ queue")]
  Rabbit --> Worker["Worker role x1-10"]
  Worker --> PG
  Worker --> Sites["External websites"]
  PG --> AlertDispatcher["Alert Dispatcher role x1"]
  AlertDispatcher --> Webhook["Alert webhook"]
  API --> Metrics["/metrics /healthz /readyz"]
  Scheduler --> Metrics
  Worker --> Metrics
```

## REST API

- [OpenAPI YAML](api/openapi.yaml)
- [Interactive API docs](https://igor-zatochniy.github.io/site-checker/)

The interactive documentation loads the explicitly pinned Redoc 2.5.0 bundle.

The running service also exposes it at:

```text
GET /api/openapi.yaml
```

Main endpoints:

```text
POST   /api/v1/monitors
GET    /api/v1/monitors
GET    /api/v1/monitors/{id}
PATCH  /api/v1/monitors/{id}
DELETE /api/v1/monitors/{id}
GET    /api/v1/monitors/{id}/checks
POST   /api/v1/monitors/{id}/check
GET    /api/v1/monitors/{id}/stats
GET    /api/v1/incidents
```

If `API_KEY` is set, REST requests must include either:

```text
X-API-Key: <key>
Authorization: Bearer <key>
```

Create monitor:

```bash
curl -sS -X POST http://localhost:8080/api/v1/monitors \
  -H "Content-Type: application/json" \
  -H "X-API-Key: change-me" \
  -d '{"url":"https://example.com","interval_seconds":60,"timeout_seconds":5,"expected_status":200}'
```

More request and response examples are available in [`docs/demo.md`](docs/demo.md).

## Run Locally

Fast local mode:

```bash
go test ./...
go run .
```

The service starts with no seeded monitors by default. Add monitors through the REST API, provide `SEED_URLS_FILE`, or explicitly enable demo links with `SEED_DEFAULT_LINKS=true` / `APP_ENV=demo`.

Production-like local mode:

```bash
STORAGE_TYPE=postgres \
DATABASE_URL='postgres://site_checker:site_checker@localhost:5432/site_checker?sslmode=disable' \
QUEUE_TYPE=rabbitmq \
RABBITMQ_URL='amqp://site_checker:site_checker@localhost:5672/' \
API_KEY='change-me' \
go run .
```

Docker Compose mode:

```bash
docker compose up --build
```

Enable the optional alert dispatcher after setting a webhook URL:

```bash
ALERT_WEBHOOK_URL='https://alerts.example.com/site-checker' docker compose --profile alerts up --build
```

RabbitMQ Management UI is exposed on:

```text
http://localhost:15672
```

The HTTP server starts on `:8080` by default:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`
- `GET /api/openapi.yaml`

## Tests

```bash
go test ./...
go test -race ./...
go vet ./...
```

Make targets:

```bash
make ci
make integration
make docker-smoke
make k8s-dry-run
```

Integration tests use testcontainers-go and require Docker:

```bash
go test -tags=integration ./...
```

Benchmarks:

```bash
go test -bench=. -benchmem
```

## pprof

pprof is disabled by default. Enable it only in trusted development or internal networks:

```bash
ENABLE_PPROF=true go run .
```

Then open:

```text
GET /debug/pprof/
```

## Run With Docker

```bash
docker build \
  --build-arg VERSION=local \
  --build-arg COMMIT="$(git rev-parse --short HEAD)" \
  --build-arg BUILD_DATE="$(date -u +%FT%TZ)" \
  -t site-checker .
```

```bash
docker run --rm \
  --name site-checker \
  --read-only \
  --cpus=0.5 \
  --memory=256m \
  -p 8080:8080 \
  site-checker
```

## Kubernetes

Manifests live in:

```text
deploy/kubernetes/
```

Apply:

```bash
kubectl apply -k deploy/kubernetes/
```

The checked-in Kustomize base uses a fixed version tag and never uses `latest`. A `v*` Git tag publishes both the release tag and an immutable `sha-<commit>` tag to GHCR. CI also captures the pushed image digest and uploads a rendered Kubernetes manifest artifact whose application containers use `image@sha256:...`. Use that artifact for production releases:

```bash
kubectl apply -f site-checker-kubernetes-vX.Y.Z.yaml
```

The Kubernetes setup demonstrates:

- API deployment with 2 replicas.
- Scheduler deployment with 1 replica.
- Worker deployment with 3 replicas.
- Alert Dispatcher deployment with 1 replica.
- PostgreSQL and RabbitMQ demo deployments.
- Optional KEDA `ScaledObject` for RabbitMQ queue-length scaling from 1 to 10 workers.
- NetworkPolicy default-deny baseline with explicit DNS, PostgreSQL, RabbitMQ, and HTTP/HTTPS egress.
- Probes, Services, ConfigMap, Secret, resource requests/limits, rolling updates, graceful termination, and non-root security contexts.

Apply optional KEDA scaling after installing the KEDA operator:

```bash
kubectl apply -f deploy/kubernetes/keda/
```

Scale workers manually to observe queue-processing behavior:

```bash
kubectl -n site-checker scale deployment/site-checker-worker --replicas=1
kubectl -n site-checker scale deployment/site-checker-worker --replicas=3
kubectl -n site-checker scale deployment/site-checker-worker --replicas=6
```

Expected demonstration:

- 1 worker: backlog grows.
- 3 workers: backlog stabilizes.
- 6 workers: backlog disappears faster.

## Engineering trade-offs

- The service keeps one binary and selects behavior through `APP_ROLE`. This keeps deployment artifacts simple while still allowing API, Scheduler, Worker, and Alert Dispatcher roles to scale independently.
- The repository and queue interfaces support both local in-memory mode and production PostgreSQL/RabbitMQ mode. In-memory mode is fast for development, but PostgreSQL and RabbitMQ are the durable path for multi-replica deployments.
- Scheduler and worker coordination uses explicit job IDs, monitor pending state, stale lease recovery, and RabbitMQ ack/nack semantics. This avoids duplicate persisted results and prevents stale queued jobs from creating repeated external HTTP checks.
- SSRF protection is implemented in application code and reinforced by Kubernetes NetworkPolicy. Application-level checks give portable behavior; network policy adds defense in depth in clusters.
- A check result, incident transition, cooldown decision, and alert outbox insert share one PostgreSQL transaction. Webhook delivery is intentionally at-least-once; a stable `Idempotency-Key` lets receivers deduplicate the rare retry after an ambiguous network outcome.
- Built-in demo URLs are opt-in. Normal deployments start with an empty monitor set to avoid sending unintended traffic to third-party websites.
- RabbitMQ publication and consumption recover from runtime connection loss with bounded exponential backoff. Delivery remains at-least-once because a connection can fail after the broker accepted a publish or acknowledgement; persisted job IDs and processing leases make those retry windows safe.
- Kubernetes source manifests use a fixed readable tag for local rendering, while release CI produces a digest-pinned deployment bundle so production rollouts reference immutable image content.

## Known limitations

- The in-memory repository and queue are intended for local development and tests, not durable production use.
- Webhook receivers should honor `Idempotency-Key` because no distributed system can guarantee exactly-once delivery across a database commit and an external HTTP endpoint.
- PostgreSQL and RabbitMQ Kubernetes manifests are suitable for local or demonstration clusters. Production deployments should use managed services or hardened StatefulSets with backups, persistence, TLS, monitoring, and secret rotation.
- KEDA queue-based scaling requires the KEDA operator to be installed separately.
- The project exposes Prometheus-format metrics, but alert rules and dashboards are intentionally left environment-specific.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `APP_ENV` | `production` | Runtime environment label. `demo` enables built-in demo seed links. |
| `APP_ROLE` | `all` | Runtime role: `all`, `api`, `scheduler`, `worker`, or `alert-dispatcher`. |
| `STORAGE_TYPE` | `memory` or `postgres` when `DATABASE_URL` is set | Storage backend. |
| `DATABASE_URL` | empty | PostgreSQL connection string. Required for `STORAGE_TYPE=postgres`. |
| `RUN_MIGRATIONS` | `true` | Runs embedded SQL migrations on startup. |
| `API_KEY` | empty | Enables REST API key authentication when set. |
| `QUEUE_TYPE` | `memory` or `rabbitmq` when `RABBITMQ_URL` is set | Job queue backend. |
| `RABBITMQ_URL` | empty | RabbitMQ AMQP URL. Required for `QUEUE_TYPE=rabbitmq`. |
| `RABBITMQ_CONNECT_TIMEOUT` | `5s` | Timeout for one RabbitMQ TCP and protocol connection attempt. |
| `RABBITMQ_RECONNECT_INITIAL_BACKOFF` | `1s` | Initial delay before retrying a failed RabbitMQ connection or consumer session. |
| `RABBITMQ_RECONNECT_MAX_BACKOFF` | `30s` | Maximum RabbitMQ reconnect delay. |
| `QUEUE_NAME` | `site_checker.checks` | Main check job queue. |
| `DEAD_LETTER_QUEUE_NAME` | `site_checker.checks.dead` | RabbitMQ dead-letter queue. |
| `QUEUE_BUFFER_SIZE` | `1000` | In-memory queue buffer size. |
| `QUEUE_PREFETCH` | `10` | RabbitMQ consumer prefetch. |
| `MAX_JOB_ATTEMPTS` | `3` | Retry attempts before dead-lettering infrastructure failures. |
| `WORKER_COUNT` | `10` | Number of worker goroutines in each worker process. |
| `SCHEDULER_BATCH_SIZE` | `100` | Number of due monitors claimed per scheduler tick. |
| `CHECK_LEASE_TIMEOUT` | `2m` | Reclaims stale queued jobs or processing jobs. The processing lease starts when a worker begins handling the job. |
| `CHECK_INTERVAL` | `5m` | Default interval for seeded monitors. |
| `HTTP_TIMEOUT` | `5s` | Default timeout for outbound checks. |
| `HEALTH_ADDR` | `:8080` | Address for REST, health, and metrics endpoints. |
| `SEED_URLS_FILE` | empty | Explicit path to a newline file or JSON array with seed URLs. Only `all` and `scheduler` roles seed monitors. |
| `SEED_DEFAULT_LINKS` | `false` | Enables built-in demo seed links. Keep disabled for normal deployments. |
| `URLS_FILE` | empty | Legacy alias for `SEED_URLS_FILE` when `SEED_URLS_FILE` is unset. |
| `EXPECTED_STATUS` | `200-399` | Accepted status codes for legacy seeded checks. |
| `MAX_REDIRECTS` | `3` | Maximum allowed redirects. |
| `MAX_BODY_BYTES` | `65536` | Maximum response body bytes to read. |
| `MAX_HEADER_BYTES` | `65536` | Maximum response header bytes. |
| `ALLOWED_PORTS` | `80,443` | Allowed outbound destination ports. |
| `ALLOW_PRIVATE_NETWORKS` | `false` | Allows private, loopback, and link-local networks when explicitly enabled. |
| `ALLOW_PROXY_ENV` | `false` | Allows proxy settings from the environment when explicitly enabled. |
| `ALERT_WEBHOOK_URL` | empty | Optional webhook URL for persisted failure alerts. Requires PostgreSQL. |
| `ALERT_FAILURE_THRESHOLD` | `3` | Consecutive incident failures before creating an outbox event. |
| `ALERT_COOLDOWN` | `10m` | Database-enforced minimum time between alert events for one incident. |
| `ALERT_DISPATCH_INTERVAL` | `1s` | Poll interval when no immediately available outbox batch remains. |
| `ALERT_DISPATCH_BATCH_SIZE` | `50` | Maximum outbox events claimed and delivered concurrently per batch. |
| `ALERT_LEASE_TIMEOUT` | `30s` | Reclaims alert events abandoned by a stopped dispatcher. |
| `ALERT_DELIVERY_TIMEOUT` | `5s` | Timeout for one webhook delivery attempt. |
| `ALERT_MAX_ATTEMPTS` | `8` | Delivery attempts before an outbox event is marked `dead`. |
| `ALERT_RETRY_INITIAL_BACKOFF` | `1s` | Initial persisted retry delay. |
| `ALERT_RETRY_MAX_BACKOFF` | `5m` | Maximum persisted retry delay. |
| `USER_AGENT` | `site-checker` | User-Agent used for checks. |
| `ENABLE_PPROF` | `false` | Enables `/debug/pprof/` endpoints. |
| `READINESS_STALE_AFTER` | `CHECK_INTERVAL*3 + HTTP_TIMEOUT` | Marks readiness unhealthy if checks are stale. |
| `STARTUP_GRACE_PERIOD` | `CHECK_INTERVAL + HTTP_TIMEOUT + 30s` | Allows startup before the first completed check. |

## URL File Example

```text
# urls.example.txt
https://example.com
https://openai.com
```

Run with an external URL file:

```bash
SEED_URLS_FILE=urls.example.txt go run .
```

Run with built-in demo links:

```bash
APP_ENV=demo go run .
```

## Security Defaults

By default, Site Checker blocks private networks, loopback addresses, link-local ranges, metadata IPs such as `169.254.169.254`, unsupported schemes, userinfo in URLs, unexpected ports, unsafe redirects, and environment proxies. Enable overrides only for trusted internal deployments.

Kubernetes `secret.yaml` contains local-demo placeholder values. Production deployments should use External Secrets Operator, SOPS, Sealed Secrets, or a managed secret store with rotation.

## License

This project is licensed under the [MIT License](LICENSE).
