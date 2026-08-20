# Site Checker

[![CI](https://github.com/igor-zatochniy/site-checker/actions/workflows/ci.yml/badge.svg)](https://github.com/igor-zatochniy/site-checker/actions/workflows/ci.yml)

Site Checker is a backend monitoring platform written in Go. It manages website monitors through a REST API, schedules checks, processes jobs with workers, stores history in PostgreSQL, publishes check jobs through RabbitMQ, exposes Prometheus-style metrics, and protects outbound HTTP checks from SSRF.

## Technology

- Go 1.26, `net/http`, `log/slog`, goroutines, channels, and `context.Context`.
- PostgreSQL with `pgxpool`, embedded SQL migrations, persisted check jobs, monitor history, incidents, and a transactional alert outbox.
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

Without `DATABASE_URL` or `RABBITMQ_URL`, the `all` role can use in-memory backends only in `local`, `development`, or `demo` environments. Production requires PostgreSQL, while split scheduler and worker roles additionally require RabbitMQ.

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

## Source layout

- `main.go` is the thin binary entrypoint and build metadata target.
- `internal/sitechecker/` contains the application runtime, domain model, storage, queue, HTTP API, observability, and tests.
- `api/openapi.yaml` is the public API contract; the runtime embeds a synchronized copy under `internal/sitechecker/api/`.
- `migrations/` is the public SQL migration directory; the runtime embeds a synchronized copy under `internal/sitechecker/migrations/`.

This keeps the executable boundary separate from application code while avoiding a risky package split that would move multiple production concerns at once.

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

`POST /api/v1/monitors/{id}/check` persists an asynchronous manual job and returns `202 Accepted`. The scheduler and workers process it through the same PostgreSQL and RabbitMQ lifecycle as periodic checks; the API role does not require external HTTP egress.

API-enabled roles fail at startup unless authentication is configured. Set an `API_KEY` with at least 24 characters and send either:

```text
X-API-Key: <key>
Authorization: Bearer <key>
```

Create monitor:

```bash
curl -sS -X POST http://localhost:8080/api/v1/monitors \
  -H "Content-Type: application/json" \
  -H "X-API-Key: local-site-checker-api-key" \
  -d '{"url":"https://example.com","interval_seconds":60,"timeout_seconds":5,"expected_status":200}'
```

More request and response examples are available in [`docs/demo.md`](docs/demo.md).

## Run Locally

Fast local mode:

```bash
go test ./...
APP_ENV=local AUTH_DISABLED=true go run .
```

The service starts with no seeded monitors by default. Add monitors through the REST API, provide `SEED_URLS_FILE`, or explicitly enable demo links with `SEED_DEFAULT_LINKS=true` / `APP_ENV=demo`.

Production-like local mode:

```bash
STORAGE_TYPE=postgres \
DATABASE_URL='postgres://site_checker:site_checker@localhost:5432/site_checker?sslmode=disable' \
QUEUE_TYPE=rabbitmq \
RABBITMQ_URL='amqp://site_checker:site_checker@localhost:5672/' \
API_KEY='replace-with-a-random-api-key' \
go run .
```

Docker Compose mode:

```bash
cp .env.example .env
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
APP_ENV=local AUTH_DISABLED=true ENABLE_PPROF=true go run .
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
  -e APP_ENV=local \
  -e AUTH_DISABLED=true \
  -p 8080:8080 \
  site-checker
```

The image-level Docker `HEALTHCHECK` probes `http://127.0.0.1:8080/healthz`. Keep `HEALTH_ADDR=:8080` when relying on that check. For a headless background role or a custom listener, replace the container healthcheck or start it with `--no-healthcheck`; Kubernetes deployments use their own probes.

## Kubernetes

Manifests live in:

```text
deploy/kubernetes/
```

Create the local Secret file first:

```bash
cp deploy/kubernetes/secret.example.yaml deploy/kubernetes/local/secret.yaml
```

Apply the untracked Secret explicitly, then render the secret-free base:

```bash
kubectl apply -f deploy/kubernetes/namespace.yaml
kubectl apply -f deploy/kubernetes/local/secret.yaml
kubectl apply -k deploy/kubernetes/
```

The local `deploy/kubernetes/local/secret.yaml` file is ignored by git. The checked-in base stays secret-free so CI can render immutable release manifests and production deployments can provide secrets through External Secrets, SOPS, Sealed Secrets, or a managed secret store.

The checked-in Kustomize base uses a fixed version tag and never uses `latest`. A `v*` Git tag publishes both the release tag and an immutable `sha-<commit>` tag to GHCR. CI also captures the pushed image digest and uploads a rendered Kubernetes manifest artifact whose application containers use `image@sha256:...`. Use that artifact for production releases:

```bash
kubectl apply -f site-checker-kubernetes-vX.Y.Z.yaml
```

When alerts are enabled, apply the matching digest-pinned `site-checker-alerts-vX.Y.Z.yaml` release artifact after configuring the webhook Secret.

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
- `package main` is intentionally thin. Application code lives in `internal/sitechecker`, which gives the binary a clear boundary while keeping related internals close enough to refactor safely.
- The repository and queue interfaces support both local in-memory mode and production PostgreSQL/RabbitMQ mode. In-memory mode is fast for development, but PostgreSQL and RabbitMQ are the durable path for multi-replica deployments.
- PostgreSQL `check_jobs` is the authoritative job lifecycle: `scheduled → queued → processing → completed`, with explicit `failed` retry and terminal `dead` states. Periodic and manual jobs share this pipeline. RabbitMQ transports job IDs, while database attempts and unique processing lease tokens fence stale workers from completing a newer attempt. Lease recovery atomically marks a final exhausted attempt `dead`, so the scheduler cannot publish an attempt above `MAX_JOB_ATTEMPTS`.
- SSRF protection is implemented in application code and reinforced by Kubernetes NetworkPolicy. Application-level checks give portable behavior; network policy adds defense in depth in clusters.
- A check result, incident transition, cooldown decision, and alert outbox insert share one PostgreSQL transaction. Webhook delivery is intentionally at-least-once; a stable `Idempotency-Key` lets receivers deduplicate the rare retry after an ambiguous network outcome.
- Built-in demo URLs are opt-in. Normal deployments start with an empty monitor set to avoid sending unintended traffic to third-party websites.
- RabbitMQ publication and consumption recover from runtime connection loss with bounded exponential backoff. Confirmed `queued` jobs are not replayed by elapsed time; they are returned to the persisted publish path only after the application proves that the main queue topology was lost. Delivery remains at-least-once because a connection can fail after the broker accepted a publish or acknowledgement; persisted job states, monotonic attempts, unique result job IDs, and fenced processing leases make those retry windows safe.
- PostgreSQL retention removes expired history in indexed batches owned by the scheduler. The feature is opt-in in application configuration and explicitly enabled in the supplied scheduler deployments.
- Kubernetes source manifests use a fixed readable tag for local rendering, while release CI produces a digest-pinned deployment bundle so production rollouts reference immutable image content.

## Known limitations

- The in-memory repository and queue are intended for local development and tests, not durable production use.
- External HTTP checks are at-least-once: if a worker exits after the request completes but before the result transaction commits, the persisted lease recovery can repeat that request.
- PostgreSQL retention uses bounded deletes; table partitioning and archival to long-term storage remain deployment-specific.
- Webhook receivers should honor `Idempotency-Key` because no distributed system can guarantee exactly-once delivery across a database commit and an external HTTP endpoint.
- PostgreSQL and RabbitMQ Kubernetes manifests are suitable for local or demonstration clusters. Production deployments should use managed services or hardened StatefulSets with backups, persistence, TLS, monitoring, and secret rotation.
- KEDA queue-based scaling requires the KEDA operator to be installed separately.
- The default Kubernetes egress policy permits public IPv4 HTTP/HTTPS destinations only. IPv6 monitoring requires an environment-specific CNI policy with equivalent private and special-range exclusions.
- The project exposes Prometheus-format metrics, but alert rules and dashboards are intentionally left environment-specific.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `APP_ENV` | `production` | Strict runtime environment: `production`, `local`, `development`, or `demo`. Unknown values fail startup; `demo` enables built-in demo seed links. |
| `APP_ROLE` | `all` | Runtime role: `all`, `api`, `scheduler`, `worker`, or `alert-dispatcher`. |
| `STORAGE_TYPE` | `memory` or `postgres` when `DATABASE_URL` is set | Storage backend. Production and split `api`/`scheduler`/`worker` roles require PostgreSQL. |
| `DATABASE_URL` | empty | PostgreSQL connection string. Required for `STORAGE_TYPE=postgres`. |
| `DATABASE_OPERATION_TIMEOUT` | `15s` | Upper bound for each PostgreSQL runtime operation and API request business logic. The HTTP response write timeout is always configured with an additional safety margin. |
| `DATABASE_MIGRATION_TIMEOUT` | `2m` | Upper bound for PostgreSQL pool initialization and startup migrations. |
| `RUN_MIGRATIONS` | `true` | Runs embedded SQL migrations on startup. |
| `API_KEY` | empty | API key with at least 24 characters. Required for `api` and `all` roles unless local authentication is explicitly disabled. |
| `AUTH_DISABLED` | `false` | Explicitly disables API authentication. Allowed only with `APP_ENV=local`, `development`, or `demo`; rejected in production. |
| `QUEUE_TYPE` | `memory` or `rabbitmq` when `RABBITMQ_URL` is set | Job queue backend. Split `scheduler` and `worker` roles require RabbitMQ. |
| `RABBITMQ_URL` | empty | RabbitMQ AMQP URL. Required for `QUEUE_TYPE=rabbitmq`. |
| `RABBITMQ_CONNECT_TIMEOUT` | `5s` | Timeout for one RabbitMQ TCP and protocol connection attempt. |
| `RABBITMQ_PUBLISH_TIMEOUT` | `10s` | End-to-end limit for publishing and receiving a broker confirmation; socket writes use the same upper bound. |
| `RABBITMQ_RECONNECT_INITIAL_BACKOFF` | `1s` | Initial delay before retrying a failed RabbitMQ connection or consumer session. |
| `RABBITMQ_RECONNECT_MAX_BACKOFF` | `30s` | Maximum RabbitMQ reconnect delay. |
| `QUEUE_NAME` | `site_checker.checks` | Main check job queue. |
| `DEAD_LETTER_QUEUE_NAME` | `site_checker.checks.dead` | RabbitMQ dead-letter queue. |
| `QUEUE_BUFFER_SIZE` | `1000` | In-memory queue buffer size. |
| `QUEUE_PREFETCH` | `10` | RabbitMQ consumer prefetch. |
| `MAX_JOB_ATTEMPTS` | `3` | Strict maximum processing attempts. PostgreSQL lease recovery terminalizes an expired final attempt without publishing another delivery. |
| `WORKER_COUNT` | `10` | Number of worker goroutines in each worker process. |
| `SCHEDULER_BATCH_SIZE` | `100` | Number of due monitors claimed per scheduler tick. |
| `CHECK_LEASE_TIMEOUT` | `2m` | Bounds publication claims and processing recovery leases. Confirmed queued deliveries do not expire by time. It must be at least 90 seconds: the maximum 60-second monitor timeout plus a 30-second persistence margin. |
| `CHECK_INTERVAL` | `5m` | Default interval for seeded monitors. |
| `HTTP_TIMEOUT` | `5s` | Default `timeout_seconds` for seeded monitors. Each monitor context owns its actual timeout; this value is not a global client cap. |
| `HEALTH_ADDR` | `:8080` | Address for REST, health, and metrics endpoints. Must not be empty for `api` and `all` roles. |
| `SEED_URLS_FILE` | empty | Explicit path to a newline file or JSON array with seed URLs. Only `all` and `scheduler` roles seed monitors. |
| `SEED_DEFAULT_LINKS` | `false` | Enables built-in demo seed links. Keep disabled for normal deployments. |
| `URLS_FILE` | empty | Legacy alias for `SEED_URLS_FILE` when `SEED_URLS_FILE` is unset. |
| `EXPECTED_STATUS` | `200-399` | Status policy for legacy seeded checks. Seeded monitors store the first accepted code because each monitor uses one exact expected status. |
| `MAX_REDIRECTS` | `3` | Maximum allowed redirects. |
| `MAX_BODY_BYTES` | `65536` | Maximum response body bytes to read. |
| `MAX_HEADER_BYTES` | `65536` | Maximum response header bytes. |
| `ALLOWED_PORTS` | `80,443` | Allowed outbound destination ports. |
| `ALLOW_PRIVATE_NETWORKS` | `false` | Allows private, loopback IP, and link-local addresses when explicitly enabled; `localhost` hostnames remain blocked. Kubernetes also requires an explicit trusted-CIDR egress overlay. |
| `ALLOW_PROXY_ENV` | `false` | Uses proxy settings from the environment only in the explicit `ALLOW_PRIVATE_NETWORKS=true` trust mode; proxy-side DNS resolution is outside the local SSRF boundary. |
| `ALERT_WEBHOOK_URL` | empty | Optional webhook URL for persisted failure alerts. Requires PostgreSQL and is mandatory when `APP_ROLE=alert-dispatcher`. |
| `ALERT_FAILURE_THRESHOLD` | `3` | Consecutive incident failures before creating an outbox event. |
| `ALERT_COOLDOWN` | `10m` | Database-enforced minimum time between alert events for one incident. |
| `ALERT_DISPATCH_INTERVAL` | `1s` | Poll interval when no immediately available outbox batch remains. |
| `ALERT_DISPATCH_BATCH_SIZE` | `50` | Maximum outbox events claimed and delivered concurrently per batch. |
| `ALERT_LEASE_TIMEOUT` | `30s` | Reclaims alert events abandoned by a stopped dispatcher. |
| `ALERT_DELIVERY_TIMEOUT` | `5s` | Timeout for one webhook delivery attempt. |
| `ALERT_MAX_ATTEMPTS` | `8` | Delivery attempts before an outbox event is marked `dead`. |
| `ALERT_RETRY_INITIAL_BACKOFF` | `1s` | Initial persisted retry delay. |
| `ALERT_RETRY_MAX_BACKOFF` | `5m` | Maximum exponential retry backoff. A valid webhook `Retry-After` response may extend the persisted delay up to 24 hours. |
| `RETENTION_ENABLED` | `false` | Enables PostgreSQL history cleanup. Allowed only for `all` or `scheduler` roles, required for production `all`, and explicitly enabled by the supplied scheduler deployments. |
| `RETENTION_INTERVAL` | `1m` | Delay between bounded cleanup passes. |
| `RETENTION_BATCH_SIZE` | `10000` | Maximum rows deleted from each retained table in one pass. |
| `CHECK_RESULTS_RETENTION` | `2160h` | Retains check results for 90 days. |
| `CHECK_JOBS_RETENTION` | `720h` | Retains completed/dead jobs for 30 days. Active jobs are never deleted. |
| `ALERT_OUTBOX_RETENTION` | `720h` | Retains delivered/dead alert events for 30 days. Pending/processing events are never deleted. |
| `RESOLVED_INCIDENT_RETENTION` | `8760h` | Retains resolved incidents for 365 days and deletes them only after all related outbox rows are gone. |
| `USER_AGENT` | `site-checker` | User-Agent used for checks. |
| `ENABLE_PPROF` | `false` | Enables `/debug/pprof/` endpoints outside production. `true` is rejected when `APP_ENV=production`. |

## URL File Example

```text
# urls.example.txt
https://example.com
https://openai.com
```

Run with an external URL file:

```bash
APP_ENV=local AUTH_DISABLED=true SEED_URLS_FILE=urls.example.txt go run .
```

Run with built-in demo links:

```bash
APP_ENV=demo AUTH_DISABLED=true go run .
```

## Security Defaults

By default, Site Checker blocks private networks, loopback addresses, link-local ranges, metadata IPs such as `169.254.169.254`, unsupported schemes, userinfo in URLs, unexpected ports, unsafe redirects, and environment proxies. Enable overrides only for trusted internal deployments.

`deploy/kubernetes/secret.example.yaml` is a template only. Copy it to an untracked `deploy/kubernetes/local/secret.yaml`, apply that file explicitly, and then apply the secret-free Kustomize base. For production, use External Secrets Operator, SOPS, Sealed Secrets, or a managed secret store with rotation.

## License

This project is licensed under the [MIT License](LICENSE).
