# Site Checker

Site Checker is a production-oriented Go service for monitoring website availability. It provides a REST API for managing monitors, runs background checks with a worker pool, exposes health and Prometheus-style metrics, protects outbound HTTP requests from SSRF-style abuse, and runs cleanly in Docker or Kubernetes.

## Technology

- Go 1.26 with `net/http`, `log/slog`, goroutines, channels, and `context.Context`.
- REST API with versioned `/api/v1` routes.
- Contract-first API documentation in `api/openapi.yaml`.
- Prometheus-compatible text metrics on `/metrics`.
- Optional pprof endpoints on `/debug/pprof/`.
- Docker multi-stage build with `golang:1.26.5-alpine3.24` and `alpine:3.24`.
- GitHub Actions CI with tests, race detector, `go vet`, Staticcheck, govulncheck, and container build.
- Kubernetes manifests with probes, resource limits, security context, rolling update, and graceful termination.

## Features

- Create, list, update, delete, and manually check monitors through REST.
- Check history and monitor statistics.
- Concurrent worker pool with bounded queueing.
- Scheduler with duplicate-pending protection.
- Validated environment configuration with safe defaults.
- SSRF protection for URL schemes, redirects, DNS resolution, private networks, metadata IP ranges, proxy usage, and allowed egress ports.
- Response header and body limits.
- Optional external seed URL file in newline or JSON-array format.
- Optional webhook alerts after repeated failures.
- Build metadata embedded through Docker build arguments.

## REST API

OpenAPI contract:

```text
api/openapi.yaml
```

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
```

Create monitor:

```bash
curl -sS -X POST http://localhost:8080/api/v1/monitors \
  -H "Content-Type: application/json" \
  -d '{"url":"https://example.com","interval_seconds":60,"timeout_seconds":5,"expected_status":200}'
```

## Run Locally

```bash
go test ./...
go run .
```

The service starts on `:8080` by default:

- `GET /healthz`
- `GET /readyz`
- `GET /metrics`
- `GET /api/openapi.yaml`

## Benchmarks

```bash
go test -bench=. -benchmem
```

The benchmark suite compares worker-pool behavior at different worker counts.

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
  --memory=128m \
  -p 8080:8080 \
  site-checker
```

## Kubernetes

Baseline manifests live in:

```text
deploy/kubernetes/
```

Apply:

```bash
kubectl apply -f deploy/kubernetes/
```

The current implementation stores monitor state in memory, so the Kubernetes `Deployment` intentionally uses `replicas: 1`. Horizontal API/Scheduler/Worker scaling should be introduced together with PostgreSQL and RabbitMQ so checks are not duplicated and monitor state is not lost.

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `WORKER_COUNT` | `10` | Number of concurrent workers. |
| `CHECK_INTERVAL` | `5m` | Default interval for seeded monitors. |
| `HTTP_TIMEOUT` | `5s` | Default timeout for outbound requests. |
| `HEALTH_ADDR` | `:8080` | Address for API, health, and metrics endpoints. Set to empty to disable. |
| `URLS_FILE` | empty | Optional path to a newline file or JSON array with seed URLs. |
| `EXPECTED_STATUS` | `200-399` | Accepted status codes for legacy seeded checks. |
| `MAX_REDIRECTS` | `3` | Maximum allowed redirects. |
| `MAX_BODY_BYTES` | `65536` | Maximum response body bytes to read. |
| `MAX_HEADER_BYTES` | `65536` | Maximum response header bytes. |
| `ALLOWED_PORTS` | `80,443` | Allowed outbound destination ports. |
| `ALLOW_PRIVATE_NETWORKS` | `false` | Allows private, loopback, and link-local networks when explicitly enabled. |
| `ALLOW_PROXY_ENV` | `false` | Allows proxy settings from the environment when explicitly enabled. |
| `ALERT_WEBHOOK_URL` | empty | Optional webhook URL for failure alerts. |
| `ALERT_FAILURE_THRESHOLD` | `3` | Consecutive failures before sending an alert. |
| `ALERT_COOLDOWN` | `10m` | Minimum time between alerts for the same URL. |
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
URLS_FILE=urls.example.txt go run .
```

## Security Defaults

By default, Site Checker blocks private networks, loopback addresses, link-local ranges, metadata IPs such as `169.254.169.254`, unsupported schemes, userinfo in URLs, unexpected ports, unsafe redirects, and environment proxies. Enable overrides only for trusted internal deployments.
