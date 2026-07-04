# Site Checker Demo

This walkthrough shows the main API and observability endpoints. It assumes the service is running on `http://localhost:8080`.

If `API_KEY` is configured, include it in REST API requests:

```bash
export BASE_URL=http://localhost:8080
export API_KEY=change-me
```

If `API_KEY` is empty, omit the `X-API-Key` header.

## Create a monitor

```bash
curl -sS -X POST "$BASE_URL/api/v1/monitors" \
  -H "Content-Type: application/json" \
  -H "X-API-Key: $API_KEY" \
  -d '{
    "url": "https://example.com",
    "interval_seconds": 60,
    "timeout_seconds": 5,
    "expected_status": 200
  }'
```

Example response:

```json
{
  "id": "mon_8f5d9e0fd6f04d9e9f0d8f1a2b3c4d5e",
  "url": "https://example.com",
  "interval_seconds": 60,
  "timeout_seconds": 5,
  "expected_status": 200,
  "status": "active",
  "enabled": true,
  "next_check_at": "2026-07-12T18:00:00Z",
  "created_at": "2026-07-12T18:00:00Z",
  "updated_at": "2026-07-12T18:00:00Z"
}
```

Store the returned monitor ID:

```bash
export MONITOR_ID=mon_8f5d9e0fd6f04d9e9f0d8f1a2b3c4d5e
```

## List monitors

```bash
curl -sS "$BASE_URL/api/v1/monitors?limit=10" \
  -H "X-API-Key: $API_KEY"
```

Example response:

```json
{
  "items": [
    {
      "id": "mon_8f5d9e0fd6f04d9e9f0d8f1a2b3c4d5e",
      "url": "https://example.com",
      "interval_seconds": 60,
      "timeout_seconds": 5,
      "expected_status": 200,
      "status": "active",
      "enabled": true,
      "next_check_at": "2026-07-12T18:01:00Z",
      "created_at": "2026-07-12T18:00:00Z",
      "updated_at": "2026-07-12T18:00:00Z"
    }
  ],
  "total": 1,
  "offset": 0,
  "limit": 10
}
```

## Run a manual check

```bash
curl -sS -X POST "$BASE_URL/api/v1/monitors/$MONITOR_ID/check" \
  -H "X-API-Key: $API_KEY"
```

Example response:

```json
{
  "id": "chk_7b9d91e5e1a94d4e8d6c0a0b1c2d3e4f",
  "job_id": "manual_9dd15e2e4e264c6e9a81c0ad9f117f53",
  "monitor_id": "mon_8f5d9e0fd6f04d9e9f0d8f1a2b3c4d5e",
  "status_code": 200,
  "latency_ms": 123,
  "success": true,
  "checked_at": "2026-07-12T18:00:05Z"
}
```

## Health, readiness, and metrics

Health:

```bash
curl -sS "$BASE_URL/healthz"
```

Readiness:

```bash
curl -sS "$BASE_URL/readyz"
```

Metrics:

```bash
curl -sS "$BASE_URL/metrics" | head -40
```

Example metric lines:

```text
site_checker_checks_total 1
site_checker_checks_healthy_total 1
site_checker_jobs_scheduled_total 0
site_checker_site_up{url="https://example.com"} 1
```

## OpenAPI

The OpenAPI contract is available in the repository and from a running service:

```text
api/openapi.yaml
GET /api/openapi.yaml
```
