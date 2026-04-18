# API Reference (Dashboard Backend)

Base URL: `http://localhost:8080`

All endpoints are served by `dashboard/api_server.py`.

## Auth

- `POST /api/auth/login`
- `POST /api/auth/logout`
- `GET /api/auth/sessions`

## Bootstrap & Session

- `POST /api/bootstrap`
- `POST /api/bootstrap/custom`
- `GET /api/session`
- `GET /api/ingestion/timings`

## Schema & Entities

- `GET /api/schema`
- `GET /api/schema/plan`
- `GET /api/entities`
- `GET /api/entities/all`
- `GET /api/entities/{name}`
- `GET /api/stats`

## CRUD & Query

- `POST /api/crud`
- `POST /api/query/preview`
- `POST /api/query/execute`

## Query History

- `GET /api/query/history`
- `GET /api/query/history/{entry_id}`
- `POST /api/query/history/delete`
- `POST /api/query/history/clear`

## Metrics, Benchmark, and Evidence

- `GET /api/metrics`
- `POST /api/metrics/reset`
- `POST /api/benchmark/run`
- `GET /api/benchmark/results`
- `POST /api/benchmark/k6`
- `GET /api/evidence/export`

## ACID Test Endpoints

- `POST /api/acid/run/{property_name}`
- `POST /api/acid/run-all`
- `GET /api/acid/stream/{property_name}`
- `GET /api/acid/stream-all`

## UI Entry Points

- `GET /` serves dashboard HTML.
- `GET /static/*` serves frontend assets.

## Example cURL: Bootstrap

```bash
curl -X POST http://localhost:8080/api/bootstrap \
  -H "Content-Type: application/json" \
  -d '{"record_count": 100}'
```

## Example cURL: CRUD Create

```bash
curl -X POST http://localhost:8080/api/crud \
  -H "Content-Type: application/json" \
  -d '{"operation":"create","records":[{"username":"demo_user"}]}'
```
