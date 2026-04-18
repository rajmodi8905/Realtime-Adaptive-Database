# Quickstart

## Prerequisites

- Docker and Docker Compose
- Python 3.10+
- Node.js 20+ (only for local frontend development)

## 1) Start Databases

From the repository root:

```bash
docker compose up -d
```

Verify services:

```bash
docker compose ps
```

Expected core services:

- `adaptive_db_mysql`
- `adaptive_db_mongodb`

## 2) Setup Python Environment

```bash
python3 -m venv venv
source venv/bin/activate
pip install -r requirements.txt
```

## 3) Start Backend API

```bash
python -m dashboard.api_server
```

Backend URL:

- `http://localhost:8080`

## 4) Start Frontend (Local Node)

In a separate terminal:

```bash
cd dashboard/frontend
npm install
npm run dev
```

Frontend URL (Vite):

- `http://localhost:5173/static/`

## Optional Docker Tooling

The compose file includes optional profiles for frontend and load testing.

### Frontend in Docker (npm included)

```bash
docker compose --profile frontend up frontend-dev
```

### k6 Load Test Runner in Docker

```bash
docker compose --profile loadtest run --rm k6
```

Default targets used by these optional containers:

- `VITE_API_PROXY_TARGET=http://host.docker.internal:8080`
- `LOAD_TEST_BASE_URL=http://host.docker.internal:8080`

Override them if your backend runs elsewhere.
