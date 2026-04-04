# Logical Dashboard — Full Specification

## Objective

Build a **single-page FastAPI + vanilla HTML/CSS/JS** dashboard that exposes the Assignment 3 (A3) pipeline's logical data layer through a rich, interactive UI. The dashboard must:

1. **Bootstrap a fresh session** — register schema, generate + ingest records, build storage strategy, and insert data via `Assignment3Pipeline` → all from the UI.
2. **Browse logical entities** — list entities, view instances with fields/values, paginate.
3. **Execute CRUD operations** — Create, Read, Update, Delete records via transactional API with full input validation.
4. **Run ACID + R experiments** — Run individual tests (A = Atomicity, C = Consistency, I = Isolation, D = Durability, R = Reconstruction) or run-all, with live log streaming, pass/fail per test, and detailed results.
5. **Query workspace** — preview query plan and execute arbitrary JSON queries.
6. **Strict logical abstraction** — no SQL table names, Mongo collection names, indexes, or strategy internals ever leak into the UI output.

---

## Architecture

```
┌─────────────────────────────────────────────┐
│           Browser (Single Page App)         │
│   HTML + Vanilla CSS + Vanilla JS           │
│   No build step — served via FastAPI static │
└──────────────┬──────────────────────────────┘
               │  REST (JSON) + SSE (logs)
┌──────────────┴──────────────────────────────┐
│           FastAPI Backend (api_server.py)    │
│   Thin wrapper over Assignment3Pipeline     │
│   /api/session, /api/entities, /api/crud,   │
│   /api/acid, /api/query, /api/bootstrap     │
└──────────────┬──────────────────────────────┘
               │
┌──────────────┴──────────────────────────────┐
│        Assignment3Pipeline (existing)        │
│  orchestrator → txn_coordinator → crud_engine│
│  + acid_experiments + logical_reconstructor  │
└──────────────┬──────────────────────────────┘
        ┌──────┴──────┐
        │ MySQL  Mongo│
        └─────────────┘
```

### Backend Contract (FastAPI endpoints)

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/api/bootstrap` | POST | Register schema + generate records + ingest + build strategy + create records. Accepts `{record_count: int}` |
| `/api/session` | GET | Return `SessionInfo` (schema name, version, root entity, connection status, field count) |
| `/api/entities` | GET | List logical entity names |
| `/api/entities/{name}` | GET | Return `LogicalEntity` (fields + instances) with `?limit=&offset=` pagination |
| `/api/entities/all` | GET | Return all data merged from both backends |
| `/api/stats` | GET | Row/doc counts per backend table/collection |
| `/api/crud` | POST | Execute transactional CRUD. Body: `{operation, ...payload}` |
| `/api/query/preview` | POST | Preview query plan (no execution) |
| `/api/query/execute` | POST | Execute query via `execute_query` |
| `/api/acid/run/{property}` | POST | Run single ACID test: atomicity, consistency, isolation, durability, reconstruction |
| `/api/acid/run-all` | POST | Run all 5 ACID+R tests |
| `/api/acid/stream/{property}` | GET (SSE) | Stream live logs for a specific ACID test |
| `/api/acid/stream-all` | GET (SSE) | Stream live logs for full test suite |

### Response Format

All API responses follow:
```json
{
  "success": true|false,
  "data": { ... },
  "error": "string or null"
}
```

ACID test results follow:
```json
{
  "property_name": "atomicity",
  "passed": true,
  "description": "...",
  "duration_ms": 42.5,
  "details": { ... }
}
```

---

## Required Features

### 1. Bootstrap Flow
- **Setup panel** at the top: "Bootstrap Database" button with configurable record count (default 100)
- On click: calls `/api/bootstrap` → displays progress/status
- Shows resulting session info (schema, connections, entity count)
- Bootstrap should cleanly handle re-runs (metadata clear + rebuild)

### 2. Session Info Bar
- Always visible: schema name, version, root entity, MySQL/Mongo connection indicators (green/red dots), field count
- Auto-refreshes on bootstrap

### 3. Entity Browser
- Left panel or tab: list of logical entity names
- On select: shows entity fields and paginated instances in a table
- Pagination controls (prev/next, page size)
- Each row shows clean field names (no internal keys)

### 4. CRUD Operations (4 views)

#### CREATE
- JSON editor for input records (array of objects)
- Form mode: auto-generated fields from schema with validation
- Submit → shows TransactionResult (status, errors, rolled_back)
- **Data integrity**: validate required fields, types, uniqueness before submission

#### READ
- Filter builder (key-value pairs for `filters`)
- Field selector (multi-select for `fields`)
- Limit control
- Results displayed in clean table

#### UPDATE
- Filter builder for record selection
- Update fields builder (key-value for `updates`)
- Submit → shows result with before/after comparison if possible

#### DELETE
- Filter builder for record selection
- Confirmation dialog before execution
- Shows result (rows/docs affected)

### 5. ACID + R Test Runner

**Navigation**: Sidebar with R, A, C, I, D buttons + "Run All" button.

Where:
- **A** = Atomicity test
- **C** = Consistency test
- **I** = Isolation test (3 sub-tests: lost_update, dirty_read, lock_timeout)
- **D** = Durability test
- **R** = Reconstruction test (logical data reconstruction validation)

Each test button:
- Triggers test execution via SSE endpoint
- Shows live log output (streaming)
- Displays pass/fail result with icon
- Shows execution duration
- Expands to show detailed results JSON

"Run All" button:
- Runs all 5 tests sequentially
- Shows aggregate pass/fail summary
- Individual results expandable

### 6. Query Workspace
- JSON editor for query input (with example templates)
- "Preview Plan" button → shows query plan (SQL queries, Mongo queries, merge strategy)
- "Execute" button → shows results
- Query plan display should **mask physical details** (table names, collection names) or display them under an expandable "debug" section

---

## UX Requirements

1. **Single page** — no routing, use tabs/panels for navigation
2. **Responsive** — works on 1280px+ screens
3. **Dark mode** — modern dark theme with accent colors
4. **Feedback** — loading spinners, success/error toasts, pass/fail badges
5. **Data integrity** — form validation before submission, confirm on destructive ops
6. **Live logs** — SSE-powered real-time log streaming for ACID tests
7. **No build step** — served as static files via FastAPI, no npm/webpack required
8. **Premium feel** — smooth animations, glassmorphism, modern typography (Google Fonts)

---

## Correctness Rules

1. **Input validation**: required fields enforced, type checking for numbers/strings/arrays
2. **Uniqueness**: warn if creating duplicate records (based on unique_candidates from schema)
3. **Confirmation**: all DELETE operations require explicit confirmation
4. **Error states**: clear error messages with context (not raw tracebacks)
5. **Logical-only output**: never show SQL table names, Mongo collection names, column names, join keys, or storage backend identifiers in the main UI
6. **Deterministic pass/fail**: ACID test results are green ✅ / red ❌ with unambiguous status

---

## Data Integrity Rules for Record Input

When users create records through the dashboard:

1. **Schema-driven validation**: fields are auto-generated from the registered schema's `json_schema.properties`
2. **Required field enforcement**: fields listed in `json_schema.required` must be non-empty
3. **Type coercion**: numbers validated as numbers, arrays validated as arrays
4. **Nested structure support**: objects rendered as nested form groups, arrays as dynamic add/remove fields
5. **Unique candidate warning**: if a record's unique fields match an existing record, warn before submission
6. **Constraint display**: show which fields are unique/not-null/indexed in the form labels

---

## Relevant Files

| File | Role |
|------|------|
| [orchestrator.py](../../src/a3/orchestrator.py) | Main API surface — `Assignment3Pipeline` |
| [contracts.py](../../src/a3/contracts.py) | `TransactionResult`, `SessionInfo`, `LogicalEntity`, `AcidTestResult` |
| [acid_experiments.py](../../src/a3/acid_experiments.py) | `AcidExperimentRunner` — tests Atomicity, Consistency, Isolation, Durability, Reconstruction |
| [logical_reconstructor.py](../../src/a3/logical_reconstructor.py) | Unified entity view across backends |
| [session_manager.py](../../src/a3/session_manager.py) | Schema/connection state |
| [transaction_coordinator.py](../../src/a3/transaction_coordinator.py) | Multi-backend transaction coordination |
| [concurrency_manager.py](../../src/a3/concurrency_manager.py) | Per-entity read/write locking |
| [a2/contracts.py](../../src/a2/contracts.py) | `CrudOperation`, `FieldLocation`, `QueryPlan`, `SchemaRegistration` |
| [a2/orchestrator.py](../../src/a2/orchestrator.py) | `Assignment2Pipeline` — schema registration, ingestion, strategy, CRUD |
| [run_pipeline.py](../../run_pipeline.py) | CLI pipeline runner — reference for data flow |
| [schema template](../../schemas/assignment2_schema.template.json) | Default schema (social_iot_hybrid_v1) |

---

## Acceptance Criteria

1. ✅ Dashboard loads as single-page app at `http://localhost:8080`
2. ✅ Bootstrap creates fresh session with configurable record count
3. ✅ Session info bar shows schema, connections, entity count
4. ✅ Entity browser shows logical entities with paginated instances
5. ✅ All 4 CRUD operations work correctly with proper feedback
6. ✅ ACID + R tests run individually and as suite with live logs
7. ✅ Pass/fail results are clear and deterministic
8. ✅ Query preview and execution work with JSON input
9. ✅ No physical backend details leak into main UI
10. ✅ Data integrity maintained: validation, confirmation, error handling

---

## Verification Checklist

| # | Check | How |
|---|-------|-----|
| 1 | Bootstrap creates session | Click bootstrap → see session info populate |
| 2 | Entity list populated | After bootstrap → sidebar shows entity names |
| 3 | Entity data displays | Click entity → see table with fields + values |
| 4 | CREATE works | Submit new record → see success toast → verify in entity browser |
| 5 | READ works | Set filters → see filtered results |
| 6 | UPDATE works | Update a field → verify change in read-back |
| 7 | DELETE works | Delete record with confirmation → verify absent |
| 8 | ACID individual tests | Run each A/C/I/D/R → see pass/fail + logs |
| 9 | ACID run-all | Run all → see 5 results with aggregate status |
| 10 | No physical leakage | Inspect all UI output → no table/collection names visible |
| 11 | Error handling | Submit invalid data → see helpful error message |
| 12 | Live log streaming | Run ACID test → see logs appear in real-time |

---

## Explicit Non-Goals

- ❌ Authentication / authorization
- ❌ Production deployment hardening
- ❌ Distributed locking across multiple app instances
- ❌ React/Vite/TypeScript — using vanilla HTML/CSS/JS instead for zero build step
- ❌ Exposing SQL table names, Mongo collections, indexes, or schema strategy internals in UI

---

## Stack Decision

- **Backend**: FastAPI (Python) — thin API layer over existing `Assignment3Pipeline`
- **Frontend**: Vanilla HTML + CSS + JS (no build step, served as FastAPI static files)
- **Streaming**: SSE (Server-Sent Events) for live ACID test logs
- **Styling**: Custom CSS with dark theme, glassmorphism, Google Fonts (Inter)
