# Assignment 3 — Logical Dashboard & Transactional Validation

## Overview

This package extends the A2 pipeline with two layers:

1. **Transaction Coordination** — wraps every multi-backend CRUD operation in a logical transaction with all-or-nothing semantics.
2. **ACID Validation** — four controlled experiments proving Atomicity, Consistency, Isolation, and Durability across the hybrid SQL + MongoDB storage system.

A dashboard API layer (not yet implemented) will expose these capabilities through a web UI.

---

## File-by-File Workflow

### 1. `contracts.py` — Data Classes

Defines the four core data structures used across A3:

| Class | Role |
|---|---|
| `TransactionResult` | Outcome of a coordinated operation — `status` is `"committed"` or `"rolled_back"`, carries per-backend results and errors |
| `SessionInfo` | Snapshot of current state: schema name, version, connection statuses, table/collection lists |
| `LogicalEntity` | Backend-agnostic entity with clean field names and instance rows |
| `AcidTestResult` | Pass/fail result of one ACID experiment — property name, description, timing, details dict |

No logic — pure data containers.

---

### 2. `transaction_coordinator.py` — Transaction Coordination Layer

**The core A3 component.** Ensures that a single CRUD operation touching both MySQL and MongoDB either fully commits or fully rolls back.

#### How It Works

```
execute_in_transaction(operation, payload, field_locations, mysql_client, mongo_client)
```

**For READ:** Passes through directly to `CrudEngine` — no transaction wrapping needed.

**For CREATE / UPDATE / DELETE:**

```
Phase 1 ─ Suppress MySQL auto-commits
          (monkey-patch connection.commit → no-op so individual
           CrudEngine calls don't finalize writes prematurely)

Phase 2 ─ Execute SQL-only QueryPlan via CrudEngine
          → If SQL fails: ROLLBACK, return rolled_back result

Phase 3 ─ Snapshot affected MongoDB documents (for compensation)
          → UPDATE/DELETE: query existing docs before mutation
          → INSERT: store document content to identify inserted docs

Phase 4 ─ Execute Mongo-only QueryPlan via CrudEngine
          → If Mongo fails: ROLLBACK SQL + compensate Mongo, return rolled_back

Phase 5 ─ COMMIT SQL, return committed result
```

The plan is split into two sub-plans (`_sql_only_plan` and `_mongo_only_plan`) so SQL executes first within a pending transaction, and Mongo executes second with a compensation safety net.

#### MongoDB Compensation

Since standalone MongoDB doesn't support multi-collection transactions, compensation works by:

| Operation | Compensation |
|---|---|
| INSERT failed | `delete_one` each inserted document by matching field values |
| UPDATE failed | `replace_one` each affected document from the pre-update snapshot |
| DELETE failed | `insert_one` each deleted document from the pre-delete snapshot |

---

### 3. `logical_reconstructor.py` — Logical Data Reconstruction

Presents data as unified logical entities without exposing backend details (table names, column names, join keys).

| Method | What It Does |
|---|---|
| `list_entities(field_locations)` | Extracts top-level entity names from field paths (e.g., `post`, `device`, `root`) |
| `get_entity_instances(name, ...)` | Queries both backends for fields belonging to one entity, merges, and strips backend prefixes |
| `get_all_data(...)` | Fetches all data as merged logical records |
| `get_table_stats(...)` | Returns row/document counts per SQL table and Mongo collection |

Internally reuses `QueryPlanner.build_plan()` and `CrudEngine.execute()` — no query logic is duplicated.

---

### 4. `session_manager.py` — Session Tracking

Reads persisted metadata files and checks client connection state.

| Method | Source |
|---|---|
| `get_session_info()` | `schema.json` + MySQL/Mongo connection objects |
| `get_schema()` | `metadata/schema.json` |
| `get_sql_plan()` | `metadata/sql_plan.json` |
| `get_mongo_plan()` | `metadata/mongo_plan.json` |
| `get_field_locations()` | `metadata/field_locations.json` via `MetadataCatalog` |

---

### 5. `acid_experiments.py` — ACID Validation Experiments

Four self-contained experiments, each following the pattern: **setup → act → assert → cleanup**.

#### Atomicity

1. Insert a test record normally → committed in both backends.
2. Monkey-patch `mongo_client.insert_batch` to raise an exception.
3. Attempt a second INSERT via `TransactionCoordinator`.
4. **Assert:** SQL was rolled back (count = 0 for the failed record), Mongo has no partial data.
5. Restore `insert_batch`, clean up test data.

#### Consistency

1. Insert a valid record → committed.
2. Re-insert the same record (duplicate primary key).
3. **Assert:** The system either rejects the duplicate or handles it via upsert. Record count is consistent, no data corruption.

#### Isolation

1. Insert a base record.
2. Spawn two threads behind a `threading.Barrier`:
   - **Writer:** updates the record's title field.
   - **Reader:** reads the record after a small delay.
3. **Assert:** Reader sees a consistent snapshot — either the original or the updated value, never a partial/corrupt state.

#### Durability

1. Insert a record via `TransactionCoordinator`.
2. Disconnect both MySQL and MongoDB clients.
3. Reconnect.
4. **Assert:** The record is still present in both backends.

All experiments use unique tags (`_acid_{property}_{uuid}`) and clean up after themselves.

---

### 6. `orchestrator.py` — Assignment3Pipeline

Top-level class that composes all A3 components on top of `Assignment2Pipeline`.

```python
pipeline = Assignment3Pipeline(config)

# Transactional CRUD
result = pipeline.execute_transactional(CrudOperation.CREATE, {"records": [...]})

# Logical query (JSON-based, like the dashboard would send)
result = pipeline.execute_query({"operation": "read", "fields": ["username", "title"]})

# Entity exploration
entities = pipeline.list_entities()          # ["device", "post", "root"]
entity   = pipeline.get_entity_data("post")  # LogicalEntity with clean field names

# Session info
info = pipeline.get_session_info()  # SessionInfo dataclass

# ACID experiments
results = pipeline.run_acid_experiments()       # all 4
result  = pipeline.run_acid_experiment("atomicity")  # single
```

---

### 7. `__init__.py` — Package Exports

Re-exports all public classes for clean imports:

```python
from src.a3 import Assignment3Pipeline, TransactionResult, AcidTestResult
```

---

## How to Test

### Prerequisites

The A2 pipeline must have been run first (data ingested, tables/collections created, metadata persisted in `metadata/`).

```bash
python run_pipeline.py --records 100
```

### Quick Smoke Test

```python
from src.a3 import Assignment3Pipeline

pipeline = Assignment3Pipeline()
pipeline.ensure_connected()

# Session
info = pipeline.get_session_info()
print(f"Schema: {info.schema_name} v{info.version}")
print(f"MySQL: {'✓' if info.mysql_connected else '✗'}")
print(f"Mongo: {'✓' if info.mongo_connected else '✗'}")
print(f"Tables: {info.sql_tables}")
print(f"Collections: {info.mongo_collections}")

# Transactional read
from src.a2.contracts import CrudOperation
result = pipeline.execute_transactional(
    CrudOperation.READ,
    {"fields": ["username", "event_id"], "limit": 5},
)
print(f"Status: {result.status}, Records: {len(result.sql_result.get('records', []))}")

# Entities
for entity in pipeline.list_entities():
    data = pipeline.get_entity_data(entity, limit=3)
    print(f"Entity: {data.entity_name}, Fields: {data.fields}, Instances: {len(data.instances)}")

# ACID
for test in pipeline.run_acid_experiments():
    print(f"[{'PASS' if test.passed else 'FAIL'}] {test.property_name} ({test.duration_ms:.0f}ms)")

pipeline.close()
```

### Running Individual ACID Tests

```python
result = pipeline.run_acid_experiment("atomicity")
print(result.description)
print(result.details)
```

---

## Dashboard UI — Possibilities

The A3 core logic is API-ready. The dashboard can be built with any of these approaches:

### Option A: Flask + Vanilla HTML/CSS/JS

- Lightweight, no build step, minimal dependencies.
- Flask serves a REST API (`/api/session`, `/api/entities`, `/api/query`, `/api/acid/run`).
- A single-page HTML+JS app calls the API and renders results.
- Best for: simple deployment, minimal overhead.

### Option B: Streamlit

- Pure Python — no HTML/CSS/JS needed.
- Built-in widgets for tables, JSON display, charts, forms.
- Extremely fast to prototype.
- Best for: rapid prototyping, data-centric dashboards.

### Option C: FastAPI + React/Vue/Svelte

- FastAPI for the API layer (auto-generates OpenAPI docs).
- Frontend framework for a richer interactive UI.
- Best for: polished, production-grade dashboards.

### Option D: Gradio

- Python-only, like Streamlit but with more UI control.
- Good for input forms (JSON query editor) + output display.
- Best for: quick demo-ready interfaces.

### What the Dashboard Must Show (per Assignment Spec)

| View | Data Source |
|---|---|
| **Session Panel** | `get_session_info()` → schema name, version, connection status, table/collection counts |
| **Entity Explorer** | `list_entities()` + `get_entity_data(name)` → logical entities with instances, no backend details |
| **Query Console** | `execute_query(json)` → submit read/create/update/delete, show input + result + status |
| **ACID Lab** | `run_acid_experiments()` → per-property pass/fail, timing, detailed explanation |

### Key Constraint

> The interface must **not** reveal backend-specific details such as SQL table names, MongoDB collections, indexing, or schema decisions.

All A3 APIs already abstract this away — the dashboard just needs to render the output.
