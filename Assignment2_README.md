# Assignment 2 - Metadata-Driven Hybrid Storage Pipeline

A schema-aware extension of the A1 adaptive framework that automatically normalizes data into **SQL tables** and **MongoDB collections**, then provides **metadata-driven CRUD** across both backends - users query using simple JSON payloads without knowing which backend stores what.

Video explanation: [Watch here](https://drive.google.com/file/d/1kc24Cd8-Kcc7NmE76cIpBPlsq8I_Xqhz/view?usp=sharing)

---

## How A1 Fits In (Brief Context)

Assignment 1 README: [Adaptive Database Framework (A1)](Assignment1_README.md)

Assignment 1 built the `IngestAndClassify` pipeline that:
1. **Ingests** raw JSON records and detects field types (int, string, datetime, array, etc.)
2. **Analyzes** patterns - presence ratio, type stability, nesting depth
3. **Classifies** each field as SQL, MongoDB, or BOTH using heuristic rules:
   - Flat, stable, present ≥70% → SQL
   - Nested or sparse → MongoDB
   - Linking fields (`username`, `sys_ingested_at`) → BOTH

A2 consumes A1's classification output and builds normalized storage plans on top of it.

---

## Project Structure

```
├── src/
│   ├── a2/                                  # Assignment 2 modules
│   │   ├── orchestrator.py                  # Pipeline controller (ties everything together)
│   │   ├── contracts.py                     # Shared data structures (CrudOperation, QueryPlan, etc.)
│   │   ├── sql_normalization_engine.py      # SQL table plan generation + DDL execution
│   │   ├── mongo_decomposition_engine.py    # MongoDB collection plan generation
│   │   ├── storage_strategy_generator.py    # Combines SQL + Mongo plans into field locations
│   │   ├── query_planner.py                 # Builds backend-specific query plans from JSON payloads
│   │   ├── crud_engine.py                   # Executes query plans against MySQL + MongoDB
│   │   ├── metadata_catalog.py              # Persists all plans to JSON files
│   │   └── schema_registry.py              # Schema registration service
│   │
│   ├── analysis/                            # A1: Field analysis + classification
│   ├── normalization/                       # A1: Type detection + record normalization
│   ├── storage/                             # A1: MySQL/MongoDB clients + record routing
│   ├── persistence/                         # A1: Metadata persistence
│   ├── config.py                            # Configuration loader (.env)
│   └── ingest_and_classify.py               # A1: Main orchestrator
│
├── schemas/
│   └── assignment2_schema.template.json     # Schema defining the data shape
│
├── metadata/                                # Generated plans and field mappings
│   ├── schema.json
│   ├── sql_plan.json
│   ├── mongo_plan.json
│   └── field_locations.json
│
├── run_pipeline.py                          # CLI: runs the full A2 pipeline
├── test_a2_components.py                    # Component-level smoke test
├── docker-compose.yml
├── requirements.txt
└── .env
```

---

## Core Logic (Per File)

### `orchestrator.py` - Pipeline Controller

Entry point for the entire A2 pipeline. Exposes 5 key methods:

| Method | What it does |
|--------|-------------|
| `register_schema(registration)` | Saves the JSON schema to the metadata catalog |
| `run_ingestion(records)` | Feeds records through A1 → bridges classification into A2's `ClassifiedField` format |
| `build_storage_strategy(registration)` | Generates SQL tables + Mongo collections + field locations, executes DDL, persists everything |
| `execute_operation(operation, payload)` | Takes a CRUD operation + JSON payload, builds a query plan, executes across both backends |
| `generate_records(n)` | Walks the JSON schema to produce N synthetic records for testing |

### `contracts.py` - Data Structures

Defines all shared types used across A2:
- `CrudOperation` - enum: CREATE, READ, UPDATE, DELETE
- `SchemaRegistration` - schema name, version, JSON schema, constraints
- `ClassifiedField` - bridges A1 classification to A2 (field path, backend, type, nesting info)
- `SqlTablePlan` - table name, columns, PK, FKs, indexes
- `CollectionPlan` - collection name, embedded paths, referenced paths
- `FieldLocation` - maps a field path to its backend, table/collection, and column/path
- `QueryPlan` - the executable plan: SQL queries + Mongo queries + merge strategy

### `sql_normalization_engine.py` - SQL Normalization (1NF→3NF)

Generates normalized SQL table plans from classified fields:
- **1NF**: Arrays of scalars or flat objects become separate child tables with PK/FK relationships
- **2NF**: Each child table's non-key columns depend on the whole primary key
- **3NF**: 1:1 nested objects (like `device`, `metrics`) are flattened into the parent table using dot-notation columns

Also executes `CREATE TABLE` DDL on MySQL via the client.

### `mongo_decomposition_engine.py` - MongoDB Decomposition

Generates MongoDB collection plans using heuristics:
- Decides embed vs. reference per field based on array size, nesting depth, and growth patterns
- Skips child fields of MongoDB-bound arrays (they're embedded in their parent, not separate collections)
- Applies 5 heuristics (H1–H5) for embedding/referencing decisions

### `storage_strategy_generator.py` - Field Location Mapping

Combines SQL table plans and MongoDB collection plans into a unified `FieldLocation` list. Each entry maps:
- `field_path` → which backend (SQL/MongoDB/BOTH)
- `table_or_collection` → exact table or collection name
- `column_or_path` → column name or document path
- `join_keys` → keys used to join across backends

### `query_planner.py` - Query Plan Builder

Converts user JSON payloads into executable `QueryPlan` objects:
- Looks up each requested field in the field location map
- Splits queries by backend (SQL queries + Mongo queries)
- Supports **alias resolution**: `"title"` automatically resolves to `"post.title"` if unambiguous
- Handles filter splitting - routes each filter condition to the correct backend

### `crud_engine.py` - Query Executor

Executes `QueryPlan` objects against MySQL and MongoDB:
- **CREATE**: Inserts records, splitting data across SQL tables and Mongo collections while maintaining join keys
- **READ**: Queries both backends, merges results on shared keys (`username`, `event_id`)
- **UPDATE**: Routes SET operations to the correct backend per field
- **DELETE**: Cascades across SQL (FK-aware) and MongoDB

### `metadata_catalog.py` - Plan Persistence

Saves and loads all planning artifacts as JSON files:
- `schema.json` - registered schema
- `sql_plan.json` - normalized SQL tables + relationships
- `mongo_plan.json` - MongoDB collection plans
- `field_locations.json` - field-to-backend mapping

---

## Setup

### Prerequisites

- Python 3.12+
- Docker & Docker Compose

### 1. Clone and Navigate

```bash
git clone https://github.com/rajmodi8905/Realtime-Adaptive-Database.git
cd Realtime-Adaptive-Database
```

### 2. Start Databases

```bash
docker-compose up -d
docker ps
```

Verify: `adaptive_db_mysql` and `adaptive_db_mongodb` should be running.

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Configure Environment

Ensure `.env` exists with:

```
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=rootpassword
MYSQL_DATABASE=adaptive_db

MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DATABASE=adaptive_db

BUFFER_SIZE=50
BUFFER_TIMEOUT=30.0
```

### 5. Run the Pipeline

```bash
python run_pipeline.py --records 100
python run_pipeline.py -n 50
```

### 6. Component Test

```bash
python test_a2_components.py --execute-sql --execute-mongo
```

---

## Querying (User Guide)

Users interact with the pipeline through `execute_operation(operation, payload)`. The payload is a simple JSON dict - **no knowledge of SQL tables, MongoDB collections, or field locations is needed**. The pipeline automatically routes queries to the right backends.

### Payload Formats

**READ** - Fetch records matching filters:
```python
pipeline.execute_operation(CrudOperation.READ, {
    "fields": ["username", "event_id", "title"],
    "filters": {"username": "user_1"},
    "limit": 10,
})
```

**CREATE** - Insert new records:
```python
pipeline.execute_operation(CrudOperation.CREATE, {
    "records": [
        {"username": "u1", "event_id": "e1", "timestamp": "2026-01-01T00:00:00Z", ...}
    ],
})
```

**UPDATE** - Modify fields matching filters:
```python
pipeline.execute_operation(CrudOperation.UPDATE, {
    "updates": {"title": "new_title"},
    "filters": {"username": "user_1", "event_id": "event_id_1"},
})
```

**DELETE** - Remove records matching filters:
```python
pipeline.execute_operation(CrudOperation.DELETE, {
    "filters": {"username": "user_1", "event_id": "event_id_1"},
})
```

### Run CRUD Queries from Terminal

```bash
# READ using inline JSON payload
python -m src.cli query --op read --payload '{"fields":["username","event_id"],"filters":{"username":"username_1"},"limit":5}'

# UPDATE using payload file
python -m src.cli query --op update --payload-file payloads/update.json

# Any operation with interactive stdin JSON
python -m src.cli query --op delete --interactive
```

Notes:
- `--op` supports: `create`, `read`, `update`, `delete`
- Use exactly one of: `--payload`, `--payload-file`, `--interactive`
- Metadata (especially `field_locations.json`) must already exist from a prior strategy build

---

### Field Alias Resolution

You do **not** need to use full dotted paths. The query planner resolves short names automatically:

| You write | Resolves to | Why |
|-----------|-------------|-----|
| `"title"` | `"post.title"` | Only one field ends in `.title` |
| `"username"` | `"username"` | Exact match |
| `"event_id"` | `"event_id"` | Exact match |
| `"latency_ms"` | `"metrics.latency_ms"` | Only one field ends in `.latency_ms` |

Full dotted paths (`"post.title"`, `"metrics.latency_ms"`) always work.

### What's Allowed / Not Allowed

| Action | Supported | Notes |
|--------|-----------|-------|
| Read with field selection | Yes | Specify which fields to return |
| Read with filters | Yes | Filter on any field (SQL or Mongo) |
| Read with limit | Yes | Cap number of returned records |
| Create single/batch records | Yes | Automatically split across backends |
| Update specific fields | Yes | Routes to correct backend per field |
| Delete with filters | Yes | Cascades across SQL tables + Mongo collections |
| Ambiguous short field names | No | If two fields share the same leaf name at the same depth, the field is dropped silently |
| Update/Delete without filters | No | Will error - filters are required to prevent accidental bulk operations |
| Query fields not in schema | No | Unknown fields are skipped (not in field_locations) |
| Cross-backend joins in READ | Yes | Records are merged on shared keys (`username`, `event_id`) |

---

## Examples

### Example 1: Read with filter

```python
from src.a2.contracts import CrudOperation
from src.a2.orchestrator import Assignment2Pipeline

pipeline = Assignment2Pipeline()
schema = load_registration(Path("schemas/assignment2_schema.template.json"))
pipeline.register_schema(schema)

records = pipeline.generate_records(10, schema)
pipeline.run_ingestion(records)
pipeline.build_storage_strategy(schema)
pipeline.execute_operation(CrudOperation.CREATE, {"records": records})

result = pipeline.execute_operation(CrudOperation.READ, {
    "fields": ["username", "event_id", "title", "latency_ms"],
    "filters": {"username": "username_1"},
    "limit": 5,
})
```

**Output:**
```
{
    "status": "success",
    "records": [
        {
            "username": "username_1",
            "event_id": "event_id_1",
            "post": {"title": "title_abcd_1"},
            "metrics": {"latency_ms": 45.23}
        }
    ],
    "sql_rows": 1,
    "mongo_docs": 1,
    "errors": []
}
```

### Example 2: Update a nested field

```python
result = pipeline.execute_operation(CrudOperation.UPDATE, {
    "updates": {"title": "updated_title"},
    "filters": {"username": "username_1", "event_id": "event_id_1"},
})
```

**Output:**
```
{
    "status": "success",
    "sql_updated": 1,
    "mongo_updated": 1,
    "errors": []
}
```

`"title"` is resolved to `"post.title"` → the update is routed to the SQL `event` table (where `post.title` is stored as a flattened column) and any MongoDB collections that embed it.

### Example 3: Delete a record

```python
result = pipeline.execute_operation(CrudOperation.DELETE, {
    "filters": {"username": "username_2", "event_id": "event_id_2"},
})
```

**Output:**
```
{
    "status": "success",
    "sql_deleted": 3,
    "mongo_deleted": 2,
    "errors": []
}
```

The delete cascades: the root row in `event`, its child rows in `post_tags` and `post_attachments` (FK cascade), plus matching documents in all MongoDB collections.

---

## Generated Metadata

After a pipeline run, the `metadata/` directory contains:

| File | Contents |
|------|----------|
| `schema.json` | Registered schema definition |
| `sql_plan.json` | Normalized SQL tables - columns, PKs, FKs, indexes |
| `mongo_plan.json` | MongoDB collection plans - embedded and referenced paths |
| `field_locations.json` | Field → backend/table/column routing map (used by query planner) |
| `decisions.json` | A1 per-field classification decisions |
| `field_stats.json` | A1 accumulated field statistics |

---

## Inspecting Data

### MySQL
```bash
docker exec -it adaptive_db_mysql mysql -uroot -prootpassword adaptive_db
```
```sql
SHOW TABLES;
SELECT * FROM event LIMIT 5;
SELECT * FROM post_tags LIMIT 5;
SELECT * FROM post_attachments LIMIT 5;
```

### MongoDB
```bash
docker exec -it adaptive_db_mongodb mongosh adaptive_db
```
```javascript
show collections
db.events.find().limit(5)
db.events_post_comments.find().limit(5)
```

---

## Reset

```bash
rm -rf metadata/
docker-compose down -v
docker-compose up -d
```

---
