# Adaptive Database Framework

A self-adaptive database framework that autonomously ingests a live JSON data stream, infers field types, classifies every field into **MySQL** (structured) or **MongoDB** (semi-structured), and routes records to the appropriate backend — all without hardcoded schemas.

---

## Overview

This framework solves the problem of handling heterogeneous JSON data streams where schema is unknown or evolving. Instead of requiring predefined schemas, the system:

1. **Observes** incoming data to learn field patterns (types, presence rates, nesting)
2. **Classifies** each field as SQL-suitable or document-suitable using heuristics
3. **Routes** records to MySQL and/or MongoDB based on classification
4. **Adapts** to schema evolution with automatic type widening and migrations

### Key Capabilities

| Capability | Description |
|---|---|
| **Dynamic Schema Inference** | No predefined schemas — field types discovered from data |
| **Adaptive Placement** | Heuristic rules decide SQL vs MongoDB per field |
| **Cross-DB Linking** | `username` + `sys_ingested_at` stored in both backends for joins |
| **Upsert Handling** | Automatic primary key detection; updates existing records instead of duplicating |
| **Crash Recovery** | JSONL write-ahead log ensures no data loss on failure |
| **Type Widening** | Automatic INT→BIGINT, VARCHAR(50)→VARCHAR(255) migrations |
| **Metadata Persistence** | Classification decisions survive process restarts |

---

## Architecture

The codebase is organized into **4 topics** + a central orchestrator:

```
                         ┌──────────────────┐
                         │  Data Stream API │
                         │  (FastAPI :8000) │
                         └────────┬─────────┘
                                  │  raw JSON records
                                  ▼
 ┌────────────────────────────────────────────────────────────┐
 │                    IngestAndClassify                       │
 │                                                            │
 │  ┌──────────────────────────────────────────────────┐      │
 │  │ TOPIC 1 — NORMALIZATION              src/normalization/ │
 │  │  TypeDetector · RecordNormalizer                │      │
 │  │  • Detect semantic types (IP, UUID, datetime)   │      │
 │  │  • Flatten nested structures                     │      │
 │  │  • Inject sys_ingested_at timestamp              │      │
 │  └──────────────────┬───────────────────────────────┘      │
 │                     │ normalized records                    │
 │                     ▼                                       │
 │          ┌─────────────────────┐                            │
 │          │  BUFFER + WAL       │  crash-safe staging        │
 │          │  (pending.jsonl)    │                            │
 │          └─────────┬───────────┘                            │
 │                    │ flush (size/timeout)                   │
 │                    ▼                                        │
 │  ┌──────────────────────────────────────────────────┐      │
 │  │ TOPIC 2 — ANALYSIS & CLASSIFICATION  src/analysis/      │
 │  │  FieldAnalyzer · FieldStats · Classifier         │      │
 │  │  • Track presence %, type stability, nesting     │      │
 │  │  • Apply heuristic rules → PlacementDecision     │      │
 │  │  • Dynamic primary key assignment                │      │
 │  └──────────────────┬───────────────────────────────┘      │
 │                     │ decisions                             │
 │                     ▼                                       │
 │  ┌──────────────────────────────────────────────────┐      │
 │  │ TOPIC 3 — STORAGE                   src/storage/        │
 │  │  MySQLClient · MongoClient · RecordRouter        │      │
 │  │  • Dynamic CREATE TABLE / ALTER TABLE            │      │
 │  │  • Upsert with primary key deduplication         │      │
 │  │  • Type migration (INT→BIGINT, etc.)             │      │
 │  └──────────────────┬───────────────────────────────┘      │
 │                     │                                       │
 │                     ▼                                       │
 │  ┌──────────────────────────────────────────────────┐      │
 │  │ TOPIC 4 — PERSISTENCE               src/persistence/   │
 │  │  MetadataStore                                   │      │
 │  │  • Save/load decisions, stats to JSON            │      │
 │  │  • Enables restart without re-analysis           │      │
 │  └──────────────────────────────────────────────────┘      │
 └────────────────────────────────────────────────────────────┘
```

### Classification Rules

| # | Condition | → Backend |
|---|---|---|
| 1 | Field is `username`, `sys_ingested_at`, or `t_stamp` | **BOTH** |
| 2 | Value is nested (dict / list) | **MongoDB** |
| 3 | Presence ≥ 70% AND type stability ≥ 90% | **SQL** |
| 4 | Everything else | **MongoDB** |

### Primary Key Selection

The system automatically determines the best primary key from observed data:

1. **Must be present in 100% of records** (not nullable)
2. **Must have ≥90% unique values** (candidate key)
3. **Prefers identifier-like names** (`*_id`, `*name`, `*key`, etc.)
4. **Excludes timestamps** (not suitable as identifiers)

This ensures upserts work correctly — when the same `username` appears again, the record is updated rather than duplicated.

---

## Quick Start

### Prerequisites

- **Python 3.12+**
- **Docker & Docker Compose**

### 1. Clone and Navigate

```bash
git clone https://github.com/rajmodi8905/Realtime-Adaptive-Database.git
cd Realtime-Adaptive-Database
```

### 2. Start Databases

```bash
docker-compose up -d
docker ps  # Verify: adaptive_db_mysql, adaptive_db_mongodb
```

### 3. Install Dependencies

```bash
pip install -r requirements.txt
```

### 4. Test the Pipeline

```bash
python test_pipeline.py
```

Expected output:
```
============================================================
Testing Adaptive Database Pipeline
============================================================
✓ Pipeline initialized successfully
✓ Record ingested into buffer
✓ Flushed 1 records
   SQL fields: username, age, email, score, is_active
   MongoDB fields: metadata
   Both backends: username, sys_ingested_at
✓ All tests passed!
============================================================
```

### 5. (Optional) Stream Live Data

```bash
# Start the course data stream API (separate terminal)
uvicorn simulation_code:app --reload --port 8000

# Stream records into the adaptive database
python stream_from_api.py
```

---

## Crash Recovery

The system uses a **Write-Ahead Log (WAL)** for durability:

1. Every ingested record is appended to `metadata/pending.jsonl` *before* buffering
2. On successful flush, the WAL is cleared
3. On restart, any pending records in the WAL are automatically recovered

This ensures **no data loss** even if the process crashes mid-batch.

---

## Database Management

### MySQL
```bash
docker exec -it adaptive_db_mysql mysql -uroot -prootpassword adaptive_db
```
```sql
SHOW TABLES;
SELECT * FROM records LIMIT 10;
DESCRIBE records;  -- See dynamically created schema
```

### MongoDB
```bash
docker exec -it adaptive_db_mongodb mongosh adaptive_db
```
```javascript
db.records.find().limit(10)
db.records.countDocuments()
db.records.getIndexes()  // See auto-created unique index
```

---

## Programmatic Usage

### Basic Batch Processing

```python
from src.ingest_and_classify import IngestAndClassify

pipeline = IngestAndClassify()

records = [
    {"username": "alice", "age": 30, "city": "NYC"},
    {"username": "bob", "score": 95.5, "metadata": {"level": 5}}
]

pipeline.ingest_batch(records)
result = pipeline.flush()

print(f"Processed: {result['records_processed']} records")
print(f"SQL fields: {result['decisions_sql']}")
print(f"Mongo fields: {result['decisions_mongo']}")

pipeline.close()
```

### Streaming with Context Manager

```python
from src.pipeline import StreamingPipeline

with StreamingPipeline() as pipeline:
    summary = pipeline.start_streaming(max_records=100)
    print(f"Rate: {summary['records_per_second']:.1f} rec/sec")
```

### Inspect Field Statistics

```python
pipeline = IngestAndClassify()
pipeline.ingest_batch(records)

for field, stats in pipeline.get_field_stats().items():
    print(f"{field}: presence={stats.presence_ratio:.0%}, "
          f"type_stability={stats.type_stability:.0%}, "
          f"unique_ratio={stats.unique_ratio:.0%}")
```

---

## Project Structure

```
.
├── src/
│   ├── normalization/               # Topic 1: Type detection & normalization
│   │   ├── type_detector.py         #   Semantic type detection (IP, UUID, etc.)
│   │   └── record_normalizer.py     #   Flatten, coerce, inject timestamps
│   │
│   ├── analysis/                    # Topic 2: Statistics & classification
│   │   ├── field_stats.py           #   Per-field statistics tracking
│   │   ├── field_analyzer.py        #   Observation engine
│   │   ├── decision.py              #   PlacementDecision, Backend enum
│   │   └── classifier.py            #   Heuristic classification rules
│   │
│   ├── storage/                     # Topic 3: Database operations
│   │   ├── mysql_client.py          #   Dynamic DDL, upserts, migrations
│   │   ├── mongo_client.py          #   Document inserts, unique indexes
│   │   ├── record_router.py         #   Split & route records to backends
│   │   └── migrator.py              #   Type widening handler
│   │
│   ├── persistence/                 # Topic 4: Metadata persistence
│   │   └── metadata_store.py        #   JSON-based state persistence
│   │
│   ├── config.py                    # Configuration from .env
│   ├── ingest_and_classify.py       # ★ Main orchestrator
│   └── pipeline.py                  # StreamingPipeline wrapper
│
├── metadata/                        # Persisted state (auto-created)
│   ├── decisions.json               #   Field → backend mapping
│   ├── field_stats.json             #   Per-field statistics
│   ├── state.json                   #   Total records, last flush
│   └── pending.jsonl                #   WAL for crash recovery
│
├── docker-compose.yml               # MySQL 8.0 + MongoDB 7.0
├── requirements.txt                 # Python dependencies
├── test_pipeline.py                 # Quick integration test
└── stream_from_api.py               # Live data stream consumer
```

---

## Configuration

Environment variables (`.env`):

```bash
# MySQL
MYSQL_HOST=localhost
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=rootpassword
MYSQL_DATABASE=adaptive_db

# MongoDB
MONGO_HOST=localhost
MONGO_PORT=27017
MONGO_DATABASE=adaptive_db

# Buffer
BUFFER_SIZE=50           # Records before auto-flush
BUFFER_TIMEOUT=30.0      # Seconds before auto-flush

# Data Stream
DATA_STREAM_URL=http://localhost:8000/GET/record
```

---

## Troubleshooting

### Docker Issues
```bash
docker-compose logs mysql     # Check MySQL logs
docker-compose logs mongodb   # Check MongoDB logs
docker-compose down -v        # Reset everything (deletes data)
```

### Connection Refused
```bash
# Wait for containers to be healthy
docker-compose ps  # Look for "Up (healthy)"
```

### Reset Pipeline State
```bash
rm -rf metadata/              # Clear all persisted state
docker-compose down -v        # Clear database data
docker-compose up -d          # Restart fresh
```

---

## Key Dependencies

| Package | Purpose |
|---------|---------|
| `pymysql` | MySQL connector |
| `pymongo` | MongoDB driver |
| `python-dotenv` | Environment configuration |
| `requests` | HTTP client for data streams |

---

## Course Information

**Course:** CS432 Databases (Spring 2026)  
**Track:** Track 2 - Adaptive Database Framework
