# Adaptive Database Framework

A self-adaptive database framework that autonomously ingests a live JSON data stream, infers field types, classifies every field into **MySQL** (structured) or **MongoDB** (semi-structured), and routes records to the appropriate backend — all without hardcoded schemas.


---

## 📚 Project Overview

The system consumes health-tracker JSON records from a FastAPI data stream, normalises naming conventions, observes field patterns through statistical analysis, and dynamically decides which database backend each field belongs to. Linking fields (`username`, `sys_ingested_at`) are stored in **both** backends to enable cross-database joins.

### Key Capabilities

| Capability | Description |
|---|---|
| **Dynamic Schema Inference** | No predefined schemas — field types discovered from data |
| **Adaptive Placement** | Heuristic rules decide SQL vs MongoDB per field |
| **Cross-DB Linking** | `username` + `sys_ingested_at` in both backends for joins |
| **Metadata Persistence** | Classification decisions survive process restarts |
| **Bi-temporal Timestamps** | `t_stamp` (client) + `sys_ingested_at` (server) |

---

## 🏗️ Architecture

The codebase is organized into **4 topics** + a final orchestrator:

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
 │  │  FieldNormalizer · TypeDetector · RecordNormalizer│      │
 │  │  • camelCase/PascalCase → snake_case             │      │
 │  │  • Detect IP vs float, UUID, datetime            │      │
 │  │  • Inject sys_ingested_at timestamp              │      │
 │  └──────────────────┬───────────────────────────────┘      │
 │                     │ normalized records                    │
 │                     ▼                                       │
 │               ┌───────────┐                                 │
 │               │  BUFFER   │  in-memory staging              │
 │               └─────┬─────┘                                 │
 │                     │ flush (size or timeout)               │
 │                     ▼                                       │
 │  ┌──────────────────────────────────────────────────┐      │
 │  │ TOPIC 2 — ANALYSIS & CLASSIFICATION  src/analysis/      │
 │  │  FieldAnalyzer · FieldStats · Classifier         │      │
 │  │  • Track presence %, type stability, nesting     │      │
 │  │  • Apply heuristic rules → PlacementDecision     │      │
 │  └──────────────────┬───────────────────────────────┘      │
 │                     │ decisions                             │
 │                     ▼                                       │
 │  ┌──────────────────────────────────────────────────┐      │
 │  │ TOPIC 3 — STORAGE                   src/storage/        │
 │  │  MySQLClient · MongoClient · RecordRouter        │      │
 │  │  • Dynamic CREATE TABLE / ALTER TABLE            │      │
 │  │  • Split record → SQL part + Mongo part          │      │
 │  │  • Batch insert into both backends               │      │
 │  └──────────────────┬───────────────────────────────┘      │
 │                     │                                       │
 │                     ▼                                       │
 │  ┌──────────────────────────────────────────────────┐      │
 │  │ TOPIC 4 — PERSISTENCE               src/persistence/   │
 │  │  MetadataStore                                   │      │
 │  │  • Save/load decisions, stats, mappings to JSON  │      │
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

---

## 🚀 Quick Start

### Prerequisites

- **Python 3.12+**
- **Docker & Docker Compose**
- **Poetry** (`pip install poetry`)

### 1 · Start databases

```bash
docker-compose up -d          # MySQL 8.0  +  MongoDB 7.0
```

### 2 · Install dependencies

```bash
poetry install
```

### 3 · Configure environment

```bash
cp .env.example .env
# Edit .env if you need non-default ports/passwords
```

### 4 · Run the data stream API (separate terminal)

```bash
git clone https://github.com/YogeshKMeena/Course_Resources.git
cd Course_Resources/CS432_Databases/Assignments/T2
pip install -r requirements.txt
uvicorn simulation_code:app --reload --port 8000
```

### 5 · Run the pipeline

```bash
# Ingest 100 records
poetry run python -m src.cli ingest --count 100

# Or run continuously
poetry run python -m src.cli ingest --continuous --interval 0.5

# Check status
poetry run python -m src.cli status

# View placement decisions
poetry run python -m src.cli decisions
```

---

## � Programmatic Usage

### Using the Pipeline Class Directly

The `IngestAndClassify` class provides the complete pipeline orchestration. For convenience, a `StreamingPipeline` wrapper is also available.

#### Example 1: Basic Batch Processing

```python
from src.ingest_and_classify import IngestAndClassify

# Initialize pipeline (auto-loads config from .env)
pipeline = IngestAndClassify()

# Process records
records = [
    {"username": "alice", "age": 30, "city": "NYC"},
    {"username": "bob", "score": 95.5, "metadata": {"level": 5}}
]

# Ingest batch
pipeline.ingest_batch(records)

# Check status
status = pipeline.get_status()
print(f"Buffer size: {status['buffer_size']}")
print(f"Total processed: {status['total_records_processed']}")

# Get classification summary
summary = pipeline.get_classification_summary()
print(f"SQL fields: {summary['counts']['sql']}")
print(f"MongoDB fields: {summary['counts']['mongo']}")

# Close connections
pipeline.close()
```

#### Example 2: Streaming with Context Manager

```python
from src.pipeline import StreamingPipeline

# Use context manager for automatic cleanup
with StreamingPipeline() as pipeline:
    # Stream 100 records from the data source
    summary = pipeline.start_streaming(max_records=100)
    
    # Results are auto-flushed and connections closed
    print(f"Rate: {summary['records_per_second']} rec/sec")
```

#### Example 3: Manual Record-by-Record Processing

```python
from src.ingest_and_classify import IngestAndClassify

pipeline = IngestAndClassify()

# Process one record at a time
for i in range(100):
    record = fetch_from_somewhere()  # Your data source
    pipeline.ingest(record)
    
    # Manual flush when needed
    if i % 50 == 0:
        result = pipeline.flush()
        print(f"Flushed: {result['records_processed']} records")

# Get placement decisions
decisions = pipeline.get_decisions()
for field_name, decision in decisions.items():
    print(f"{field_name} → {decision.backend.name} ({decision.reason})")

pipeline.close()
```

#### Example 4: Inspect Field Statistics

```python
from src.ingest_and_classify import IngestAndClassify

pipeline = IngestAndClassify()

# Process some data
pipeline.ingest_batch(your_records)

# Get detailed field statistics
field_stats = pipeline.get_field_stats()

for field_name, stats in field_stats.items():
    print(f"\nField: {field_name}")
    print(f"  Presence: {stats.presence_count} records")
    print(f"  Dominant type: {stats.dominant_type}")
    print(f"  Type stability: {stats.type_stability:.2%}")
    print(f"  Unique ratio: {stats.unique_ratio:.2%}")
    print(f"  Is nested: {stats.is_nested}")

pipeline.close()
```

#### Example 5: Using the Streaming Wrapper

```python
from src.pipeline import StreamingPipeline

# Create pipeline
pipeline = StreamingPipeline()

# Option 1: Stream from configured data source
pipeline.start_streaming(max_records=50, interval_seconds=0.1)

# Option 2: Process your own batch
my_records = [...]
result = pipeline.process_batch(my_records)

# Option 3: Process single records
pipeline.process_single({"username": "test", "value": 123})

# Check current status
status = pipeline.get_pipeline_status()
print(f"Will auto-flush: {status['will_auto_flush']}")

# Get field placement decisions
decisions = pipeline.get_field_decisions()

# Cleanup
pipeline.close()
```

#### Configuration Options

The pipeline uses configuration from `.env` or can be passed directly:

```python
from src.config import AppConfig, MySQLConfig, MongoConfig, BufferConfig
from src.ingest_and_classify import IngestAndClassify

# Custom configuration
config = AppConfig(
    mysql=MySQLConfig(host="localhost", port=3306, database="my_db"),
    mongo=MongoConfig(host="localhost", port=27017, database="my_db"),
    buffer=BufferConfig(buffer_size=100, buffer_timeout_seconds=10.0),
    data_stream_url="http://localhost:8000/GET/record",
    metadata_dir="./my_metadata/"
)

pipeline = IngestAndClassify(config)
```

#### CLI Wrapper

For quick testing, use the pipeline module directly:

```bash
# Run demo with sample data
poetry run python -m src.pipeline demo

# Stream from data source
poetry run python -m src.pipeline stream

# Stream specific number of records
poetry run python -m src.pipeline stream 100
```

---

## �📁 Project Structure

```
.
├── src/
│   ├── normalization/               # Topic 1
│   │   ├── field_normalizer.py      #   FieldNormalizer  — name canonicalization
│   │   ├── type_detector.py         #   TypeDetector     — semantic type detection
│   │   └── record_normalizer.py     #   RecordNormalizer — full record pipeline
│   │
│   ├── analysis/                    # Topic 2
│   │   ├── field_stats.py           #   FieldStats       — per-field statistics
│   │   ├── field_analyzer.py        #   FieldAnalyzer    — observation engine
│   │   ├── decision.py              #   PlacementDecision, Backend enum, thresholds
│   │   └── classifier.py            #   Classifier       — heuristic rules
│   │
│   ├── storage/                     # Topic 3
│   │   ├── mysql_client.py          #   MySQLClient      — dynamic DDL + inserts
│   │   ├── mongo_client.py          #   MongoClient      — document inserts + indexes
│   │   └── record_router.py         #   RecordRouter     — split & route records
│   │
│   ├── persistence/                 # Topic 4
│   │   └── metadata_store.py        #   MetadataStore    — JSON-based persistence
│   │
│   ├── config.py                    # Configuration (env vars / .env)
│   ├── ingest_and_classify.py       # ★ IngestAndClassify orchestrator
│   └── cli.py                       # CLI entry point
│
├── tests/
│   ├── conftest.py                  # Shared fixtures
│   └── test_ingest_and_classify.py  # Test skeleton
│
├── docker-compose.yml               # MySQL 8.0 + MongoDB 7.0
├── pyproject.toml                   # Poetry config + ruff/mypy/pytest
├── .env.example                     # Environment template
├── .pre-commit-config.yaml          # Ruff + mypy hooks
└── .github/
    ├── workflows/ci.yml             # Lint → Test CI pipeline
    └── PULL_REQUEST_TEMPLATE.md
```

---

## 🧪 Testing

```bash
# All tests
poetry run pytest

# With coverage report
poetry run pytest --cov=src --cov-report=term-missing

# Specific topic
poetry run pytest tests/test_normalization.py
poetry run pytest tests/test_analysis.py
poetry run pytest tests/test_storage.py
poetry run pytest tests/test_persistence.py

# Integration tests only
poetry run pytest -m integration
```

---

## 🔧 Development

### Code Quality

```bash
poetry run ruff format .          # Auto-format
poetry run ruff check . --fix     # Lint + auto-fix
poetry run mypy src/              # Type checking
```

### Pre-commit Hooks

```bash
poetry run pre-commit install     # One-time setup
poetry run pre-commit run --all-files   # Manual run
```

### Branching Convention

```
main                ← stable, passing CI
├── topic1/…        ← normalization work
├── topic2/…        ← analysis & classification work
├── topic3/…        ← storage work
└── topic4/…        ← persistence work
```

---



## 📖 References

- [Course Project Document](./Databases_CS432__2026_%20Track%202.pdf)
- [Data Stream API (Course Repo)](https://github.com/YogeshKMeena/Course_Resources/tree/main/CS432_Databases/Assignments/T2)
- [API Endpoint](http://127.0.0.1:8000/GET/record/{count})


