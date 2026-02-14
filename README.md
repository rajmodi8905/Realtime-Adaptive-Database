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
 │  │  TypeDetector · RecordNormalizer                │      │
 │  │  • Detect IP vs float, UUID, datetime            │      │
 │  │  • Inject sys_ingested_at timestamp              │      │
 │  │  • Aggressive type coercion                      │      │
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
- **pip** (Python package installer)

### 1 · Clone and Navigate to Project

```bash
cd /path/to/Realtime-Adaptive-Database
```

### 2 · Configure Environment

```bash
# .env file is already configured for Docker
# Default settings:
# - MySQL: localhost:3306 (user: root, password: rootpassword)
# - MongoDB: localhost:27017 (no auth)
# - Database name: adaptive_db (auto-created)
```

### 3 · Start Docker Databases

```bash
docker-compose up -d          # MySQL 8.0 + MongoDB 7.0
docker ps                     # Verify containers are running
```

You should see two containers running:
- `adaptive_db_mysql` on port 3306
- `adaptive_db_mongodb` on port 27017

### 4 · Install Python Dependencies

```bash
pip install -r requirements.txt
```

This installs:
- `pymysql` - MySQL database connector
- `pymongo` - MongoDB driver
- `python-dotenv` - Environment variable loader
- `requests` - HTTP client for data streams

### 5 · Test the Pipeline

```bash
python test_pipeline.py
```

Expected output:
```
============================================================
Testing Adaptive Database Pipeline
============================================================

1. Initializing pipeline...
✓ Pipeline initialized successfully

2. Ingesting test record...
✓ Record ingested into buffer

3. Pipeline Status:
   - Buffer size: 1
   - Total records processed: 0

4. Flushing buffer and routing to databases...
✓ Flushed 1 records
✓ Flush completed

5. Classification Decisions:
   SQL fields: username, age, email, score, is_active
   MongoDB fields: metadata
   Both backends: username, sys_ingested_at

6. Closing connections...
✓ All tests passed! Pipeline is working correctly.
============================================================
```

### 6 · (Optional) Run with Data Stream API

If you have access to the course data stream API:

```bash
# In a separate terminal, start the data stream
git clone https://github.com/YogeshKMeena/Course_Resources.git
cd Course_Resources/CS432_Databases/Assignments/T2
pip install -r requirements.txt
uvicorn simulation_code:app --reload --port 8000

# In your project terminal, use the streaming pipeline
python -m src.pipeline
```

---

## 🗄️ Database Management

### Connect to MySQL
```bash
docker exec -it adaptive_db_mysql mysql -uroot -prootpassword
```

```sql
USE adaptive_db;
SHOW TABLES;
SELECT * FROM records LIMIT 10;
```

### Connect to MongoDB
```bash
docker exec -it adaptive_db_mongodb mongosh
```

```javascript
use adaptive_db
db.records.find().limit(10)
db.records.countDocuments()
```

### Stop/Restart Databases
```bash
docker-compose down              # Stop containers
docker-compose down -v           # Stop and remove data volumes
docker-compose restart           # Restart containers
docker-compose logs -f mysql     # View MySQL logs
docker-compose logs -f mongodb   # View MongoDB logs
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
# Run with sample data
python -m src.pipeline

# Or use the test script
python test_pipeline.py
```

---

## 📁 Project Structure

```
.
├── src/
│   ├── normalization/               # Topic 1
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
│   │   ├── mysql_client.py          #   MySQLClient      — dynamic DDL + inserts (PyMySQL)
│   │   ├── mongo_client.py          #   MongoClient      — document inserts + indexes
│   │   ├── record_router.py         #   RecordRouter     — split & route records
│   │   └── migrator.py              #   Migrator         — type migration handler
│   │
│   ├── persistence/                 # Topic 4
│   │   └── metadata_store.py        #   MetadataStore    — JSON-based persistence
│   │
│   ├── config.py                    # Configuration (env vars / .env)
│   ├── ingest_and_classify.py       # ★ IngestAndClassify orchestrator
│   ├── pipeline.py                  # StreamingPipeline wrapper
│   └── cli.py                       # CLI entry point (placeholder)
│
├── tests/
│   ├── conftest.py                  # Shared fixtures
│   └── test_*.py                    # Test suites
│
├── test_pipeline.py                 # Quick integration test
├── requirements.txt                 # Python dependencies
├── docker-compose.yml               # MySQL 8.0 + MongoDB 7.0
├── pyproject.toml                   # Poetry config (optional)
├── .env                             # Environment configuration
├── .env.example                     # Environment template
└── README.md                        # This file
```

---

## 🧪 Testing

### Quick Integration Test

```bash
# Run the included test pipeline
python test_pipeline.py
```

### Running Test Suite

```bash
# All tests (requires pytest)
pip install pytest pytest-cov
pytest

# With coverage report
pytest --cov=src --cov-report=term-missing

# Specific test files
pytest tests/test_normalization.py
pytest tests/test_analysis.py
pytest tests/test_storage.py
pytest tests/test_integration.py
```

---

## 🔧 Development

### Code Quality (Optional)

If you want to use linting and formatting tools:

```bash
pip install ruff mypy
ruff format .                 # Auto-format
ruff check . --fix            # Lint + auto-fix
mypy src/                     # Type checking
```

---

## 🐛 Troubleshooting

### Docker Issues

**Containers won't start:**
```bash
# Check if ports are already in use
lsof -i :3306  # MySQL
lsof -i :27017 # MongoDB

# View container logs
docker-compose logs mysql
docker-compose logs mongodb
```

**Database connection refused:**
```bash
# Wait for containers to be healthy
docker-compose ps
# Look for "Up (healthy)" status

# Test MySQL connection
docker exec -it adaptive_db_mysql mysql -uroot -prootpassword -e "SELECT 1"

# Test MongoDB connection
docker exec -it adaptive_db_mongodb mongosh --eval "db.version()"
```

### Python Issues

**Module not found errors:**
```bash
# Make sure you're in the project root
pwd  # Should show .../Realtime-Adaptive-Database

# Reinstall dependencies
pip install -r requirements.txt

# Verify installations
pip list | grep -E "pymysql|pymongo|dotenv"
```

**pydantic build errors:**
- The requirements.txt has been simplified to avoid pydantic compilation issues
- If you see pydantic errors, they're only warnings from unused pyproject.toml

### Pipeline Issues

**KeyError or AttributeError:**
- Make sure Docker containers are running
- Check that .env file exists and has correct values
- Run `python test_pipeline.py` to verify setup

**Database permissions:**
```bash
# MySQL - check permissions
docker exec -it adaptive_db_mysql mysql -uroot -prootpassword \
  -e "SHOW GRANTS FOR 'root'@'%'"

# MongoDB - no auth required for local development
```

---

## 📖 References

- [Course Project Document](./Databases_CS432__2026_%20Track%202.pdf)
- [Data Stream API (Course Repo)](https://github.com/YogeshKMeena/Course_Resources/tree/main/CS432_Databases/Assignments/T2)
- [API Endpoint](http://127.0.0.1:8000/GET/record/{count})

---

## 📝 Key Dependencies

| Package | Version | Purpose |
|---------|---------|---------|
| `pymysql` | 1.1.0 | MySQL database connector |
| `pymongo` | 4.6.0 | MongoDB driver |
| `python-dotenv` | 1.0.0 | Environment variable loader |
| `requests` | 2.31.0 | HTTP client for data streams |

---

## ✅ Quick Verification Checklist

After setup, verify everything works:

- [ ] Docker containers running: `docker ps` shows both MySQL and MongoDB
- [ ] Python dependencies installed: `pip list | grep pymysql`
- [ ] Config file exists: `cat .env` shows database settings
- [ ] MySQL accessible: `docker exec -it adaptive_db_mysql mysql -uroot -prootpassword -e "SELECT 1"`
- [ ] MongoDB accessible: `docker exec -it adaptive_db_mongodb mongosh --eval "db.version()"`
- [ ] Test passes: `python test_pipeline.py` completes successfully

---

## 🎓 Course Information

**Course:** CS432 Databases (Spring 2026)  
**Track:** Track 2 - Adaptive Database Framework  
**Institution:** [Your Institution Name]

---


