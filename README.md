# Realtime Adaptive Database System

A sophisticated adaptive database framework managing a hybrid architecture combining the relational structure of **MySQL** with the schemaless flexibility of **MongoDB**. Under the hood, this system manages real-time distributed ingestion, concurrent transactional validation, automatic storage strategy generation, and pessimistic Isolation locking via a complex Multi-Backend Pipeline Coordinator.

To interact with this powerful pipeline directly in the browser, the system contains a `Logical Dashboard` (FastAPI + Vanilla JS) which acts as an abstraction layer simulating the database logic and guaranteeing Data Atomicity and Integrity without leaking physical database tables.

---

## 📚 Documentation

For a structured documentation set, start here:

- [Project Docs Hub](docs/README.md)
- [Quickstart](docs/quickstart.md)
- [Architecture](docs/architecture.md)
- [API Reference](docs/api.md)
- [Developer Guide](docs/developer-guide.md)

Assignment-specific deep dives are in:

- [Assignment 1 README](docs/assignments/assignment1.md)
- [Assignment 2 README](docs/assignments/assignment2.md)
- [Assignment 3 README](docs/assignments/assignment3.md)

---

## 📁 Repository Structure

The codebase is organized into logical domains for clean separation of concerns:

- **`src/query_engine/`**: (Formerly `a2`) Handles SQL normalization, MongoDB document decomposition, dynamic schema generation, and storage strategy distribution.
- **`src/transaction/`**: (Formerly `a3`) The Multi-Backend Pipeline Coordinator, managing cross-database ACID compliance, row locking, state rollback, and concurrency.
- **`benchmarks/`**: Contains the `k6` automated load testing scripts (e.g., `load_test.js`) used to measure the hybrid database's throughput.
- **`scripts/`**: Executable scripts for interacting with the database or simulating ingestion streams (e.g., `run_pipeline.py`).
- **`tests/`**: Unit tests and E2E tests (including Pytest and Puppeteer suites) to validate concurrency locks and UI functionality.
- **`dashboard/`**: The FastAPI server and Vite+React frontend that drives the web interface.

---

## 🛠 Prerequisites

Before launching the project, ensure your environment runs the following:
* **Docker & Docker Compose**: Required for hybrid database scaffolding.
* **Python**: `3.10` or higher.

---

## 💻 Installation & Setup

All operations scale natively from the default installation. Execute everything below directly in root directory.

### 1. Launch the Full Stack
Start the databases, Python backend, and React frontend — all in one command:
```bash
docker compose up -d
```
*(Verify all services are online with `docker compose ps`)*

### 2. Access the Dashboard
The application is live. Open your browser and navigate to:

**[http://localhost:8080](http://localhost:8080)**

---

## 🚀 Running Load Tests

To run the automated k6 load test against the running stack, use the `loadtest` profile:

```bash
docker compose --profile loadtest run --rm k6
```

> **Note:** The database must be bootstrapped first (Step 1 of the Usage Guide below) before running the load test.

---


## 📖 Usage Guide

When viewing the Dashboard, complete the following sequences:

### Step 1: Database Bootstrap
1. Locate **Bootstrap** within the GUI Setup Sidebar.
2. Hit the generation criteria form and use the preset 100/50 parameters.
3. Select **⚡️ Bootstrap Database**. This initializes the relational `event` schema internally, ingests dummy metrics, writes out automatic strategies for SQL / Mongo tables, and begins storage distribution.

### Step 2: The Entity Browser
After Bootstrapping successfully maps schemas into RAM:
1. Select the **Entity Browser** tab.
2. Browse through native entities—the coordinator reconstructs unified logical views abstracting any awareness of internal MySQL relationships vs unstructured MongoDB dictionaries completely!

### Step 3: Validating ACID Rules
1. Navigate to the **Run All Tests** sidebar section.
2. Play the suite.
3. Observe live Server-Sent Events showing the actual Python backend requesting, releasing, and migrating complex row locks securely demonstrating pessimistic isolated concurrency guaranteeing no race conditions overwrite tables!

---

## 🛠 Tested CRUD Dummy Examples (UI Generation)

To manipulate records via the `CRUD Operations > Create` tabs successfully without throwing backend rollback errors from relational constraints conflicting, supply fully structured JSON mappings validating against the `social_iot_hybrid_v1` specification logic.

### ➕ CREATE Valid Example Payload
*Navigate to the **Create** Tab, select Generate Template, and perfectly overwrite the JSON box with the data below to bypass backend rejection successfully:*

```json
[
  {
    "username": "demo_sensor_admin",
    "event_id": "event_demo_01",
    "timestamp": "2026-05-10T12:00:00Z",
    "device": {
      "device_id": "sensor_r1_hub",
      "model": "RaspberryProbe",
      "firmware": "v2.0-beta",
      "sensors": [
        {
          "sensor_id": "sens_temp_001",
          "type": "temperature",
          "readings": [{"timestamp": "2026-05-10T12:00:00Z", "value": 45.2}]
        }
      ]
    },
    "metrics": {
      "latency_ms": 11.2,
      "signal_quality": "strong",
      "battery_pct": 98
    },
    "post": {
      "post_id": "post_alert_01",
      "title": "System Active",
      "comments": [
        {
          "comment_id": "c_alert_01",
          "commenter": "system_admin",
          "text": "All metrics green.",
          "reactions": [{"reaction_type": "upvote", "count": 2}]
        }
      ]
    }
  }
]
```

### 📖 READ Example Filters
*Navigate to the **Read** Tab, add Filters dynamically:*
* **Field name mapping**: `username`
* **Value mapping**: `demo_sensor_admin`
*(This directly queries hybrid architecture simultaneously extracting your inserted record!)*

### ✏️ UPDATE Example Configurations
*Navigate to the **Update** Tab:*
1. Add Filter Target: `username` = `demo_sensor_admin`
2. Add Update Value Target:
    - **Field Name**: `timestamp`
    - **New Value**: `2026-08-01T15:00:00Z`
*(View the real-time commit transaction state!)*

### 🗑 DELETE Example Configurations
*Navigate to the **Delete** Tab:*
1. Filter Constraint: `username` = `demo_sensor_admin`
2. Run deletion sequence manually to observe modal safeties.
