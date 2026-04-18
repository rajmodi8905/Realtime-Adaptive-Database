# Assignment 3 - Logical Dashboard & Transactional Validation

An architectural extension of the Assignment 2 metadata-driven hybrid database that introduces a **Logical Dashboard**, multi-backend **Transaction Coordination**, concurrency control via **Reader/Writer Locking**, and robust **ACID Validation Experiments**.

Video explanation: [Watch here](https://drive.google.com/file/d/1lI_NIiaVbkQX4OpkC2DuhKUpDkCOfwT1/view?usp=sharing)

---

## How A2 Fits In (Brief Context)

Assignment 2 README: [Metadata-Driven Hybrid Storage Pipeline](Assignment2_README.md)

Assignment 2 built the `Assignment2Pipeline` that successfully:
1. Generates dual normalized schemas (SQL 3NF and MongoDB Embedded collections) automatically based on metadata definitions.
2. Persists cross-database tracking using `field_locations.json`.
3. Excutes physical query breakdowns natively per sub-system.

A3 seamlessly wraps the A2 backbone. It coordinates the cross-backend queries atomistically (so a failure midway rolls everything back) and abstracts the complex mapping completely away behind a "Logical Entity" layer exposed to a React-based interactive web dashboard.

---

## Project Structure

```text
├── src/
│   ├── a3/                                  # Assignment 3 modules
│   │   ├── orchestrator.py                  # A3 Pipeline controller
│   │   ├── contracts.py                     # Shared transaction types (AcidTestResult, etc.)
│   │   ├── transaction_coordinator.py       # Cross-backend atomicity and rollback manager
│   │   ├── concurrency_manager.py           # Thread-safe entity lock timeout mechanism
│   │   ├── logical_reconstructor.py         # Merges SQL + Mongo arrays into single JSON entities
│   │   ├── session_manager.py               # Metadata and active session health service
│   │   └── acid_experiments.py              # Suite verifying Atomicity, Consistency, etc.
│   │
│   ├── a2/                                  # A2 Schema Normalization & Query Planner
│   └── classify/                            # A1 Type Detection & Router
│
├── dashboard/                               # FastAPI Backend & React Frontend Dashboard
│   ├── api_server.py                        # FastAPI routes serving logical queries
│   └── frontend/
│       ├── src/
│       │   ├── views/EntityBrowser.jsx      # Abstract Data Table & ER-Diagram UI
│       │   ├── views/QueryWorkspace.jsx     # CRUD Simulator UI
│       │   ├── views/AcidValidation.jsx     # Experiment Executor UI
│       │   └── components/SessionBar.jsx    # Session metadata banner
│       └── package.json
│
├── report.tex                               # Academic Submission Report
└── Assignment3_README.md                    # This document
```

---

## Core Logic (Per File)

### `src/a3/orchestrator.py` - Pipeline Controller
Entry point for the A3 application layer. Binds the `TransactionCoordinator` to incoming requests and supplies the dashboard's FastAPI server with logical endpoints:
- `get_session_info()`: Fetches database active health.
- `execute_logical_query()`: Handles dashboard CRUD payloads and fires them transactionally.
- `run_acid_experiments()`: Kicks off the synthetic workload verification threads.

### `src/a3/transaction_coordinator.py` - Atomicity Layer
Ensures logical queries operating across two unlinked database engines execute with **All-or-Nothing** semantics:
- Deterministic Ordering: SQL `INSERT` commands strictly precede Mongo insertions establishing FK invariants securely; Mongo `DELETE`s securely precede SQL cascades.
- Failure Detection: Catches mid-operation errors and reliably performs reverse-topological rollbacks (e.g., executing native `ROLLBACK` for MySQL transactions and manual compensatory drops for MongoDB artifacts).

### `src/a3/concurrency_manager.py` - Isolation Layer
Shields multi-threaded requests from corrupting shared data leveraging dynamic Entity locks:
- Utilizes Reader-Writer locks prioritizing exclusive write locks per extracted logical ID boundary (e.g. `username`).
- Protects against deadlocks naturally through a robust `LockTimeoutError` (defaulting to 5 seconds) rather than graphing cyclic dependencies.

### `src/a3/logical_reconstructor.py` - Hydration Layer
Abstracts physical implementation mapping entirely away from the end user.
- Utilizes `keyed_merge` mechanics to group split representations back together securely targeting the parent index (`username` / `root_entity`).
- Formats deeply nested Mongo arrays inside standard mapped JSON structures identically outputting identical entity definitions cleanly to the dashboard.

### `src/a3/session_manager.py` - Health Monitoring
Continuously pings the MySQL & MongoDB client sockets determining current connection uptimes, parsing available table counts, active schemas, and database versions to power the top abstraction banner of the Dashboard.

### `src/a3/acid_experiments.py` - Synthetic Validation
Runs 5 distinct experiments natively returning `AcidTestResult` objects indicating PASS/FAIL status along with granular `details`.

**1. Atomicity**
Injects a fake connectivity exception deep inside the MongoDB client mid-transaction.
*Example Outcome*: `SQL rollback verification (0 rows), Mongo pre-commit abort (0 documents), transaction.rolled_back = True`.

**2. Consistency**
Forces an array constraint violation by pushing identical `username` primary keys simultaneously.
*Example Outcome*: Database driver strictly rejects the operation (`status: failed`); the overarching transaction coordinator intercepts it.

**3. Isolation**
Throws heavy multi-threading collision tasks simultaneously targeting identical Logical entities.
*Example Outcome*:
- **Lost Updates**: Two threads write `title_A` and `title_B`. Output strictly serializes to one final survivor without truncation.
- **Dirty Reads**: A secondary reader queries the document *during* the writer's commit latency; Output correctly returns the previous unmodified state.

**4. Durability**
Disconnects both Database engines entirely (`mysql_client.disconnect()`, `mongo_client.disconnect()`) immediately following a transaction commit, then restores them to prove data remained permanently preserved.

**5. Reconstruction**
Syntactically merges massive arrays spanning dual backends.
*Example Outcome*: The payload returns full JSON without exposing raw internal keys or generating empty objects.

### `dashboard/...` - The Logical Dashboard Interface
A complete, un-opinionated FastAPI + React Vite application enforcing the primary abstraction-first Assignment 3 architectural rule (exposing zero SQL tables/relations directly):
- `EntityBrowser.jsx`: Maps logical data columns via `extractValue()` dynamically bypassing internal boundaries. Generates an exclusive Logical **ER-Diagram** charting topological compositions without directly charting DB instances.
- `SessionBar.jsx`: Strictly obfuscates physical schemas.

---

## Setup & Execution

### Prerequisites
- Python 3.12+
- Docker & Docker Compose
- Node 20 (or use Docker compiler)

### 1. Launch Databases
```bash
docker-compose up -d
docker ps
```
Ensure `adaptive_db_mysql` and `adaptive_db_mongodb` are actively linked.

### 2. Compile React (Docker Method)
Compile the dashboard frontend natively via Node volume-mounting to avoid local dependency clutter:
```bash
docker run --rm -v "${PWD}/dashboard:/dashboard" -w /dashboard/frontend node:20 sh -c "npm install && npm run build"
```

### 3. Launch the Application Server
Run the FastAPI Server directly supplying the logical API routes to your compiled React app:
```bash
python dashboard/api_server.py
```
**Access the Dashboard:** [http://localhost:8080](http://localhost:8080)

---

## Utilizing the Environment (User Guide)

The central workflow happens directly inside the Web Dashboard:

1. **Bootstrap Initialization**:
   Navigate to the target subtab to cleanly flush outdated persistent metadata configurations and auto-generate clean starting schemas for `event` relationships across both database engines.
2. **Browse Logical Topologies**:
   Target the **Entity Browser** to browse visually mapped hierarchies un-tainted by backend terminology. Select between the conceptual *ER-Diagram* abstraction and the live *Data Instances* explorer.
3. **Trigger Transactions**:
   Utilize the **Query Workspace** typing structured JSON instructions. Submitting identical data blocks repetitively naturally triggers concurrency restrictions correctly halting collisions.
4. **Stress Testing**:
   Click **ACID Validation** to watch the Transaction Coordinator gracefully detect engineered catastrophic outages, enforcing total system rollbacks actively verifiable using UI metrics.

---

## Allowed vs. Not Allowed

| Feature | Supported | System Policy Notes |
|---------|-----------|--------------------|
| Read physical SQL lists via UI | **No**  | Strictly breaches Logical Abstraction definitions. |
| Automatic Rollback Execution   | **Yes** | Fully native execution protecting data limits cleanly. |
| Cascading Multi-DB Deletes     | **Yes** | Supported logically navigating internal `field_locations.json`. |
| Fine Field-Level Locking       | **No**  | Concurrency architecture exclusively isolates strictly at the Entity limit. |
