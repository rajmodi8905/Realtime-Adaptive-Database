# Assignment 3 — Concurrency Control & ACID Transactions

This module (`src/a3/`) adds **transaction coordination** and **concurrency control** to the hybrid SQL/MongoDB database framework, ensuring all CRUD operations satisfy full ACID guarantees.

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────┐
│                Assignment3Pipeline                       │
│              (src/a3/orchestrator.py)                     │
├──────────┬──────────┬──────────┬────────────────────────┤
│ Session  │ Logical  │ ACID     │ Transaction            │
│ Manager  │ Recon-   │ Experi-  │ Coordinator            │
│          │ structor │ ments    │  + ConcurrencyManager  │
└──────────┴──────────┴──────────┴────────────────────────┘
                          │
              ┌───────────┴───────────┐
              │  Assignment2Pipeline  │
              │   (CrudEngine, etc.)  │
              └───────────┬───────────┘
              ┌───────────┴───────────┐
              │   MySQL     MongoDB   │
              └───────────────────────┘
```

---

## ACID Property Implementation

### A — Atomicity

**File:** [`src/a3/transaction_coordinator.py`](src/a3/transaction_coordinator.py)

| Backend | Mechanism |
|---------|-----------|
| **MySQL** | Auto-commit is **suppressed** before execution (`conn.commit = lambda: None`). After both backends succeed, a single `COMMIT` is issued. On any failure, `ROLLBACK` is called. |
| **MongoDB** | Standalone MongoDB lacks multi-document transactions. A **compensating transaction** pattern is used: documents are **snapshotted** before mutation (`_snapshot_mongo`), and restored on failure (`_compensate_mongo`). For inserts, the inserted docs are deleted; for updates, the originals are restored; for deletes, the originals are re-inserted. |

**How it works:**
1. SQL operations execute first (with commit suppressed)
2. MongoDB documents are snapshotted
3. MongoDB operations execute
4. If either fails → MySQL `ROLLBACK` + MongoDB compensating restore
5. If both succeed → MySQL `COMMIT`

---

### C — Consistency

**File:** [`src/a3/transaction_coordinator.py`](src/a3/transaction_coordinator.py), MySQL schema setup in `src/storage/mysql_client.py`

Consistency is enforced through:
- **MySQL schema constraints**: `PRIMARY KEY`, `NOT NULL`, `UNIQUE`, and `FOREIGN KEY` constraints reject invalid data
- **MongoDB schema validators**: Applied during collection creation to enforce document shape
- **Upsert semantics**: Duplicate inserts are handled gracefully (upsert or rejection) without data corruption
- **All operations go through the transaction coordinator**, ensuring no raw writes bypass constraint checks

---

### I — Isolation

**Files:**
- [`src/a3/concurrency_manager.py`](src/a3/concurrency_manager.py) — Lock manager
- [`src/a3/transaction_coordinator.py`](src/a3/transaction_coordinator.py) — Lock integration

**Mechanism:** Pessimistic per-entity **read/write locking** using threading primitives.

| Lock Type | Semantics |
|-----------|-----------|
| **Shared (Read)** | Multiple readers can hold the lock concurrently |
| **Exclusive (Write)** | Writer blocks until all readers and prior writers release; new readers are blocked while a writer holds the lock |
| **Write Priority** | Pending writers block new readers to prevent write starvation |
| **Timeout** | Default 5s; raises `LockTimeoutError` to prevent deadlocks |

**Lock Key Extraction:** The system derives a logical lock key from query payloads:
1. `filters` → e.g., `entity:username=alice`
2. First record in `records` list (for CREATE)
3. `updates` dict
4. Global fallback key

**Integration:** Every call to `execute_in_transaction()` acquires the appropriate lock **before** any work begins and releases it in a `finally` block — guaranteeing release even on exceptions.

---

### D — Durability

**File:** [`src/a3/transaction_coordinator.py`](src/a3/transaction_coordinator.py)

| Backend | Mechanism |
|---------|-----------|
| **MySQL** | `conn.commit()` flushes the transaction to InnoDB on disk |
| **MongoDB** | Default write concern (`w:1`) ensures the write is acknowledged by the primary before returning |

Once `execute_in_transaction()` returns `status="committed"`, the data is persisted to disk in both backends and survives server restarts.

---

## File Structure

```
src/a3/
├── __init__.py                  # Package exports
├── contracts.py                 # TransactionResult, AcidTestResult, etc.
├── concurrency_manager.py       # Per-entity read/write lock manager
├── transaction_coordinator.py   # Multi-backend transaction coordination
├── acid_experiments.py          # ACID validation experiment suite
├── orchestrator.py              # Assignment3Pipeline (top-level entry point)
├── logical_reconstructor.py     # Unified entity view across backends
└── session_manager.py           # Schema/connection state management

src/storage/
└── mysql_client.py              # Thread-safe MySQL client (threading.Lock added)
```

---

## Test Files

### 1. Unit Tests — `test_concurrency.py`

Tests the lock primitives in isolation (no database required):

| Test | What it validates |
|------|-------------------|
| Exclusive lock blocks second writer | Two threads can't hold write lock simultaneously |
| Shared reads are concurrent | Multiple readers don't block each other |
| Write blocks readers | A writer blocks incoming readers |
| Write priority over readers | Pending writers get priority over new readers |
| Lock timeout raises error | `LockTimeoutError` raised when timeout expires |
| Lock key extraction | Correct keys derived from query payloads |

**Run:**
```bash
python test_concurrency.py
```

### 2. End-to-End Tests — `test_concurrency_e2e.py`

Full integration tests that **bootstrap the database from scratch** (empty DB → schema → records → tests):

| Test | ACID Property | What it validates |
|------|---------------|-------------------|
| 1. Lost Update Prevention | **Isolation** | 2 concurrent writers on same record → final value is one of the two (serialized, not lost) |
| 2. 5-Way Write Contention | **Isolation** | 5 threads update same record → all 5 commit, final value is one of the 5 |
| 3. Dirty Read Prevention | **Isolation** | Reader during write sees only committed state, never in-flight data |
| 4. Concurrent Reads | **Isolation** | 4 parallel readers complete without blocking each other |
| 5. Read/Write Isolation | **Isolation** | Concurrent read + write on same entity both succeed, reader sees consistent state |
| 6. Lock Key Observability | **Isolation** | `TransactionResult.lock_key` is populated correctly |
| 7. Full ACID Suite | **All** | Runs `AcidExperimentRunner.run_all()` which tests Atomicity, Consistency, Isolation (3 sub-tests), and Durability |

**Run:**
```bash
# Start databases
docker-compose up -d
# Wait for MySQL to be ready
sleep 10
# Run tests (bootstraps DB from scratch)
rm -rf metadata/
python test_concurrency_e2e.py
```

**Expected output:**
```
Total: 24  |  Passed: 24  |  Failed: 0
✅ ALL 24 TESTS PASSED — concurrency control is working correctly
```

---

## Quick Start

```bash
# 1. Start databases
docker-compose up -d

# 2. Set MySQL password
echo "MYSQL_PASSWORD=rootpassword" > .env

# 3. Run concurrency tests
sleep 10 && rm -rf metadata/ && python test_concurrency_e2e.py
```

---

## Key Design Decisions

1. **Pessimistic Locking** (vs. optimistic): Chosen for guaranteed serializability. In a hybrid SQL/MongoDB system, optimistic approaches would require cross-backend version tracking, adding significant complexity.

2. **Per-Entity Lock Scope**: Locks are keyed by logical entity (e.g., `username=alice`), not by table/collection. This serializes cross-backend operations for specific records while allowing unrelated records to proceed in parallel.

3. **Thread-Safe MySQL Client**: `pymysql` is not thread-safe. A `threading.Lock` was added to `MySQLClient` to serialize all operations on the connection, preventing packet interleaving corruption during concurrent transactions.

4. **Compensating Transactions for MongoDB**: Since standalone MongoDB doesn't support multi-document transactions, we snapshot documents before mutation and restore them on failure — achieving atomicity at the application level.
