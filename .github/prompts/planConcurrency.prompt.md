## Plan: Assignment 3 Concurrency Control (Isolation)

### Problem

The current A3 layer handles atomicity (MySQL commit suppression + Mongo snapshot compensation) but has **no concurrency control**. Two concurrent transactions can interleave freely, causing dirty reads, lost updates, and inconsistent cross-backend state. The assignment requires: *"Concurrent transactions must not interfere with each other or produce inconsistent results."*

### Strategy

Use **pessimistic per-entity read/write locking** at the A3 layer. Write transactions acquire exclusive locks; read transactions acquire shared locks. Locks are keyed by a logical entity identifier derived from the query payload (e.g., `username`, `user_id`, join keys). This keeps all concurrency control in one Python process — no distributed locking needed.

### Implementation Steps

1. **Create `src/a3/concurrency_manager.py`** (NEW)
   - `ConcurrencyManager` class with a per-key `ReadWriteLock`
   - Shared lock for READs (multiple readers OK), exclusive lock for CREATE/UPDATE/DELETE
   - Lock timeout (default 5s) — raises `LockTimeoutError` instead of hanging
   - Key extraction: derives lock key from the query payload's filter/identifier fields; falls back to table-level key for bulk ops
   - Thread-safe via `threading.Condition` for read/write coordination

2. **Modify `src/a3/transaction_coordinator.py`**
   - Accept `ConcurrencyManager` in constructor
   - In `execute_in_transaction()`: extract lock key → acquire lock (shared for READ, exclusive for writes) → do backend work → release lock in `finally`
   - The lock spans both SQL and Mongo operations, serializing the entire cross-backend transaction per entity
   - Add `_extract_lock_key(operation, payload)` helper

3. **Modify `src/a3/contracts.py`**
   - Add optional `lock_key: str` to `TransactionResult` for observability
   - Add `LockTimeoutError` exception class

4. **Modify `src/a3/acid_experiments.py`**
   - Replace the timing-based `test_isolation()` with three real contention tests:
     - **Lost Update Prevention**: Two threads update the same record simultaneously → final value must be one of the two updates, not a corrupted mix
     - **Dirty Read Prevention**: Writer starts an uncommitted update; concurrent reader must see only committed state
     - **Lock Timeout**: One thread holds a write lock; another thread's write attempt is blocked → must succeed after release or fail with `LockTimeoutError`, never corrupt data

5. **Modify `src/a3/orchestrator.py`**
   - Create `ConcurrencyManager` in `Assignment3Pipeline.__init__`, inject into `TransactionCoordinator`

6. **Modify `src/a3/__init__.py`**
   - Export `ConcurrencyManager` and `LockTimeoutError`

### Relevant Files

| File | Role |
|------|------|
| `src/a3/concurrency_manager.py` | **NEW** — per-key read/write lock manager |
| `src/a3/transaction_coordinator.py` | Add lock acquire/release around backend work |
| `src/a3/contracts.py` | Add `lock_key` field, `LockTimeoutError` |
| `src/a3/acid_experiments.py` | Real contention-based isolation tests |
| `src/a3/orchestrator.py` | Wire ConcurrencyManager into pipeline |
| `src/a3/__init__.py` | Export new symbols |

### Verification

1. **Lost Update test**: Two concurrent updaters → final state matches exactly one of the two updates (serialized)
2. **Dirty Read test**: Concurrent reader during write → reader sees only committed state
3. **Lock Timeout test**: Blocked writer gets `LockTimeoutError` or succeeds after lock release — no corruption
4. **Regression**: Existing atomicity, consistency, and durability experiments still pass
5. **Lock release on failure**: Failed transaction releases its lock (verified via the atomicity test's exception path)

### Decisions

- **Pessimistic locking** over optimistic: the repo has a single coordination layer, no OCC infrastructure, and pessimistic is simpler to implement correctly
- **Single-process scope**: cross-process / multi-replica locking is out of scope
- **Preserve Mongo compensation**: concurrency control prevents interference first; compensation handles failures after locks are held
- **Minimal A2 changes**: A2 stays the source of field routing, plan generation, and CRUD execution; A3 adds isolation on top
- **Serializable isolation**: we implement serializable behavior at the logical record level, which cleanly satisfies the assignment requirement
