#!/usr/bin/env python3
"""Concurrency control test suite for A3 isolation layer.

Tests that the ConcurrencyManager and its integration with
TransactionCoordinator correctly prevent:
  1. Lost updates  (two concurrent writers on the same entity)
  2. Dirty reads   (reader sees uncommitted state)
  3. Lock timeout  (blocked writer degrades gracefully)
  4. Read/write isolation under load (N concurrent threads)
  5. Lock release on failure  (locks freed even on exception)

Part 1 tests the lock primitives in isolation (no DB needed).
Part 2 tests the full transaction layer against live MySQL + MongoDB.

Usage:
  python test_concurrency.py                # both parts
  python test_concurrency.py --unit-only    # Part 1 only (no DB)
  python test_concurrency.py --e2e-only     # Part 2 only (needs DB)
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
import traceback
from typing import Any

# ── Pretty-print helpers ─────────────────────────────────────────────

_pass_count = 0
_fail_count = 0


def _section(title: str) -> None:
    bar = "─" * 64
    print(f"\n{bar}")
    print(f"  {title}")
    print(bar)


def _check(condition: bool, label: str) -> bool:
    global _pass_count, _fail_count
    symbol = "PASS" if condition else "FAIL"
    if condition:
        _pass_count += 1
    else:
        _fail_count += 1
    print(f"  [{symbol}]  {label}")
    return condition


# ═════════════════════════════════════════════════════════════════════
# PART 1: Unit-level lock tests (no database required)
# ═════════════════════════════════════════════════════════════════════

def run_unit_tests() -> bool:
    _section("PART 1 · Unit Tests  (lock primitives — no DB)")
    from src.a3.concurrency_manager import ConcurrencyManager, LockTimeoutError

    all_ok = True

    # --- 1. Exclusive lock blocks second writer ---
    print("\n  Test 1.1: Exclusive lock blocks second writer")
    mgr = ConcurrencyManager(default_timeout=2.0)
    key = "entity:username=alice"
    results: list[str] = []
    order: list[int] = []

    mgr.acquire(key, exclusive=True)

    def delayed_writer():
        try:
            mgr.acquire(key, exclusive=True, timeout=5.0)
            order.append(2)
            results.append("acquired")
            mgr.release(key, exclusive=True)
        except LockTimeoutError:
            results.append("timeout")

    t = threading.Thread(target=delayed_writer, daemon=True)
    t.start()
    time.sleep(0.3)  # let thread block on lock
    order.append(1)
    mgr.release(key, exclusive=True)  # release → thread unblocks
    t.join(timeout=5)

    all_ok &= _check(
        results == ["acquired"] and order == [1, 2],
        f"Writer blocked then acquired after release (order={order}, results={results})"
    )

    # --- 2. Lock timeout raises LockTimeoutError ---
    print("\n  Test 1.2: Lock timeout raises LockTimeoutError")
    mgr2 = ConcurrencyManager(default_timeout=0.3)
    key2 = "entity:username=bob"
    mgr2.acquire(key2, exclusive=True)

    timeout_raised = False
    def timeout_writer():
        nonlocal timeout_raised
        try:
            mgr2.acquire(key2, exclusive=True)  # uses 0.3s default
        except LockTimeoutError:
            timeout_raised = True

    t2 = threading.Thread(target=timeout_writer, daemon=True)
    t2.start()
    t2.join(timeout=3)
    mgr2.release(key2, exclusive=True)

    all_ok &= _check(timeout_raised, "LockTimeoutError raised after 0.3s timeout")

    # --- 3. Multiple readers allowed concurrently ---
    print("\n  Test 1.3: Multiple readers can hold shared lock")
    mgr3 = ConcurrencyManager(default_timeout=2.0)
    key3 = "entity:username=carol"
    reader_count = threading.Semaphore(0)
    all_reading = threading.Event()
    readers_done = []

    def shared_reader(reader_id: int):
        mgr3.acquire(key3, exclusive=False)
        reader_count.release()
        all_reading.wait(timeout=5)  # wait for all to be reading
        readers_done.append(reader_id)
        mgr3.release(key3, exclusive=False)

    threads = [threading.Thread(target=shared_reader, args=(i,), daemon=True) for i in range(4)]
    for t in threads:
        t.start()

    # Wait for all 4 readers to acquire the lock concurrently
    for _ in range(4):
        reader_count.acquire(timeout=5)
    all_reading.set()
    for t in threads:
        t.join(timeout=5)

    all_ok &= _check(
        len(readers_done) == 4,
        f"4 concurrent readers held shared lock simultaneously (got {len(readers_done)})"
    )

    # --- 4. Writer blocks readers, readers block writer ---
    print("\n  Test 1.4: Exclusive lock blocks shared readers")
    mgr4 = ConcurrencyManager(default_timeout=3.0)
    key4 = "entity:username=dave"
    mgr4.acquire(key4, exclusive=True)  # hold write lock

    reader_blocked = []
    def blocked_reader():
        start_t = time.monotonic()
        mgr4.acquire(key4, exclusive=False, timeout=5.0)
        wait_time = time.monotonic() - start_t
        reader_blocked.append(wait_time)
        mgr4.release(key4, exclusive=False)

    tr = threading.Thread(target=blocked_reader, daemon=True)
    tr.start()
    time.sleep(0.5)  # reader must be blocked
    mgr4.release(key4, exclusive=True)  # unblock
    tr.join(timeout=5)

    all_ok &= _check(
        len(reader_blocked) == 1 and reader_blocked[0] >= 0.4,
        f"Reader blocked for {reader_blocked[0]:.2f}s while writer held lock"
    )

    # --- 5. Key extraction from payloads ---
    print("\n  Test 1.5: Lock key extraction from query payloads")
    k1 = ConcurrencyManager.extract_lock_key("read", {"filters": {"username": "alice"}})
    k2 = ConcurrencyManager.extract_lock_key("create", {"records": [{"username": "bob"}]})
    k3 = ConcurrencyManager.extract_lock_key("update", {"updates": {"title": "x"}, "filters": {"event_id": "e1"}})
    k4 = ConcurrencyManager.extract_lock_key("delete", {"filters": {}})

    all_ok &= _check(k1 == "entity:username=alice", f"filters key → {k1}")
    all_ok &= _check(k2 == "entity:username=bob", f"records key → {k2}")
    all_ok &= _check(k3 == "entity:event_id=e1", f"updates+filters key → {k3}")
    all_ok &= _check(k4 == "__global__", f"empty filters key → {k4}")

    # --- 6. Lock released even when exception occurs ---
    print("\n  Test 1.6: Lock released after exception in critical section")
    mgr6 = ConcurrencyManager(default_timeout=2.0)
    key6 = "entity:username=eve"

    # Simulate: acquire → exception → release in finally
    mgr6.acquire(key6, exclusive=True)
    try:
        raise RuntimeError("simulated failure")
    except RuntimeError:
        pass
    finally:
        mgr6.release(key6, exclusive=True)

    # If lock was properly released, another acquire should succeed immediately
    acquired_after = False
    def try_acquire():
        nonlocal acquired_after
        try:
            mgr6.acquire(key6, exclusive=True, timeout=1.0)
            acquired_after = True
            mgr6.release(key6, exclusive=True)
        except LockTimeoutError:
            pass

    ta = threading.Thread(target=try_acquire, daemon=True)
    ta.start()
    ta.join(timeout=3)
    all_ok &= _check(acquired_after, "Lock re-acquired after exception-triggered release")

    # --- 7. Write priority: pending writer blocks new readers ---
    print("\n  Test 1.7: Write priority — pending writer blocks new readers")
    mgr7 = ConcurrencyManager(default_timeout=3.0)
    key7 = "entity:username=frank"

    # Reader 1 holds shared lock
    mgr7.acquire(key7, exclusive=False)

    writer_acquired = threading.Event()
    reader2_result: list[str] = []

    def pending_writer():
        mgr7.acquire(key7, exclusive=True, timeout=5.0)
        writer_acquired.set()
        time.sleep(0.3)
        mgr7.release(key7, exclusive=True)

    def new_reader():
        # This reader arrives after the writer is waiting
        time.sleep(0.1)
        try:
            mgr7.acquire(key7, exclusive=False, timeout=0.5)
            reader2_result.append("acquired")
            mgr7.release(key7, exclusive=False)
        except LockTimeoutError:
            reader2_result.append("timeout")

    tw = threading.Thread(target=pending_writer, daemon=True)
    tr2 = threading.Thread(target=new_reader, daemon=True)
    tw.start()
    tr2.start()

    time.sleep(0.2)  # let writer & reader2 start
    # New reader should be blocked because writer is waiting (write priority)
    tr2.join(timeout=3)

    # Now release reader 1 → writer gets the lock
    mgr7.release(key7, exclusive=False)
    tw.join(timeout=5)

    all_ok &= _check(
        reader2_result == ["timeout"],
        f"New reader blocked by pending writer (reader2={reader2_result})"
    )

    return all_ok


# ═════════════════════════════════════════════════════════════════════
# PART 2: End-to-end tests against live database
# ═════════════════════════════════════════════════════════════════════

def run_e2e_tests() -> bool:
    _section("PART 2 · E2E Tests  (live MySQL + MongoDB)")

    import json
    from pathlib import Path
    from src.a2.contracts import CrudOperation, SchemaRegistration
    from src.a3.orchestrator import Assignment3Pipeline
    from src.a3.concurrency_manager import LockTimeoutError
    from src.config import get_config
    from src.persistence.metadata_store import MetadataStore

    cfg = get_config()
    MetadataStore(cfg.metadata_dir).clear()

    pipeline = Assignment3Pipeline(config=cfg)
    all_ok = True

    try:
        # ── Setup: register schema + ingest + build strategy ──
        print("\n  Setting up: schema registration, ingestion, storage strategy...")
        schema_path = Path("schemas/assignment2_schema.template.json")
        if not schema_path.exists():
            print("  FAIL: schema file not found")
            return False

        data = json.loads(schema_path.read_text(encoding="utf-8"))
        registration = SchemaRegistration(
            schema_name=data["schema_name"],
            version=data["version"],
            root_entity=data["root_entity"],
            json_schema=data["json_schema"],
            constraints=data.get("constraints", {}),
        )

        pipeline.a2.register_schema(registration)
        records = pipeline.a2.generate_records(10, registration)
        pipeline.a2.run_ingestion(records)
        pipeline.a2.build_storage_strategy(registration)
        pipeline.a2.execute_operation(CrudOperation.CREATE, {"records": records})
        print("  Setup complete.\n")

        field_locations = pipeline._get_field_locations()

        # ── Test 2.1: ACID isolation experiment (3 sub-tests) ──
        print("  Test 2.1: Run built-in ACID isolation experiment")
        iso_result = pipeline.run_acid_experiment("isolation")
        all_ok &= _check(
            iso_result.passed,
            f"ACID isolation experiment: {iso_result.description}"
        )
        if iso_result.details:
            for sub_name in ("lost_update", "dirty_read", "lock_timeout"):
                sub = iso_result.details.get(sub_name, {})
                status = "PASS" if sub and not sub.get("errors") else "FAIL"
                print(f"    ↳ {sub_name}: {status}  {sub}")

        # ── Test 2.2: Concurrent updates produce serialized result ──
        print("\n  Test 2.2: 5 concurrent updaters on same record")
        import uuid
        tag = f"_conc_test_{uuid.uuid4().hex[:6]}"
        test_record = _build_e2e_record(tag, field_locations)

        ins = pipeline.execute_transactional(CrudOperation.CREATE, {"records": [test_record]})
        all_ok &= _check(ins.status == "committed", f"Setup insert: {ins.status}")

        title_field = _find_field("title", field_locations)
        username_field = _find_field("username", field_locations)

        if title_field and username_field:
            errors: list[str] = []
            committed: list[str] = []
            barrier = threading.Barrier(5, timeout=15)

            def updater(tid: int):
                try:
                    barrier.wait()
                    result = pipeline.execute_transactional(
                        CrudOperation.UPDATE,
                        {
                            "updates": {title_field: f"{tag}_t{tid}"},
                            "filters": {username_field: tag},
                        },
                    )
                    committed.append(f"t{tid}:{result.status}")
                except Exception as exc:
                    errors.append(f"t{tid}: {exc}")

            threads = [threading.Thread(target=updater, args=(i,), daemon=True) for i in range(5)]
            for t in threads:
                t.start()
            for t in threads:
                t.join(timeout=30)

            # Read final state
            read_res = pipeline.execute_transactional(
                CrudOperation.READ,
                {"filters": {username_field: tag}, "limit": 1},
            )
            recs = read_res.sql_result.get("records", [])
            final_title = None
            for r in recs:
                if isinstance(r, dict):
                    final_title = r.get("title")
                    if final_title is None and isinstance(r.get("post"), dict):
                        final_title = r["post"].get("title")
                    break

            expected_titles = [f"{tag}_t{i}" for i in range(5)]
            all_ok &= _check(
                final_title in expected_titles,
                f"Final title is one of the 5 updates: {final_title}"
            )
            all_ok &= _check(
                not errors,
                f"No thread errors ({len(committed)} committed, {len(errors)} errors)"
            )
            if errors:
                for e in errors:
                    print(f"    ↳ ERROR: {e}")

        # ── Test 2.3: Concurrent reads don't block each other ──
        print("\n  Test 2.3: 4 concurrent readers on same entity")
        read_times: list[float] = []
        read_errors: list[str] = []
        read_barrier = threading.Barrier(4, timeout=10)

        def timed_reader(rid: int):
            try:
                read_barrier.wait()
                start = time.monotonic()
                pipeline.execute_transactional(
                    CrudOperation.READ,
                    {"filters": {username_field: tag} if username_field else {}, "limit": 1},
                )
                elapsed = time.monotonic() - start
                read_times.append(elapsed)
            except Exception as exc:
                read_errors.append(f"r{rid}: {exc}")

        r_threads = [threading.Thread(target=timed_reader, args=(i,), daemon=True) for i in range(4)]
        for t in r_threads:
            t.start()
        for t in r_threads:
            t.join(timeout=15)

        all_ok &= _check(
            len(read_times) == 4 and not read_errors,
            f"4 readers completed ({[f'{t:.3f}s' for t in read_times]})"
        )

        # ── Test 2.4: Transaction result includes lock_key ──
        print("\n  Test 2.4: TransactionResult includes lock_key for observability")
        obs_result = pipeline.execute_transactional(
            CrudOperation.READ,
            {"filters": {username_field: tag} if username_field else {}, "limit": 1},
        )
        all_ok &= _check(
            obs_result.lock_key is not None and obs_result.lock_key != "",
            f"lock_key = '{obs_result.lock_key}'"
        )

        # ── Test 2.5: Full ACID suite still passes ──
        print("\n  Test 2.5: Full ACID experiment suite (A, C, I, D)")
        acid_results = pipeline.run_acid_experiments()
        for r in acid_results:
            all_ok &= _check(r.passed, f"{r.property_name}: {r.description[:80]}")

        # ── Cleanup ──
        try:
            pipeline.execute_transactional(
                CrudOperation.DELETE,
                {"filters": {"username": tag}},
            )
        except Exception:
            pass

    except Exception as exc:
        print(f"\n  [FAIL]  {exc}")
        traceback.print_exc()
        return False
    finally:
        pipeline.close()

    return all_ok


# ── Helpers for E2E tests ────────────────────────────────────────────

def _build_e2e_record(tag: str, field_locations) -> dict[str, Any]:
    from src.a2.contracts import FieldLocation
    record: dict[str, Any] = {}
    for loc in field_locations:
        path = loc.field_path
        if "." in path:
            continue
        canonical = path.lower()
        if canonical in ("username", "user_id", "id"):
            record[path] = tag
        elif canonical in ("event_id",):
            record[path] = f"evt_{tag}"
        elif canonical in ("title", "name"):
            record[path] = tag
        elif canonical in ("timestamp", "sys_ingested_at", "created_at"):
            record[path] = "2026-01-01T00:00:00Z"
        else:
            record[path] = f"test_{tag}"
    return record


def _find_field(short_name: str, field_locations) -> str | None:
    for loc in field_locations:
        if loc.field_path == short_name or loc.field_path.endswith(f".{short_name}"):
            return loc.field_path
    return None


# ── Main ─────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="Concurrency control test suite")
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--unit-only", action="store_true", help="Run Part 1 only (no DB)")
    group.add_argument("--e2e-only", action="store_true", help="Run Part 2 only (needs DB)")
    args = parser.parse_args()

    banner = "═" * 64
    print(f"\n{banner}")
    print("  CONCURRENCY CONTROL TEST SUITE")
    print(f"{banner}")

    unit_ok = True
    e2e_ok = True

    if not args.e2e_only:
        unit_ok = run_unit_tests()

    if not args.unit_only:
        e2e_ok = run_e2e_tests()

    # ── Summary ──
    _section("FINAL SUMMARY")
    total = _pass_count + _fail_count
    print(f"\n  Total: {total}  |  Passed: {_pass_count}  |  Failed: {_fail_count}")

    if _fail_count == 0:
        print(f"\n  ✅ ALL {_pass_count} TESTS PASSED — concurrency control is working\n")
        return 0
    else:
        print(f"\n  ❌ {_fail_count} TEST(S) FAILED\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
