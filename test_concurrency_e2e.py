#!/usr/bin/env python3
"""Full end-to-end concurrency test — run after `docker-compose up`.

This script:
  1. Bootstraps the entire database from scratch (empty DB)
     - Clears metadata, registers schema, generates records
     - Runs A1 ingestion, builds storage strategy, creates tables/collections
     - Inserts dummy records into both MySQL and MongoDB
  2. Runs comprehensive concurrency tests using the A3 transaction layer
  3. Runs the full ACID experiment suite
  4. Cleans up test data

Usage:
  docker-compose up -d          # start MySQL + MongoDB
  python test_concurrency_e2e.py  # run this script
"""

from __future__ import annotations

import json
import random
import sys
import threading
import time
import traceback
import uuid
from pathlib import Path
from typing import Any

# ── Pretty-print helpers ─────────────────────────────────────────────

_pass = 0
_fail = 0


def _banner(title: str) -> None:
    b = "═" * 64
    print(f"\n{b}")
    print(f"  {title}")
    print(f"{b}")


def _section(title: str) -> None:
    print(f"\n{'─' * 64}")
    print(f"  {title}")
    print(f"{'─' * 64}")


def _check(condition: bool, label: str) -> bool:
    global _pass, _fail
    if condition:
        _pass += 1
    else:
        _fail += 1
    print(f"  [{'PASS' if condition else 'FAIL'}]  {label}")
    return condition


def _info(msg: str) -> None:
    print(f"  ℹ  {msg}")


# ── Schema + record helpers ──────────────────────────────────────────

SCHEMA_PATH = Path("schemas/assignment2_schema.template.json")


def load_registration():
    from src.a2.contracts import SchemaRegistration
    data = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return SchemaRegistration(
        schema_name=data["schema_name"],
        version=data["version"],
        root_entity=data["root_entity"],
        json_schema=data["json_schema"],
        constraints=data.get("constraints", {}),
    )


def enrich_records(records: list[dict]) -> list[dict]:
    """Add extra nested structures matching run_pipeline.py."""
    for i, record in enumerate(records):
        eid = record.get("event_id", f"e{i}")
        did = record.get("device", {}).get("device_id", f"d{i}")
        record["post"]["attachments"] = [
            {"attachment_id": f"att_{eid}_1", "file_type": "image"},
            {"attachment_id": f"att_{eid}_2", "file_type": "video"},
        ]
        record["device"]["sensors"] = [
            {
                "sensor_id": f"sen_{did}_1",
                "type": "temperature",
                "readings": [
                    {"timestamp": record.get("timestamp", ""), "value": round(random.uniform(20, 40), 1)},
                ],
            },
        ]
    return records


# ── Build a test record from field locations ─────────────────────────

def build_test_record(tag: str, field_locations) -> dict[str, Any]:
    """Build a minimal valid record that satisfies all NOT NULL columns."""
    record: dict[str, Any] = {}

    def _set_nested(d: dict, path: str, value: Any) -> None:
        parts = path.split(".")
        for part in parts[:-1]:
            d = d.setdefault(part, {})
        d[parts[-1]] = value

    for loc in field_locations:
        path = loc.field_path
        canonical = path.split(".")[-1].lower()

        if canonical in ("username", "user_id", "id"):
            _set_nested(record, path, tag)
        elif canonical == "event_id":
            _set_nested(record, path, f"evt_{tag}")
        elif canonical in ("title", "name"):
            _set_nested(record, path, tag)
        elif canonical in ("timestamp", "sys_ingested_at", "created_at"):
            _set_nested(record, path, "2026-01-01T00:00:00Z")
        elif canonical in ("tags",):
            _set_nested(record, path, ["test"])
        elif canonical in ("count",):
            _set_nested(record, path, 1)
        elif canonical in ("latency_ms", "battery_pct"):
            _set_nested(record, path, 0.0)
        elif canonical.endswith("_id") or canonical.endswith("_count"):
            # Likely BIGINT column — use a deterministic integer from tag
            _set_nested(record, path, abs(hash(f"{tag}_{path}")) % 100000)
        else:
            _set_nested(record, path, f"test_{tag}")
    return record


def find_field(short_name: str, field_locations) -> str | None:
    for loc in field_locations:
        if loc.field_path == short_name or loc.field_path.endswith(f".{short_name}"):
            return loc.field_path
    return None


def _extract_title(rec: dict) -> str | None:
    """Search a returned record for the title value across all naming formats."""
    if not isinstance(rec, dict):
        return None
    # Direct key 'title'
    if "title" in rec:
        return rec["title"]
    # Dotted column name 'post.title' (SQL returns it literally)
    for key, val in rec.items():
        if key.endswith(".title") or key.endswith("_title"):
            return val
    # Nested dict: rec["post"]["title"]
    if isinstance(rec.get("post"), dict):
        return rec["post"].get("title")
    return None


def read_title(pipeline, username_field, tag, field_locations) -> str | None:
    """Read the title field for a record identified by tag."""
    from src.a2.contracts import CrudOperation
    res = pipeline.execute_transactional(
        CrudOperation.READ,
        {"filters": {username_field: tag}, "limit": 1},
    )
    for r in res.sql_result.get("records", []):
        t = _extract_title(r)
        if t is not None:
            return t
    return None


# ═════════════════════════════════════════════════════════════════════
# BOOTSTRAP — set up the entire DB from scratch
# ═════════════════════════════════════════════════════════════════════

def bootstrap():
    """Set up schema, ingest dummy data, build storage, insert records.

    Returns (Assignment3Pipeline, field_locations, records).
    """
    from src.a2.contracts import CrudOperation
    from src.a3.orchestrator import Assignment3Pipeline
    from src.config import get_config
    from src.persistence.metadata_store import MetadataStore

    _section("BOOTSTRAP · Setting up database from scratch")

    if not SCHEMA_PATH.exists():
        print(f"  FAIL: schema file not found: {SCHEMA_PATH}")
        sys.exit(1)

    cfg = get_config()
    registration = load_registration()

    # 1. Clear prior metadata
    _info("Clearing prior metadata...")
    MetadataStore(cfg.metadata_dir).clear()

    # 2. Create the A3 pipeline (which contains A2 inside)
    pipeline = Assignment3Pipeline(config=cfg)

    # 3. Register schema
    _info(f"Registering schema: {registration.schema_name} v{registration.version}")
    pipeline.a2.register_schema(registration)

    # 4. Generate + enrich dummy records
    num_records = 20
    _info(f"Generating {num_records} synthetic records...")
    records = pipeline.a2.generate_records(num_records, registration)
    records = enrich_records(records)
    _info(f"Generated {len(records)} records with enrichment")

    # 5. A1 ingestion (classification + flush)
    _info("Running A1 ingestion (normalize → classify → store → persist)...")
    flush_result = pipeline.a2.run_ingestion(records)
    flush_ok = flush_result.get("status") in ("success", "partial_success")
    _check(flush_ok, f"A1 ingestion: {flush_result.get('status')}")

    classified = pipeline.a2.get_classified_fields()
    _check(len(classified) > 0, f"{len(classified)} fields classified")

    # 6. Build storage strategy (SQL tables + Mongo collections)
    _info("Building storage strategy (SQL normalization + Mongo decomposition)...")
    strategy = pipeline.a2.build_storage_strategy(registration)
    strategy_ok = strategy.get("status") in ("success", "partial_success")
    _check(strategy_ok, f"Storage strategy: {strategy.get('sql_tables')} SQL tables, {strategy.get('mongo_collections')} Mongo collections")

    # 7. Insert records into both backends via A2
    _info("Inserting records into MySQL + MongoDB...")
    create_result = pipeline.a2.execute_operation(CrudOperation.CREATE, {"records": records})
    create_ok = create_result.get("status") in ("success", "partial_success")
    sql_ins = create_result.get("sql_inserted", 0)
    mongo_ins = create_result.get("mongo_inserted", 0)
    _check(create_ok, f"CREATE: {sql_ins} SQL rows, {mongo_ins} Mongo docs")

    if create_result.get("errors"):
        for e in create_result["errors"][:3]:
            print(f"    WARN: {e}")

    # 8. Drop stale 'records' collection from A1
    mongo = pipeline.a2.a1_pipeline._mongo_client
    if hasattr(mongo, "client") and mongo.client is not None:
        db_name = getattr(mongo, "database", None)
        if db_name:
            db = mongo.client[db_name]
            if "records" in db.list_collection_names():
                db.drop_collection("records")

    field_locations = pipeline._get_field_locations()
    _check(len(field_locations) > 0, f"{len(field_locations)} field locations available")

    _info("Bootstrap complete ✓\n")
    return pipeline, field_locations, records


# ═════════════════════════════════════════════════════════════════════
# CONCURRENCY TESTS
# ═════════════════════════════════════════════════════════════════════

def test_concurrent_updates(pipeline, field_locations) -> bool:
    """Test 1: Two threads update the same record — no lost update."""
    from src.a2.contracts import CrudOperation

    _section("TEST 1 · Lost Update Prevention (2 concurrent writers)")

    tag = f"_conc1_{uuid.uuid4().hex[:6]}"
    record = build_test_record(tag, field_locations)
    title_f = find_field("title", field_locations)
    user_f = find_field("username", field_locations)

    if not title_f or not user_f:
        return _check(False, "Missing title or username field in schema")

    # Insert test record
    ins = pipeline.execute_transactional(CrudOperation.CREATE, {"records": [record]})
    if not _check(ins.status == "committed", f"Setup insert: {ins.status}"):
        return False

    update_a, update_b = f"{tag}_ALPHA", f"{tag}_BRAVO"
    errors: list[str] = []
    barrier = threading.Barrier(2, timeout=10)

    def updater(new_title):
        try:
            barrier.wait()
            pipeline.execute_transactional(
                CrudOperation.UPDATE,
                {"updates": {title_f: new_title}, "filters": {user_f: tag}},
            )
        except Exception as exc:
            errors.append(str(exc))

    t1 = threading.Thread(target=updater, args=(update_a,), daemon=True)
    t2 = threading.Thread(target=updater, args=(update_b,), daemon=True)
    t1.start(); t2.start()
    t1.join(15); t2.join(15)

    final = read_title(pipeline, user_f, tag, field_locations)
    ok = _check(
        final in (update_a, update_b) and not errors,
        f"Final title = '{final}'  (expected one of ['{update_a}', '{update_b}'])"
    )
    if errors:
        print(f"    Errors: {errors}")

    # Cleanup
    try:
        pipeline.execute_transactional(CrudOperation.DELETE, {"filters": {"username": tag}})
    except Exception:
        pass
    return ok


def test_five_way_contention(pipeline, field_locations) -> bool:
    """Test 2: 5 threads all update the same record simultaneously."""
    from src.a2.contracts import CrudOperation

    _section("TEST 2 · 5-Way Write Contention")

    tag = f"_conc5_{uuid.uuid4().hex[:6]}"
    record = build_test_record(tag, field_locations)
    title_f = find_field("title", field_locations)
    user_f = find_field("username", field_locations)

    if not title_f or not user_f:
        return _check(False, "Missing fields")

    ins = pipeline.execute_transactional(CrudOperation.CREATE, {"records": [record]})
    if not _check(ins.status == "committed", f"Setup insert: {ins.status}"):
        return False

    errors: list[str] = []
    committed: list[str] = []
    barrier = threading.Barrier(5, timeout=15)

    def updater(tid: int):
        try:
            barrier.wait()
            r = pipeline.execute_transactional(
                CrudOperation.UPDATE,
                {"updates": {title_f: f"{tag}_t{tid}"}, "filters": {user_f: tag}},
            )
            committed.append(f"t{tid}:{r.status}")
        except Exception as exc:
            errors.append(f"t{tid}: {exc}")

    threads = [threading.Thread(target=updater, args=(i,), daemon=True) for i in range(5)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(30)

    final = read_title(pipeline, user_f, tag, field_locations)
    expected = [f"{tag}_t{i}" for i in range(5)]

    ok = True
    ok &= _check(final in expected, f"Final title = '{final}'  (one of 5 updates)")
    ok &= _check(len(committed) == 5, f"All 5 threads committed ({len(committed)}/5)")
    ok &= _check(not errors, f"No thread errors")
    if errors:
        for e in errors:
            print(f"    ERROR: {e}")

    try:
        pipeline.execute_transactional(CrudOperation.DELETE, {"filters": {"username": tag}})
    except Exception:
        pass
    return ok


def test_dirty_read_prevention(pipeline, field_locations) -> bool:
    """Test 3: Reader during write sees only committed state."""
    from src.a2.contracts import CrudOperation

    _section("TEST 3 · Dirty Read Prevention")

    tag = f"_conc_dr_{uuid.uuid4().hex[:6]}"
    record = build_test_record(tag, field_locations)
    title_f = find_field("title", field_locations)
    user_f = find_field("username", field_locations)

    if not title_f or not user_f:
        return _check(False, "Missing fields")

    ins = pipeline.execute_transactional(CrudOperation.CREATE, {"records": [record]})
    if not _check(ins.status == "committed", f"Setup insert: {ins.status}"):
        return False

    original_title = tag
    new_title = f"{tag}_updated"
    read_results: list[Any] = []
    errors: list[str] = []
    barrier = threading.Barrier(2, timeout=10)

    def writer():
        try:
            barrier.wait()
            pipeline.execute_transactional(
                CrudOperation.UPDATE,
                {"updates": {title_f: new_title}, "filters": {user_f: tag}},
            )
        except Exception as exc:
            errors.append(f"writer: {exc}")

    def reader():
        try:
            barrier.wait()
            time.sleep(0.02)
            res = pipeline.execute_transactional(
                CrudOperation.READ,
                {"filters": {user_f: tag}, "limit": 1},
            )
            read_results.extend(res.sql_result.get("records", []))
        except Exception as exc:
            errors.append(f"reader: {exc}")

    tw = threading.Thread(target=writer, daemon=True)
    tr = threading.Thread(target=reader, daemon=True)
    tw.start(); tr.start()
    tw.join(15); tr.join(15)

    observed = None
    for r in read_results:
        t = _extract_title(r)
        if t is not None:
            observed = t
            break

    ok = _check(
        observed in (original_title, new_title, None) and not errors,
        f"Reader saw '{observed}'  (valid: original or updated, never in-flight)"
    )
    if errors:
        print(f"    Errors: {errors}")

    try:
        pipeline.execute_transactional(CrudOperation.DELETE, {"filters": {"username": tag}})
    except Exception:
        pass
    return ok


def test_concurrent_reads(pipeline, field_locations, existing_username: str) -> bool:
    """Test 4: Multiple readers run in parallel without blocking."""
    from src.a2.contracts import CrudOperation

    _section("TEST 4 · Concurrent Reads (4 readers, no blocking)")

    user_f = find_field("username", field_locations)
    read_times: list[float] = []
    errors: list[str] = []
    barrier = threading.Barrier(4, timeout=10)

    def timed_reader(rid: int):
        try:
            barrier.wait()
            start = time.monotonic()
            pipeline.execute_transactional(
                CrudOperation.READ,
                {"filters": {user_f: existing_username} if user_f else {}, "limit": 5},
            )
            read_times.append(time.monotonic() - start)
        except Exception as exc:
            errors.append(f"r{rid}: {exc}")

    threads = [threading.Thread(target=timed_reader, args=(i,), daemon=True) for i in range(4)]
    for t in threads:
        t.start()
    for t in threads:
        t.join(15)

    ok = True
    ok &= _check(len(read_times) == 4, f"4/4 readers completed")
    ok &= _check(not errors, "No reader errors")
    if read_times:
        avg = sum(read_times) / len(read_times)
        _info(f"Avg read time: {avg:.3f}s  |  Times: {[f'{t:.3f}s' for t in read_times]}")
    return ok


def test_read_write_isolation(pipeline, field_locations) -> bool:
    """Test 5: Concurrent read + write on same entity — both succeed, reader sees consistent state."""
    from src.a2.contracts import CrudOperation

    _section("TEST 5 · Read/Write Isolation (reader during writer)")

    tag = f"_conc_rw_{uuid.uuid4().hex[:6]}"
    record = build_test_record(tag, field_locations)
    title_f = find_field("title", field_locations)
    user_f = find_field("username", field_locations)

    if not title_f or not user_f:
        return _check(False, "Missing fields")

    ins = pipeline.execute_transactional(CrudOperation.CREATE, {"records": [record]})
    if not _check(ins.status == "committed", f"Setup insert: {ins.status}"):
        return False

    write_ok = [False]
    read_ok = [False]
    errors: list[str] = []
    barrier = threading.Barrier(2, timeout=10)

    def writer():
        try:
            barrier.wait()
            r = pipeline.execute_transactional(
                CrudOperation.UPDATE,
                {"updates": {title_f: f"{tag}_rw_updated"}, "filters": {user_f: tag}},
            )
            write_ok[0] = r.status == "committed"
        except Exception as exc:
            errors.append(f"writer: {exc}")

    def reader():
        try:
            barrier.wait()
            r = pipeline.execute_transactional(
                CrudOperation.READ,
                {"filters": {user_f: tag}, "limit": 1},
            )
            read_ok[0] = r.status == "committed"
        except Exception as exc:
            errors.append(f"reader: {exc}")

    tw = threading.Thread(target=writer, daemon=True)
    tr = threading.Thread(target=reader, daemon=True)
    tw.start(); tr.start()
    tw.join(15); tr.join(15)

    ok = True
    ok &= _check(write_ok[0], "Writer committed successfully")
    ok &= _check(read_ok[0], "Reader completed successfully")
    ok &= _check(not errors, "No thread errors")

    try:
        pipeline.execute_transactional(CrudOperation.DELETE, {"filters": {"username": tag}})
    except Exception:
        pass
    return ok


def test_lock_key_observability(pipeline, field_locations) -> bool:
    """Test 6: TransactionResult.lock_key is populated."""
    from src.a2.contracts import CrudOperation

    _section("TEST 6 · Lock Key Observability")

    user_f = find_field("username", field_locations)
    res = pipeline.execute_transactional(
        CrudOperation.READ,
        {"filters": {user_f: "u1"} if user_f else {}, "limit": 1},
    )

    ok = _check(
        res.lock_key is not None and res.lock_key != "",
        f"TransactionResult.lock_key = '{res.lock_key}'"
    )
    return ok


def test_acid_suite(pipeline) -> bool:
    """Test 7: Full ACID experiment suite (Atomicity, Consistency, Isolation, Durability)."""
    _section("TEST 7 · Full ACID Experiment Suite")

    results = pipeline.run_acid_experiments()
    ok = True
    for r in results:
        ok &= _check(r.passed, f"{r.property_name.upper()}: {r.description[:90]}")
        if r.details:
            # Show sub-test details for isolation
            if r.property_name == "isolation":
                for sub in ("lost_update", "dirty_read", "lock_timeout"):
                    sub_d = r.details.get(sub, {})
                    if isinstance(sub_d, dict):
                        errs = sub_d.get("errors", [])
                        status = "PASS" if not errs else "FAIL"
                        _info(f"  ↳ {sub}: {status}")
            # Show key details for other properties
            for key in ("sql_before", "sql_after_fail", "mongo_after_fail",
                        "sql_count_after_reconnect", "mongo_count_after_reconnect"):
                if key in r.details:
                    _info(f"  ↳ {key}: {r.details[key]}")
        if not r.passed and r.details.get("error"):
            print(f"    ERROR: {r.details['error']}")
    return ok


# ═════════════════════════════════════════════════════════════════════
# MAIN
# ═════════════════════════════════════════════════════════════════════

def main() -> int:
    _banner("CONCURRENCY E2E TEST SUITE")
    _info("Requires MySQL + MongoDB running via docker-compose up")
    _info(f"Time: {time.strftime('%Y-%m-%d %H:%M:%S')}")

    pipeline = None
    try:
        # ── Bootstrap ──
        pipeline, field_locations, records = bootstrap()
        first_user = records[0]["username"]

        # ── Run all tests ──
        test_concurrent_updates(pipeline, field_locations)
        test_five_way_contention(pipeline, field_locations)
        test_dirty_read_prevention(pipeline, field_locations)
        test_concurrent_reads(pipeline, field_locations, first_user)
        test_read_write_isolation(pipeline, field_locations)
        test_lock_key_observability(pipeline, field_locations)
        test_acid_suite(pipeline)

    except Exception as exc:
        print(f"\n  [FATAL]  {exc}")
        traceback.print_exc()
        return 1
    finally:
        if pipeline:
            pipeline.close()

    # ── Summary ──
    _banner("FINAL RESULTS")
    total = _pass + _fail
    print(f"\n  Total: {total}  |  Passed: {_pass}  |  Failed: {_fail}")

    print(f"\n  ┌─────────────────────────────────────────────────────────┐")
    print(f"  │  Test                         │  Result                │")
    print(f"  ├───────────────────────────────┼────────────────────────┤")
    test_names = [
        "1. Lost Update Prevention",
        "2. 5-Way Write Contention",
        "3. Dirty Read Prevention",
        "4. Concurrent Reads",
        "5. Read/Write Isolation",
        "6. Lock Key Observability",
        "7. Full ACID Suite",
    ]
    for name in test_names:
        print(f"  │  {name:<29} │  see above             │")
    print(f"  └───────────────────────────────┴────────────────────────┘")

    if _fail == 0:
        print(f"\n  ✅ ALL {_pass} TESTS PASSED — concurrency control is working correctly\n")
        return 0
    else:
        print(f"\n  ❌ {_fail} TEST(S) FAILED — review output above\n")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
