#!/usr/bin/env python3
"""Run the full A2 pipeline end-to-end.

Usage:
  python run_pipeline.py --records 100
  python run_pipeline.py --records 50
  python run_pipeline.py              # defaults to 100 records
"""

from __future__ import annotations

import argparse
import json
import random
import sys
from pathlib import Path

from src.a2.contracts import CrudOperation, SchemaRegistration
from src.a2.orchestrator import Assignment2Pipeline
from src.config import get_config
from src.persistence.metadata_store import MetadataStore

# ── Pretty-print helpers ──────────────────────────────────────────────────────

def _section(title: str) -> None:
    """Print a bold section banner."""
    bar = "─" * 60
    print(f"\n{bar}")
    print(f"  {title}")
    print(bar)


def _check(condition: bool, label: str) -> None:
    """Print a PASS / FAIL line for a named assertion."""
    symbol = "PASS" if condition else "FAIL"
    print(f"  [{symbol}]  {label}")


# ── Schema loader ────────────────────────────────────────────────────────────

def load_registration(schema_path: Path) -> SchemaRegistration:
    data = json.loads(schema_path.read_text(encoding="utf-8"))
    return SchemaRegistration(
        schema_name=data["schema_name"],
        version=data["version"],
        root_entity=data["root_entity"],
        json_schema=data["json_schema"],
        constraints=data.get("constraints", {}),
    )


# ── Record enrichment ─────────────────────────────────────────────────────────

def enrich_records(records: list[dict]) -> list[dict]:
    """Add extra nested structures to generated records.

    - post.attachments: array of flat objects → creates an additional SQL child table
    - device.sensors: array of objects with nested readings array → creates an additional MongoDB collection
    """
    for i, record in enumerate(records):
        eid = record.get("event_id", f"e{i}")
        did = record.get("device", {}).get("device_id", f"d{i}")

        # Flat array of simple objects → SQL child table (post_attachments)
        record["post"]["attachments"] = [
            {"attachment_id": f"att_{eid}_1", "file_type": "image"},
            {"attachment_id": f"att_{eid}_2", "file_type": "video"},
        ]

        # Deeply nested array (objects containing sub-arrays) → MongoDB collection
        record["device"]["sensors"] = [
            {
                "sensor_id": f"sen_{did}_1",
                "type": "temperature",
                "readings": [
                    {"timestamp": record.get("timestamp", ""), "value": round(random.uniform(20, 40), 1)},
                    {"timestamp": record.get("timestamp", ""), "value": round(random.uniform(20, 40), 1)},
                ],
            },
            {
                "sensor_id": f"sen_{did}_2",
                "type": "humidity",
                "readings": [
                    {"timestamp": record.get("timestamp", ""), "value": round(random.uniform(30, 90), 1)},
                ],
            },
        ]

    return records


def cleanup_stale_records_collection(pipeline: Assignment2Pipeline) -> None:
    """Drop the 'records' collection left over by A1's default routing."""
    mongo = pipeline.a1_pipeline._mongo_client
    if hasattr(mongo, "client") and getattr(mongo, "client", None) is not None:
        db_name = getattr(mongo, "database", None)
        if db_name:
            db = mongo.client[db_name]
            if "records" in db.list_collection_names():
                db.drop_collection("records")


# ── Main ─────────────────────────────────────────────────────────────────────

def main() -> int:
    parser = argparse.ArgumentParser(description="Run the full A2 pipeline")
    parser.add_argument(
        "--records", "-n",
        type=int,
        default=100,
        help="Number of synthetic records to generate (default: 100)",
    )
    parser.add_argument(
        "--schema",
        default="schemas/assignment2_schema.template.json",
        help="Path to schema template JSON",
    )
    args = parser.parse_args()

    schema_path = Path(args.schema)
    if not schema_path.exists():
        print(f"FAIL: schema file not found: {schema_path}")
        return 1

    cfg = get_config()
    registration = load_registration(schema_path)

    # Clear prior metadata for a clean run
    MetadataStore(cfg.metadata_dir).clear()

    pipeline = Assignment2Pipeline(cfg)
    try:
        # ─────────────────────────────────────────────────────────────
        # PHASE 1: Schema Registration
        # ─────────────────────────────────────────────────────────────
        _section("PHASE 1 · Schema Registration")
        print(f"  Schema  : {registration.schema_name}  v{registration.version}")
        print(f"  Root    : {registration.root_entity}")
        constraints = registration.constraints or {}
        print(f"  Unique candidates : {constraints.get('unique_candidates', [])}")
        print(f"  Index  candidates : {constraints.get('index_candidates', [])}")
        pipeline.register_schema(registration)
        _check(True, "schema registered in MetadataCatalog")

        # ─────────────────────────────────────────────────────────────
        # PHASE 2: Record Generation + A1 Ingestion
        # ─────────────────────────────────────────────────────────────
        _section("PHASE 2 · Record Generation & A1 Ingestion (Normalization → Classification → Storage → Persistence)")
        print(f"  Generating {args.records} synthetic records from schema …")
        records = pipeline.generate_records(args.records, registration)
        records = enrich_records(records)
        print(f"  Generated  : {len(records)} records  (enriched with sensors + attachments)")
        print(f"  Top-level keys (record[0]) : {sorted(records[0].keys())}")

        flush_result = pipeline.run_ingestion(records)
        classified   = pipeline.get_classified_fields()

        sql_fields   = [f.field_path for f in classified if f.backend in ("SQL",  "BOTH")]
        mongo_fields = [f.field_path for f in classified if f.backend in ("MONGODB", "BOTH")]

        ingest_ok = flush_result.get("status") in ("success", "partial_success")
        _check(ingest_ok, "A1 flush completed")
        _check(len(classified) > 0, f"{len(classified)} fields classified")
        print(f"  Flush status   : {flush_result.get('status')}")
        print(f"  SQL  fields    : {len(sql_fields)}   → {sql_fields[:6]}{' …' if len(sql_fields) > 6 else ''}")
        print(f"  Mongo fields   : {len(mongo_fields)}  → {mongo_fields[:6]}{' …' if len(mongo_fields) > 6 else ''}")
        if flush_result.get("errors"):
            for e in flush_result["errors"]:
                print(f"  WARN  {e}")

        # ─────────────────────────────────────────────────────────────
        # PHASE 3: Storage Strategy Generation
        # ─────────────────────────────────────────────────────────────
        _section("PHASE 3 · Storage Strategy  (SQL 1NF/2NF/3NF  +  Mongo H1–H5 heuristics)")
        strategy_result = pipeline.build_storage_strategy(registration)
        strategy_ok = strategy_result.get("status") in ("success", "partial_success")
        _check(strategy_ok, "storage strategy built")
        sql_tables   = strategy_result.get("sql_tables", 0)
        sql_rels     = strategy_result.get("sql_relationships", 0)
        mongo_cols   = strategy_result.get("mongo_collections", 0)
        field_locs   = strategy_result.get("field_locations", 0)
        _check(sql_tables > 0,
               f"SQL normalisation → {sql_tables} tables, {sql_rels} FK relationships")
        _check(mongo_cols > 0,
               f"Mongo decomposition → {mongo_cols} collections")
        print(f"  Field locations persisted : {field_locs}")
        if strategy_result.get("errors"):
            for e in strategy_result["errors"]:
                print(f"  WARN  {e}")

        # ─────────────────────────────────────────────────────────────
        # PHASE 4: CREATE
        # ─────────────────────────────────────────────────────────────
        _section("PHASE 4 · CREATE  (insert_batch → SQL tables  +  insert_batch → Mongo collections)")
        print(f"  Input    : {len(records)} records")
        print(f"  Filter   : none  (full batch)")
        create_result = pipeline.execute_operation(
            CrudOperation.CREATE,
            {"records": records},
        )
        c_sql   = create_result.get("sql_inserted",   0)
        c_mongo = create_result.get("mongo_inserted",  0)
        create_ok = create_result.get("status") in ("success", "partial_success")
        _check(create_ok,   f"CREATE status        : {create_result.get('status')}")
        _check(c_sql > 0,   f"SQL  rows inserted   : {c_sql}")
        _check(c_mongo > 0, f"Mongo docs inserted  : {c_mongo}")
        print(f"  Message  : {create_result.get('message')}")
        if create_result.get("errors"):
            for e in create_result["errors"]:
                print(f"  WARN  {e}")

        # ─────────────────────────────────────────────────────────────
        # PHASE 5: READ
        # ─────────────────────────────────────────────────────────────
        _section("PHASE 5 · READ  (SELECT … WHERE  +  find({filter})  →  keyed_merge prefer_sql)")
        first_user  = records[0]["username"]
        req_fields  = ["username", "event_id", "title"]
        print(f"  Filter   : username = '{first_user}'")
        print(f"  Fields   : {req_fields}")
        print(f"  Limit    : 10")
        read_result = pipeline.execute_operation(
            CrudOperation.READ,
            {
                "fields": req_fields,
                "filters": {"username": first_user},
                "limit": 10,
            },
        )
        read_records = read_result.get("records", [])
        read_ok = read_result.get("status") in ("success", "partial_success")
        _check(read_ok,               f"READ status           : {read_result.get('status')}")
        _check(len(read_records) > 0,  f"Merged records returned : {len(read_records)}")
        print(f"  SQL rows fetched     : {read_result.get('sql_rows', 0)}")
        print(f"  Mongo docs fetched   : {read_result.get('mongo_docs', 0)}")
        print(f"  Join keys used       : {read_result.get('join_keys', [])}")
        if read_records:
            print(f"  Sample record [0]    : {read_records[0]}")
        if read_result.get("errors"):
            for e in read_result["errors"]:
                print(f"  WARN  {e}")

        # ─────────────────────────────────────────────────────────────
        # PHASE 6: UPDATE
        # ─────────────────────────────────────────────────────────────
        _section("PHASE 6 · UPDATE  (UPDATE … SET … WHERE  +  update_many $set)")
        updated_title = "updated_pipeline_title"
        first_event   = records[0]["event_id"]
        print(f"  Filter   : username = '{first_user}', event_id = '{first_event}'")
        print(f"  Set      : title = '{updated_title}'")
        update_result = pipeline.execute_operation(
            CrudOperation.UPDATE,
            {
                "updates": {"title": updated_title},
                "filters": {
                    "username": first_user,
                    "event_id": first_event,
                },
            },
        )
        u_sql   = update_result.get("sql_updated",   0)
        u_mongo = update_result.get("mongo_updated",  0)
        update_ok = update_result.get("status") in ("success", "partial_success")
        _check(update_ok,    f"UPDATE status         : {update_result.get('status')}")
        _check(u_sql > 0,    f"SQL  rows affected    : {u_sql}")
        print(f"  Mongo docs modified  : {u_mongo}")
        print(f"  Message  : {update_result.get('message')}")
        if update_result.get("errors"):
            for e in update_result["errors"]:
                print(f"  WARN  {e}")

        # Verify UPDATE by reading back the record
        print(f"\n  -- Read-back verification (username='{first_user}', event_id='{first_event}') --")
        verify_read = pipeline.execute_operation(
            CrudOperation.READ,
            {
                "fields": ["username", "event_id", "title"],
                "filters": {"username": first_user, "event_id": first_event},
                "limit": 1,
            },
        )
        verify_records = verify_read.get("records", [])
        update_verified = any(
            isinstance(r.get("post"), dict) and r["post"].get("title") == updated_title
            for r in verify_records
        )
        _check(update_verified, f"title == '{updated_title}' confirmed in read-back")
        if not update_verified:
            print(f"  Expected title : '{updated_title}'")
            print(f"  Got records    : {verify_records}")
            return 1

        # ─────────────────────────────────────────────────────────────
        # PHASE 7: DELETE
        # ─────────────────────────────────────────────────────────────
        _section("PHASE 7 · DELETE  (child-first SQL cascade  +  delete_many Mongo wildcard sweep)")
        target    = records[1] if len(records) > 1 else records[0]
        del_user  = target["username"]
        del_event = target["event_id"]
        print(f"  Filter   : username = '{del_user}', event_id = '{del_event}'")
        print(f"  Strategy : planner orders child tables first (FK integrity), then Mongo wildcard sweep")
        delete_result = pipeline.execute_operation(
            CrudOperation.DELETE,
            {
                "filters": {
                    "username": del_user,
                    "event_id": del_event,
                },
            },
        )
        d_sql   = delete_result.get("sql_deleted",   0)
        d_mongo = delete_result.get("mongo_deleted",  0)
        delete_ok = delete_result.get("status") in ("success", "partial_success")
        _check(delete_ok,    f"DELETE status         : {delete_result.get('status')}")
        _check(d_sql > 0,    f"SQL  rows deleted     : {d_sql}")
        _check(d_mongo > 0,  f"Mongo docs deleted    : {d_mongo}")
        print(f"  Message  : {delete_result.get('message')}")
        if delete_result.get("errors"):
            for e in delete_result["errors"]:
                print(f"  WARN  {e}")

        # Verify DELETE — record must be absent
        print(f"\n  -- Read-back verification (should return 0 records) --")
        ghost_read = pipeline.execute_operation(
            CrudOperation.READ,
            {
                "fields": ["username", "event_id"],
                "filters": {"username": del_user, "event_id": del_event},
                "limit": 1,
            },
        )
        ghost_records = ghost_read.get("records", [])
        _check(len(ghost_records) == 0, "record absent in read-back after DELETE")
        if ghost_records:
            print(f"  WARN  record still visible: {ghost_records}")

        # ─────────────────────────────────────────────────────────────
        # Cleanup + Final Summary
        # ─────────────────────────────────────────────────────────────
        cleanup_stale_records_collection(pipeline)

        _section("RESULT · Full A2 Pipeline")
        print("  [PASS]  All phases completed successfully\n")
        print("  ┌─────────────────────────────────────────────────────┐")
        print("  │  Operation  │  SQL                 │  Mongo         │")
        print("  ├─────────────┼──────────────────────┼────────────────┤")
        print(f"  │  CREATE     │  {c_sql:<20} │  {c_mongo:<14} │")
        print(f"  │  READ       │  {read_result.get('sql_rows',0):<20} │  {read_result.get('mongo_docs',0):<14} │")
        print(f"  │  UPDATE     │  {u_sql:<20} │  {u_mongo:<14} │")
        print(f"  │  DELETE     │  {d_sql:<20} │  {d_mongo:<14} │")
        print("  └─────────────┴──────────────────────┴────────────────┘")
        return 0

    except Exception as exc:
        print(f"\n  [FAIL]  {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        pipeline.close()


if __name__ == "__main__":
    raise SystemExit(main())
