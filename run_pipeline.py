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


def load_registration(schema_path: Path) -> SchemaRegistration:
    data = json.loads(schema_path.read_text(encoding="utf-8"))
    return SchemaRegistration(
        schema_name=data["schema_name"],
        version=data["version"],
        root_entity=data["root_entity"],
        json_schema=data["json_schema"],
        constraints=data.get("constraints", {}),
    )


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
        # ── Phase 1: Register schema ──────────────────────────────────
        print(f"Registering schema '{registration.schema_name}' v{registration.version}")
        pipeline.register_schema(registration)

        # ── Phase 2: Generate + ingest records ────────────────────────
        print(f"Generating {args.records} synthetic records")
        records = pipeline.generate_records(args.records, registration)
        records = enrich_records(records)
        print(f"Ingesting {len(records)} records")
        flush_result = pipeline.run_ingestion(records)
        classified = pipeline.get_classified_fields()
        print(f"       Ingested: {flush_result.get('status')} | {len(classified)} fields classified")

        # ── Phase 3: Build storage strategy ───────────────────────────
        print("Building storage strategy (SQL + MongoDB)")
        strategy_result = pipeline.build_storage_strategy(registration)
        print(
            f"       SQL: {strategy_result['sql_tables']} tables, "
            f"{strategy_result['sql_relationships']} relationships | "
            f"Mongo: {strategy_result['mongo_collections']} collections | "
            f"Locations: {strategy_result['field_locations']}"
        )
        if strategy_result.get("errors"):
            for err in strategy_result["errors"]:
                print(f"       WARNING: {err}")

        # ── Phase 4: CREATE ───────────────────────────────────────────
        print(f"Inserting {len(records)} records")
        create_result = pipeline.execute_operation(
            CrudOperation.CREATE,
            {"records": records},
        )
        print(
            f"       SQL inserted: {create_result.get('sql_inserted', 0)} | "
            f"Mongo inserted: {create_result.get('mongo_inserted', 0)} | "
            f"Status: {create_result.get('status')}"
        )
        if create_result.get("errors"):
            for err in create_result["errors"]:
                print(f"       WARNING: {err}")

        # ── Phase 5: READ ─────────────────────────────────────────────
        # Read the first generated record back
        first_user = records[0]["username"]
        print(f"Reading records for username='{first_user}'")
        read_result = pipeline.execute_operation(
            CrudOperation.READ,
            {
                "fields": ["username", "event_id", "title"],
                "filters": {"username": first_user},
                "limit": 10,
            },
        )
        read_records = read_result.get("records", [])
        print(
            f"       Found {len(read_records)} records | "
            f"SQL rows: {read_result.get('sql_rows', 0)} | "
            f"Mongo docs: {read_result.get('mongo_docs', 0)}"
        )
        if read_result.get("errors"):
            for err in read_result["errors"]:
                print(f"       WARNING: {err}")

        # ── Phase 6: UPDATE ───────────────────────────────────────────
        updated_title = "updated_pipeline_title"
        first_event = records[0]["event_id"]
        print(f"Updating title for username='{first_user}', event_id='{first_event}'")
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
        print(
            f"       SQL updated: {update_result.get('sql_updated', 0)} | "
            f"Mongo updated: {update_result.get('mongo_updated', 0)} | "
            f"Status: {update_result.get('status')}"
        )
        if update_result.get("errors"):
            for err in update_result["errors"]:
                print(f"       WARNING: {err}")

        # Verify update
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
        if update_verified:
            print("       Update verified successfully")
        else:
            print("       FAIL: update verification failed")
            return 1

        # ── Phase 7: DELETE ───────────────────────────────────────────
        # Use the second record if available, else fall back to first
        target = records[1] if len(records) > 1 else records[0]
        del_user = target["username"]
        del_event = target["event_id"]
        print(f"Deleting record for username='{del_user}', event_id='{del_event}'")
        delete_result = pipeline.execute_operation(
            CrudOperation.DELETE,
            {
                "filters": {
                    "username": del_user,
                    "event_id": del_event,
                },
            },
        )
        print(
            f"       SQL deleted: {delete_result.get('sql_deleted', 0)} | "
            f"Mongo deleted: {delete_result.get('mongo_deleted', 0)} | "
            f"Status: {delete_result.get('status')}"
        )
        if delete_result.get("errors"):
            for err in delete_result["errors"]:
                print(f"       WARNING: {err}")

        # ── Cleanup ───────────────────────────────────────────────────
        cleanup_stale_records_collection(pipeline)

        print("\nPASS: Full A2 pipeline completed successfully")
        return 0

    except Exception as exc:
        print(f"\nFAIL: {exc}")
        import traceback
        traceback.print_exc()
        return 1
    finally:
        pipeline.close()


if __name__ == "__main__":
    raise SystemExit(main())
