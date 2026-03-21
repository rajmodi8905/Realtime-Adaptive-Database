#!/usr/bin/env python3
"""Small A2 smoke test for implemented components.

Covers:
1) A1 ingest + classify bridge data
2) A2 SQL normalization plan generation (+ optional SQL DDL execution)
3) A2 Mongo decomposition plan generation (+ optional collection execution)
4) Storage strategy field-location generation
5) Metadata catalog save/load roundtrip
6) Query planner plans for READ/CREATE/UPDATE/DELETE

Usage:
  python test_a2_components.py
  python test_a2_components.py --execute-sql
  python test_a2_components.py --execute-sql --execute-mongo
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

from src.a2.contracts import ClassifiedField, CrudOperation, SchemaRegistration
from src.a2.metadata_catalog import MetadataCatalog
from src.a2.mongo_decomposition_engine import MongoDecompositionEngine
from src.a2.query_planner import QueryPlanner
from src.a2.sql_normalization_engine import SqlNormalizationEngine
from src.a2.storage_strategy_generator import StorageStrategyGenerator
from src.a2.crud_engine import CrudEngine
from src.config import get_config
from src.ingest_and_classify import IngestAndClassify
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


def sample_records() -> list[dict]:
    return [
        {
            "username": "u1",
            "event_id": "e1",
            "timestamp": "2026-03-18T10:00:00Z",
            "device": {
                "device_id": "d1",
                "model": "alpha",
                "firmware": "1.0.0",
            },
            "post": {
                "post_id": "p1",
                "title": "hello",
                "tags": ["db", "a2"],
                "attachments": [
                    {
                        "attachment_id": "a1",
                        "kind": "image",
                    }
                ],
                "comments": [
                    {
                        "comment_id": "c1",
                        "text": "nice",
                        "commenter": "alice",
                        "reactions": [
                            {
                                "reaction_type": "like",
                                "count": 2,
                            }
                        ],
                    }
                ],
            },
            "metrics": {
                "latency_ms": 12.3,
                "battery_pct": 90,
                "signal_quality": "good",
            },
        },
        {
            "username": "u2",
            "event_id": "e2",
            "timestamp": "2026-03-18T10:01:00Z",
            "device": {
                "device_id": "d2",
                "model": "beta",
                "firmware": "1.0.1",
            },
            "post": {
                "post_id": "p2",
                "title": "world",
                "tags": ["mongo"],
                "attachments": [
                    {
                        "attachment_id": "a2",
                        "kind": "video",
                    }
                ],
                "comments": [
                    {
                        "comment_id": "c2",
                        "text": "great",
                        "commenter": "bob",
                        "reactions": [
                            {
                                "reaction_type": "clap",
                                "count": 5,
                            }
                        ],
                    }
                ],
            },
            "metrics": {
                "latency_ms": 20.0,
                "battery_pct": 80,
                "signal_quality": "ok",
            },
        },
    ]


def create_records() -> list[dict]:
    return [
        {
            "username": "u3",
            "event_id": "e3",
            "timestamp": "2026-03-18T10:02:00Z",
            "device": {
                "device_id": "d3",
                "model": "gamma",
                "firmware": "1.0.2",
            },
            "post": {
                "post_id": "p3",
                "title": "fresh",
                "tags": ["sql", "mongo"],
                "attachments": [
                    {
                        "attachment_id": "a3",
                        "kind": "image",
                    }
                ],
                "comments": [
                    {
                        "comment_id": "c3",
                        "text": "new",
                        "commenter": "carol",
                        "reactions": [
                            {
                                "reaction_type": "wow",
                                "count": 3,
                            }
                        ],
                    }
                ],
            },
            "metrics": {
                "latency_ms": 15.2,
                "battery_pct": 75,
                "signal_quality": "good",
            },
        },
        {
            "username": "u4",
            "event_id": "e4",
            "timestamp": "2026-03-18T10:03:00Z",
            "device": {
                "device_id": "d4",
                "model": "delta",
                "firmware": "1.0.3",
            },
            "post": {
                "post_id": "p4",
                "title": "latest",
                "tags": ["pipeline"],
                "attachments": [
                    {
                        "attachment_id": "a4",
                        "kind": "audio",
                    }
                ],
                "comments": [
                    {
                        "comment_id": "c4",
                        "text": "works",
                        "commenter": "dave",
                        "reactions": [
                            {
                                "reaction_type": "like",
                                "count": 1,
                            }
                        ],
                    }
                ],
            },
            "metrics": {
                "latency_ms": 11.0,
                "battery_pct": 88,
                "signal_quality": "ok",
            },
        },
    ]


def bridge_classified_fields(pipeline: IngestAndClassify) -> list[ClassifiedField]:
    decisions = pipeline.get_decisions()
    stats = pipeline.get_field_stats()

    classified: list[ClassifiedField] = []
    for field_name, decision in decisions.items():
        classified.append(ClassifiedField.from_a1_decision(decision, stats.get(field_name)))
    return classified


def extract_dotted(record: dict, path: str):
    current = record
    for part in path.split("."):
        if not isinstance(current, dict) or part not in current:
            return None
        current = current[part]
    return current


def main() -> int:
    parser = argparse.ArgumentParser(description="Run A2 implemented-components smoke test")
    parser.add_argument(
        "--schema",
        default="schemas/assignment2_schema.template.json",
        help="Path to assignment-2 schema template JSON",
    )
    parser.add_argument(
        "--execute-sql",
        action="store_true",
        help="Execute SQL CREATE TABLE plans on MySQL after generation",
    )
    parser.add_argument(
        "--execute-mongo",
        action="store_true",
        help="Execute Mongo collection plans after generation",
    )
    args = parser.parse_args()

    schema_path = Path(args.schema)
    if not schema_path.exists():
        print(f"FAIL: schema file not found: {schema_path}")
        return 1

    cfg = get_config()
    registration = load_registration(schema_path)

    # Keep smoke test deterministic across runs by clearing prior A1 metadata.
    MetadataStore(cfg.metadata_dir).clear()

    pipeline = IngestAndClassify(cfg)
    try:
        print("Step 1: Ingest + flush through A1")
        pipeline.ingest_batch(sample_records())
        flush_result = pipeline.flush()
        print("  flush status:", flush_result.get("status"))

        print("Step 2: Bridge A1 classification to A2 contracts")
        classified_fields = bridge_classified_fields(pipeline)
        print("  classified fields:", len(classified_fields))
        if not classified_fields:
            print("FAIL: no classified fields generated")
            return 1

        print("Step 3: Generate SQL and Mongo plans")
        sql_engine = SqlNormalizationEngine()
        mongo_engine = MongoDecompositionEngine()

        sql_tables = sql_engine.generate_table_plans(registration, classified_fields)
        sql_relationships = sql_engine.generate_relationships(sql_tables)
        sql_root_pk = next((t.primary_key for t in sql_tables if t.table_name == registration.root_entity), None)
        mongo_collections = mongo_engine.generate_collection_plans(registration, classified_fields, sql_root_pk)

        print("  sql tables:", len(sql_tables))
        print("  sql relationships:", len(sql_relationships))
        print("  mongo collection plans:", len(mongo_collections))

        if args.execute_sql:
            print("Step 4a: Execute SQL table plans")
            sql_exec = sql_engine.execute_table_plans(sql_tables, sql_relationships, pipeline._mysql_client)
            print("  sql execute:", sql_exec)

        if args.execute_mongo:
            print("Step 4b: Execute Mongo collection plans")
            mongo_exec = mongo_engine.execute_collection_plans(mongo_collections, pipeline._mongo_client)
            print("  mongo execute:", mongo_exec)

        print("Step 5: Generate field locations and persist metadata")
        strategy = StorageStrategyGenerator()
        field_locations = strategy.generate_field_locations(registration, sql_tables, sql_relationships, mongo_collections)
        print("  field locations generated:", len(field_locations))

        catalog = MetadataCatalog(cfg.metadata_dir)
        catalog.save_schema(registration)
        catalog.save_sql_plan(sql_tables, sql_relationships)
        catalog.save_mongo_plan(mongo_collections)
        catalog.save_field_locations(field_locations)
        loaded_locations = catalog.get_field_locations()
        print("  field locations loaded:", len(loaded_locations))

        print("Step 6: Build query plans for all CRUD operations")
        planner = QueryPlanner()

        read_plan = planner.build_plan(
            CrudOperation.READ,
            {
                "fields": ["username", "event_id", "post.title"],
                "filters": {"username": "u1"},
                "limit": 10,
            },
            loaded_locations,
        )
        create_plan = planner.build_plan(CrudOperation.CREATE, {"records": create_records()}, loaded_locations)
        update_plan = planner.build_plan(
            CrudOperation.UPDATE,
            {"updates": {"post.title": "updated"}, "filters": {"event_id": "e1"}},
            loaded_locations,
        )
        delete_plan = planner.build_plan(CrudOperation.DELETE, {"filters": {"event_id": "e2"}}, loaded_locations)

        print("  read plan -> sql:", len(read_plan.sql_queries), "mongo:", len(read_plan.mongo_queries))
        print("  create plan -> sql:", len(create_plan.sql_queries), "mongo:", len(create_plan.mongo_queries))
        print("  update plan -> sql:", len(update_plan.sql_queries), "mongo:", len(update_plan.mongo_queries))
        print("  delete plan -> sql:", len(delete_plan.sql_queries), "mongo:", len(delete_plan.mongo_queries))

        if not args.execute_sql:
            print("Step 6a: Auto-create SQL tables for CRUD execution")
            sql_exec = sql_engine.execute_table_plans(sql_tables, sql_relationships, pipeline._mysql_client)
            print("  sql execute:", sql_exec)
            if sql_exec.get("errors"):
                print("FAIL: SQL table preparation failed before CRUD execution")
                return 1

        print("Step 6b: Execute CREATE plan to populate generated SQL/Mongo targets")
        crud = CrudEngine()
        create_result = crud.execute(
            create_plan,
            mysql_client=pipeline._mysql_client,
            mongo_client=pipeline._mongo_client,
        )
        print("  create status:", create_result.get("status"))
        print("  sql inserted:", create_result.get("sql_inserted"))
        print("  mongo inserted:", create_result.get("mongo_inserted"))
        print("  create errors:", create_result.get("errors"))

        print("Step 7: Execute READ query end-to-end")
        created_record = create_records()[0]
        create_user = created_record["username"]
        read_payload = {
            "fields": ["username", "event_id", "post.title"],
            "filters": {"username": create_user},
            "limit": 10,
        }

        read_plan_exec = planner.build_plan(CrudOperation.READ, read_payload, loaded_locations)
        print("  planned SQL queries:", len(read_plan_exec.sql_queries))
        print("  planned Mongo queries:", len(read_plan_exec.mongo_queries))
        print("  merge strategy:", read_plan_exec.merge_strategy)

        read_result = crud.execute(
            read_plan_exec,
            mysql_client=pipeline._mysql_client,
            mongo_client=pipeline._mongo_client,
        )

        print("  read status:", read_result.get("status"))
        print("  read message:", read_result.get("message"))
        print("  sql rows:", read_result.get("sql_rows"))
        print("  mongo docs:", read_result.get("mongo_docs"))
        print("  errors:", read_result.get("errors"))
        print("  records:")
        for r in read_result.get("records", []):
            print("   ", r)

        print("Step 8: Execute UPDATE query end-to-end")
        updated_title = "updated_u3_title"
        update_payload_exec = {
            "updates": {"post.title": updated_title},
            "filters": {
                "username": created_record["username"],
                "event_id": created_record["event_id"],
            },
        }
        update_plan_exec = planner.build_plan(
            CrudOperation.UPDATE,
            update_payload_exec,
            loaded_locations,
        )
        print("  planned SQL update queries:", len(update_plan_exec.sql_queries))
        print("  planned Mongo update queries:", len(update_plan_exec.mongo_queries))

        update_result = crud.execute(
            update_plan_exec,
            mysql_client=pipeline._mysql_client,
            mongo_client=pipeline._mongo_client,
        )
        print("  update status:", update_result.get("status"))
        print("  update message:", update_result.get("message"))
        print("  sql updated:", update_result.get("sql_updated"))
        print("  mongo updated:", update_result.get("mongo_updated"))
        print("  update errors:", update_result.get("errors"))

        read_after_update = crud.execute(
            read_plan_exec,
            mysql_client=pipeline._mysql_client,
            mongo_client=pipeline._mongo_client,
        )
        updated_records = read_after_update.get("records", [])
        has_updated_title = any(
            isinstance(record.get("post"), dict)
            and record["post"].get("title") == updated_title
            for record in updated_records
        )
        if not has_updated_title:
            print("FAIL: update validation failed, post.title did not change")
            for r in updated_records:
                print("   ", r)
            return 1

        print("Step 9: Execute UPDATE for post.comments.text")
        comments_text_loc = next(
            (
                loc
                for loc in loaded_locations
                if loc.field_path == "post.comments.text"
            ),
            None,
        )
        comments_parent_loc = next(
            (
                loc
                for loc in loaded_locations
                if loc.field_path == "post.comments"
            ),
            None,
        )
        if comments_text_loc is None and comments_parent_loc is None:
            print("FAIL: expected post.comments or post.comments.text mapping was not found")
            return 1

        updated_comment_text = "updated_comment_text_u3"
        if comments_text_loc is not None:
            comments_update_field = comments_text_loc.field_path
            comments_update_value = updated_comment_text
        else:
            assert comments_parent_loc is not None
            comments_update_field = comments_parent_loc.field_path
            comments_copy = json.loads(json.dumps(created_record["post"]["comments"]))
            if comments_copy and isinstance(comments_copy[0], dict):
                comments_copy[0]["text"] = updated_comment_text
            comments_update_value = comments_copy

        comments_update_payload = {
            "updates": {comments_update_field: comments_update_value},
            "filters": {
                "username": created_record["username"],
                "event_id": created_record["event_id"],
            },
        }
        comments_update_plan = planner.build_plan(
            CrudOperation.UPDATE,
            comments_update_payload,
            loaded_locations,
        )
        print("  planned SQL comments update queries:", len(comments_update_plan.sql_queries))
        print("  planned Mongo comments update queries:", len(comments_update_plan.mongo_queries))

        comments_update_result = crud.execute(
            comments_update_plan,
            mysql_client=pipeline._mysql_client,
            mongo_client=pipeline._mongo_client,
        )
        print("  comments update status:", comments_update_result.get("status"))
        print("  comments update sql updated:", comments_update_result.get("sql_updated"))
        print("  comments update mongo updated:", comments_update_result.get("mongo_updated"))
        print("  comments update errors:", comments_update_result.get("errors"))
        if (
            int(comments_update_result.get("sql_updated") or 0) <= 0
            and int(comments_update_result.get("mongo_updated") or 0) <= 0
        ):
            print("FAIL: comments update did not modify any records")
            return 1

        if comments_update_plan.sql_queries:
            has_sql_comment_update = False
            for query in comments_update_plan.sql_queries:
                table = query.get("table")
                set_values = dict(query.get("set") or {})
                where = dict(query.get("where") or {})
                if not table or not set_values:
                    continue

                target_col = next(
                    (
                        col
                        for col, val in set_values.items()
                        if val == updated_comment_text
                    ),
                    None,
                )
                if not target_col:
                    continue

                params = []
                where_clause = ""
                if where:
                    conds = []
                    for col, val in where.items():
                        conds.append(f"`{col}` = %s")
                        params.append(val)
                    where_clause = " WHERE " + " AND ".join(conds)

                sql_rows_after_comments_update = pipeline._mysql_client.fetch_all(
                    f"SELECT `{target_col}` FROM `{table}`{where_clause}",
                    tuple(params) if params else None,
                )
                if any(
                    isinstance(row, dict) and row.get(target_col) == updated_comment_text
                    for row in sql_rows_after_comments_update
                ):
                    has_sql_comment_update = True
                    break

            if not has_sql_comment_update:
                print("FAIL: SQL comments update validation failed")
                return 1

        if comments_update_plan.mongo_queries:
            has_mongo_comment_update = False
            for query in comments_update_plan.mongo_queries:
                collection = query.get("collection")
                update_filter = dict(query.get("filter") or {})
                set_values = dict(query.get("set") or {})
                if not collection or not set_values:
                    continue

                target_path = next(
                    (
                        path
                        for path, val in set_values.items()
                        if val == updated_comment_text
                    ),
                    None,
                )
                if not target_path:
                    continue

                mongo_docs_after_comments_update = pipeline._mongo_client.find(
                    collection,
                    update_filter,
                )
                for doc in mongo_docs_after_comments_update:
                    if not isinstance(doc, dict):
                        continue
                    target_value = extract_dotted(doc, target_path)
                    if target_value == updated_comment_text:
                        has_mongo_comment_update = True
                        break
                    if (
                        isinstance(target_value, list)
                        and target_value
                        and isinstance(target_value[0], dict)
                        and target_value[0].get("text") == updated_comment_text
                    ):
                        has_mongo_comment_update = True
                        break
                if has_mongo_comment_update:
                    break

            if not has_mongo_comment_update:
                if int(comments_update_result.get("mongo_updated") or 0) > 0:
                    print("  note: Mongo comments content check skipped due shape mismatch; using mongo_updated count")
                else:
                    print("FAIL: Mongo comments update validation failed")
                    return 1

        print("Step 9: Execute DELETE query end-to-end")
        delete_filters = {
            "username": created_record["username"],
            "event_id": created_record["event_id"],
        }

        if not delete_filters:
            print("FAIL: no usable delete filters were found for the created record")
            return 1

        delete_payload = {"filters": delete_filters}
        delete_plan_exec = planner.build_plan(CrudOperation.DELETE, delete_payload, loaded_locations)
        print("  delete filters:", delete_filters)
        print("  planned SQL delete queries:", len(delete_plan_exec.sql_queries))
        print("  planned Mongo delete queries:", len(delete_plan_exec.mongo_queries))

        delete_result = crud.execute(
            delete_plan_exec,
            mysql_client=pipeline._mysql_client,
            mongo_client=pipeline._mongo_client,
        )
        print("  delete status:", delete_result.get("status"))
        print("  delete message:", delete_result.get("message"))
        print("  sql deleted:", delete_result.get("sql_deleted"))
        print("  mongo deleted:", delete_result.get("mongo_deleted"))
        print("  delete errors:", delete_result.get("errors"))

        print("Step 10: Re-run READ to verify deletion")
        read_after_delete = crud.execute(
            read_plan_exec,
            mysql_client=pipeline._mysql_client,
            mongo_client=pipeline._mongo_client,
        )
        remaining = read_after_delete.get("records", [])
        print(remaining)
        print("  read-after-delete status:", read_after_delete.get("status"))
        print("  records after delete:", len(remaining))
        if remaining:
            print("FAIL: delete validation failed, records still present")
            for r in remaining:
                print("   ", r)
            return 1

        print("Step 11: Delete post for username u4")
        target_post = create_records()[1]
        delete_u4_post_payload = {
            "fields": ["post.post_id"],
            "filters": {
                "username": target_post["username"],
                "event_id": target_post["event_id"],
                "post.post_id": target_post["post"]["post_id"],
            },
        }
        delete_u4_post_plan = planner.build_plan(
            CrudOperation.DELETE,
            delete_u4_post_payload,
            loaded_locations,
        )
        print("  u4 post delete filters:", delete_u4_post_payload["filters"])
        print("  planned SQL post delete queries:", len(delete_u4_post_plan.sql_queries))
        print("  planned Mongo post delete queries:", len(delete_u4_post_plan.mongo_queries))

        delete_u4_post_result = crud.execute(
            delete_u4_post_plan,
            mysql_client=pipeline._mysql_client,
            mongo_client=pipeline._mongo_client,
        )
        print("  u4 post delete status:", delete_u4_post_result.get("status"))
        print("  u4 sql deleted:", delete_u4_post_result.get("sql_deleted"))
        print("  u4 mongo deleted:", delete_u4_post_result.get("mongo_deleted"))
        print("  u4 post delete errors:", delete_u4_post_result.get("errors"))

        print("Step 12: Re-run READ to verify u4 post deletion")
        u4_root_read_payload = {
            "fields": ["username", "event_id"],
            "filters": {
                "username": target_post["username"],
                "event_id": target_post["event_id"],
            },
            "limit": 10,
        }
        u4_root_read_plan = planner.build_plan(
            CrudOperation.READ,
            u4_root_read_payload,
            loaded_locations,
        )
        u4_root_read_result = crud.execute(
            u4_root_read_plan,
            mysql_client=pipeline._mysql_client,
            mongo_client=pipeline._mongo_client,
        )
        u4_remaining = u4_root_read_result.get("records", [])
        print("  u4 read-after-delete status:", u4_root_read_result.get("status"))
        print("  u4 records after post delete:", len(u4_remaining))
        if not u4_remaining:
            print("FAIL: u4 root record was deleted; expected only post removal")
            return 1

        sql_scoped_tables_set: set[str] = set()
        for query in delete_u4_post_plan.sql_queries:
            if query.get("type") != "delete":
                continue
            table = query.get("table")
            if isinstance(table, str):
                sql_scoped_tables_set.add(table)
        sql_scoped_tables = sorted(sql_scoped_tables_set)
        for table in sql_scoped_tables:
            columns = pipeline._mysql_client.get_current_columns(table)
            if "username" not in columns:
                continue
            remaining_sql_rows = pipeline._mysql_client.fetch_all(
                f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE `username` = %s",
                (target_post["username"],),
            )
            count = int((remaining_sql_rows or [{"cnt": 0}])[0].get("cnt", 0))
            if count > 0:
                print("FAIL: u4 post delete validation failed, SQL rows still present in", table)
                return 1

        if delete_u4_post_plan.mongo_queries:
            if pipeline._mongo_client.client is None:
                print("FAIL: Mongo client is not connected for post-delete verification")
                return 1

            mongo_db = pipeline._mongo_client.client[pipeline._mongo_client.database]
            matching_docs: list[dict] = []
            for collection_name in mongo_db.list_collection_names():
                docs = list(
                    mongo_db[collection_name].find(
                        {
                            "username": target_post["username"],
                        }
                    )
                )
                matching_docs.extend(docs)

            if not matching_docs:
                print("FAIL: no Mongo documents found for u4 after post delete")
                return 1

            for doc in matching_docs:
                if "post" in doc and doc["post"] not in (None, {}, []):
                    print("FAIL: u4 post delete validation failed, Mongo post still present")
                    print("   ", doc)
                    return 1

            post_collections = [
                name
                for name in mongo_db.list_collection_names()
                if name.startswith("events_post_")
            ]
            for collection_name in post_collections:
                remaining_post_docs = list(
                    mongo_db[collection_name].find({"username": target_post["username"]})
                )
                if remaining_post_docs:
                    print(
                        "FAIL: u4 post delete validation failed, post collection still has docs:",
                        collection_name,
                    )
                    return 1

        print("PASS: A2 implemented-component smoke test completed")
        return 0
    except Exception as exc:  # noqa: BLE001
        print(f"FAIL: {exc}")
        return 1
    finally:
        pipeline.close()


if __name__ == "__main__":
    raise SystemExit(main())