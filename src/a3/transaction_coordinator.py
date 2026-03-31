from __future__ import annotations

import logging
from copy import deepcopy
from typing import Any

from src.a2.contracts import CrudOperation, FieldLocation, QueryPlan
from src.a2.crud_engine import CrudEngine
from src.a2.query_planner import QueryPlanner

from .contracts import TransactionResult

logger = logging.getLogger(__name__)


class TransactionCoordinator:
    """Wraps multi-backend CRUD operations in logical transactions.

    Strategy:
    - MySQL: suppress auto-commits during execution, then COMMIT or ROLLBACK
      as a single atomic unit.
    - MongoDB (standalone): use compensating transactions — snapshot affected
      documents before mutation and restore them on failure.
    """

    def __init__(self, query_planner: QueryPlanner, crud_engine: CrudEngine):
        self.query_planner = query_planner
        self.crud_engine = crud_engine

    def execute_in_transaction(
        self,
        operation: CrudOperation,
        payload: dict,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
    ) -> TransactionResult:
        if operation == CrudOperation.READ:
            return self._execute_read(payload, field_locations, mysql_client, mongo_client)

        plan = self.query_planner.build_plan(operation, payload, field_locations)
        sql_plan = self._sql_only_plan(plan)
        mongo_plan = self._mongo_only_plan(plan)

        conn = getattr(mysql_client, "connection", None)
        if conn is None:
            return TransactionResult(
                status="error",
                operation=operation.value,
                errors=["MySQL connection not available"],
            )

        original_commit = conn.commit
        original_rollback = conn.rollback
        conn.commit = lambda: None
        conn.rollback = lambda: None

        mongo_snapshot: dict[str, Any] = {}

        try:
            sql_result = self.crud_engine.execute(sql_plan, mysql_client, mongo_client)

            if sql_result.get("status") == "error":
                original_rollback()
                return TransactionResult(
                    status="rolled_back",
                    operation=operation.value,
                    sql_result=sql_result,
                    rolled_back=True,
                    errors=sql_result.get("errors", []),
                )

            mongo_snapshot = self._snapshot_mongo(mongo_client, mongo_plan, operation)
            mongo_result = self.crud_engine.execute(mongo_plan, mysql_client, mongo_client)

            if mongo_result.get("status") == "error":
                original_rollback()
                self._compensate_mongo(mongo_client, mongo_snapshot)
                all_errors = sql_result.get("errors", []) + mongo_result.get("errors", [])
                return TransactionResult(
                    status="rolled_back",
                    operation=operation.value,
                    sql_result=sql_result,
                    mongo_result=mongo_result,
                    rolled_back=True,
                    errors=all_errors,
                )

            original_commit()

            merged_errors = sql_result.get("errors", []) + mongo_result.get("errors", [])
            return TransactionResult(
                status="committed",
                operation=operation.value,
                sql_result=sql_result,
                mongo_result=mongo_result,
                errors=merged_errors,
            )

        except Exception as exc:
            try:
                original_rollback()
            except Exception:
                pass
            self._compensate_mongo(mongo_client, mongo_snapshot)
            return TransactionResult(
                status="rolled_back",
                operation=operation.value,
                rolled_back=True,
                errors=[str(exc)],
            )

        finally:
            conn.commit = original_commit
            conn.rollback = original_rollback

    def _execute_read(
        self,
        payload: dict,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
    ) -> TransactionResult:
        plan = self.query_planner.build_plan(CrudOperation.READ, payload, field_locations)
        result = self.crud_engine.execute(plan, mysql_client, mongo_client)
        return TransactionResult(
            status="committed",
            operation="read",
            sql_result=result,
            errors=result.get("errors", []),
        )

    @staticmethod
    def _sql_only_plan(plan: QueryPlan) -> QueryPlan:
        return QueryPlan(
            operation=plan.operation,
            requested_fields=plan.requested_fields,
            sql_queries=plan.sql_queries,
            mongo_queries=[],
            merge_strategy=plan.merge_strategy,
        )

    @staticmethod
    def _mongo_only_plan(plan: QueryPlan) -> QueryPlan:
        return QueryPlan(
            operation=plan.operation,
            requested_fields=plan.requested_fields,
            sql_queries=[],
            mongo_queries=plan.mongo_queries,
            merge_strategy=plan.merge_strategy,
        )

    @staticmethod
    def _get_mongo_db(mongo_client):
        if hasattr(mongo_client, "client") and mongo_client.client is not None:
            db_name = getattr(mongo_client, "database", None)
            if db_name:
                return mongo_client.client[db_name]
        return None

    @staticmethod
    def _snapshot_mongo(
        mongo_client,
        mongo_plan: QueryPlan,
        operation: CrudOperation,
    ) -> dict[str, Any]:
        snapshot: dict[str, Any] = {}
        mongo_db = TransactionCoordinator._get_mongo_db(mongo_client)
        if mongo_db is None:
            return snapshot

        for query in mongo_plan.mongo_queries:
            collection_name = query.get("collection", "")
            if not collection_name or collection_name == "*":
                continue

            query_type = (query.get("type") or "").lower()
            coll = mongo_db[collection_name]

            if query_type in ("update_many", "update_one"):
                filter_doc = query.get("filter") or {}
                docs = list(coll.find(filter_doc))
                snapshot.setdefault(collection_name, {}).setdefault("pre_update", []).extend(
                    deepcopy(docs)
                )

            elif query_type in ("delete_many", "delete_one"):
                filter_doc = query.get("filter") or {}
                docs = list(coll.find(filter_doc))
                snapshot.setdefault(collection_name, {}).setdefault("pre_delete", []).extend(
                    deepcopy(docs)
                )

            elif query_type == "insert_batch":
                insert_docs = query.get("documents") or []
                snapshot.setdefault(collection_name, {}).setdefault("insert_docs", []).extend(
                    deepcopy(insert_docs)
                )

        return snapshot

    @staticmethod
    def _compensate_mongo(
        mongo_client,
        snapshot: dict[str, Any],
    ) -> None:
        if not snapshot:
            return

        mongo_db = TransactionCoordinator._get_mongo_db(mongo_client)
        if mongo_db is None:
            return

        for collection_name, data in snapshot.items():
            coll = mongo_db[collection_name]

            if "insert_docs" in data:
                for doc in data["insert_docs"]:
                    filter_doc = {k: v for k, v in doc.items() if k != "_id"}
                    if filter_doc:
                        coll.delete_one(filter_doc)

            if "pre_update" in data:
                for doc in data["pre_update"]:
                    doc_id = doc.get("_id")
                    if doc_id:
                        restored = {k: v for k, v in doc.items() if k != "_id"}
                        coll.replace_one({"_id": doc_id}, restored)

            if "pre_delete" in data:
                for doc in data["pre_delete"]:
                    restore_doc = {k: v for k, v in doc.items() if k != "_id"}
                    if restore_doc:
                        coll.insert_one(restore_doc)
