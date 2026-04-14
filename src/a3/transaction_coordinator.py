from __future__ import annotations

import logging
import time
from copy import deepcopy
from typing import Any, Optional

from src.a2.contracts import CrudOperation, FieldLocation, QueryPlan
from src.a2.crud_engine import CrudEngine
from src.a2.query_planner import QueryPlanner

from .concurrency_manager import ConcurrencyManager
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

    def __init__(
        self,
        query_planner: QueryPlanner,
        crud_engine: CrudEngine,
        concurrency_manager: Optional[ConcurrencyManager] = None,
    ):
        self.query_planner = query_planner
        self.crud_engine = crud_engine
        self.concurrency_manager = concurrency_manager or ConcurrencyManager()

    def execute_in_transaction(
        self,
        operation: CrudOperation,
        payload: dict,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
        lock_timeout_seconds: Optional[float] = None,
    ) -> TransactionResult:
        # --- concurrency: determine lock scope ---
        lock_key = ConcurrencyManager.extract_lock_key(operation.value, payload)
        is_write = operation != CrudOperation.READ

        self.concurrency_manager.acquire(
            lock_key,
            exclusive=is_write,
            timeout=lock_timeout_seconds,
        )
        try:
            return self._execute_locked(
                operation, payload, field_locations,
                mysql_client, mongo_client, lock_key,
            )
        finally:
            self.concurrency_manager.release(lock_key, exclusive=is_write)

    def _execute_locked(
        self,
        operation: CrudOperation,
        payload: dict,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
        lock_key: str,
    ) -> TransactionResult:
        if operation == CrudOperation.READ:
            return self._execute_read(payload, field_locations, mysql_client, mongo_client, lock_key)

        t_plan_start = time.perf_counter()
        plan = self.query_planner.build_plan(operation, payload, field_locations)
        t_plan = (time.perf_counter() - t_plan_start) * 1000
        
        sql_plan = self._sql_only_plan(plan)
        mongo_plan = self._mongo_only_plan(plan)

        conn = getattr(mysql_client, "connection", None)
        if conn is None:
            return TransactionResult(
                status="error",
                operation=operation.value,
                errors=["MySQL connection not available"],
                lock_key=lock_key,
            )

        original_commit = conn.commit
        original_rollback = conn.rollback
        conn.commit = lambda: None
        conn.rollback = lambda: None

        mongo_snapshot: dict[str, Any] = {}

        try:
            t_sql_start = time.perf_counter()
            sql_result = self.crud_engine.execute(sql_plan, mysql_client, mongo_client)
            t_sql = (time.perf_counter() - t_sql_start) * 1000

            if not self._is_write_result_success(sql_result):
                original_rollback()
                return TransactionResult(
                    status="rolled_back",
                    operation=operation.value,
                    sql_result=sql_result,
                    rolled_back=True,
                    errors=sql_result.get("errors", []),
                    lock_key=lock_key,
                    timings={"query_plan_ms": round(t_plan, 2), "sql_ms": round(t_sql, 2)}
                )

            mongo_snapshot = self._snapshot_mongo(mongo_client, mongo_plan, operation)
            t_mongo_start = time.perf_counter()
            mongo_result = self.crud_engine.execute(mongo_plan, mysql_client, mongo_client)
            t_mongo = (time.perf_counter() - t_mongo_start) * 1000

            if not self._is_write_result_success(mongo_result):
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
                    lock_key=lock_key,
                    timings={"query_plan_ms": round(t_plan, 2), "sql_ms": round(t_sql, 2), "mongo_ms": round(t_mongo, 2)}
                )

            original_commit()

            merged_errors = sql_result.get("errors", []) + mongo_result.get("errors", [])
            return TransactionResult(
                status="committed",
                operation=operation.value,
                sql_result=sql_result,
                mongo_result=mongo_result,
                errors=merged_errors,
                lock_key=lock_key,
                timings={"query_plan_ms": round(t_plan, 2), "sql_ms": round(t_sql, 2), "mongo_ms": round(t_mongo, 2)}
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
                lock_key=lock_key,
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
        lock_key: str = "",
    ) -> TransactionResult:
        t_plan_start = time.perf_counter()
        plan = self.query_planner.build_plan(CrudOperation.READ, payload, field_locations)
        t_plan = (time.perf_counter() - t_plan_start) * 1000
        
        result = self.crud_engine.execute(plan, mysql_client, mongo_client)
        tx_status = "committed" if result.get("status") != "error" else "error"
        engine_timings = result.get("timings", {})
        
        return TransactionResult(
            status=tx_status,
            operation="read",
            sql_result=result,
            errors=result.get("errors", []),
            lock_key=lock_key,
            timings={
                "query_plan_ms": round(t_plan, 4),
                "sql_ms": engine_timings.get("sql_ms", 0),
                "mongo_ms": engine_timings.get("mongo_ms", 0),
                "merge_ms": engine_timings.get("merge_ms", 0),
            }
        )

    @staticmethod
    def _is_write_result_success(result: dict[str, Any]) -> bool:
        status = str((result or {}).get("status", "")).lower()
        errors = list((result or {}).get("errors", []))
        return status == "success" and not errors

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
