from __future__ import annotations

import logging
import time
from typing import Any, Optional

from src.a2.contracts import CrudOperation, FieldLocation
from src.a2.orchestrator import Assignment2Pipeline
from src.config import AppConfig, get_config

from .acid_experiments import AcidExperimentRunner
from .concurrency_manager import ConcurrencyManager
from .contracts import AcidTestResult, LogicalEntity, SessionInfo, TransactionResult
from .logical_reconstructor import LogicalReconstructor
from .query_history import QueryHistoryStore
from .session_manager import SessionManager
from .transaction_coordinator import TransactionCoordinator

logger = logging.getLogger(__name__)


class Assignment3Pipeline:
    """End-to-end Assignment-3 orchestration.

    Layers on top of Assignment2Pipeline:
    1. Transaction coordination (all-or-nothing multi-backend ops)
    2. Concurrency control (per-entity read/write locking)
    3. Logical data reconstruction (unified entity view)
    4. Session management (schema/connection state)
    5. ACID validation experiments
    """

    def __init__(
        self,
        config: Optional[AppConfig] = None,
        a2_pipeline: Optional[Assignment2Pipeline] = None,
    ):
        self.config = config or get_config()
        self.a2 = a2_pipeline or Assignment2Pipeline(self.config)

        self.concurrency_manager = ConcurrencyManager(default_timeout=5.0)
        self.transaction_coordinator = TransactionCoordinator(
            query_planner=self.a2.query_planner,
            crud_engine=self.a2.crud_engine,
            concurrency_manager=self.concurrency_manager,
        )
        self.logical_reconstructor = LogicalReconstructor(
            query_planner=self.a2.query_planner,
            crud_engine=self.a2.crud_engine,
        )
        self.session_manager = SessionManager(
            metadata_catalog=self.a2.metadata_catalog,
            mysql_client=self.a2.a1_pipeline._mysql_client,
            mongo_client=self.a2.a1_pipeline._mongo_client,
        )
        self.acid_runner = AcidExperimentRunner(
            transaction_coordinator=self.transaction_coordinator,
            query_planner=self.a2.query_planner,
            crud_engine=self.a2.crud_engine,
        )
        self.query_history = QueryHistoryStore(
            persistence_dir=self.config.metadata_dir,
            max_entries=500,
        )

    @property
    def _mysql_client(self):
        return self.a2.a1_pipeline._mysql_client

    @property
    def _mongo_client(self):
        return self.a2.a1_pipeline._mongo_client

    def _get_field_locations(self) -> list[FieldLocation]:
        return self.a2._get_field_locations()

    def ensure_connected(self) -> None:
        self.a2._ensure_storage_connected()

    def execute_transactional(
        self,
        operation: CrudOperation,
        payload: dict,
    ) -> TransactionResult:
        self.ensure_connected()
        field_locations = self._get_field_locations()
        return self.transaction_coordinator.execute_in_transaction(
            operation,
            payload,
            field_locations,
            self._mysql_client,
            self._mongo_client,
        )

    def execute_query(self, query_json: dict[str, Any]) -> dict[str, Any]:
        op_str = query_json.get("operation", "read").lower()
        operation = CrudOperation(op_str)
        payload = {k: v for k, v in query_json.items() if k != "operation"}

        t0 = time.monotonic()
        result = self.execute_transactional(operation, payload)
        duration_ms = (time.monotonic() - t0) * 1000

        out = {
            "status": result.status,
            "operation": result.operation,
            "rolled_back": result.rolled_back,
            "data": result.sql_result if operation == CrudOperation.READ else {},
            "sql_result": result.sql_result,
            "mongo_result": result.mongo_result,
            "errors": result.errors,
        }

        # Record in history
        is_success = result.status in ("committed", "success") and not result.errors
        row_count = 0
        if isinstance(result.sql_result, dict):
            rows = result.sql_result.get("rows") or result.sql_result.get("data") or []
            if isinstance(rows, list):
                row_count = len(rows)
        self.query_history.record(
            operation=op_str,
            payload=query_json,
            status="success" if is_success else "error",
            duration_ms=duration_ms,
            result_summary={
                "row_count": row_count,
                "errors": result.errors[:3] if result.errors else [],
            },
        )
        return out

    def preview_query(self, query_json: dict[str, Any]) -> dict[str, Any]:
        op_str = query_json.get("operation", "read").lower()
        operation = CrudOperation(op_str)
        payload = {k: v for k, v in query_json.items() if k != "operation"}

        t0 = time.monotonic()
        plan = self.a2.preview_plan(operation, payload)
        duration_ms = (time.monotonic() - t0) * 1000

        out = {
            "operation": plan.operation.value,
            "requested_fields": plan.requested_fields,
            "sql_queries": plan.sql_queries,
            "mongo_queries": plan.mongo_queries,
            "merge_strategy": plan.merge_strategy,
        }

        self.query_history.record(
            operation=f"preview:{op_str}",
            payload=query_json,
            status="preview",
            duration_ms=duration_ms,
            result_summary={"fields": len(plan.requested_fields)},
        )
        return out

    def get_session_info(self) -> SessionInfo:
        return self.session_manager.get_session_info()

    def list_entities(self) -> list[str]:
        field_locations = self._get_field_locations()
        return self.logical_reconstructor.list_entities(field_locations)

    def get_entity_data(
        self,
        entity_name: str,
        limit: int = 100,
        offset: int = 0,
    ) -> LogicalEntity:
        self.ensure_connected()
        field_locations = self._get_field_locations()
        return self.logical_reconstructor.get_entity_instances(
            entity_name,
            field_locations,
            self._mysql_client,
            self._mongo_client,
            limit=limit,
            offset=offset,
        )

    def get_all_data(self, limit: int = 100) -> list[dict[str, Any]]:
        self.ensure_connected()
        field_locations = self._get_field_locations()
        return self.logical_reconstructor.get_all_data(
            field_locations,
            self._mysql_client,
            self._mongo_client,
            limit=limit,
        )

    def get_stats(self) -> dict[str, int]:
        self.ensure_connected()
        field_locations = self._get_field_locations()
        return self.logical_reconstructor.get_table_stats(
            field_locations,
            self._mysql_client,
            self._mongo_client,
        )

    def run_acid_experiments(self) -> list[AcidTestResult]:
        self.ensure_connected()
        field_locations = self._get_field_locations()
        return self.acid_runner.run_all(
            field_locations,
            self._mysql_client,
            self._mongo_client,
        )

    def run_acid_experiment(self, property_name: str) -> AcidTestResult:
        self.ensure_connected()
        field_locations = self._get_field_locations()
        dispatch = {
            "atomicity": self.acid_runner.test_atomicity,
            "consistency": self.acid_runner.test_consistency,
            "isolation": self.acid_runner.test_isolation,
            "durability": self.acid_runner.test_durability,
            "reconstruction": self.acid_runner.test_reconstruction,
        }
        handler = dispatch.get(property_name.lower())
        if handler is None:
            return AcidTestResult(
                property_name=property_name,
                passed=False,
                description=f"Unknown ACID property: {property_name}",
            )
        return handler(field_locations, self._mysql_client, self._mongo_client)

    def close(self) -> None:
        self.a2.close()
