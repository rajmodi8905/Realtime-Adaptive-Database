from __future__ import annotations

import logging
import random
import string
import uuid
from datetime import datetime, timezone
from typing import Any, Optional

from src.ingest_and_classify import IngestAndClassify
from src.config import AppConfig, get_config

from .contracts import ClassifiedField, CrudOperation, FieldLocation, QueryPlan, SchemaRegistration
from .crud_engine import CrudEngine
from .metadata_catalog import MetadataCatalog
from .mongo_decomposition_engine import MongoDecompositionEngine
from .query_planner import QueryPlanner
from .schema_registry import SchemaRegistry
from .sql_normalization_engine import SqlNormalizationEngine
from .storage_strategy_generator import StorageStrategyGenerator

logger = logging.getLogger(__name__)


class Assignment2Pipeline:
    """End-to-end Assignment-2 orchestration.

    Pipeline phases represented by this class:
    1. Schema registration
    2. Data ingestion via A1 pipeline (IngestAndClassify)
    3. Metadata interpretation (A1 classification → ClassifiedField bridge)
    4. Classification (reuses A1 — no duplication)
    5. Storage strategy generation (SQL normalization + Mongo decomposition)
    6. Metadata-driven CRUD plan generation and execution

    The A1 pipeline handles Phases 2-4 (ingestion, analysis, classification).
    This orchestrator consumes A1's output and drives Phases 1, 5, and 6.
    """

    def __init__(self, config: Optional[AppConfig] = None, a1_pipeline: Optional[IngestAndClassify] = None):
        """Initialize the A2 pipeline.

        Args:
            config: Optional AppConfig. If not provided, loads from environment.
            a1_pipeline: Optional pre-created IngestAndClassify instance.
                         If not provided, one is created internally.
        """
        # Store config for later use by CRUD engine, query planner, etc.
        self.config = config or get_config()
        
        # A1 pipeline — handles ingestion, analysis, classification
        self.a1_pipeline = a1_pipeline or IngestAndClassify(self.config)

        # A2 components — handle normalization, decomposition, CRUD
        self.schema_registry = SchemaRegistry()
        self.metadata_catalog = MetadataCatalog(self.config.metadata_dir)
        self.sql_engine = SqlNormalizationEngine()
        self.mongo_engine = MongoDecompositionEngine()
        self.strategy_generator = StorageStrategyGenerator()
        self.query_planner = QueryPlanner()
        self.crud_engine = CrudEngine()

        # Classification bridge — populated after ingestion
        self._classified_fields: list[ClassifiedField] = []
        # Registration — populated after register_schema
        self._registration: Optional[SchemaRegistration] = None
        # Cached field locations — populated after build_storage_strategy
        self._field_locations: list[FieldLocation] = []

    # ------------------------------------------------------------------
    # Phase 1: Schema Registration
    # ------------------------------------------------------------------
    def register_schema(self, registration: SchemaRegistration) -> dict:
        """Register schema and persist in metadata catalog."""
        self._registration = registration
        self.metadata_catalog.save_schema(registration)
        logger.info(
            "Schema '%s' v%s registered (root_entity=%s)",
            registration.schema_name,
            registration.version,
            registration.root_entity,
        )
        return {
            "status": "registered",
            "schema_name": registration.schema_name,
            "version": registration.version,
            "root_entity": registration.root_entity,
        }

    # ------------------------------------------------------------------
    # Phase 2-4: Ingestion + Classification (delegated to A1)
    # ------------------------------------------------------------------
    def run_ingestion(self, records: list[dict]) -> dict:
        """Ingest records through A1 and bridge classification into A2.

        Flow:
        1. Feed records into A1 pipeline (normalization + buffering)
        2. Flush A1 pipeline (analysis + classification + routing)
        3. Convert A1 decisions+stats into ClassifiedField list (bridge)

        Returns:
            A1 flush result dict with ingestion stats.
        """
        # Step 1-2: Ingest and flush through A1 (does NOT re-classify)
        self.a1_pipeline.ingest_batch(records)
        flush_result = self.a1_pipeline.flush()

        # Step 3: Bridge A1 output → A2 ClassifiedField list
        self._classified_fields = self._bridge_a1_classification()

        return flush_result

    def _bridge_a1_classification(self) -> list[ClassifiedField]:
        """Convert A1's PlacementDecisions + FieldStats into ClassifiedField list.

        This is a one-way read from A1 state — no classification logic is
        duplicated.  We simply translate A1's existing results into A2's
        contract type so the normalization and decomposition engines can
        consume them.
        """
        decisions = self.a1_pipeline.get_decisions()
        field_stats = self.a1_pipeline.get_field_stats()

        classified: list[ClassifiedField] = []
        for field_name, decision in decisions.items():
            stats = field_stats.get(field_name)
            classified.append(ClassifiedField.from_a1_decision(decision, stats))

        return classified

    def get_classified_fields(self) -> list[ClassifiedField]:
        """Return the current ClassifiedField list from the last ingestion."""
        return list(self._classified_fields)

    # ------------------------------------------------------------------
    # Phase 5: Storage Strategy Generation
    # ------------------------------------------------------------------
    def build_storage_strategy(self, registration: SchemaRegistration) -> dict:
        """Generate SQL/Mongo strategy using A1 classification output.

        End-to-end flow:
        1. Generate SQL table plans + relationships from classified fields.
        2. Execute SQL DDL (CREATE TABLE) on MySQL.
        3. Generate Mongo collection plans from classified fields.
        4. Execute Mongo collection setup.
        5. Generate field-level storage locations.
        6. Persist all plans + locations via metadata catalog.

        Args:
            registration: The schema registration to use.

        Returns:
            Summary dict with counts and any errors.
        """
        self._registration = registration
        classified = self._classified_fields
        if not classified:
            return {"status": "error", "message": "No classified fields. Run run_ingestion() first."}

        errors: list[str] = []

        # ── SQL normalization ──────────────────────────────────────────
        sql_tables = self.sql_engine.generate_table_plans(registration, classified)
        sql_relationships = self.sql_engine.generate_relationships(sql_tables)
        logger.info(
            "SQL plan: %d tables, %d relationships",
            len(sql_tables), len(sql_relationships),
        )

        sql_exec_result = self.sql_engine.execute_table_plans(
            sql_tables, sql_relationships, self.a1_pipeline._mysql_client,
        )
        if sql_exec_result.get("errors"):
            errors.extend(sql_exec_result["errors"])

        # ── MongoDB decomposition ──────────────────────────────────────
        sql_root_pk = next(
            (t.primary_key for t in sql_tables if t.table_name == registration.root_entity),
            None,
        )
        mongo_collections = self.mongo_engine.generate_collection_plans(
            registration, classified, sql_root_pk,
        )
        logger.info("Mongo plan: %d collections", len(mongo_collections))

        mongo_exec_result = self.mongo_engine.execute_collection_plans(
            mongo_collections, self.a1_pipeline._mongo_client,
        )
        if mongo_exec_result.get("errors"):
            errors.extend(mongo_exec_result["errors"])

        # ── Field locations ────────────────────────────────────────────
        field_locations = self.strategy_generator.generate_field_locations(
            registration, sql_tables, sql_relationships, mongo_collections,
        )
        self._field_locations = field_locations
        logger.info("Field locations: %d mappings", len(field_locations))

        # ── Persist everything ─────────────────────────────────────────
        self.metadata_catalog.save_schema(registration)
        self.metadata_catalog.save_sql_plan(sql_tables, sql_relationships)
        self.metadata_catalog.save_mongo_plan(mongo_collections)
        self.metadata_catalog.save_field_locations(field_locations)

        status = "success" if not errors else "partial_success"
        return {
            "status": status,
            "sql_tables": len(sql_tables),
            "sql_relationships": len(sql_relationships),
            "mongo_collections": len(mongo_collections),
            "field_locations": len(field_locations),
            "errors": errors,
        }

    # ------------------------------------------------------------------
    # Phase 6: Metadata-Driven CRUD
    # ------------------------------------------------------------------
    def execute_operation(self, operation: CrudOperation, payload: dict) -> dict:
        """Generate and execute metadata-driven CRUD plan.

        Args:
            operation: The CRUD operation (READ, CREATE, UPDATE, DELETE).
            payload: Operation-specific payload (filters, records, updates, etc.)

        Returns:
            Unified result dict from CrudEngine.
        """
        self._ensure_storage_connected()
        field_locations = self._get_field_locations()
        plan = self.query_planner.build_plan(operation, payload, field_locations)
        return self.crud_engine.execute(
            plan,
            mysql_client=self.a1_pipeline._mysql_client,
            mongo_client=self.a1_pipeline._mongo_client,
        )

    def preview_plan(self, operation: CrudOperation, payload: dict) -> QueryPlan:
        """Return generated plan without execution, useful for debugging/reporting."""
        field_locations = self._get_field_locations()
        return self.query_planner.build_plan(operation, payload, field_locations)

    def _get_field_locations(self) -> list[FieldLocation]:
        """Load field locations from cache or metadata catalog."""
        if self._field_locations:
            return self._field_locations
        self._field_locations = self.metadata_catalog.get_field_locations()
        return self._field_locations

    def _ensure_storage_connected(self) -> None:
        """Connect underlying SQL/Mongo clients if not already connected."""
        mysql = self.a1_pipeline._mysql_client
        if getattr(mysql, "connection", None) is None:
            mysql.connect()

        mongo = self.a1_pipeline._mongo_client
        if getattr(mongo, "client", None) is None:
            mongo.connect()

    # ------------------------------------------------------------------
    # Record Generator
    # ------------------------------------------------------------------
    def generate_records(
        self,
        n: int,
        registration: Optional[SchemaRegistration] = None,
    ) -> list[dict]:
        """Generate *n* synthetic records conforming to the registered schema.

        Walks the ``json_schema.properties`` to produce records whose
        structure matches the schema exactly.  Values are randomised but
        type-correct so the records survive classification and CRUD
        round-trips.

        Args:
            n: Number of records to generate.
            registration: Schema to use.  Falls back to the previously
                          registered schema if omitted.

        Returns:
            A list of *n* dicts ready for ``run_ingestion()``.
        """
        reg = registration or self._registration
        if reg is None or not reg.json_schema:
            raise ValueError(
                "No schema available. Call register_schema() first or pass a registration."
            )
        schema = reg.json_schema
        records: list[dict] = []
        for i in range(n):
            record = self._generate_from_schema(schema, record_index=i)
            records.append(record)
        return records

    # ── recursive schema walker ────────────────────────────────────────

    @staticmethod
    def _generate_from_schema(
        schema: dict[str, Any],
        record_index: int = 0,
        path: str = "",
    ) -> Any:
        """Recursively generate a value conforming to *schema*."""
        schema_type = schema.get("type", "string")

        if schema_type == "object":
            obj: dict[str, Any] = {}
            for prop_name, prop_schema in (schema.get("properties") or {}).items():
                child_path = f"{path}.{prop_name}" if path else prop_name
                obj[prop_name] = Assignment2Pipeline._generate_from_schema(
                    prop_schema, record_index, child_path,
                )
            return obj

        if schema_type == "array":
            items_schema = schema.get("items", {"type": "string"})
            count = random.randint(1, 3)
            return [
                Assignment2Pipeline._generate_from_schema(
                    items_schema, record_index, f"{path}[{j}]",
                )
                for j in range(count)
            ]

        # ── scalar types ───────────────────────────────────────────────
        return Assignment2Pipeline._generate_scalar(schema_type, schema, record_index, path)

    @staticmethod
    def _generate_scalar(
        schema_type: str,
        schema: dict[str, Any],
        record_index: int,
        path: str,
    ) -> Any:
        """Generate a single scalar value."""
        leaf = path.rsplit(".", 1)[-1].rstrip("]").split("[")[0] if path else "val"

        # IDs → unique per record
        if leaf.endswith("_id") or leaf in ("id", "username"):
            return f"{leaf}_{record_index + 1}"

        if schema_type == "integer":
            return random.randint(1, 100)

        if schema_type == "number":
            return round(random.uniform(0.1, 100.0), 2)

        if schema_type == "boolean":
            return random.choice([True, False])

        # string with date-time format → ISO timestamp
        fmt = schema.get("format", "")
        if fmt == "date-time" or "time" in leaf.lower() or "date" in leaf.lower():
            base = datetime(2026, 1, 1, tzinfo=timezone.utc)
            offset_secs = record_index * 60 + random.randint(0, 59)
            ts = datetime.fromtimestamp(base.timestamp() + offset_secs, tz=timezone.utc)
            return ts.strftime("%Y-%m-%dT%H:%M:%SZ")

        # generic string
        suffix = "".join(random.choices(string.ascii_lowercase, k=4))
        return f"{leaf}_{suffix}_{record_index + 1}"

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self) -> None:
        """Close underlying A1 pipeline connections."""
        self.a1_pipeline.close()