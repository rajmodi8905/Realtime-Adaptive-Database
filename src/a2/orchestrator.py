from typing import Optional

from src.ingest_and_classify import IngestAndClassify
from src.config import AppConfig

from .contracts import ClassifiedField, CrudOperation, QueryPlan, SchemaRegistration
from .crud_engine import CrudEngine
from .metadata_catalog import MetadataCatalog
from .mongo_decomposition_engine import MongoDecompositionEngine
from .query_planner import QueryPlanner
from .schema_registry import SchemaRegistry
from .sql_normalization_engine import SqlNormalizationEngine
from .storage_strategy_generator import StorageStrategyGenerator


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
        # A1 pipeline — handles ingestion, analysis, classification
        self.a1_pipeline = a1_pipeline or IngestAndClassify(config)

        # A2 components — handle normalization, decomposition, CRUD
        self.schema_registry = SchemaRegistry()
        self.metadata_catalog = MetadataCatalog()
        self.sql_engine = SqlNormalizationEngine()
        self.mongo_engine = MongoDecompositionEngine()
        self.strategy_generator = StorageStrategyGenerator()
        self.query_planner = QueryPlanner()
        self.crud_engine = CrudEngine()

        # Classification bridge — populated after ingestion
        self._classified_fields: list[ClassifiedField] = []

    # ------------------------------------------------------------------
    # Phase 1: Schema Registration
    # ------------------------------------------------------------------
    def register_schema(self, registration: SchemaRegistration) -> dict:
        """Register schema and persist in metadata catalog."""
        raise NotImplementedError("Implement schema registration orchestration")

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

        The classified_fields carry nesting info from A1 so the engines
        can detect repeating groups (SQL) and embedding vs referencing
        (MongoDB) without re-running classification.
        """
        raise NotImplementedError("Implement strategy generation orchestration")

    # ------------------------------------------------------------------
    # Phase 6: Metadata-Driven CRUD
    # ------------------------------------------------------------------
    def execute_operation(self, operation: CrudOperation, payload: dict) -> dict:
        """Generate and execute metadata-driven CRUD plan."""
        raise NotImplementedError("Implement operation planning + execution")

    def preview_plan(self, operation: CrudOperation, payload: dict) -> QueryPlan:
        """Return generated plan without execution, useful for debugging/reporting."""
        raise NotImplementedError("Implement query plan preview")

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------
    def close(self) -> None:
        """Close underlying A1 pipeline connections."""
        self.a1_pipeline.close()
