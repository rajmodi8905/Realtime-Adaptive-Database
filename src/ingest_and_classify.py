# ==============================================
# IngestAndClassify — Final Orchestrator
# ==============================================
#
# PURPOSE:
#   This is the MAIN CLASS that ties all 4 topics together into
#   a single pipeline. Users interact with this class only.
#   Everything else is internal.
#
# HOW IT CONNECTS THE 4 TOPICS:
#
#   ┌──────────────────────────────────────────────────────────┐
#   │                   IngestAndClassify                      │
#   │                                                          │
#   │  ┌──────────────────────────────────────────────┐        │
#   │  │ TOPIC 1: NORMALIZATION                       │        │
#   │  │  TypeDetector → RecordNormalizer             │        │
#   │  └──────────────┬───────────────────────────────┘        │
#   │                 │ normalized records                     │
#   │                 ▼                                        │
#   │            [ BUFFER ]  (in-memory list)                  │
#   │                 │ on flush                               │
#   │                 ▼                                        │
#   │  ┌──────────────────────────────────────────────┐        │
#   │  │ TOPIC 2: ANALYSIS & CLASSIFICATION           │        │
#   │  │  FieldAnalyzer → Classifier →                │        │
#   │  │  dict[str, PlacementDecision]                │        │
#   │  └──────────────┬───────────────────────────────┘        │
#   │                 │ decisions                              │
#   │                 ▼                                        │
#   │  ┌──────────────────────────────────────────────┐        │
#   │  │ TOPIC 3: STORAGE                             │        │
#   │  │  RecordRouter → MySQLClient + MongoClient    │        │
#   │  └──────────────┬───────────────────────────────┘        │
#   │                 │                                        │
#   │                 ▼                                        │
#   │  ┌──────────────────────────────────────────────┐        │
#   │  │ TOPIC 4: PERSISTENCE                         │        │
#   │  │  MetadataStore.save_all(...)                  │        │
#   │  └──────────────────────────────────────────────┘        │
#   └──────────────────────────────────────────────────────────┘
#
#
# CLASS: IngestAndClassify
# -------------------------
#
#   Constructor:
#   ------------
#   - __init__(config: AppConfig | None = None)
#       1. Load config (from .env or passed in)
#       2. Initialize Topic 1: TypeDetector, RecordNormalizer
#       3. Initialize Topic 2: FieldAnalyzer, Classifier
#       4. Initialize Topic 3: MySQLClient, MongoClient, RecordRouter
#       5. Initialize Topic 4: MetadataStore
#       6. Load existing metadata if this is a restart
#       7. Initialize buffer (empty list)
#
#   Public Methods (User-facing API):
#   ---------------------------------
#   - ingest(raw_record: dict) -> None
#       Process one raw JSON record:
#         1. Normalize it (Topic 1)
#         2. Add to buffer
#         3. If buffer full → flush()
#
#   - ingest_batch(raw_records: list[dict]) -> None
#       Process multiple records.
#
#   - flush() -> dict
#       Manually trigger pipeline:
#         1. Analyze buffered records (Topic 2 - FieldAnalyzer)
#         2. Classify fields (Topic 2 - Classifier)
#         3. Route records to backends (Topic 3 - RecordRouter)
#         4. Save metadata (Topic 4 - MetadataStore)
#         5. Clear buffer
#         6. Return result stats
#
#   - get_decisions() -> dict[str, PlacementDecision]
#       Return current classification decisions.
#
#   - get_field_stats() -> dict[str, FieldStats]
#       Return current field statistics.
#
#   - get_status() -> dict
#       Return pipeline status (records processed, buffer size, etc.)
#
#   - get_classification_summary() -> dict
#       Return summary: which fields go where.
#
#   Internal Methods:
#   -----------------
#   - _should_flush() -> bool
#       Check if buffer has reached size or time threshold.
#
#   - _load_previous_state() -> None
#       On startup, check if MetadataStore has data from a previous run.
#       If yes, load decisions, stats, mappings into memory.
#
#   Attributes:
#   -----------
#   - _record_normalizer: RecordNormalizer      (Topic 1)
#   - _field_analyzer: FieldAnalyzer            (Topic 2)
#   - _classifier: Classifier                   (Topic 2)
#   - _record_router: RecordRouter              (Topic 3)
#   - _metadata_store: MetadataStore            (Topic 4)
#   - _buffer: list[dict]                       (in-memory staging)
#   - _buffer_size: int                         (max records before flush)
#   - _total_records: int                       (lifetime count)
#   - _decisions: dict[str, PlacementDecision]  (current decisions)
#
# ==============================================

import time
import json
import os
from datetime import datetime
from pathlib import Path
from typing import Optional

from src.config import AppConfig, get_config
from src.normalization.type_detector import TypeDetector
from src.normalization.record_normalizer import RecordNormalizer
from src.analysis.field_analyzer import FieldAnalyzer
from src.analysis.classifier import Classifier
from src.analysis.decision import PlacementDecision, ClassificationThresholds, Backend
from src.storage.mysql_client import MySQLClient
from src.storage.mongo_client import MongoClient
from src.storage.record_router import RecordRouter
from src.storage.migrator import Migrator
from src.persistence.metadata_store import MetadataStore


class IngestAndClassify:
    """
    Main pipeline orchestrator that integrates all 4 topics:
    1. Normalization
    2. Analysis & Classification
    3. Storage
    4. Persistence
    """

    def __init__(self, config: Optional[AppConfig] = None):
        """
        Initialize the complete pipeline with all components.
        
        Args:
            config: Application configuration. If None, loads from environment.
        
        USES:
            - Topic 1 (normalization/): TypeDetector, RecordNormalizer
            - Topic 2 (analysis/): FieldAnalyzer, Classifier, ClassificationThresholds
            - Topic 3 (storage/): MySQLClient, MongoClient, RecordRouter
            - Topic 4 (persistence/): MetadataStore
        """
        # Load configuration
        self._config = config or get_config()
        
        # TOPIC 1: Normalization
        self._type_detector = TypeDetector()
        self._record_normalizer = RecordNormalizer(
            self._type_detector
        )
        
        # TOPIC 2: Analysis & Classification
        self._field_analyzer = FieldAnalyzer(self._type_detector)
        thresholds = ClassificationThresholds()
        self._classifier = Classifier(thresholds)
        
        # TOPIC 3: Storage
        self._mysql_client = MySQLClient(
            host=self._config.mysql.host,
            port=self._config.mysql.port,
            user=self._config.mysql.user,
            password=self._config.mysql.password,
            database=self._config.mysql.database
        )
        self._mongo_client = MongoClient(
            host=self._config.mongo.host,
            port=self._config.mongo.port,
            database=self._config.mongo.database,
            user=self._config.mongo.user,
            password=self._config.mongo.password
        )
        self._record_router = RecordRouter(
            self._mysql_client,
            self._mongo_client
        )
        self._migrator = Migrator()
        
        # TOPIC 4: Persistence
        self._metadata_store = MetadataStore(self._config.metadata_dir)
        
        # Internal state
        self._buffer: list[dict] = []
        self._buffer_size = self._config.buffer.buffer_size
        self._buffer_timeout = self._config.buffer.buffer_timeout_seconds
        self._last_flush_time = time.time()
        self._total_records = 0
        self._decisions: dict[str, PlacementDecision] = {}
        
        # Write-ahead log for crash recovery
        self._wal_path = Path(self._config.buffer.wal_file)
        self._wal_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Load previous state if exists
        self._load_previous_state()
        
        # Recover any pending records from WAL (crash recovery)
        self._recover_from_wal()
        
        print(f"✓ Pipeline initialized (buffer size: {self._buffer_size}, timeout: {self._buffer_timeout}s)")
        if self._decisions:
            print(f"✓ Loaded {len(self._decisions)} previous decisions from metadata")

    def ingest(self, raw_record: dict) -> None:
        """
        Process a single raw JSON record.
        
        Args:
            raw_record: Raw JSON record from the data stream.
        
        USES:
            - Topic 1 (normalization/): RecordNormalizer.normalize()
        """
        # TOPIC 1: Normalize the record
        normalized = self._record_normalizer.normalize(raw_record)
        
        # Write to WAL for crash recovery (before adding to buffer)
        self._append_to_wal(normalized)
        
        # Add to buffer
        self._buffer.append(normalized)
        
        # Check if we should flush
        if self._should_flush():
            self.flush()

    def ingest_batch(self, raw_records: list[dict]) -> None:
        """
        Process multiple raw JSON records.
        
        Args:
            raw_records: List of raw JSON records.
        
        USES:
            - Topic 1 (normalization/): RecordNormalizer.normalize_batch()
        """
        # TOPIC 1: Normalize all records
        normalized_batch = self._record_normalizer.normalize_batch(raw_records)
        
        # Write to WAL for crash recovery (before adding to buffer)
        for record in normalized_batch:
            self._append_to_wal(record)
        
        # Add to buffer
        self._buffer.extend(normalized_batch)
        
        # Check if we should flush
        if self._should_flush():
            self.flush()

    def flush(self) -> dict:
        """
        Manually trigger the pipeline flush:
        1. Analyze buffered records
        2. Classify fields
        3. Route to backends
        4. Persist metadata
        5. Clear buffer
        
        Returns:
            Dictionary with flush results and statistics.
        
        USES:
            - Topic 2 (analysis/): FieldAnalyzer.analyze_batch(), Classifier.classify_all()
            - Topic 3 (storage/): RecordRouter.route_batch()
            - Topic 4 (persistence/): MetadataStore.save_all()
        """
        if not self._buffer:
            return {
                "status": "nothing_to_flush",
                "buffer_size": 0,
                "timestamp": datetime.utcnow().isoformat()
            }
        
        start_time = time.time()
        buffer_size = len(self._buffer)
        
        try:
            # Save old decisions to detect backend changes
            old_decisions = self._decisions.copy()
            
            # TOPIC 2: Analyze the buffered records
            self._field_analyzer.analyze_batch(self._buffer)
            field_stats = self._field_analyzer.get_stats()
            
            # TOPIC 2: Classify fields based on analysis
            total_records = self._field_analyzer.total_records
            new_decisions = self._classifier.classify_all(
                field_stats,
                total_records
            )
            
            # TOPIC 3: Connect to databases and route records
            self._mysql_client.connect()
            self._mongo_client.connect()
            
            # Detect and handle backend changes (migrate existing data)
            migrations = self._handle_backend_changes(old_decisions, new_decisions)
            
            # Update decisions after migration
            self._decisions = new_decisions
            
            route_result = self._record_router.route_batch(
                self._buffer,
                self._decisions,
                table_name="records",
                collection_name="records"
            )
            
            # TOPIC 4: Persist metadata
            self._total_records += buffer_size
            self._metadata_store.save_all(
                decisions=self._decisions,
                stats=field_stats,
                total_records=self._total_records
            )
            
            # Clear buffer and WAL, update timestamp
            self._buffer.clear()
            self._clear_wal()  # Clear WAL after successful flush
            self._last_flush_time = time.time()
            
            elapsed = time.time() - start_time
            
            result = {
                "status": "success",
                "records_processed": buffer_size,
                "total_records_lifetime": self._total_records,
                "sql_inserts": route_result.sql_inserts,
                "mongo_inserts": route_result.mongo_inserts,
                "fields_classified": len(self._decisions),
                "elapsed_seconds": round(elapsed, 3),
                "timestamp": datetime.utcnow().isoformat(),
                "errors": route_result.errors if route_result.errors else []
            }
            
            print(f"✓ Flushed {buffer_size} records in {elapsed:.2f}s "
                  f"(SQL: {route_result.sql_inserts}, Mongo: {route_result.mongo_inserts})")
            
            return result
            
        except Exception as e:
            error_msg = f"Flush failed: {str(e)}"
            print(f"✗ {error_msg}")
            return {
                "status": "error",
                "error": error_msg,
                "records_in_buffer": buffer_size,
                "timestamp": datetime.utcnow().isoformat()
            }

    def get_decisions(self) -> dict[str, PlacementDecision]:
        """
        Get current placement decisions for all fields.
        
        Returns:
            Dictionary mapping field names to placement decisions.
        """
        return self._decisions.copy()

    def get_field_stats(self) -> dict:
        """
        Get current field statistics from the analyzer.
        
        Returns:
            Dictionary of field names to their statistics.
        
        USES:
            - Topic 2 (analysis/): FieldAnalyzer.get_stats()
        """
        return self._field_analyzer.get_stats()

    def get_status(self) -> dict:
        """
        Get current pipeline status.
        
        Returns:
            Dictionary with pipeline state information.
        """
        return {
            "buffer_size": len(self._buffer),
            "buffer_capacity": self._buffer_size,
            "total_records_processed": self._total_records,
            "fields_discovered": len(self._field_analyzer.get_stats()),
            "fields_classified": len(self._decisions),
            "seconds_since_last_flush": round(time.time() - self._last_flush_time, 2),
            "buffer_timeout": self._buffer_timeout,
            "will_auto_flush": self._should_flush(),
            "timestamp": datetime.utcnow().isoformat()
        }

    def get_classification_summary(self) -> dict:
        """
        Get summary of field classifications by backend.
        
        Returns:
            Dictionary with fields grouped by their backend assignment.
        """
        from src.analysis.decision import Backend
        
        summary = {
            "SQL": [],
            "MONGODB": [],
            "BOTH": []
        }
        
        for field_name, decision in self._decisions.items():
            # Handle both Backend enum and string values
            if isinstance(decision.backend, Backend):
                backend_key = decision.backend.name
            elif isinstance(decision.backend, str):
                backend_key = decision.backend
            else:
                backend_key = str(decision.backend)
                
            summary[backend_key].append({
                "field": field_name,
                "sql_type": decision.sql_type,
                "nullable": getattr(decision, 'is_nullable', True),
                "reason": decision.reason
            })
        
        return {
            "sql_fields": summary["SQL"],
            "mongo_fields": summary["MONGODB"],
            "both_fields": summary["BOTH"],
            "counts": {
                "sql": len(summary["SQL"]),
                "mongo": len(summary["MONGODB"]),
                "both": len(summary["BOTH"]),
                "total": len(self._decisions)
            }
        }

    def _handle_backend_changes(
        self,
        old_decisions: dict[str, PlacementDecision],
        new_decisions: dict[str, PlacementDecision]
    ) -> list[dict]:
        """
        Detect and handle backend changes by migrating existing data.
        
        Args:
            old_decisions: Previous placement decisions
            new_decisions: New placement decisions after re-classification
            
        Returns:
            List of migration results for changed fields
        """
        migrations = []
        
        for field_name, new_decision in new_decisions.items():
            old_decision = old_decisions.get(field_name)
            
            # Skip if field is new (no old decision)
            if old_decision is None:
                continue
            
            # Skip if backend hasn't changed
            if old_decision.backend == new_decision.backend:
                continue
            
            # Backend changed - trigger migration
            print(f"⟳ Backend change detected for '{field_name}': "
                  f"{old_decision.backend.value} → {new_decision.backend.value}")
            
            result = self._migrator.migrate_backend(
                field_name=field_name,
                old_backend=old_decision.backend,
                new_backend=new_decision.backend,
                new_decision=new_decision,
                mysql_client=self._mysql_client,
                mongo_client=self._mongo_client,
                table_name="records",
                collection_name="records"
            )
            
            migrations.append(result)
            
            if result["success"]:
                print(f"  ✓ Migrated {result['records_migrated']} records")
            else:
                print(f"  ✗ Migration failed: {result['error']}")
        
        return migrations

    def _should_flush(self) -> bool:
        """
        Check if buffer should be flushed based on size or time threshold.
        
        Returns:
            True if flush should occur, False otherwise.
        """
        # Flush if buffer is full
        if len(self._buffer) >= self._buffer_size:
            return True
        
        # Flush if timeout exceeded and buffer is not empty
        time_elapsed = time.time() - self._last_flush_time
        if self._buffer and time_elapsed >= self._buffer_timeout:
            return True
        
        return False

    def _append_to_wal(self, record: dict) -> None:
        """
        Append a record to the Write-Ahead Log for crash recovery.
        
        Args:
            record: Normalized record to persist.
        """
        try:
            with open(self._wal_path, "a") as f:
                f.write(json.dumps(record) + "\n")
        except Exception as e:
            # Log but don't fail - WAL is a safety net, not critical path
            print(f"⚠ WAL write warning: {e}")

    def _clear_wal(self) -> None:
        """
        Clear the Write-Ahead Log after successful flush.
        """
        try:
            if self._wal_path.exists():
                self._wal_path.unlink()
        except Exception as e:
            print(f"⚠ WAL clear warning: {e}")

    def _recover_from_wal(self) -> None:
        """
        Recover pending records from WAL after a crash/restart.
        If WAL contains records, they are loaded into the buffer for processing.
        """
        if not self._wal_path.exists():
            return
        
        try:
            recovered_records = []
            with open(self._wal_path, "r") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        recovered_records.append(json.loads(line))
            
            if recovered_records:
                print(f"⚠ Recovering {len(recovered_records)} records from WAL (previous crash detected)")
                self._buffer.extend(recovered_records)
                # Flush immediately to persist recovered records
                if self._buffer:
                    self.flush()
        except Exception as e:
            print(f"⚠ WAL recovery warning: {e}")

    def _load_previous_state(self) -> None:
        """
        Load previous state from metadata store if it exists.
        This allows the pipeline to resume after a restart.
        
        USES:
            - Topic 4 (persistence/): MetadataStore.exists(), MetadataStore.load_all()
            - Topic 2 (analysis/): FieldAnalyzer.stats
        """
        if not self._metadata_store.exists():
            print("✓ No previous metadata found, starting fresh")
            return
        
        try:
            # Load all persisted metadata
            decisions, stats, state = self._metadata_store.load_all()
            
            # Restore decisions
            self._decisions = decisions
            
            # Restore field analyzer state
            if stats:
                for field_name, field_stats in stats.items():
                    self._field_analyzer.stats[field_name] = field_stats
            
            # Restore total records count
            if state and "total_records" in state:
                self._total_records = state["total_records"]
            
            print(f"✓ Restored state: {self._total_records} records processed, "
                  f"{len(self._decisions)} decisions loaded")
            
        except Exception as e:
            print(f"⚠ Warning: Could not load previous state: {e}")
            print("✓ Starting with fresh state")

    def close(self) -> None:
        """
        Close all connections and clean up resources.
        
        USES:
            - Topic 3 (storage/): MySQLClient.disconnect(), MongoClient.disconnect()
        """
        try:
            self._mysql_client.disconnect()
            self._mongo_client.disconnect()
            print("✓ Pipeline connections closed")
        except Exception as e:
            print(f"⚠ Warning during close: {e}")

    def __enter__(self):
        """Context manager entry."""
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """Context manager exit."""
        # Flush any remaining records
        if self._buffer:
            self.flush()
        
        # Close connections
        self.close()
        
        return False  # Don't suppress exceptions
