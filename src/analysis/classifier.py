# ==============================================
# Classifier
# ==============================================
#
# PURPOSE:
#   Takes accumulated FieldStats from the FieldAnalyzer and applies
#   heuristic rules to produce PlacementDecision for each field.
#   This is the "brain" — it decides SQL vs MongoDB.
#
# WHY THIS CLASS EXISTS:
#   The assignment says: "No Hardcoding. Field mappings must be
#   discovered dynamically." This class implements the dynamic
#   discovery using heuristic rules on observed statistics.
#
# CLASS: Classifier
# -----------------
#   Stateless — takes stats in, produces decisions out.
#
#   Constructor:
#   ------------
#   - __init__(thresholds: ClassificationThresholds)
#
#   Methods:
#   --------
#   - classify_all(
#         stats: dict[str, FieldStats],
#         total_records: int
#     ) -> dict[str, PlacementDecision]
#       Classify every observed field. Returns mapping of
#       field_name → PlacementDecision.
#
#   - classify_field(
#         field_name: str,
#         stats: FieldStats,
#         total_records: int
#     ) -> PlacementDecision
#       Classify a single field. Applies rules in order:
#
#       RULE 1: LINKING FIELDS → BOTH
#         If field_name in ("username", "sys_ingested_at", "t_stamp"):
#           → Backend.BOTH (required in both DBs for cross-DB joins)
#
#       RULE 2: NESTED STRUCTURES → MONGODB
#         If stats.is_nested (value is dict or list):
#           → Backend.MONGODB (SQL can't handle nested objects)
#
#       RULE 3: STABLE + PRESENT → SQL
#         If presence_ratio >= threshold AND type_stability >= threshold:
#           → Backend.SQL (structured, predictable, good for SQL)
#
#       RULE 4: EVERYTHING ELSE → MONGODB
#         Low presence, type drift, or sparse fields:
#           → Backend.MONGODB (schema-flexible)
#
#   - _determine_sql_type(stats: FieldStats) -> str
#       Map the dominant detected type to a MySQL column type:
#         "int"      → "BIGINT"
#         "float"    → "DOUBLE"
#         "bool"     → "BOOLEAN"
#         "str"      → "VARCHAR(255)"
#         "ip"       → "VARCHAR(45)"
#         "uuid"     → "CHAR(36)"
#         "datetime" → "DATETIME"
#         default    → "TEXT"
#
# ==============================================

from typing import Dict, List
from .field_stats import FieldStats
from .decision import PlacementDecision, ClassificationThresholds, Backend, TypeConflict


class Classifier:
    """
    Applies heuristic rules to FieldStats to produce PlacementDecisions.
    
    This is where the autonomous decision-making happens. Based on observed
    field characteristics (type stability, presence frequency, nesting),
    the Classifier decides whether each field should go to SQL, MongoDB,
    or both (for critical linking fields).
    """

    # Special fields that MUST be in both backends for cross-record linking
    LINKING_FIELDS = {"username", "sys_ingested_at", "t_stamp"}

    def __init__(
        self,
        thresholds: ClassificationThresholds = None
    ):
        """
        Initialize the Classifier with configurable thresholds.
        
        Args:
            thresholds: Optional ClassificationThresholds. If not provided,
                       defaults will be used (70% presence, 90% type stability, etc.)
        """
        self.thresholds = thresholds or ClassificationThresholds()

    def classify_all(
        self,
        stats: Dict[str, FieldStats],
        total_records: int
    ) -> Dict[str, PlacementDecision]:
        """
        Classify all observed fields.
        
        Args:
            stats: Dictionary of field_name → FieldStats from the analyzer
            total_records: Total number of records analyzed
            
        Returns:
            Dictionary of field_name → PlacementDecision
        """
        decisions = {}
        for field_name, field_stats in stats.items():
            decision = self.classify_field(
                field_name,
                field_stats,
                total_records
            )
            decisions[field_name] = decision
        return decisions

    def classify_field(
        self,
        field_name: str,
        stats: FieldStats,
        total_records: int
    ) -> PlacementDecision:
        """
        Classify a single field using type-based heuristic rules.
        
        Rules are applied in order (short-circuit evaluation):
        1. LINKING FIELDS → BOTH
        2. ARRAY FIELDS → MONGODB (arrays always stay nested)
        3. OBJECT FIELDS → MONGODB (objects stay nested unless shallow & stable)
        4. SCALAR FIELDS → SQL (if stable & present)
        5. SPARSE/UNSTABLE → MONGODB
        
        Key insight: Type determines routing more than nesting depth.
        - metadata.sensor_data.version (scalar at depth 2) → SQL ✅
        - metadata.tags (array at depth 1) → MongoDB ✅
        
        Args:
            field_name: The field to classify (using dot notation)
            stats: The accumulated statistics for this field
            total_records: Total records analyzed (for ratio calculation)
            
        Returns:
            A PlacementDecision with backend choice and routing info
        """

        # RULE 1: LINKING FIELDS must go to both backends
        if field_name in self.LINKING_FIELDS:
            reason = (
                f"Critical linking field '{field_name}' required in both backends "
                f"for cross-database joins and record traceability."
            )
            sql_column = field_name  # Use as-is for top-level fields
            return PlacementDecision(
                field_name=field_name,
                backend=Backend.BOTH,
                sql_column_name=sql_column,
                sql_type="VARCHAR(255)",
                mongo_path=field_name,
                canonical_type="str",
                is_nullable=False,
                is_unique=(field_name == "username"),
                reason=reason,
            )

        # Calculate metrics
        presence_ratio = stats.presence_count / total_records if total_records > 0 else 0.0
        type_stability = stats.type_stability
        unique_ratio = stats.unique_ratio
        dominant_type = stats.dominant_type

        # Helper to get SQL column name from field name (flatten dots to underscores)
        sql_column_name = field_name.replace(".", "_")

        # RULE 2: ARRAY FIELDS → MONGODB
        # Arrays must stay in MongoDB because SQL doesn't handle arrays natively
        if dominant_type == "array":
            reason = (
                f"Field '{field_name}' is an array type. "
                f"MongoDB's native array support is required; SQL would need junction tables."
            )
            return PlacementDecision(
                field_name=field_name,
                backend=Backend.MONGODB,
                mongo_path=field_name,
                canonical_type="array",
                is_nullable=stats.null_count > 0,
                reason=reason,
            )

        # RULE 3: OBJECT FIELDS → MONGODB
        # Objects (dicts) stay in MongoDB unless very shallow and stable
        if dominant_type == "object":
            # Deep objects always go to MongoDB
            if stats.nesting_depth > 1:
                reason = (
                    f"Field '{field_name}' is a nested object (depth={stats.nesting_depth}). "
                    f"MongoDB's schema flexibility is better suited for complex structures."
                )
            else:
                reason = (
                    f"Field '{field_name}' is an object type. "
                    f"MongoDB is schema-flexible for structural variation."
                )
            
            return PlacementDecision(
                field_name=field_name,
                backend=Backend.MONGODB,
                mongo_path=field_name,
                canonical_type="object",
                is_nullable=stats.null_count > 0,
                reason=reason,
            )

        # RULE 4: SCALAR FIELDS - Check stability and presence
        # Scalars can go to SQL if they're stable and present enough
        if dominant_type in ("int", "float", "bool", "str", "datetime", "ip", "uuid"):
            
            if (
                presence_ratio >= self.thresholds.min_presence_ratio
                and type_stability >= self.thresholds.min_type_stability
            ):
                # STABLE SCALAR → SQL
                sql_type = self._determine_sql_type(stats)
                # Only mark as unique if field name suggests it's an ID AND it's highly unique
                field_lower = field_name.lower()
                is_id_field = any(x in field_lower for x in ['_id', 'id_', 'uuid', 'key'])
                is_unique = is_id_field and unique_ratio > self.thresholds.max_unique_ratio
                # Nullable if field has nulls OR is not present in all records
                is_nullable = stats.null_count > 0 or presence_ratio < 1.0

                reason = (
                    f"Scalar field '{field_name}' is structured: "
                    f"present in {presence_ratio*100:.1f}% of records, "
                    f"type-stable at {type_stability*100:.1f}%. "
                    f"Type={dominant_type}, Unique={is_unique}"
                )

                return PlacementDecision(
                    field_name=field_name,
                    backend=Backend.SQL,
                    sql_column_name=sql_column_name,
                    sql_type=sql_type,
                    mongo_path=field_name,  # Also tracked for reference
                    canonical_type=dominant_type,
                    is_nullable=is_nullable,
                    is_unique=is_unique,
                    reason=reason,
                )

        # RULE 5: SPARSE OR UNSTABLE → MONGODB
        # Fall back to MongoDB for anything not matching above rules
        reason = (
            f"Field '{field_name}' is sparse or unstable: "
            f"present in {presence_ratio*100:.1f}% of records, "
            f"type-stable at {type_stability*100:.1f}%. "
            f"MongoDB's schema-flexibility accommodates variability."
        )

        return PlacementDecision(
            field_name=field_name,
            backend=Backend.MONGODB,
            mongo_path=field_name,
            canonical_type=dominant_type,
            is_nullable=stats.null_count > 0,
            reason=reason,
        )

    def _determine_sql_type(self, stats: FieldStats) -> str:
        """
        Map the field's detected type to a MySQL column type.
        
        Uses the dominant (most frequently observed) type to decide
        the SQL column type.
        
        Args:
            stats: The field statistics
            
        Returns:
            A MySQL column type string (e.g., "BIGINT", "VARCHAR(255)")
        """
        dominant_type = stats.dominant_type

        if dominant_type == "int":
            return "BIGINT"
        elif dominant_type == "float":
            return "DOUBLE"
        elif dominant_type == "bool":
            return "BOOLEAN"
        elif dominant_type == "ip":
            # IPv6 can be up to 45 chars
            return "VARCHAR(45)"
        elif dominant_type == "uuid":
            # UUID format: 36 chars (8-4-4-4-12 with dashes)
            return "CHAR(36)"
        elif dominant_type == "datetime":
            return "DATETIME"
        elif dominant_type == "str":
            # Default string type with reasonable size
            return "VARCHAR(255)"
        else:
            # Unknown type, use TEXT as safest option
            return "TEXT"

    def get_backend_distribution(
        self,
        decisions: Dict[str, PlacementDecision]
    ) -> dict:
        """
        Analyze distribution of decisions across backends.
        
        Useful for understanding the overall data placement strategy.
        
        Args:
            decisions: Dictionary of field_name → PlacementDecision
            
        Returns:
            Dict with counts of SQL, MONGODB, BOTH field placements
        """
        distribution = {
            "sql": 0,
            "mongodb": 0,
            "both": 0,
        }

        for decision in decisions.values():
            if decision.backend == Backend.SQL:
                distribution["sql"] += 1
            elif decision.backend == Backend.MONGODB:
                distribution["mongodb"] += 1
            elif decision.backend == Backend.BOTH:
                distribution["both"] += 1

        return distribution

    def detect_type_conflicts(
        self,
        incoming_stats: Dict[str, FieldStats],
        existing_decisions: Dict[str, PlacementDecision],
        total_records_processed: int
    ) -> List[TypeConflict]:
        """
        Detect type conflicts between incoming data and existing schema.
        
        Args:
            incoming_stats: New field statistics from current batch
            existing_decisions: Previous placement decisions with canonical types
            total_records_processed: How many records are already stored
            
        Returns:
            List of TypeConflict objects representing detected conflicts
        """
        conflicts = []
        
        for field_name, incoming_stat in incoming_stats.items():
            # Only check fields we've previously classified
            if field_name not in existing_decisions:
                continue
            
            existing_decision = existing_decisions[field_name]
            
            # Skip if we don't have canonical type stored (old metadata)
            if not hasattr(existing_decision, 'canonical_type') or existing_decision.canonical_type is None:
                continue
            
            stored_type = existing_decision.canonical_type
            incoming_type = incoming_stat.dominant_type
            
            # No conflict if types match
            if stored_type == incoming_type:
                continue
            
            # TYPE CONFLICT DETECTED
            # Determine if we can widen the type safely
            can_widen, widened_type, reason = self._can_widen_type(
                stored_type, 
                incoming_type,
                existing_decision.backend
            )
            
            conflict = TypeConflict(
                field_name=field_name,
                stored_type=stored_type,
                incoming_type=incoming_type,
                stored_backend=existing_decision.backend,
                records_affected=total_records_processed,
                can_widen=can_widen,
                widened_type=widened_type,
                reason=reason
            )
            
            conflicts.append(conflict)
        
        return conflicts

    def _can_widen_type(
        self, 
        stored_type: str, 
        incoming_type: str,
        backend: Backend
    ) -> tuple[bool, str | None, str]:
        """
        Determine if we can safely widen a type without data loss.
        
        Args:
            stored_type: The type currently stored in the schema
            incoming_type: The new type detected in incoming data
            backend: Which backend this field is stored in
            
        Returns:
            Tuple of (can_widen, widened_type, reason)
        """
        
        # Handle null incoming type - keep original
        if incoming_type == "null":
            return (False, None, "Incoming value is null - no widening needed")
        
        # STRING is the universal widener - everything can become a string
        if incoming_type == "str":
            reason = (
                f"Type conflict: stored as {stored_type}, but incoming data is string. "
                f"Must widen to VARCHAR to accommodate text values."
            )
            return (True, "str", reason)
        
        # Specific safe widening paths
        widening_rules = {
            ("int", "float"): (True, "float", "int → float is safe: 10 → 10.0"),
            ("int", "str"): (True, "str", "int → str is safe: 10 → '10'"),
            ("float", "str"): (True, "str", "float → str is safe: 10.5 → '10.5'"),
            ("bool", "int"): (True, "int", "bool → int is safe: True → 1"),
            ("bool", "float"): (True, "float", "bool → float is safe: True → 1.0"),
            ("bool", "str"): (True, "str", "bool → str is safe: True → 'True'"),
            ("datetime", "str"): (True, "str", "datetime → str is safe: ISO format"),
            ("ip", "str"): (True, "str", "ip → str is safe: already string format"),
            ("uuid", "str"): (True, "str", "uuid → str is safe: already string format"),
        }
        
        key = (stored_type, incoming_type)
        if key in widening_rules:
            return widening_rules[key]
        
        # Array/object types should stay in MongoDB
        if incoming_type in ("array", "object"):
            if backend == Backend.MONGODB or backend == Backend.BOTH:
                # Already in MongoDB, no problem
                return (False, None, "Complex type already in MongoDB")
            else:
                # In SQL - this is a problem
                reason = (
                    f"CRITICAL: Field stored as {stored_type} in SQL, "
                    f"but incoming data is {incoming_type}. "
                    f"Must migrate to MongoDB."
                )
                return (False, None, reason)
        
        # Default: unsafe conversion
        reason = (
            f"Cannot safely widen {stored_type} → {incoming_type}. "
            f"This would result in data loss or conversion errors."
        )
        return (False, None, reason)