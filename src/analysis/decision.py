# ==============================================
# Decision (Data Classes)
# ==============================================
#
# PURPOSE:
#   Data classes that represent the OUTPUT of classification.
#   These are the placement decisions and the thresholds that
#   control how decisions are made.
#
# WHY THIS FILE EXISTS:
#   Separating data classes from logic keeps the classifier clean.
#   These classes are also used by Topic 3 (Storage) to know where
#   to route records, and by Topic 4 (Persistence) to save/load decisions.
#
# ENUMS:
# ------
# - Backend(Enum): SQL, MONGODB, BOTH
#     Which backend a field is assigned to.
#
# CLASSES:
# --------
# - PlacementDecision (dataclass)
#     The decision for a single field.
#
#     Attributes:
#     -----------
#     - field_name: str              → Canonical field name
#     - backend: Backend             → SQL, MONGODB, or BOTH
#     - sql_type: str | None         → MySQL column type (e.g., "BIGINT", "VARCHAR(255)")
#     - is_nullable: bool            → Can this column be NULL in SQL?
#     - is_unique: bool              → Should this column have a UNIQUE constraint?
#     - reason: str                  → Human-readable explanation for the decision
#
#     Methods:
#     --------
#     - to_dict() -> dict            → Serialize for persistence
#     - from_dict(data: dict) -> PlacementDecision  (classmethod) → Deserialize
#
# - ClassificationThresholds (dataclass)
#     Configurable thresholds that the classifier uses.
#
#     Attributes:
#     -----------
#     - min_presence_ratio: float    → Field must appear in X% of records for SQL (default 0.7)
#     - min_type_stability: float    → Field must have same type X% of time for SQL (default 0.9)
#     - max_unique_ratio: float      → If >X% unique, it's a potential key (default 0.95)
#     - min_records_for_decision: int → Don't classify until we've seen N records (default 50)
#
# ==============================================

from enum import Enum

class Backend(Enum):
    SQL = "SQL"
    MONGODB = "MONGODB"
    BOTH = "BOTH"

class PlacementDecision:
    def __init__(
        self,
        field_name: str,
        backend: Backend,
        sql_type: str | None = None,
        sql_column_name: str | None = None,
        mongo_path: str | None = None,
        canonical_type: str | None = None,
        is_nullable: bool = True,
        is_unique: bool = False,
        is_primary_key: bool = False,
        reason: str = ""
    ):
        self.field_name = field_name
        # Ensure backend is always a Backend enum
        if isinstance(backend, str):
            self.backend = Backend(backend)
        elif isinstance(backend, Backend):
            self.backend = backend
        else:
            raise TypeError(f"backend must be Backend enum or string, got {type(backend)}")
        self.sql_type = sql_type
        self.sql_column_name = sql_column_name
        self.mongo_path = mongo_path
        self.canonical_type = canonical_type
        self.is_nullable = is_nullable
        self.is_unique = is_unique
        self.is_primary_key = is_primary_key
        self.reason = reason

    def to_dict(self) -> dict:
        return {
            "field_name": self.field_name,
            "backend": self.backend.value,
            "sql_type": self.sql_type,
            "sql_column_name": self.sql_column_name,
            "mongo_path": self.mongo_path,
            "canonical_type": self.canonical_type,
            "is_nullable": self.is_nullable,
            "is_unique": self.is_unique,
            "is_primary_key": self.is_primary_key,
            "reason": self.reason
        }

    @classmethod
    def from_dict(cls, data: dict) -> 'PlacementDecision':
        return cls(
            field_name=data["field_name"],
            backend=Backend(data["backend"]),
            sql_type=data.get("sql_type"),
            sql_column_name=data.get("sql_column_name"),
            mongo_path=data.get("mongo_path"),
            canonical_type=data.get("canonical_type"),
            is_nullable=data.get("is_nullable", True),
            is_unique=data.get("is_unique", False),
            is_primary_key=data.get("is_primary_key", False),
            reason=data.get("reason", "")
        )


class ClassificationThresholds:
    """Configurable thresholds that control classification decisions."""
    
    def __init__(
        self,
        min_presence_ratio: float = 0.7,
        min_type_stability: float = 0.9,
        max_unique_ratio: float = 0.95,
        min_records_for_decision: int = 50
    ):
        self.min_presence_ratio = min_presence_ratio
        self.min_type_stability = min_type_stability
        self.max_unique_ratio = max_unique_ratio
        self.min_records_for_decision = min_records_for_decision


class TypeConflict:
    """Represents a detected type conflict requiring schema migration."""
    
    def __init__(
        self,
        field_name: str,
        stored_type: str,
        incoming_type: str,
        stored_backend: Backend,
        records_affected: int,
        can_widen: bool,
        widened_type: str | None = None,
        reason: str = ""
    ):
        self.field_name = field_name
        self.stored_type = stored_type
        self.incoming_type = incoming_type
        self.stored_backend = stored_backend
        self.records_affected = records_affected
        self.can_widen = can_widen  # True if we can safely widen the type
        self.widened_type = widened_type  # The type we should migrate to
        self.reason = reason
    
    def to_dict(self) -> dict:
        return {
            "field_name": self.field_name,
            "stored_type": self.stored_type,
            "incoming_type": self.incoming_type,
            "stored_backend": self.stored_backend.value,
            "records_affected": self.records_affected,
            "can_widen": self.can_widen,
            "widened_type": self.widened_type,
            "reason": self.reason
        }
    
    def __repr__(self) -> str:
        return (
            f"TypeConflict(field='{self.field_name}', "
            f"{self.stored_type} -> {self.incoming_type}, "
            f"can_widen={self.can_widen}, affected={self.records_affected})"
        )
