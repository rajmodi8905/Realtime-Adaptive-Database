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
        is_nullable: bool = True,
        is_unique: bool = False,
        is_primary_key: bool = False,
        reason: str = ""
    ):
        self.field_name = field_name
        self.backend = backend
        self.sql_type = sql_type
        self.is_nullable = is_nullable
        self.is_unique = is_unique
        self.is_primary_key = is_primary_key
        self.reason = reason

    def to_dict(self) -> dict:
        return {
            "field_name": self.field_name,
            "backend": self.backend.value,
            "sql_type": self.sql_type,
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
            is_nullable=data.get("is_nullable", True),
            is_unique=data.get("is_unique", False),
            is_primary_key=data.get("is_primary_key", False),
            reason=data.get("reason", "")
        )
