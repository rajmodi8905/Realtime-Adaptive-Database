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

pass
