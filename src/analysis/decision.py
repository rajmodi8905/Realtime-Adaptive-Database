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
from dataclasses import dataclass
from typing import Optional, Dict, Any


class Backend(Enum):
    """
    Enumeration of storage backends for record placement.
    
    - SQL: Store in MySQL relational database
    - MONGODB: Store in MongoDB document database
    - BOTH: Store in both backends (for linking fields like username, timestamps)
    """
    SQL = "sql"
    MONGODB = "mongodb"
    BOTH = "both"


@dataclass
class PlacementDecision:
    """
    Represents the classification decision for a single field.
    
    This is what the Classifier produces and what Storage uses
    to route individual fields to the appropriate backend(s).
    
    For dual-backend queries:
    - If backend=SQL: Query using flattened column name (e.g., "metadata_sensor_data_version")
    - If backend=MONGODB: Query using nested path (e.g., "metadata.sensor_data.readings")
    - If backend=BOTH: Field exists in both backends with linking field "username"
    """

    # --- Core decision ---
    field_name: str  # Canonical name using dot notation (e.g., "metadata.sensor_data.version")
    backend: Backend  # Where to store this field: SQL, MONGODB, or BOTH

    # --- SQL-specific metadata ---
    sql_type: Optional[str] = None  # MySQL column type, e.g., "BIGINT", "VARCHAR(255)"
    sql_column_name: Optional[str] = None  # Flattened column name for SQL (e.g., "metadata_sensor_data_version")
    is_nullable: bool = True  # Can the column be NULL?
    is_unique: bool = False  # Should this column have a UNIQUE constraint?

    # --- MongoDB metadata ---
    mongo_path: Optional[str] = None  # Nested path for MongoDB (e.g., "metadata.sensor_data.readings")

    # --- Reasoning ---
    reason: str = ""  # Human-readable explanation for the decision

    def to_dict(self) -> Dict[str, Any]:
        """
        Serialize the decision to a dictionary for persistence.
        
        Returns:
            A JSON-serializable dictionary representation
        """
        return {
            "field_name": self.field_name,
            "backend": self.backend.value,  # Convert enum to string
            "sql_type": self.sql_type,
            "sql_column_name": self.sql_column_name,
            "is_nullable": self.is_nullable,
            "is_unique": self.is_unique,
            "mongo_path": self.mongo_path,
            "reason": self.reason,
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "PlacementDecision":
        """
        Reconstruct a PlacementDecision from stored metadata.
        
        Args:
            data: Dictionary with saved decision information
            
        Returns:
            A PlacementDecision instance
        """
        return cls(
            field_name=data["field_name"],
            backend=Backend(data["backend"]),  # Convert string back to enum
            sql_type=data.get("sql_type"),
            sql_column_name=data.get("sql_column_name"),
            is_nullable=data.get("is_nullable", True),
            is_unique=data.get("is_unique", False),
            mongo_path=data.get("mongo_path"),
            reason=data.get("reason", ""),
        )


@dataclass
class ClassificationThresholds:
    """
    Configurable thresholds that control the classification logic.
    
    These values determine when a field should be placed in SQL vs MongoDB.
    Adjust these based on your desired SQL vs MongoDB balance.
    """

    # --- SQL thresholds ---
    min_presence_ratio: float = 0.7
    """
    Minimum fraction of records where the field must appear to be eligible for SQL.
    Default 0.7 = field must be present in at least 70% of records.
    """

    min_type_stability: float = 0.9
    """
    Minimum fraction of occurrences that must have the dominant type for SQL.
    Default 0.9 = the dominant type must appear in at least 90% of field occurrences.
    """

    # --- Uniqueness detection ---
    max_unique_ratio: float = 0.95
    """
    Maximum unique ratio to avoid mistaking primary keys as regular fields.
    Fields with unique_ratio > this value go to MongoDB.
    Default 0.95 = if more than 95% of values are unique, treat as MongoDB field.
    """

    # --- Decision readiness ---
    min_records_for_decision: int = 50
    """
    Minimum number of records to observe before making classification decisions.
    Prevents premature decisions based on insufficient data.
    Default 50 = wait for at least 50 records before classifying.
    """
