# ==============================================
# FieldStats
# ==============================================
#
# PURPOSE:
#   Data class that holds all observed statistics for a single field.
#   This is the "evidence" that the classifier uses to make decisions.
#
# WHY THIS CLASS EXISTS:
#   During the buffering phase, we observe many records. For each field
#   we need to track metrics like: how often does it appear? does its type
#   stay consistent? is it nested? is it always unique?
#   This class is the container for all that evidence.
#
# CLASS: FieldStats (dataclass)
# -----------------------------
#   Attributes:
#   -----------
#   - name: str                     → Canonical field name
#   - presence_count: int           → How many records contain this field
#   - type_counts: dict[str, int]   → {"int": 45, "str": 3, "null": 2}
#   - null_count: int               → How many times value was None/null
#   - unique_values: set            → Set of unique values seen (capped for memory)
#   - max_unique_tracked: int       → Cap on unique values set size (default 1000)
#   - is_nested: bool               → True if value is dict or list
#   - unique_count: int             → Count of unique values (for serialization)
#   - sample_values: list           → Small list of sample values (for debugging)
#
#   Computed Properties:
#   --------------------
#   - dominant_type -> str | None
#       The most frequently observed type for this field.
#
#   - type_stability -> float
#       (count of dominant type) / (total type observations)
#       1.0 = perfectly stable, 0.5 = half the records had a different type
#
#   - unique_ratio -> float
#       (unique values seen) / (presence count)
#       High ratio = possibly a key/identifier field
#
#   Methods:
#   --------
#   - update(value: Any, detected_type: str) -> None
#       Update all stats with a new observed value.
#
#   - to_dict() -> dict
#       Serialize for metadata persistence.
#
#   - from_dict(data: dict) -> FieldStats  (classmethod)
#       Deserialize from stored metadata.
#
# ==============================================

from dataclasses import dataclass, field
from typing import Any, Dict, Set, List, Optional


@dataclass
class FieldStats:
    """
    Holds observed statistics for a single field across many records.
    
    Supports both flat and nested fields with unified dot-notation naming.
    """

    # --- Core identity ---
    name: str  # The canonical field name (e.g., "metadata.sensor_data.version")
    nesting_depth: int = 0  # Number of dots in the field name (metadata.sensor_data.version = 2)

    # --- Counters ---
    presence_count: int = 0  # How many times the field has occurred in records

    type_counts: Dict[str, int] = field(default_factory=dict)
    # Counts of distinct types: {"int": 45, "str": 3, "array": 5}

    null_count: int = 0  # How many times is the field value NULL or None

    # --- Uniqueness tracking ---
    unique_values: Set[Any] = field(default_factory=set)
    # Stores unique hashable values of the field up till a max limit
    max_unique_tracked: int = 1000  # Max number of unique values that can be stored
    unique_count: int = 0  # Track count when values are unhashable

    # --- Structure info ---
    is_nested: bool = False  # True if value is dict or list (from the original record)

    # --- Debugging / inspection ---
    sample_values: List[Any] = field(default_factory=list)  # List of sample values for debugging
    max_samples: int = 5  # Max number of samples

    def __post_init__(self):
        """Calculate nesting depth from field name."""
        self.nesting_depth = self.name.count(".")

    # ======================================
    # Update logic
    # ======================================
    def update(self, value: Any, detected_type: str) -> None:
        """
        Update statistics based on a newly observed value.
        
        Args:
            value: The field value to record
            detected_type: The type of the value (e.g., "int", "str", "array", "object")
        """

        # Field appeared in this record
        self.presence_count += 1

        # Track type frequency
        self.type_counts[detected_type] = (
            self.type_counts.get(detected_type, 0) + 1
        )

        # Track nulls
        if value is None:
            self.null_count += 1
            return

        # Track nesting (for backwards compatibility, though we track in name now)
        if isinstance(value, (dict, list)):
            self.is_nested = True

        # Track unique values (bounded) — only for hashable types
        try:
            if len(self.unique_values) < self.max_unique_tracked:
                # Only hash it if it's hashable
                hash(value)
                self.unique_values.add(value)
        except TypeError:
            # Value is unhashable (dict, list), just count it
            self.unique_count += 1

        # Track sample values (small, bounded)
        if len(self.sample_values) < self.max_samples:
            self.sample_values.append(value)

    # ======================================
    # Computed properties
    # ======================================
    @property
    def dominant_type(self) -> Optional[str]:
        """
        Return the most frequently observed type.
        
        Returns:
            The type string with the highest count, or None if no type counts recorded.
        """
        if not self.type_counts:
            return None
        return max(self.type_counts, key=self.type_counts.get)

    @property
    def type_stability(self) -> float:
        """
        Fraction of observations that match the dominant type.
        
        Returns:
            A value between 0.0 and 1.0 where 1.0 means all records had the dominant type.
        """
        if not self.type_counts or self.presence_count == 0:
            return 0.0
        dominant_count = self.type_counts.get(self.dominant_type, 0)
        return dominant_count / self.presence_count

    @property
    def unique_ratio(self) -> float:
        """
        Ratio of unique values to total appearances (presence_count).
        
        High ratio suggests the field might be a key or identifier.
        
        Returns:
            A value between 0.0 and 1.0 where 1.0 means all values are unique.
        """
        if self.presence_count == 0:
            return 0.0
        # Use unique_count for unhashable values + hashable unique_values
        total_unique = len(self.unique_values) + self.unique_count
        return total_unique / self.presence_count

    # ======================================
    # Serialization
    # ======================================
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert stats to a serializable dictionary for persistence.
        
        Note: unique_values set is converted to count for serialization
        since sets aren't directly JSON serializable.
        
        Returns:
            A dictionary representation suitable for JSON storage.
        """
        return {
            "name": self.name,
            "nesting_depth": self.nesting_depth,
            "presence_count": self.presence_count,
            "type_counts": dict(self.type_counts),
            "null_count": self.null_count,
            "unique_count": len(self.unique_values) + self.unique_count,
            "is_nested": self.is_nested,
            "sample_values": list(self.sample_values),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FieldStats":
        """
        Reconstruct FieldStats from stored metadata.
        
        Note: We lose the actual unique values when deserializing,
        but preserve the count for uniqueness calculations.
        
        Args:
            data: Dictionary with saved field statistics
            
        Returns:
            A FieldStats instance reconstructed from the data
        """
        fs = cls(name=data["name"])  # Create instance with the field name
        fs.nesting_depth = data.get("nesting_depth", 0)
        fs.presence_count = data.get("presence_count", 0)
        fs.type_counts = data.get("type_counts", {})
        fs.null_count = data.get("null_count", 0)
        fs.unique_count = data.get("unique_count", 0)  # Restore unique count
        fs.is_nested = data.get("is_nested", False)
        fs.sample_values = data.get("sample_values", [])
        return fs