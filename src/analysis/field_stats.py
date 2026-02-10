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
    """

    # --- Core identity ---
    name: str # The name of the field whose stats we are storing

    # --- Counters ---

    presence_count: int = 0  # How many times the field has occured in records

    type_counts: Dict[str, int] = field(default_factory=dict)
    # Counts of the distinct types the field has come in e.g. "int" : 10, "str" : 3

    null_count: int = 0  # How many times is the field value NULL or None

    # --- Uniqueness tracking ---
    unique_values: Set[Any] = field(default_factory=set)
    #Stores unique values of the fields up till a max limit
    max_unique_tracked: int = 1000 # Max number of unique values that can be stored

    # --- Structure info ---
    is_nested: bool = False #Specifies if the value of the field is a dict or a list or a flat value

    # --- Debugging / inspection ---
    sample_values: List[Any] = field(default_factory=list) #List of sample values for debugging
    max_samples: int = 5 # Max number of samples

    # ======================================
    # Update logic
    # ======================================
    def update(self, value: Any, detected_type: str) -> None:
        """
        Update statistics based on a newly observed value.
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

        # Track nesting
        if isinstance(value, (dict, list)):
            self.is_nested = True

        # Track unique values (bounded)
        if len(self.unique_values) < self.max_unique_tracked:
            self.unique_values.add(value)

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
        """
        if not self.type_counts:
            return None
        return max(self.type_counts, key=self.type_counts.get)

    @property
    def type_stability(self) -> float:
        """
        Fraction of observations that match the dominant type.
        """
        if not self.type_counts or self.presence_count == 0:
            return 0.0
        dominant = self.type_counts[self.dominant_type]
        return dominant / self.presence_count

    @property
    def unique_ratio(self) -> float:
        """
        Ratio of unique values to total appearances.
        """
        if self.presence_count == 0:
            return 0.0
        return len(self.unique_values) / self.presence_count

    # ======================================
    # Serialization
    # ======================================
    def to_dict(self) -> Dict[str, Any]:
        """
        Convert stats to a serializable dictionary.
        """
        return {
            "name": self.name,
            "presence_count": self.presence_count,
            "type_counts": dict(self.type_counts),
            "null_count": self.null_count,
            "unique_count": len(self.unique_values),
            "is_nested": self.is_nested,
            "sample_values": list(self.sample_values),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "FieldStats":
        """
        Reconstruct FieldStats from stored metadata.
        """
        fs = cls(name=data["name"]) # Creates a class instance using the "name" from metadata
        fs.presence_count = data.get("presence_count", 0)
        fs.type_counts = data.get("type_counts", {})
        fs.null_count = data.get("null_count", 0)
        fs.is_nested = data.get("is_nested", False)
        fs.sample_values = data.get("sample_values", [])
        return fs
pass
