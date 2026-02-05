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

pass
