# ==============================================
# FieldAnalyzer
# ==============================================
#
# PURPOSE:
#   Observe a batch of normalized records and accumulate
#   per-field statistics (FieldStats). This is the "observation
#   engine" — it watches data and builds evidence.
#
# WHY THIS CLASS EXISTS:
#   The assignment says: "While data resides in the buffer, the
#   system observes: field presence frequency, type variation,
#   update patterns, object consistency over time."
#   This class is that observer.
#
# CLASS: FieldAnalyzer
# --------------------
#   Stateful — accumulates FieldStats across multiple batches.
#
#   Constructor:
#   ------------
#   - __init__(type_detector: TypeDetector)
#       Needs the type detector from Topic 1 to detect value types.
#
#   Attributes:
#   -----------
#   - stats: dict[str, FieldStats]  → All accumulated field stats
#   - total_records: int            → Total records observed
#
#   Methods:
#   --------
#   - analyze_batch(records: list[dict]) -> None
#       Observe a batch of records. For each record, for each field:
#         1. Detect type using TypeDetector
#         2. Update FieldStats for that field
#       Handles nested objects by flattening keys with dot notation
#       (e.g., metadata.sensor_data.version).
#
#   - get_stats() -> dict[str, FieldStats]
#       Return all accumulated stats.
#
#   - get_presence_ratio(field_name: str) -> float
#       Return presence_count / total_records for a field.
#
#   - reset() -> None
#       Clear all stats (for testing or re-analysis).
#
#   Internal helpers:
#   -----------------
#   - _analyze_single_record(record: dict, prefix: str = "") -> None
#       Walk through one record, update stats for each field.
#
#   - _flatten_key(prefix: str, key: str) -> str
#       Combine prefix and key with dot: "metadata" + "version" → "metadata.version"
#
# ==============================================

from typing import Dict, List
from .field_stats import FieldStats
from src.normalization import TypeDetector


class FieldAnalyzer:
    """
    Observes normalized records and accumulates field statistics.
    
    This is the data-gathering phase where we build evidence about
    each field's presence, types, stability, and uniqueness.
    """

    def __init__(self, type_detector: TypeDetector = None):
        """
        Initialize the FieldAnalyzer.
        
        Args:
            type_detector: Optional TypeDetector instance. If not provided,
                          a new one will be created.
        """
        self.type_detector = type_detector or TypeDetector()
        self.stats: Dict[str, FieldStats] = {}  # field_name → FieldStats
        self.total_records: int = 0  # Total records analyzed

    def analyze_batch(self, records: List[dict]) -> None:
        """
        Analyze a batch of normalized records and accumulate statistics.
        
        Process:
        1. Flatten each record into dot-notation keys
        2. Detect type of each flattened field value
        3. Update FieldStats for that field
        
        Args:
            records: List of normalized record dictionaries
        """
        for record in records:
            # Step 1: Flatten the record
            flattened = self._flatten_record(record)
            
            # Step 2: Analyze the flattened record
            self._analyze_flattened_record(flattened)
            
            # Step 3: Increment total record count
            self.total_records += 1

    def _flatten_record(self, record: dict, prefix: str = "") -> dict:
        """
        Flatten nested structures into dot-notation keys.
        
        Recursively walks through nested dicts and lists, creating flattened
        keys like "metadata.sensor_data.version".
        
        Examples:
            {"username": "john"} → {"username": "john"}
            {"metadata": {"version": "2.1"}} → {"metadata.version": "2.1"}
            {"metadata": {"tags": ["a", "b"]}} → {"metadata.tags": ["a", "b"]}
        
        Args:
            record: The record (or sub-object) to flatten
            prefix: The dot-notation prefix for nested keys
            
        Returns:
            A dictionary with flattened keys
        """
        flattened = {}
        
        for key, value in record.items():
            # Skip internal/system fields that shouldn't be analyzed as content fields
            if key.startswith("_"):
                continue
            
            # Build the full flattened key name
            canonical_key = self._flatten_key(prefix, key)
            
            # Decide whether to recurse or store
            if isinstance(value, dict):
                # Recurse into nested dicts
                nested_flat = self._flatten_record(value, canonical_key)
                flattened.update(nested_flat)
            elif isinstance(value, list) and value and isinstance(value[0], dict):
                # For list of dicts, flatten the first one as representative
                # But also track the list itself as a field (for array detection)
                nested_flat = self._flatten_record(value[0], canonical_key)
                flattened.update(nested_flat)
                # Also store the array itself (for type detection)
                flattened[canonical_key] = value
            else:
                # Scalar value (including list of scalars, plain objects)
                # Store it directly
                flattened[canonical_key] = value
        
        return flattened

    def _analyze_flattened_record(self, flattened: dict) -> None:
        """
        Analyze a flattened record and update statistics.
        
        For each flattened field:
        1. Detect its type using TypeDetector
        2. Create or fetch FieldStats for this field
        3. Update stats with the value
        
        Args:
            flattened: A dictionary with flattened dot-notation keys
        """
        for key, value in flattened.items():
            # Detect the type of this value
            detected_type = self.type_detector.detect(value)
            
            # Create or fetch the FieldStats for this field
            if key not in self.stats:
                self.stats[key] = FieldStats(name=key)
            
            # Update the stats with this value
            self.stats[key].update(value, detected_type)

    def _flatten_key(self, prefix: str, key: str) -> str:
        """
        Combine a prefix with a key using dot notation.
        
        Examples:
            _flatten_key("", "username") → "username"
            _flatten_key("metadata", "version") → "metadata.version"
            _flatten_key("data.sensors", "id") → "data.sensors.id"
        
        Args:
            prefix: The parent path (may be empty string)
            key: The current key
            
        Returns:
            The flattened dot-notation key
        """
        if not prefix:
            return key
        return f"{prefix}.{key}"

    def get_stats(self) -> Dict[str, FieldStats]:
        """
        Return all accumulated field statistics.
        
        Returns:
            Dictionary mapping field names to their FieldStats objects
        """
        return self.stats

    def get_presence_ratio(self, field_name: str) -> float:
        """
        Get the presence ratio for a specific field.
        
        Presence ratio = (how many records had this field) / (total records analyzed)
        
        Args:
            field_name: The field to check
            
        Returns:
            A float between 0.0 and 1.0
        """
        if self.total_records == 0:
            return 0.0
        if field_name not in self.stats:
            return 0.0
        
        presence_count = self.stats[field_name].presence_count
        return presence_count / self.total_records

    def reset(self) -> None:
        """
        Clear all accumulated statistics and record count.
        
        Useful for testing or restarting analysis.
        """
        self.stats = {}
        self.total_records = 0

    def get_field_count(self) -> int:
        """
        Get the number of unique fields observed.
        
        Returns:
            Number of different fields seen across all records
        """
        return len(self.stats)