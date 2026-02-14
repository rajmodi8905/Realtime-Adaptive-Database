import json
import os
from pathlib import Path
from typing import Dict, Tuple, Any
from datetime import datetime


# ==============================================
# MetadataStore
# ==============================================
#
# PURPOSE:
#   Persist all framework metadata to disk so that the system
#   can recover after a restart without re-analyzing data.
#
# WHY THIS CLASS EXISTS:
#   The assignment explicitly requires:
#     "Metadata Persistence: Ability to remember decisions across restarts."
#   If the process crashes after classifying 1000 records, on restart
#   it should NOT re-analyze. It should load previous decisions and
#   continue from where it left off.
#
# WHAT IS PERSISTED:
#   1. PlacementDecisions   → Which field goes to which backend
#   2. FieldStats           → Accumulated statistics per field
#   3. Field name mappings  → Original name → canonical name map
#   4. Total records count  → How many records were observed
#
class PlacementDecision:
    """Represents a decision about where to store a field"""
    def __init__(self, backend: str, sql_type: str = None, reasoning: str = ""):
        self.backend = backend  # "SQL" or "NoSQL"
        self.sql_type = sql_type  # e.g., "VARCHAR(50)", "INTEGER"
        self.reasoning = reasoning
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "backend": self.backend,
            "sql_type": self.sql_type,
            "reasoning": self.reasoning
        }
    
    @staticmethod
    def from_dict(data: dict) -> 'PlacementDecision':
        """Create from dictionary (deserialization)"""
        return PlacementDecision(
            backend=data.get("backend"),
            sql_type=data.get("sql_type"),
            reasoning=data.get("reasoning", "")
        )


class FieldStats:
    """Statistics about a field collected during analysis"""
    def __init__(self):
        self.presence_count = 0  # How many records have this field
        self.total_records = 0   # Total records seen
        self.type_counts = {}    # {"string": 100, "integer": 50, "null": 10}
        self.min_length = float('inf')
        self.max_length = 0
        self.avg_length = 0.0
        self.sample_values = []  # Keep a few examples
    
    def to_dict(self) -> dict:
        """Convert to dictionary for JSON serialization"""
        return {
            "presence_count": self.presence_count,
            "total_records": self.total_records,
            "type_counts": self.type_counts,
            "min_length": self.min_length if self.min_length != float('inf') else 0,
            "max_length": self.max_length,
            "avg_length": self.avg_length,
            "sample_values": self.sample_values[:10]  # Only save first 10
        }
    
    @staticmethod
    def from_dict(data: dict) -> 'FieldStats':
        """Create from dictionary (deserialization)"""
        stats = FieldStats()
        stats.presence_count = data.get("presence_count", 0)
        stats.total_records = data.get("total_records", 0)
        stats.type_counts = data.get("type_counts", {})
        stats.min_length = data.get("min_length", 0)
        stats.max_length = data.get("max_length", 0)
        stats.avg_length = data.get("avg_length", 0.0)
        stats.sample_values = data.get("sample_values", [])
        return stats
    

# CLASS: MetadataStore
# --------------------
#   Stateful — holds a reference to the storage directory.
#
#   Constructor:
#   ------------
#   - __init__(storage_dir: str = "metadata/")
#       Create storage directory if it doesn't exist.
#
class MetadataStore:
    """
    Handles persistence of all framework metadata to disk.
    
    Files created:
    - metadata/decisions.json      → Placement decisions
    - metadata/field_stats.json    → Field statistics
    - metadata/name_mappings.json  → Field name mappings
    - metadata/state.json          → Pipeline state
    """
    
    def __init__(self, storage_dir: str = "metadata/"):
        """
        Initialize the metadata store.
        
        Args:
            storage_dir: Directory to store metadata files
        """
        self.storage_dir = Path(storage_dir)
        
        # Create directory if it doesn't exist
        self.storage_dir.mkdir(parents=True, exist_ok=True)
        
        # Define file paths
        self.decisions_file = self.storage_dir / "decisions.json"
        self.stats_file = self.storage_dir / "field_stats.json"
        self.state_file = self.storage_dir / "state.json"
#   Methods:
#   --------
#   SAVING:
#   - save_decisions(decisions: dict[str, PlacementDecision]) -> None
#       Serialize decisions to JSON file.
#
#   - save_field_stats(stats: dict[str, FieldStats]) -> None
#       Serialize field stats to JSON file.
#
#   - save_state(total_records: int) -> None
#       Save pipeline state (record count, last flush time, etc.)
#
#   - save_all(decisions, stats, total_records) -> None
#       Convenience method to save everything at once.
#
    def save_decisions(self, decisions: Dict[str, PlacementDecision]) -> None:
        """
        Save placement decisions to disk.
        
        Args:
            decisions: Dictionary mapping field_name -> PlacementDecision
        """
        # Convert PlacementDecision objects to dictionaries
        decisions_dict = {
            field: decision.to_dict() 
            for field, decision in decisions.items()
        }
        
        with open(self.decisions_file, 'w') as f:
            json.dump(decisions_dict, f, indent=2)
        
        print(f"Saved {len(decisions)} decisions to {self.decisions_file}")
    
    def save_field_stats(self, stats: Dict[str, FieldStats]) -> None:
        """
        Save field statistics to disk.
        
        Args:
            stats: Dictionary mapping field_name -> FieldStats
        """
        # Convert FieldStats objects to dictionaries
        stats_dict = {
            field: stat.to_dict() 
            for field, stat in stats.items()
        }
        
        with open(self.stats_file, 'w') as f:
            json.dump(stats_dict, f, indent=2)
        
        print(f"Saved stats for {len(stats)} fields to {self.stats_file}")
    
    def save_state(self, total_records: int) -> None:
        """
        Save pipeline state to disk.
        
        Args:
            total_records: Total number of records processed
        """
        state = {
            "total_records": total_records,
            "last_flush": datetime.now().isoformat(),
            "version": "1.0"
        }
        
        with open(self.state_file, 'w') as f:
            json.dump(state, f, indent=2)
        
        print(f"Saved state (total_records={total_records}) to {self.state_file}")
    
    def save_all(
        self, 
        decisions: Dict[str, PlacementDecision],
        stats: Dict[str, FieldStats],
        total_records: int
    ) -> None:
        """
        Convenience method to save everything at once.
        
        Args:
            decisions: Placement decisions
            stats: Field statistics
            total_records: Total records processed
        """
        self.save_decisions(decisions)
        self.save_field_stats(stats)
        self.save_state(total_records)
        print(f"All metadata saved successfully!")
#   LOADING:
#   - load_decisions() -> dict[str, PlacementDecision]
#       Deserialize decisions from JSON file. Return empty dict if no file.
#
#   - load_field_stats() -> dict[str, FieldStats]
#       Deserialize field stats. Return empty dict if no file.
#
#   - load_state() -> dict
#       Load pipeline state. Return defaults if no file.
#
#   - load_all() -> tuple
#       Convenience method to load everything at once.
#
    def load_decisions(self) -> Dict[str, PlacementDecision]:
        """
        Load placement decisions from disk.
        
        Returns:
            Dictionary mapping field_name -> PlacementDecision
            Empty dict if file doesn't exist
        """
        if not self.decisions_file.exists():
            print(f"No decisions file found at {self.decisions_file}")
            return {}
        
        with open(self.decisions_file, 'r') as f:
            decisions_dict = json.load(f)
        
        # Convert dictionaries back to PlacementDecision objects
        decisions = {
            field: PlacementDecision.from_dict(data)
            for field, data in decisions_dict.items()
        }
        
        print(f"Loaded {len(decisions)} decisions from {self.decisions_file}")
        return decisions
    
    def load_field_stats(self) -> Dict[str, FieldStats]:
        """
        Load field statistics from disk.
        
        Returns:
            Dictionary mapping field_name -> FieldStats
            Empty dict if file doesn't exist
        """
        if not self.stats_file.exists():
            print(f" No stats file found at {self.stats_file}")
            return {}
        
        with open(self.stats_file, 'r') as f:
            stats_dict = json.load(f)
        
        # Convert dictionaries back to FieldStats objects
        stats = {
            field: FieldStats.from_dict(data)
            for field, data in stats_dict.items()
        }
        
        print(f"Loaded stats for {len(stats)} fields from {self.stats_file}")
        return stats
    
    def load_state(self) -> Dict[str, Any]:
        """
        Load pipeline state from disk.
        
        Returns:
            Dictionary with state information
            Default values if file doesn't exist
        """
        if not self.state_file.exists():
            print(f"No state file found at {self.state_file}")
            return {
                "total_records": 0,
                "last_flush": None,
                "version": "1.0"
            }
        
        with open(self.state_file, 'r') as f:
            state = json.load(f)
        
        print(f"Loaded state from {self.state_file}")
        return state
    
    def load_all(self) -> Tuple[Dict, Dict, Dict]:
        """
        Convenience method to load everything at once.
        
        Returns:
            Tuple of (decisions, stats, state)
        """
        decisions = self.load_decisions()
        stats = self.load_field_stats()
        state = self.load_state()
        
        print(f"All metadata loaded successfully!")
        return decisions, stats, state
    
#   UTILITY:
#   - exists() -> bool
#       Check if any metadata files exist (i.e., is this a restart?).
#
#   - clear() -> None
#       Delete all metadata files (for testing or reset).
#
    def exists(self) -> bool:
        """
        Check if any metadata files exist.
        
        Returns:
            True if this is a restart (metadata exists), False if fresh start
        """
        return (
            self.decisions_file.exists() or 
            self.stats_file.exists() or 
            self.state_file.exists()
        )
    
    def clear(self) -> None:
        """
        Delete all metadata files (for testing or reset).
        """
        files_to_delete = [
            self.decisions_file,
            self.stats_file,
            self.state_file
        ]
        
        for file in files_to_delete:
            if file.exists():
                file.unlink()
                print(f"🗑️  Deleted {file}")
        
        print(f"All metadata cleared!")
# FILE STRUCTURE:
# ---------------
#   metadata/
#   ├── decisions.json      → {field_name: {backend, sql_type, ...}}
#   ├── field_stats.json    → {field_name: {presence_count, type_counts, ...}}
#   └── state.json          → {total_records, last_flush, ...}
#
# =============================================

