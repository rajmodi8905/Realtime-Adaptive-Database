from dataclasses import dataclass, field
from enum import Enum
from typing import Any


class CrudOperation(str, Enum):
    """Supported metadata-driven operations."""

    CREATE = "create"
    READ = "read"
    UPDATE = "update"
    DELETE = "delete"


@dataclass
class SchemaRegistration:
    """User-provided schema registration payload and constraints.

    This structure corresponds to Assignment-2 Phase 1 where users register
    expected object shape, key semantics, and validation constraints.
    """

    schema_name: str
    version: str
    root_entity: str
    json_schema: dict[str, Any]
    constraints: dict[str, Any] = field(default_factory=dict)


@dataclass
class RelationshipPlan:
    """Relational decomposition relationship between parent and child entities."""

    parent_table: str
    child_table: str
    cardinality: str
    parent_key: str
    child_foreign_key: str
    source_path: str


@dataclass
class SqlTablePlan:
    """Normalized SQL table plan generated from schema + observed metadata."""

    table_name: str
    columns: dict[str, str]
    primary_key: str
    foreign_keys: list[dict[str, str]] = field(default_factory=list)
    indexes: list[list[str]] = field(default_factory=list)


@dataclass
class CollectionPlan:
    """MongoDB storage plan for embedded vs referenced documents."""

    collection_name: str
    embedded_paths: list[str] = field(default_factory=list)
    referenced_paths: list[str] = field(default_factory=list)
    reference_collections: dict[str, str] = field(default_factory=dict)


@dataclass
class FieldLocation:
    """Metadata map entry telling where and how a field is persisted."""

    field_path: str
    backend: str
    table_or_collection: str
    column_or_path: str
    join_keys: list[str] = field(default_factory=list)

@dataclass
class QueryPlan:
    """Executable multi-backend query plan produced from user JSON request."""

    operation: CrudOperation
    requested_fields: list[str]
    sql_queries: list[dict[str, Any]] = field(default_factory=list)
    mongo_queries: list[dict[str, Any]] = field(default_factory=list)
    merge_strategy: dict[str, Any] = field(default_factory=dict)


@dataclass
class ClassifiedField:
    """Bridge between A1 classification output and A2 normalization/decomposition engines.

    Carries the full nesting information from A1's FieldAnalyzer + Classifier so that
    A2 engines can detect repeating groups (for SQL table splitting) and embedding vs
    referencing decisions (for MongoDB) without re-running classification.
    """

    field_path: str            # dot-notation path, e.g. "post.comments.text"
    backend: str               # "SQL", "MONGODB", or "BOTH"
    canonical_type: str        # "int", "str", "array", "object", etc.
    is_array: bool = False     # True if dominant type is "array"
    is_nested: bool = False    # True if value was dict or list
    nesting_depth: int = 0     # number of dots in field_path
    sql_type: str | None = None
    is_primary_key: bool = False
    is_nullable: bool = True
    is_unique: bool = False
    parent_path: str = ""      # parent entity path, e.g. "post.comments" → parent is "post"

    @classmethod
    def from_a1_decision(cls, decision, stats=None) -> "ClassifiedField":
        """Convert an A1 PlacementDecision (+ optional FieldStats) into a ClassifiedField.

        Args:
            decision: A1 PlacementDecision instance from src.analysis.decision
            stats: Optional A1 FieldStats instance from src.analysis.field_stats
        """
        field_path = decision.field_name
        parts = field_path.rsplit(".", 1)
        parent = parts[0] if len(parts) > 1 else ""

        canonical = decision.canonical_type or ""
        is_array = canonical == "array"
        is_nested = stats.is_nested if stats else (canonical in ("array", "object"))
        depth = stats.nesting_depth if stats else field_path.count(".")

        return cls(
            field_path=field_path,
            backend=decision.backend.value if hasattr(decision.backend, "value") else str(decision.backend),
            canonical_type=canonical,
            is_array=is_array,
            is_nested=is_nested,
            nesting_depth=depth,
            sql_type=decision.sql_type,
            is_primary_key=decision.is_primary_key,
            is_nullable=decision.is_nullable,
            is_unique=decision.is_unique,
            parent_path=parent,
        )
    
    

