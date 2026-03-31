from dataclasses import dataclass, field
from typing import Any


@dataclass
class TransactionResult:
    """Result of a coordinated multi-backend operation."""

    status: str  # "committed", "rolled_back", "error"
    operation: str
    sql_result: dict[str, Any] = field(default_factory=dict)
    mongo_result: dict[str, Any] = field(default_factory=dict)
    rolled_back: bool = False
    errors: list[str] = field(default_factory=list)


@dataclass
class SessionInfo:
    """Active session metadata for the dashboard."""

    schema_name: str
    version: str
    root_entity: str
    mysql_connected: bool
    mongo_connected: bool
    sql_tables: list[str] = field(default_factory=list)
    mongo_collections: list[str] = field(default_factory=list)
    field_count: int = 0


@dataclass
class LogicalEntity:
    """Unified logical entity with clean field names and instance data."""

    entity_name: str
    fields: list[str] = field(default_factory=list)
    instances: list[dict[str, Any]] = field(default_factory=list)


@dataclass
class AcidTestResult:
    """Result of a single ACID validation experiment."""

    property_name: str
    passed: bool
    description: str
    duration_ms: float = 0.0
    details: dict[str, Any] = field(default_factory=dict)
