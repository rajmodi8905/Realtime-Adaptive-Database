"""Assignment 2 extension package.

This package contains the Autonomous Normalization & CRUD Engine components
required by Assignment 2. The A1 pipeline (IngestAndClassify) is consumed
as a dependency.
"""

from .contracts import (
    ClassifiedField,
    CollectionPlan,
    CrudOperation,
    FieldLocation,
    QueryPlan,
    RelationshipPlan,
    SchemaRegistration,
    SqlTablePlan,
)
from .crud_engine import CrudEngine
from .metadata_catalog import MetadataCatalog
from .mongo_decomposition_engine import MongoDecompositionEngine
from .orchestrator import Assignment2Pipeline
from .query_planner import QueryPlanner
from .schema_registry import SchemaRegistry
from .sql_normalization_engine import SqlNormalizationEngine
from .storage_strategy_generator import StorageStrategyGenerator

__all__ = [
    "Assignment2Pipeline",
    "ClassifiedField",
    "CollectionPlan",
    "CrudEngine",
    "CrudOperation",
    "FieldLocation",
    "MetadataCatalog",
    "MongoDecompositionEngine",
    "QueryPlan",
    "QueryPlanner",
    "RelationshipPlan",
    "SchemaRegistration",
    "SchemaRegistry",
    "SqlNormalizationEngine",
    "SqlTablePlan",
    "StorageStrategyGenerator",
]
