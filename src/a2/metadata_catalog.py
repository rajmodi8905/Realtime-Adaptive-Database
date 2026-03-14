from .contracts import CollectionPlan, FieldLocation, RelationshipPlan, SchemaRegistration, SqlTablePlan


class MetadataCatalog:
    """Central metadata manager for Assignment 2.

    Working model:
    - Stores schema registrations.
    - Stores SQL normalization output (table/relationship plans).
    - Stores Mongo decomposition output (embedding/reference decisions).
    - Stores field-to-storage routing map used by query planner.

    This module is the core enabler for metadata-driven CRUD generation.
    """

    def save_schema(self, registration: SchemaRegistration) -> None:
        """Persist schema registration and version metadata."""
        raise NotImplementedError("Implement schema persistence")

    def save_sql_plan(self, tables: list[SqlTablePlan], relationships: list[RelationshipPlan]) -> None:
        """Persist normalized relational planning artifacts."""
        raise NotImplementedError("Implement SQL plan persistence")

    def save_mongo_plan(self, collections: list[CollectionPlan]) -> None:
        """Persist Mongo embedding/reference planning artifacts."""
        raise NotImplementedError("Implement Mongo plan persistence")

    def save_field_locations(self, mappings: list[FieldLocation]) -> None:
        """Persist field-level storage location metadata."""
        raise NotImplementedError("Implement field map persistence")

    def get_field_locations(self) -> list[FieldLocation]:
        """Return the current field routing map for query planning."""
        raise NotImplementedError("Implement field map retrieval")
