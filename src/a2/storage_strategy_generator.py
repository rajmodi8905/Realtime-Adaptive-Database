from .contracts import CollectionPlan, FieldLocation, RelationshipPlan, SqlTablePlan


class StorageStrategyGenerator:
    """Combines SQL + Mongo planning into executable field-level routing metadata.

    Working model:
    1. Accept normalized SQL plans and Mongo collection plans.
    2. Resolve overlap rules and linking keys.
    3. Produce canonical FieldLocation mappings consumed by CRUD engine.
    """

    def generate_field_locations(
        self,
        sql_tables: list[SqlTablePlan],
        sql_relationships: list[RelationshipPlan],
        mongo_collections: list[CollectionPlan],
    ) -> list[FieldLocation]:
        """Generate final field-level storage map."""
        raise NotImplementedError("Implement strategy fusion logic")
