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
        field_locations: list[FieldLocation] = []

        # Process SQL tables and relationships
        for table in sql_tables:
            for column in table.columns:
                field_locations.append(FieldLocation(
                    field_path=column,
                    backend="sql",
                    table_or_collection=table.table_name,
                    column_or_path=column
                ))

        # Process Mongo collections and embedding/reference plans
        for collection in mongo_collections:
            for embedded_path in collection.embedded_paths:
                field_locations.append(FieldLocation(
                    field_path=embedded_path,
                    backend="mongo",
                    table_or_collection=collection.collection_name,
                    column_or_path=embedded_path
                ))
            for ref_path, ref_collection in collection.reference_collections.items():
                relationship = next(
                    (
                        rel
                        for rel in sql_relationships
                        if rel.child_table == ref_collection or rel.parent_table == ref_collection
                    ),
                    None,
                )
                join_keys = []
                if relationship:
                    join_keys = [relationship.parent_key]
                    if relationship.child_foreign_key != relationship.parent_key:
                        join_keys.append(relationship.child_foreign_key)

                field_locations.append(FieldLocation(
                    field_path=ref_path,
                    backend="mongo",
                    table_or_collection=collection.collection_name,
                    column_or_path=ref_path,
                    join_keys=join_keys,
                ))

        return self._deduplicate_locations(field_locations)

    def _deduplicate_locations(self, locations: list[FieldLocation]) -> list[FieldLocation]:
        """Collapse duplicate logical fields while keeping deterministic source selection.

        Strategy:
        - One logical field is identified by `field_path`.
        - Prefer SQL over Mongo if both exist for the same field_path.
        - Merge join keys from all occurrences to preserve linking metadata.
        """
        by_field: dict[str, FieldLocation] = {}

        for location in locations:
            existing = by_field.get(location.field_path)
            if existing is None:
                by_field[location.field_path] = FieldLocation(
                    field_path=location.field_path,
                    backend=location.backend,
                    table_or_collection=location.table_or_collection,
                    column_or_path=location.column_or_path,
                    join_keys=list(location.join_keys),
                )
                continue

            merged_join_keys = list(dict.fromkeys(existing.join_keys + location.join_keys))

            preferred = self._prefer_location(existing, location)
            backend = preferred.backend
            if existing.backend != location.backend:
                backend = "both"

            by_field[location.field_path] = FieldLocation(
                field_path=preferred.field_path,
                backend=backend,
                table_or_collection=preferred.table_or_collection,
                column_or_path=preferred.column_or_path,
                join_keys=merged_join_keys,
            )

        return list(by_field.values())

    @staticmethod
    def _prefer_location(current: FieldLocation, candidate: FieldLocation) -> FieldLocation:
        """Choose canonical storage location for duplicate logical fields."""
        if current.backend == candidate.backend:
            return current
        if current.backend == "sql":
            return current
        if candidate.backend == "sql":
            return candidate
        return current
