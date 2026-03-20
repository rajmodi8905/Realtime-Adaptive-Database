from .contracts import CollectionPlan, FieldLocation, RelationshipPlan, SqlTablePlan, SchemaRegistration


class StorageStrategyGenerator:
    """Combines SQL + Mongo planning into executable field-level routing metadata.

    Working model:
    1. Accept normalized SQL plans and Mongo collection plans.
    2. Resolve overlap rules and linking keys.
    3. Produce canonical FieldLocation mappings consumed by CRUD engine.
    """

    def generate_field_locations(
        self,
        registration: SchemaRegistration,
        sql_tables: list[SqlTablePlan],
        sql_relationships: list[RelationshipPlan],
        mongo_collections: list[CollectionPlan],
    ) -> list[FieldLocation]:
        """Generate final field-level storage map."""
        field_locations: list[FieldLocation] = []

        # Process SQL tables and relationships
        for table in sql_tables:
            foreign_keys = [fk['column'] for fk in table.foreign_keys]
            for column in table.columns:
                is_foreign_key = column in foreign_keys
                join_keys = [table.primary_key] if not is_foreign_key else []
                field_locations.append(FieldLocation(
                    field_path=column,
                    backend="sql",
                    table_or_collection=table.table_name,
                    column_or_path=column,
                    join_keys=join_keys + [fk['column'] for fk in table.foreign_keys],
                ))

        # primary key of root_entity in SQL
        root_sql_table = next((t for t in sql_tables if t.table_name == registration.root_entity), None)
        root_primary_key = root_sql_table.primary_key if root_sql_table else None
        join_keys = [root_primary_key] if root_primary_key else []

        # Process Mongo collections and embedding/reference plans
        for collection in mongo_collections:
            for embedded_path in collection.embedded_paths:
                field_locations.append(FieldLocation(
                    field_path=embedded_path,
                    backend="mongo",
                    table_or_collection=collection.collection_name,
                    column_or_path=embedded_path,
                    join_keys=join_keys,
                ))
            for ref_path in collection.referenced_paths:
                field_locations.append(FieldLocation(
                    field_path=ref_path,
                    backend="mongo",
                    table_or_collection=collection.collection_name,
                    column_or_path=ref_path,
                    join_keys=join_keys,
                ))

        return self._deduplicate_locations(field_locations, sql_relationships)

    def _deduplicate_locations(
        self,
        locations: list[FieldLocation],
        sql_relationships: list[RelationshipPlan],
    ) -> list[FieldLocation]:
        """Collapse duplicate logical fields while keeping deterministic source selection.

        Strategy:
        - One logical field is identified by `field_path`.
        - Prefer SQL over Mongo if both exist for the same field_path.
        - Merge join keys from all occurrences to preserve linking metadata.
        """
        by_field: dict[str, FieldLocation] = {}
        parent_pairs = {
            (rel.parent_table, rel.child_table)
            for rel in sql_relationships
        }

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
            
            existing_is_parent = (
                existing.table_or_collection,
                location.table_or_collection,
            ) in parent_pairs
            location_is_parent = (
                location.table_or_collection,
                existing.table_or_collection,
            ) in parent_pairs

            if existing_is_parent:
                merged_join_keys = list(dict.fromkeys(k for k in existing.join_keys if k))
            elif location_is_parent:
                merged_join_keys = list(dict.fromkeys(k for k in location.join_keys if k))
            else:
                merged_join_keys = list(
                    dict.fromkeys(
                        k for k in (existing.join_keys + location.join_keys) if k
                    )
                )

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
