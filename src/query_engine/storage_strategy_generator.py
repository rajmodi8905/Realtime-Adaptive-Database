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

        sql_entity_paths = self._build_sql_entity_paths(
            registration.root_entity,
            sql_relationships,
        )

        # Process SQL tables and relationships
        for table in sql_tables:
            foreign_keys = [fk['column'] for fk in table.foreign_keys]
            entity_path = sql_entity_paths.get(
                table.table_name,
                "" if table.table_name == registration.root_entity else table.table_name,
            )
            for column in table.columns:
                is_foreign_key = column in foreign_keys
                logical_field_path = self._derive_sql_field_path(
                    entity_path=entity_path,
                    column=column,
                    is_foreign_key=is_foreign_key,
                )
                join_keys = [table.primary_key] if not is_foreign_key else []
                normalized_join_keys = self._normalize_join_keys(
                    join_keys + [fk['column'] for fk in table.foreign_keys],
                    field_path=logical_field_path,
                    column_or_path=column,
                )
                field_locations.append(FieldLocation(
                    field_path=logical_field_path,
                    backend="sql",
                    table_or_collection=table.table_name,
                    column_or_path=column,
                    join_keys=normalized_join_keys,
                ))

        # primary key of root_entity in SQL
        root_sql_table = next((t for t in sql_tables if t.table_name == registration.root_entity), None)
        root_primary_key = root_sql_table.primary_key if root_sql_table else None
        join_keys = [root_primary_key] if root_primary_key else []

        # Process Mongo collections and embedding/reference plans
        root_mongo_collections = {
            collection.collection_name
            for collection in mongo_collections
            if collection.reference_collections or collection.referenced_paths
        }

        for collection in mongo_collections:
            for embedded_path in collection.embedded_paths:
                field_locations.append(FieldLocation(
                    field_path=embedded_path,
                    backend="mongo",
                    table_or_collection=collection.collection_name,
                    column_or_path=embedded_path,
                    join_keys=self._normalize_join_keys(
                        join_keys,
                        field_path=embedded_path,
                        column_or_path=embedded_path,
                    ),
                ))
            for ref_path in collection.referenced_paths:
                field_locations.append(FieldLocation(
                    field_path=ref_path,
                    backend="mongo",
                    table_or_collection=collection.reference_collections[ref_path],
                    column_or_path=ref_path,
                    join_keys=self._normalize_join_keys(
                        join_keys,
                        field_path=ref_path,
                        column_or_path=ref_path,
                    ),
                ))

        return self._deduplicate_locations(
            field_locations,
            sql_relationships,
            root_mongo_collections,
        )

    def _deduplicate_locations(
        self,
        locations: list[FieldLocation],
        sql_relationships: list[RelationshipPlan],
        root_mongo_collections: set[str],
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
                    join_keys=self._normalize_join_keys(
                        location.join_keys,
                        field_path=location.field_path,
                        column_or_path=location.column_or_path,
                    ),
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
                merged_join_keys = self._normalize_join_keys(
                    existing.join_keys,
                    field_path=location.field_path,
                    column_or_path=location.column_or_path,
                )
            elif location_is_parent:
                merged_join_keys = self._normalize_join_keys(
                    location.join_keys,
                    field_path=location.field_path,
                    column_or_path=location.column_or_path,
                )
            else:
                merged_join_keys = self._normalize_join_keys(
                    existing.join_keys + location.join_keys,
                    field_path=location.field_path,
                    column_or_path=location.column_or_path,
                )

            preferred = self._prefer_location(
                existing,
                location,
                parent_pairs,
                root_mongo_collections,
            )
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
    def _prefer_location(
        current: FieldLocation,
        candidate: FieldLocation,
        parent_pairs: set[tuple[str, str]],
        root_mongo_collections: set[str],
    ) -> FieldLocation:
        """Choose canonical storage location for duplicate logical fields.

        Priority:
        1. Prefer SQL over Mongo when backend differs.
        2. If same backend and parent-child relation exists, prefer parent location.
        3. Otherwise prefer richer join context (more join keys), then stable name order.
        """
        if current.backend != candidate.backend:
            if current.backend == "sql":
                return current
            if candidate.backend == "sql":
                return candidate
            return current

        if current.table_or_collection == candidate.table_or_collection:
            return current

        if current.backend == "mongo" and candidate.backend == "mongo":
            current_is_root = current.table_or_collection in root_mongo_collections
            candidate_is_root = (
                candidate.table_or_collection in root_mongo_collections
            )
            if current_is_root and not candidate_is_root:
                return current
            if candidate_is_root and not current_is_root:
                return candidate

        current_is_parent = (
            current.table_or_collection,
            candidate.table_or_collection,
        ) in parent_pairs
        candidate_is_parent = (
            candidate.table_or_collection,
            current.table_or_collection,
        ) in parent_pairs

        if current_is_parent:
            return current
        if candidate_is_parent:
            return candidate

        if len(candidate.join_keys) > len(current.join_keys):
            return candidate
        if len(current.join_keys) > len(candidate.join_keys):
            return current

        return (
            current
            if current.table_or_collection <= candidate.table_or_collection
            else candidate
        )

    @staticmethod
    def _normalize_join_keys(
        keys: list[str],
        field_path: str,
        column_or_path: str,
    ) -> list[str]:
        """Normalize join keys by removing empties, self-keys, and duplicates."""
        normalized: list[str] = []
        seen: set[str] = set()

        for key in keys:
            if not key:
                continue
            if key in (field_path, column_or_path):
                continue
            if key in seen:
                continue
            seen.add(key)
            normalized.append(key)

        return normalized

    @staticmethod
    def _build_sql_entity_paths(
        root_entity: str,
        sql_relationships: list[RelationshipPlan],
    ) -> dict[str, str]:
        """Map SQL table names to logical JSON entity paths."""
        entity_paths: dict[str, str] = {root_entity: ""}
        for relationship in sql_relationships:
            source_path = (relationship.source_path or "").strip()
            if not source_path:
                continue
            entity_paths.setdefault(relationship.child_table, source_path)
        return entity_paths

    @staticmethod
    def _derive_sql_field_path(
        entity_path: str,
        column: str,
        is_foreign_key: bool,
    ) -> str:
        """Build a logical JSON field path for a SQL column.

        Root-table and FK columns keep plain logical names for linking.
        Child-table non-FK attributes are namespaced by entity path.
        """
        if is_foreign_key or not entity_path:
            return column
        if column.startswith(f"{entity_path}."):
            return column
        return f"{entity_path}.{column}"
