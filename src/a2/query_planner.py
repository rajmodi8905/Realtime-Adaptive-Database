from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from .contracts import CrudOperation, FieldLocation, QueryPlan


class QueryPlanner:
    """Phase 6: Metadata-driven query plan generator.

    Working model:
    - Input: operation request JSON + field location metadata.
    - Output: backend-specific SQL/Mongo query plans + merge strategy.

    The planner stays backend-agnostic and only emits plans.
    Execution is delegated to CrudEngine.
    """

    def build_plan(self, operation: CrudOperation, payload: dict, field_locations: list[FieldLocation]) -> QueryPlan:
        """Generate a query plan for create/read/update/delete."""
        op = self._normalize_operation(operation)
        payload = payload or {}
        field_index = {f.field_path: f for f in field_locations}

        if op == CrudOperation.READ:
            return self._build_read_plan(payload, field_locations, field_index)
        if op == CrudOperation.CREATE:
            return self._build_create_plan(payload, field_locations, field_index)
        if op == CrudOperation.UPDATE:
            return self._build_update_plan(payload, field_locations, field_index)
        if op == CrudOperation.DELETE:
            return self._build_delete_plan(payload, field_locations, field_index)

        raise ValueError(f"Unsupported operation: {operation}")

    def _build_read_plan(self, payload: dict, field_locations: list[FieldLocation],field_index: dict[str, FieldLocation]) -> QueryPlan:
        requested_fields = self._resolve_requested_fields(
            list(payload.get("fields") or payload.get("requested_fields") or []),
            field_index,
        )
        if not requested_fields:
            requested_fields = sorted(field_index.keys())

        filters = self._resolve_filters(
            payload.get("filters") or payload.get("where") or {},
            field_index,
        )
        limit = payload.get("limit")
        offset = payload.get("offset")
        sort = payload.get("sort")

        sql_by_table: dict[str, set[str]] = {}
        mongo_by_collection: dict[str, set[str]] = {}

        for field in requested_fields:
            location = field_index.get(field)
            if not location:
                continue
            backend = location.backend.lower()
            if backend in ("sql", "both"):
                sql_by_table.setdefault(location.table_or_collection, set()).add(location.column_or_path)
            if backend == "mongo":
                mongo_by_collection.setdefault(location.table_or_collection, set()).add(location.column_or_path)

        sql_where, mongo_filter = self._split_filters(filters, field_index)

        sql_queries = [
            {
                "type": "select",
                "table": table,
                "columns": sorted(columns),
                "where": sql_where.get(table, {}),
                "limit": limit,
                "offset": offset,
                "sort": sort,
            }
            for table, columns in sorted(sql_by_table.items())
        ]

        mongo_queries = [
            {
                "type": "find",
                "collection": collection,
                "projection": sorted(paths),
                "filter": mongo_filter.get(collection, {}),
                "limit": limit,
                "sort": sort,
            }
            for collection, paths in sorted(mongo_by_collection.items())
        ]

        merge_strategy = {
            "mode": "keyed_merge",
            "join_keys": self._infer_join_keys(field_locations),
            "requested_fields": requested_fields,
            "source_priority": ["sql", "mongo"],
            "conflict_policy": "prefer_sql",
            "missing_field_policy": "omit",
        }

        return QueryPlan(
            operation=CrudOperation.READ,
            requested_fields=requested_fields,
            sql_queries=sql_queries,
            mongo_queries=mongo_queries,
            merge_strategy=merge_strategy,
        )

    def _build_create_plan(self, payload: dict, field_locations: list[FieldLocation], field_index: dict[str, FieldLocation]) -> QueryPlan:
        records = payload.get("records")
        if records is None:
            if "record" in payload and isinstance(payload["record"], dict):
                records = [payload["record"]]
            elif isinstance(payload, dict):
                records = [payload]
            else:
                records = []

        sql_rows: dict[str, list[dict[str, Any]]] = {}
        mongo_docs: dict[str, list[dict[str, Any]]] = {}
        join_keys = self._infer_join_keys(field_locations)

        sql_locations_by_table: dict[str, list[FieldLocation]] = {}
        for loc in field_locations:
            if loc.backend.lower() in ("sql", "both"):
                sql_locations_by_table.setdefault(loc.table_or_collection, []).append(loc)

        table_entity_paths: dict[str, str] = {
            table: self._infer_entity_path(locations)
            for table, locations in sql_locations_by_table.items()
        }

        for record in records:
            if not isinstance(record, dict):
                continue

            collection_doc_buffer: dict[str, dict[str, Any]] = {}

            for table, locations in sql_locations_by_table.items():
                entity_path = table_entity_paths.get(table, "")
                contexts = self._expand_entity_contexts(record, entity_path)

                for context_node, ancestors in contexts:
                    row: dict[str, Any] = {}
                    for loc in locations:
                        value = self._extract_sql_value(
                            record=record,
                            context_node=context_node,
                            ancestors=ancestors,
                            entity_path=entity_path,
                            location=loc,
                        )
                        if value is _MISSING:
                            continue
                        row[loc.column_or_path] = value

                    required_join_keys = {
                        key
                        for location in locations
                        for key in location.join_keys
                        if key
                    }
                    for key in sorted(required_join_keys):
                        if key in row:
                            continue
                        key_value = self._extract_join_key_value(
                            record,
                            ancestors,
                            key,
                        )
                        if key_value is _MISSING:
                            continue
                        row[key] = key_value

                    if row:
                        sql_rows.setdefault(table, []).append(row)

            for loc in field_locations:
                value = self._extract_value(record, loc.field_path)
                if value is _MISSING:
                    continue

                backend = loc.backend.lower()
                if backend in ("mongo", "both"):
                    doc = collection_doc_buffer.setdefault(loc.table_or_collection, {})
                    self._set_dotted_path(doc, loc.column_or_path, value)

            root_join_key = next((key for key in join_keys if key in record), None)
            if root_join_key:
                root_value = record[root_join_key]
                for doc in collection_doc_buffer.values():
                    doc.setdefault(root_join_key, root_value)

            # Ensure required ingest timestamp exists when metadata expects it.
            if "sys_ingested_at" not in record:
                now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                for loc in field_locations:
                    if loc.field_path != "sys_ingested_at":
                        continue
                    backend = loc.backend.lower()
                    if backend in ("sql", "both"):
                        table = loc.table_or_collection
                        rows_for_table = sql_rows.setdefault(table, [])
                        if rows_for_table:
                            for row in rows_for_table:
                                row.setdefault(loc.column_or_path, now_utc)
                        else:
                            rows_for_table.append({loc.column_or_path: now_utc})
                    if backend in ("mongo", "both"):
                        doc = collection_doc_buffer.setdefault(loc.table_or_collection, {})
                        self._set_dotted_path(doc, loc.column_or_path, now_utc)

            for collection, doc in collection_doc_buffer.items():
                if doc:
                    mongo_docs.setdefault(collection, []).append(doc)

        sql_queries = [
            {"type": "insert_batch", "table": table, "rows": rows}
            for table, rows in sorted(sql_rows.items())
            if rows
        ]
        mongo_queries = [
            {"type": "insert_batch", "collection": collection, "documents": docs}
            for collection, docs in sorted(mongo_docs.items())
            if docs
        ]

        requested_fields = sorted({k for r in records if isinstance(r, dict) for k in r.keys()})
        return QueryPlan(
            operation=CrudOperation.CREATE,
            requested_fields=requested_fields,
            sql_queries=sql_queries,
            mongo_queries=mongo_queries,
            merge_strategy={"mode": "none"},
        )

    @staticmethod
    def _infer_entity_path(locations: list[FieldLocation]) -> str:
        prefixes: list[str] = []
        for loc in locations:
            if "." not in loc.field_path:
                continue
            prefixes.append(loc.field_path.rsplit(".", 1)[0])

        if not prefixes:
            return ""

        common = prefixes[0]
        for prefix in prefixes[1:]:
            common = QueryPlanner._common_dot_prefix(common, prefix)
            if not common:
                break
        return common

    @staticmethod
    def _common_dot_prefix(left: str, right: str) -> str:
        left_parts = left.split(".")
        right_parts = right.split(".")
        common_parts: list[str] = []

        for left_part, right_part in zip(left_parts, right_parts):
            if left_part != right_part:
                break
            common_parts.append(left_part)

        return ".".join(common_parts)

    @staticmethod
    def _expand_entity_contexts(
        record: dict[str, Any],
        entity_path: str,
    ) -> list[tuple[Any, list[dict[str, Any]]]]:
        if not entity_path:
            return [(record, [record])]

        contexts: list[tuple[Any, list[dict[str, Any]]]] = [(record, [record])]
        for segment in entity_path.split("."):
            next_contexts: list[tuple[Any, list[dict[str, Any]]]] = []
            for node, ancestors in contexts:
                if not isinstance(node, dict):
                    continue
                value = node.get(segment, _MISSING)
                if value is _MISSING:
                    continue

                if isinstance(value, list):
                    for item in value:
                        if isinstance(item, dict):
                            next_contexts.append((item, ancestors + [item]))
                        else:
                            next_contexts.append((item, ancestors))
                elif isinstance(value, dict):
                    next_contexts.append((value, ancestors + [value]))
                else:
                    next_contexts.append((value, ancestors))

            contexts = next_contexts
            if not contexts:
                break

        return contexts

    @staticmethod
    def _extract_value_from_context(context_node: Any, relative_path: str) -> Any:
        if (
            not isinstance(context_node, dict)
            and (relative_path == "value" or relative_path.endswith("_value"))
        ):
            return context_node
        if not relative_path:
            return context_node

        current: Any = context_node
        for segment in relative_path.split("."):
            if not isinstance(current, dict) or segment not in current:
                return _MISSING
            current = current[segment]
        return current

    @staticmethod
    def _extract_sql_value(
        record: dict[str, Any],
        context_node: Any,
        ancestors: list[dict[str, Any]],
        entity_path: str,
        location: FieldLocation,
    ) -> Any:
        field_path = location.field_path

        if entity_path and field_path.startswith(f"{entity_path}."):
            relative = field_path[len(entity_path) + 1:]
            value = QueryPlanner._extract_value_from_context(context_node, relative)
        else:
            value = QueryPlanner._extract_value(record, field_path)

        if value is not _MISSING:
            return value

        if "." not in field_path:
            for ancestor in reversed(ancestors):
                if field_path in ancestor:
                    return ancestor[field_path]

        if field_path == "value" and not isinstance(context_node, dict):
            return context_node

        return _MISSING

    @staticmethod
    def _extract_join_key_value(
        record: dict[str, Any],
        ancestors: list[dict[str, Any]],
        key: str,
    ) -> Any:
        value = QueryPlanner._extract_value(record, key)
        if value is not _MISSING:
            return value

        for ancestor in reversed(ancestors):
            if key in ancestor:
                return ancestor[key]

        return _MISSING

    def _build_update_plan(self, payload: dict, field_locations: list[FieldLocation], field_index: dict[str, FieldLocation]) -> QueryPlan:
        updates = self._resolve_filters(
            payload.get("updates") or payload.get("set") or {},
            field_index,
        )
        filters = self._resolve_filters(
            payload.get("filters") or payload.get("where") or {},
            field_index,
        )
        join_keys = self._infer_join_keys(field_locations)

        sql_set: dict[str, dict[str, Any]] = {}
        mongo_set: dict[str, dict[str, Any]] = {}
        for field_path, value in updates.items():
            loc = field_index.get(field_path)
            if not loc:
                continue
            backend = loc.backend.lower()
            if backend in ("sql", "both"):
                sql_set.setdefault(loc.table_or_collection, {})[loc.column_or_path] = value
            if backend in ("mongo", "both"):
                mongo_set.setdefault(loc.table_or_collection, {})[loc.column_or_path] = value

        sql_where, mongo_filter = self._split_filters(filters, field_index)

        cross_backend_filter = {
            field: value
            for field, value in filters.items()
            if field in join_keys
        }

        sql_filterable_columns: dict[str, set[str]] = {}
        for loc in field_locations:
            if loc.backend.lower() not in ("sql", "both"):
                continue
            cols = sql_filterable_columns.setdefault(loc.table_or_collection, set())
            cols.add(loc.column_or_path)
            for key in loc.join_keys:
                if key:
                    cols.add(key)

        sql_queries: list[dict[str, Any]] = []
        for table, set_values in sorted(sql_set.items()):
            if not set_values:
                continue

            table_where = dict(sql_where.get(table, {}))
            if not table_where:
                filterable = sql_filterable_columns.get(table, set())
                table_where = {
                    key: value
                    for key, value in cross_backend_filter.items()
                    if key in filterable
                }

            sql_queries.append(
                {
                    "type": "update",
                    "table": table,
                    "set": set_values,
                    "where": table_where,
                }
            )

        mongo_queries = [
            {
                "type": "update_many",
                "collection": collection,
                "set": set_values,
                "filter": mongo_filter.get(collection, {}) or dict(cross_backend_filter),
            }
            for collection, set_values in sorted(mongo_set.items())
            if set_values
        ]

        return QueryPlan(
            operation=CrudOperation.UPDATE,
            requested_fields=sorted(updates.keys()),
            sql_queries=sql_queries,
            mongo_queries=mongo_queries,
            merge_strategy={"mode": "none"},
        )

    def _build_delete_plan(self, payload: dict, field_locations: list[FieldLocation], field_index: dict[str, FieldLocation]) -> QueryPlan:
        filters = self._resolve_filters(
            payload.get("filters") or payload.get("where") or {},
            field_index,
        )
        join_keys = self._infer_join_keys(field_locations)
        explicit_target_fields = self._resolve_requested_fields(
            list(payload.get("fields") or []),
            field_index,
        )
        target_fields = list(explicit_target_fields)
        if not target_fields:
            target_fields = list(filters.keys())

        sql_where, mongo_filter = self._split_filters(filters, field_index)

        scoped_prefixes = {
            field.split(".", 1)[0]
            for field in explicit_target_fields
            if "." in field
        }

        sql_targets: set[str] = set()
        mongo_targets: set[str] = set()
        for field in target_fields:
            loc = field_index.get(field)
            if not loc:
                continue
            backend = loc.backend.lower()
            if backend in ("sql", "both"):
                sql_targets.add(loc.table_or_collection)
            if backend == "mongo":
                mongo_targets.add(loc.table_or_collection)

        all_mongo_collections = sorted(
            {
                loc.table_or_collection
                for loc in field_locations
                if loc.backend.lower() == "mongo"
            }
        )

        cross_backend_filter = {
            field: value
            for field, value in filters.items()
            if field in join_keys
        }

        # If no target fields are given, use tables/collections referenced by filters.
        if not sql_targets and sql_where:
            sql_targets = set(sql_where.keys())
        if not mongo_targets and mongo_filter:
            mongo_targets = set(mongo_filter.keys())
        if not explicit_target_fields and not mongo_targets and cross_backend_filter:
            mongo_targets = set(all_mongo_collections)

        if explicit_target_fields:
            scoped_sql_tables = sorted(
                {
                    loc.table_or_collection
                    for loc in field_locations
                    if loc.backend.lower() in ("sql", "both")
                    and any(
                        loc.table_or_collection == prefix
                        or loc.table_or_collection.startswith(f"{prefix}_")
                        or loc.field_path == prefix
                        or loc.field_path.startswith(f"{prefix}.")
                        for prefix in scoped_prefixes
                    )
                    and loc.table_or_collection not in {"event", "records"}
                }
            )

            sql_queries = []
            for table in scoped_sql_tables:
                table_locations = [
                    loc for loc in field_locations
                    if loc.table_or_collection == table and loc.backend.lower() in ("sql", "both")
                ]
                table_columns = {
                    loc.column_or_path
                    for loc in table_locations
                }
                for loc in table_locations:
                    table_columns.update(loc.join_keys)

                table_where = dict(sql_where.get(table, {}))
                if not table_where:
                    table_where = {
                        key: value
                        for key, value in cross_backend_filter.items()
                        if key in table_columns
                    }

                if table_where:
                    sql_queries.append(
                        {
                            "type": "delete",
                            "table": table,
                            "where": table_where,
                        }
                    )
        else:
            sql_tables = sorted(
                {
                    loc.table_or_collection
                    for loc in field_locations
                    if loc.backend.lower() in ("sql", "both")
                }
            )

            table_columns_map: dict[str, set[str]] = {}
            table_join_keys_map: dict[str, set[str]] = {}
            for table in sql_tables:
                table_locations = [
                    loc for loc in field_locations
                    if loc.table_or_collection == table and loc.backend.lower() in ("sql", "both")
                ]
                table_columns_map[table] = {
                    loc.column_or_path
                    for loc in table_locations
                }
                table_join_keys_map[table] = {
                    key
                    for loc in table_locations
                    for key in loc.join_keys
                    if key
                }

            prioritized_sql_queries: list[dict[str, Any]] = []
            for table in sql_tables:
                table_columns = set(table_columns_map.get(table, set()))
                table_columns.update(table_join_keys_map.get(table, set()))

                table_where = dict(sql_where.get(table, {}))
                if not table_where:
                    table_where = {
                        key: value
                        for key, value in cross_backend_filter.items()
                        if key in table_columns
                    }

                if not table_where:
                    continue

                priority = 1 if table in sql_targets else 0
                prioritized_sql_queries.append(
                    {
                        "type": "delete",
                        "table": table,
                        "where": table_where,
                        "priority": priority,
                        "depth_hint": table.count("_"),
                    }
                )

            sql_queries = [
                {k: v for k, v in query.items() if k not in ("priority", "depth_hint")}
                for query in sorted(
                    prioritized_sql_queries,
                    key=lambda q: (q["priority"], -q["depth_hint"], q["table"]),
                )
            ]
        mongo_queries: list[dict[str, Any]] = []
        if not explicit_target_fields:
            mongo_queries = [
                {
                    "type": "delete_many",
                    "collection": collection,
                    "filter": mongo_filter.get(collection, {}) or dict(cross_backend_filter),
                }
                for collection in sorted(mongo_targets)
            ]

        if explicit_target_fields and cross_backend_filter:
            scoped_filters = set(explicit_target_fields) | scoped_prefixes
            scoped_collections = sorted(
                {
                    loc.table_or_collection
                    for loc in field_locations
                    if loc.backend.lower() == "mongo"
                    and any(
                        loc.field_path == scope or loc.field_path.startswith(f"{scope}.")
                        for scope in scoped_filters
                    )
                }
            )

            for collection in scoped_collections:
                mongo_queries.append(
                    {
                        "type": "delete_many",
                        "collection": collection,
                        "filter": mongo_filter.get(collection, {}) or dict(cross_backend_filter),
                    }
                )

            unset_paths: set[str] = set()
            for field in explicit_target_fields:
                if "." in field:
                    unset_paths.add(field.split(".", 1)[0])
                else:
                    unset_paths.add(field)

            if unset_paths:
                mongo_queries.append(
                    {
                        "type": "unset_many",
                        "collection": "*",
                        "filter": dict(cross_backend_filter),
                        "unset_paths": sorted(unset_paths),
                    }
                )

        if cross_backend_filter and not explicit_target_fields:
            mongo_queries.append(
                {
                    "type": "delete_many",
                    "collection": "*",
                    "filter": dict(cross_backend_filter),
                }
            )

        return QueryPlan(
            operation=CrudOperation.DELETE,
            requested_fields=target_fields,
            sql_queries=sql_queries,
            mongo_queries=mongo_queries,
            merge_strategy={"mode": "none"},
        )

    @staticmethod
    def _normalize_operation(operation: CrudOperation | str) -> CrudOperation:
        if isinstance(operation, CrudOperation):
            return operation
        return CrudOperation(str(operation).lower())

    @staticmethod
    def _split_filters(filters: dict[str, Any], field_index: dict[str, FieldLocation]) -> tuple[dict[str, dict[str, Any]], dict[str, dict[str, Any]]]:
        sql_where: dict[str, dict[str, Any]] = {}
        mongo_filter: dict[str, dict[str, Any]] = {}

        for field_path, value in (filters or {}).items():
            loc = field_index.get(field_path)
            if not loc:
                continue
            backend = loc.backend.lower()
            if backend in ("sql", "both"):
                sql_where.setdefault(loc.table_or_collection, {})[loc.column_or_path] = value
            if backend == "mongo":
                mongo_filter.setdefault(loc.table_or_collection, {})[loc.column_or_path] = value

        return sql_where, mongo_filter

    @staticmethod
    def _infer_join_keys(field_locations: list[FieldLocation]) -> list[str]:
        join_keys: list[str] = []
        for loc in field_locations:
            for key in loc.join_keys:
                if key and key not in join_keys:
                    join_keys.append(key)

        if not join_keys:
            fallbacks = ["username", "id", "user_id", "_ref_id", "sys_ingested_at"]
            known_fields = {loc.field_path for loc in field_locations}
            join_keys = [k for k in fallbacks if k in known_fields]

        return join_keys

    @staticmethod
    def _resolve_requested_fields(
        requested_fields: list[str],
        field_index: dict[str, FieldLocation],
    ) -> list[str]:
        resolved: list[str] = []
        for field in requested_fields:
            canonical = QueryPlanner._resolve_field_alias(field, field_index)
            if canonical and canonical not in resolved:
                resolved.append(canonical)
        return resolved

    @staticmethod
    def _resolve_filters(
        filters: dict[str, Any],
        field_index: dict[str, FieldLocation],
    ) -> dict[str, Any]:
        resolved: dict[str, Any] = {}
        for field, value in (filters or {}).items():
            canonical = QueryPlanner._resolve_field_alias(field, field_index)
            if canonical:
                resolved[canonical] = value
        return resolved

    @staticmethod
    def _resolve_field_alias(
        field: str,
        field_index: dict[str, FieldLocation],
    ) -> str | None:
        if field in field_index:
            return field

        candidates = [
            path
            for path in field_index.keys()
            if path == field or path.endswith(f".{field}")
        ]

        if not candidates:
            return None

        # Ambiguous suffixes are resolved by preferring the shallowest path.
        # If still tied, use lexical order for determinism.
        candidates.sort(key=lambda p: (p.count("."), p))
        best = candidates[0]
        if len(candidates) > 1 and candidates[0].count(".") == candidates[1].count("."):
            return None
        return best

    @staticmethod
    def _extract_value(record: dict[str, Any], field_path: str) -> Any:
        if field_path in record:
            return record[field_path]

        current: Any = record
        for segment in field_path.split("."):
            if not isinstance(current, dict) or segment not in current:
                return _MISSING
            current = current[segment]
        return current

    @staticmethod
    def _set_dotted_path(doc: dict[str, Any], path: str, value: Any) -> None:
        if "." not in path:
            doc[path] = value
            return

        parts = path.split(".")
        cursor = doc
        for part in parts[:-1]:
            child = cursor.get(part)
            if not isinstance(child, dict):
                child = {}
                cursor[part] = child
            cursor = child
        cursor[parts[-1]] = value


_MISSING = object()
