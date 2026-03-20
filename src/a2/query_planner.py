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

        for record in records:
            if not isinstance(record, dict):
                continue

            table_row_buffer: dict[str, dict[str, Any]] = {}
            collection_doc_buffer: dict[str, dict[str, Any]] = {}

            for loc in field_locations:
                value = self._extract_value(record, loc.field_path)
                if value is _MISSING:
                    continue

                backend = loc.backend.lower()
                if backend in ("sql", "both"):
                    row = table_row_buffer.setdefault(loc.table_or_collection, {})
                    row[loc.column_or_path] = value

                if backend in ("mongo", "both"):
                    doc = collection_doc_buffer.setdefault(loc.table_or_collection, {})
                    self._set_dotted_path(doc, loc.column_or_path, value)

            # Ensure required ingest timestamp exists when metadata expects it.
            if "sys_ingested_at" not in record:
                now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
                for loc in field_locations:
                    if loc.field_path != "sys_ingested_at":
                        continue
                    backend = loc.backend.lower()
                    if backend in ("sql", "both"):
                        row = table_row_buffer.setdefault(loc.table_or_collection, {})
                        row.setdefault(loc.column_or_path, now_utc)
                    if backend in ("mongo", "both"):
                        doc = collection_doc_buffer.setdefault(loc.table_or_collection, {})
                        self._set_dotted_path(doc, loc.column_or_path, now_utc)

            for table, row in table_row_buffer.items():
                if row:
                    sql_rows.setdefault(table, []).append(row)
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

    def _build_update_plan(self, payload: dict, field_locations: list[FieldLocation], field_index: dict[str, FieldLocation]) -> QueryPlan:
        updates = self._resolve_filters(
            payload.get("updates") or payload.get("set") or {},
            field_index,
        )
        filters = self._resolve_filters(
            payload.get("filters") or payload.get("where") or {},
            field_index,
        )

        sql_set: dict[str, dict[str, Any]] = {}
        mongo_set: dict[str, dict[str, Any]] = {}
        for field_path, value in updates.items():
            loc = field_index.get(field_path)
            if not loc:
                continue
            backend = loc.backend.lower()
            if backend in ("sql", "both"):
                sql_set.setdefault(loc.table_or_collection, {})[loc.column_or_path] = value
            if backend == "mongo":
                mongo_set.setdefault(loc.table_or_collection, {})[loc.column_or_path] = value

        sql_where, mongo_filter = self._split_filters(filters, field_index)

        sql_queries = [
            {
                "type": "update",
                "table": table,
                "set": set_values,
                "where": sql_where.get(table, {}),
            }
            for table, set_values in sorted(sql_set.items())
            if set_values
        ]

        mongo_queries = [
            {
                "type": "update_many",
                "collection": collection,
                "set": set_values,
                "filter": mongo_filter.get(collection, {}),
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
        target_fields = self._resolve_requested_fields(
            list(payload.get("fields") or []),
            field_index,
        )
        if not target_fields:
            target_fields = list(filters.keys())

        sql_where, mongo_filter = self._split_filters(filters, field_index)

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

        # If no target fields are given, use tables/collections referenced by filters.
        if not sql_targets and sql_where:
            sql_targets = set(sql_where.keys())
        if not mongo_targets and mongo_filter:
            mongo_targets = set(mongo_filter.keys())

        sql_queries = [
            {
                "type": "delete",
                "table": table,
                "where": sql_where.get(table, {}),
            }
            for table in sorted(sql_targets)
        ]
        mongo_queries = [
            {
                "type": "delete_many",
                "collection": collection,
                "filter": mongo_filter.get(collection, {}),
            }
            for collection in sorted(mongo_targets)
        ]

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
