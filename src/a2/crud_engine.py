from typing import Any

from .contracts import CrudOperation, FieldLocation, QueryPlan


class CrudEngine:
    """Executes generated QueryPlan objects on hybrid backends.

    Each CRUD operation has distinct execution logic:
    - Read: translate fields → SQL/Mongo queries → merge results
    - Insert: split JSON → insert into SQL + MongoDB → maintain join keys
    - Delete: cascade across SQL + MongoDB
    - Update: ensure schema consistency

    The engine accepts backend clients at execution time so it stays
    decoupled from connection management.
    """

    def execute(self, plan: QueryPlan, mysql_client=None, mongo_client=None) -> dict:
        """Dispatch a QueryPlan to the correct operation handler.

        Args:
            plan: The QueryPlan produced by QueryPlanner.
            mysql_client: A1 MySQLClient instance (connected).
            mongo_client: A1 MongoClient instance (connected).

        Returns:
            Unified JSON result dict.
        """
        dispatch = {
            CrudOperation.READ: self._execute_read,
            CrudOperation.CREATE: self._execute_insert,
            CrudOperation.UPDATE: self._execute_update,
            CrudOperation.DELETE: self._execute_delete,
        }
        handler = dispatch.get(plan.operation)
        if handler is None:
            raise ValueError(f"Unsupported operation: {plan.operation}")
        return handler(plan, mysql_client, mongo_client)

    def _execute_read(self, plan: QueryPlan, mysql_client, mongo_client) -> dict:
        """Read: translate requested fields into SQL/Mongo queries, merge results.

        Steps:
        1. Run each SQL query via mysql_client.fetch_all().
        2. Run each Mongo query via mongo_client.find().
        3. Merge partial results using plan.merge_strategy (join on shared keys).
        4. Return unified JSON response containing only the requested fields.
        """
        requested_fields = list(plan.requested_fields or [])
        merge_strategy = dict(plan.merge_strategy or {})
        mode = str(merge_strategy.get("mode", "none")).lower()
        join_keys = list(merge_strategy.get("join_keys") or [])
        source_priority = list(merge_strategy.get("source_priority") or ["sql", "mongo"])
        conflict_policy = str(merge_strategy.get("conflict_policy", "prefer_sql")).lower()

        errors: list[str] = []
        sql_rows: list[dict[str, Any]] = []
        mongo_docs: list[dict[str, Any]] = []

        def flatten_doc(doc: Any, prefix: str = "") -> dict[str, Any]:
            flat: dict[str, Any] = {}
            if isinstance(doc, dict):
                for key, value in doc.items():
                    if key == "_id":
                        continue
                    full = f"{prefix}.{key}" if prefix else key
                    if isinstance(value, dict):
                        flat.update(flatten_doc(value, full))
                    else:
                        flat[full] = value
            return flat

        def set_dotted(target: dict[str, Any], path: str, value: Any) -> None:
            if "." not in path:
                target[path] = value
                return
            parts = path.split(".")
            cursor = target
            for part in parts[:-1]:
                child = cursor.get(part)
                if not isinstance(child, dict):
                    child = {}
                    cursor[part] = child
                cursor = child
            cursor[parts[-1]] = value

        def build_sql_select(query: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
            table = query.get("table")
            if not table:
                raise ValueError("SQL read query missing 'table'")

            columns = list(query.get("columns") or ["*"])

            if columns == ["*"]:
                select_clause = "*"
            else:
                select_clause = ", ".join(f"`{c}`" for c in columns)

            sql = f"SELECT {select_clause} FROM `{table}`"
            params: list[Any] = []

            where = query.get("where") or {}
            if where:
                conds: list[str] = []
                for col, val in where.items():
                    conds.append(f"`{col}` = %s")
                    params.append(val)
                sql += " WHERE " + " AND ".join(conds)

            sort = query.get("sort")
            order_parts: list[str] = []
            if isinstance(sort, dict):
                for col, direction in sort.items():
                    dir_token = "DESC" if str(direction).lower() == "desc" else "ASC"
                    order_parts.append(f"`{col}` {dir_token}")
            elif isinstance(sort, list):
                for item in sort:
                    if isinstance(item, str):
                        order_parts.append(f"`{item}` ASC")
                    elif isinstance(item, dict):
                        for col, direction in item.items():
                            dir_token = "DESC" if str(direction).lower() == "desc" else "ASC"
                            order_parts.append(f"`{col}` {dir_token}")
            elif isinstance(sort, str):
                order_parts.append(f"`{sort}` ASC")

            if order_parts:
                sql += " ORDER BY " + ", ".join(order_parts)

            limit = query.get("limit")
            offset = query.get("offset")
            if isinstance(limit, int) and limit >= 0:
                sql += " LIMIT %s"
                params.append(limit)
                if isinstance(offset, int) and offset >= 0:
                    sql += " OFFSET %s"
                    params.append(offset)

            return sql, tuple(params)

        def run_mongo_find(query: dict[str, Any]) -> list[dict[str, Any]]:
            collection = query.get("collection")
            if not collection:
                raise ValueError("Mongo read query missing 'collection'")

            mongo_filter = dict(query.get("filter") or {})
            projection_paths = list(query.get("projection") or [])
            for jk in join_keys:
                if jk and jk not in projection_paths:
                    projection_paths.append(jk)

            limit = query.get("limit")
            sort = query.get("sort")

            # Prefer direct pymongo cursor path if available to support projection/limit/sort.
            if hasattr(mongo_client, "client") and getattr(mongo_client, "client", None) is not None:
                db_name = getattr(mongo_client, "database", None)
                if not db_name:
                    raise ValueError("Mongo client missing database name")
                coll = mongo_client.client[db_name][collection]

                projection = None
                if projection_paths:
                    projection = {p: 1 for p in projection_paths}
                    projection["_id"] = 0

                cursor = coll.find(mongo_filter, projection)
                if isinstance(sort, dict):
                    sort_args = []
                    for col, direction in sort.items():
                        sort_args.append((col, -1 if str(direction).lower() == "desc" else 1))
                    if sort_args:
                        cursor = cursor.sort(sort_args)
                if isinstance(limit, int) and limit >= 0:
                    cursor = cursor.limit(limit)
                return list(cursor)

            # Fallback for A1 MongoClient interface.
            docs = mongo_client.find(collection, mongo_filter)
            if projection_paths:
                projected: list[dict[str, Any]] = []
                for doc in docs:
                    flat = flatten_doc(doc)
                    out: dict[str, Any] = {}
                    for path in projection_paths:
                        if path in flat:
                            set_dotted(out, path, flat[path])
                    projected.append(out)
                docs = projected
            if isinstance(limit, int) and limit >= 0:
                docs = docs[:limit]
            return docs

        # ---- Execute SQL reads -------------------------------------------------
        for idx, query in enumerate(plan.sql_queries or []):
            if query.get("type") not in (None, "select"):
                msg = f"SQL query #{idx + 1} skipped: unsupported type '{query.get('type')}'"
                print(msg)
                errors.append(msg)
                continue
            if mysql_client is None:
                msg = "SQL query execution failed: mysql_client is not provided"
                print(msg)
                errors.append(msg)
                break
            try:
                sql, params = build_sql_select(query)
                rows = mysql_client.fetch_all(sql, params if params else None)
                for row in rows:
                    if isinstance(row, dict):
                        sql_rows.append(row)
            except Exception as exc:  # noqa: BLE001
                msg = f"SQL query #{idx + 1} execution failed: {exc}"
                print(msg)
                errors.append(msg)

        # ---- Execute Mongo reads -----------------------------------------------
        for idx, query in enumerate(plan.mongo_queries or []):
            if query.get("type") not in (None, "find"):
                msg = f"Mongo query #{idx + 1} skipped: unsupported type '{query.get('type')}'"
                print(msg)
                errors.append(msg)
                continue
            if mongo_client is None:
                msg = "Mongo query execution failed: mongo_client is not provided"
                print(msg)
                errors.append(msg)
                break
            try:
                docs = run_mongo_find(query)
                for doc in docs:
                    if isinstance(doc, dict):
                        mongo_docs.append(doc)
            except Exception as exc:  # noqa: BLE001
                msg = f"Mongo query #{idx + 1} execution failed: {exc}"
                print(msg)
                errors.append(msg)

        # ---- Merge --------------------------------------------------------------
        sql_flat = [flatten_doc(r) for r in sql_rows]
        mongo_flat = [flatten_doc(d) for d in mongo_docs]

        effective_join_keys = [key for key in join_keys if any(key in row for row in sql_flat) and any(key in row for row in mongo_flat)]

        def row_key(row: dict[str, Any]) -> tuple[Any, ...] | None:
            if not effective_join_keys:
                return None
            key_values: list[Any] = []
            for key in effective_join_keys:
                if key not in row:
                    return None
                key_values.append(row.get(key))
            return tuple(key_values)

        merged_flat_rows: list[dict[str, Any]] = []
        by_key: dict[tuple[Any, ...], dict[str, Any]] = {}

        sources = {
            "sql": sql_flat,
            "mongo": mongo_flat,
        }

        if mode == "keyed_merge" and effective_join_keys:
            # Apply lower-priority first, then preferred source overwrites on conflict.
            apply_order = list(reversed(source_priority))
            seen_order: list[tuple[Any, ...]] = []

            for src in apply_order:
                for row in sources.get(src, []):
                    key = row_key(row)
                    if key is None:
                        merged_flat_rows.append(dict(row))
                        continue
                    if key not in by_key:
                        by_key[key] = {jk: row.get(jk) for jk in effective_join_keys if jk in row}
                        seen_order.append(key)

                    target = by_key[key]
                    for f, v in row.items():
                        if f in join_keys:
                            continue
                        if conflict_policy == "prefer_sql" and src == "mongo" and f in target:
                            continue
                        target[f] = v

            merged_flat_rows.extend(by_key[k] for k in seen_order)
        else:
            merged_flat_rows = sql_flat + mongo_flat

        # Project final output fields.
        records: list[dict[str, Any]] = []
        for flat in merged_flat_rows:
            if requested_fields:
                out: dict[str, Any] = {}
                for field in requested_fields:
                    if field in flat:
                        set_dotted(out, field, flat[field])
                for key in effective_join_keys:
                    if key in flat and key not in requested_fields:
                        out[key] = flat[key]
            else:
                out = {}
                for field, value in flat.items():
                    set_dotted(out, field, value)
            records.append(out)

        if errors and not records:
            return {
                "status": "error",
                "operation": "read",
                "message": "Read query could not be executed.",
                "records": [],
                "requested_fields": requested_fields,
                "errors": errors,
            }

        status = "partial_success" if errors else "success"
        return {
            "status": status,
            "operation": "read",
            "message": "Read query executed with warnings." if errors else "Read query executed successfully.",
            "records": records,
            "requested_fields": requested_fields,
            "join_keys": effective_join_keys,
            "sql_rows": len(sql_rows),
            "mongo_docs": len(mongo_docs),
            "errors": errors,
        }

    def _execute_insert(self, plan: QueryPlan, mysql_client, mongo_client) -> dict:
        """Insert: split JSON record, insert into SQL tables + MongoDB collections.

        Steps:
        1. For each sql_query in plan, extract the row dict and call
           mysql_client.insert_batch() on the target table.
        2. For each mongo_query in plan, call mongo_client.insert_batch()
           on the target collection.
        3. Ensure join keys (e.g. username, foreign keys) are consistent
           across both backends.
        4. Return insert counts per backend.
        """
        raise NotImplementedError("Implement insert execution across backends")

    def _execute_update(self, plan: QueryPlan, mysql_client, mongo_client) -> dict:
        """Update: modify existing records while ensuring schema consistency.

        Steps:
        1. For each sql_query in plan, run the UPDATE statement via
           mysql_client.execute().
        2. For each mongo_query in plan, run the update operation via
           mongo_client (update_one/update_many).
        3. Validate that the updated values still conform to the schema
           (type consistency check).
        4. Return update counts per backend.
        """
        raise NotImplementedError("Implement update execution with schema validation")

    def _execute_delete(self, plan: QueryPlan, mysql_client, mongo_client) -> dict:
        """Delete: cascade deletion across SQL + MongoDB.

        Steps:
        1. Determine all related tables/collections from plan.
        2. Delete child records first (respect FK ordering).
        3. For SQL, run DELETE via mysql_client.execute() on each table.
        4. For MongoDB, run delete_one/delete_many on each collection.
        5. Return deletion counts per backend.
        """
        raise NotImplementedError("Implement cascading delete across backends")
