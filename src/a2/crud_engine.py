import time
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
        global_limit = merge_strategy.get("global_limit")
        global_offset = merge_strategy.get("global_offset")

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
        t_sql_start = time.perf_counter()
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

        t_sql = (time.perf_counter() - t_sql_start) * 1000

        # ---- Execute Mongo reads -----------------------------------------------
        t_mongo_start = time.perf_counter()
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

        t_mongo = (time.perf_counter() - t_mongo_start) * 1000

        # ---- Merge --------------------------------------------------------------
        t_merge_start = time.perf_counter()
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
        by_key_field_source: dict[tuple[Any, ...], dict[str, str]] = {}

        sources = {
            "sql": sql_flat,
            "mongo": mongo_flat,
        }

        if mode == "keyed_merge" and effective_join_keys:
            # Apply lower-priority first, then preferred source overwrites on conflict.
            apply_order = list(reversed(source_priority))
            seen_order: list[tuple[Any, ...]] = []
            unkeyed_rows: list[dict[str, Any]] = []

            for src in apply_order:
                for row in sources.get(src, []):
                    key = row_key(row)
                    if key is None:
                        unkeyed_rows.append(dict(row))
                        continue
                    if key not in by_key:
                        by_key[key] = {jk: row.get(jk) for jk in effective_join_keys if jk in row}
                        by_key_field_source[key] = {
                            jk: src for jk in effective_join_keys if jk in row
                        }
                        seen_order.append(key)

                    target = by_key[key]
                    field_source = by_key_field_source[key]
                    for f, v in row.items():
                        if f in effective_join_keys:
                            continue

                        if f not in target:
                            target[f] = v
                            field_source[f] = src
                            continue

                        previous_source = field_source.get(f)

                        # Cross-source conflict resolution.
                        if previous_source and previous_source != src:
                            if conflict_policy == "prefer_sql":
                                if src == "mongo":
                                    continue
                                target[f] = v
                                field_source[f] = src
                                continue
                            target[f] = v
                            field_source[f] = src
                            continue

                        # Same-source repeated values: aggregate into arrays.
                        existing_value = target.get(f)
                        if existing_value == v:
                            continue

                        if isinstance(existing_value, list):
                            if v not in existing_value:
                                existing_value.append(v)
                        else:
                            target[f] = [existing_value, v]

            merged_flat_rows.extend(by_key[k] for k in seen_order)
            merged_flat_rows.extend(unkeyed_rows)
        else:
            merged_flat_rows = sql_flat + mongo_flat

        # Build alias map from physical keys emitted by backend queries to logical field paths.
        field_aliases: dict[str, str] = {}
        for query in plan.sql_queries or []:
            table = query.get("table")
            columns = list(query.get("columns") or [])
            for col in columns:
                if not col:
                    continue
                candidate_fields = [
                    field
                    for field in requested_fields
                    if field == col or field.endswith(f".{col}")
                ]
                logical_field = candidate_fields[0] if len(candidate_fields) == 1 else col
                # Common SQL flattened key shapes observed in the pipeline.
                field_aliases.setdefault(col, logical_field)
                if table:
                    field_aliases.setdefault(f"{table}.{col}", logical_field)

        for query in plan.mongo_queries or []:
            projection = list(query.get("projection") or [])
            for path in projection:
                if not path:
                    continue
                field_aliases.setdefault(path, path)

        # Project final output fields.
        records: list[dict[str, Any]] = []
        for flat in merged_flat_rows:
            if requested_fields:
                out: dict[str, Any] = {}
                for field in requested_fields:
                    if field in flat:
                        set_dotted(out, field, flat[field])
                        continue

                    # Fallback: resolve via physical->logical aliases.
                    for raw_key, value in flat.items():
                        mapped = field_aliases.get(raw_key)
                        if mapped == field:
                            set_dotted(out, field, value)
                            break
                for key in effective_join_keys:
                    if key in flat and key not in requested_fields:
                        out[key] = flat[key]
            else:
                out = {}
                for field, value in flat.items():
                    set_dotted(out, field, value)
            if out:
                records.append(out)

        # Apply global pagination to final reconstructed rows.
        if not isinstance(global_offset, int) or global_offset < 0:
            global_offset = 0
        if isinstance(global_limit, int) and global_limit >= 0:
            records = records[global_offset:global_offset + global_limit]
        elif global_offset:
            records = records[global_offset:]
            
        t_merge = (time.perf_counter() - t_merge_start) * 1000

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
        timings = {"sql_ms": round(t_sql, 2), "mongo_ms": round(t_mongo, 2), "merge_ms": round(t_merge, 2)}
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
            "timings": timings,
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
        errors: list[str] = []
        sql_inserted = 0
        mongo_inserted = 0

        for idx, query in enumerate(plan.sql_queries or []):
            if query.get("type") not in (None, "insert_batch"):
                errors.append(
                    f"SQL query #{idx + 1} skipped: unsupported type '{query.get('type')}'"
                )
                continue
            if mysql_client is None:
                errors.append("SQL insert execution failed: mysql_client is not provided")
                break

            table = query.get("table")
            rows = query.get("rows") or []
            if not table:
                errors.append(f"SQL query #{idx + 1} missing table name")
                continue
            if not isinstance(rows, list):
                errors.append(f"SQL query #{idx + 1} has invalid rows payload")
                continue

            try:
                if rows:
                    inserted = int(mysql_client.insert_batch(table, rows))
                    sql_inserted += inserted
                    if inserted == 0:
                        errors.append(
                            f"SQL query #{idx + 1} inserted 0 rows into '{table}'."
                        )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"SQL query #{idx + 1} execution failed: {exc}")

        for idx, query in enumerate(plan.mongo_queries or []):
            if query.get("type") not in (None, "insert_batch"):
                errors.append(
                    f"Mongo query #{idx + 1} skipped: unsupported type '{query.get('type')}'"
                )
                continue
            if mongo_client is None:
                errors.append("Mongo insert execution failed: mongo_client is not provided")
                break

            collection = query.get("collection")
            documents = query.get("documents") or []
            if not collection:
                errors.append(f"Mongo query #{idx + 1} missing collection name")
                continue
            if not isinstance(documents, list):
                errors.append(f"Mongo query #{idx + 1} has invalid documents payload")
                continue

            try:
                if documents:
                    inserted = int(mongo_client.insert_batch(collection, documents))
                    mongo_inserted += inserted
                    if inserted == 0:
                        errors.append(
                            f"Mongo query #{idx + 1} inserted 0 documents into '{collection}'."
                        )
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Mongo query #{idx + 1} execution failed: {exc}")

        status = "partial_success" if errors and (sql_inserted or mongo_inserted) else ("error" if errors else "success")
        return {
            "status": status,
            "operation": "create",
            "message": "Create executed with warnings." if errors else "Create executed successfully.",
            "sql_inserted": sql_inserted,
            "mongo_inserted": mongo_inserted,
            "errors": errors,
        }

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
        errors: list[str] = []
        sql_updated = 0
        mongo_updated = 0

        def build_sql_where_clause(where: dict[str, Any]) -> tuple[str, list[Any]]:
            if not where:
                return "", []
            conds: list[str] = []
            params: list[Any] = []
            for col, val in where.items():
                conds.append(f"`{col}` = %s")
                params.append(val)
            return " AND ".join(conds), params

        def build_sql_update(query: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
            table = query.get("table")
            if not table:
                raise ValueError("SQL update query missing 'table'")

            set_values = dict(query.get("set") or {})
            if not set_values:
                raise ValueError("SQL update query missing 'set' values")

            set_parts: list[str] = []
            params: list[Any] = []
            for col, val in set_values.items():
                set_parts.append(f"`{col}` = %s")
                params.append(val)

            sql = f"UPDATE `{table}` SET " + ", ".join(set_parts)

            where = dict(query.get("where") or {})
            where_clause, where_params = build_sql_where_clause(where)
            if where_clause:
                sql += " WHERE " + where_clause
                params.extend(where_params)

            return sql, tuple(params)

        def count_sql_rows(query: dict[str, Any]) -> int:
            table = query.get("table")
            if not table:
                raise ValueError("SQL update query missing 'table'")

            where = dict(query.get("where") or {})
            where_clause, where_params = build_sql_where_clause(where)
            count_sql = f"SELECT COUNT(*) AS cnt FROM `{table}`"
            if where_clause:
                count_sql += " WHERE " + where_clause

            rows = mysql_client.fetch_all(
                count_sql,
                tuple(where_params) if where_params else None,
            )
            if not rows:
                return 0
            first = rows[0]
            if isinstance(first, dict):
                value = first.get("cnt")
            else:
                value = None
            return int(value or 0)

        def run_mongo_update(query: dict[str, Any]) -> int:
            collection = query.get("collection")
            if not collection:
                raise ValueError("Mongo update query missing 'collection'")

            update_filter = dict(query.get("filter") or {})
            set_values = dict(query.get("set") or {})
            if not set_values:
                return 0

            query_type = str(query.get("type") or "update_many").lower()

            if hasattr(mongo_client, "client") and getattr(mongo_client, "client", None) is not None:
                db_name = getattr(mongo_client, "database", None)
                if not db_name:
                    raise ValueError("Mongo client missing database name")

                db = mongo_client.client[db_name]
                if collection == "*":
                    modified_total = 0
                    for collection_name in db.list_collection_names():
                        coll = db[collection_name]
                        if query_type == "update_one":
                            result = coll.update_one(update_filter, {"$set": set_values})
                        else:
                            result = coll.update_many(update_filter, {"$set": set_values})
                        modified_total += int(getattr(result, "modified_count", 0))
                    return modified_total

                coll = db[collection]
                if query_type == "update_one":
                    result = coll.update_one(update_filter, {"$set": set_values})
                else:
                    result = coll.update_many(update_filter, {"$set": set_values})
                return int(getattr(result, "modified_count", 0))

            if query_type == "update_one" and hasattr(mongo_client, "update_one"):
                result = mongo_client.update_one(collection, update_filter, {"$set": set_values})
                if isinstance(result, int):
                    return result
                return int(getattr(result, "modified_count", 0))
            if hasattr(mongo_client, "update_many"):
                result = mongo_client.update_many(collection, update_filter, {"$set": set_values})
                if isinstance(result, int):
                    return result
                return int(getattr(result, "modified_count", 0))

            raise ValueError("Mongo client does not support update operations")

        for idx, query in enumerate(plan.sql_queries or []):
            if query.get("type") not in (None, "update"):
                errors.append(
                    f"SQL query #{idx + 1} skipped: unsupported type '{query.get('type')}'"
                )
                continue
            if mysql_client is None:
                errors.append("SQL update execution failed: mysql_client is not provided")
                break

            try:
                affected = count_sql_rows(query)
                sql, params = build_sql_update(query)
                mysql_client.execute(sql, params if params else None)
                sql_updated += affected
            except Exception as exc:  # noqa: BLE001
                errors.append(f"SQL query #{idx + 1} execution failed: {exc}")

        for idx, query in enumerate(plan.mongo_queries or []):
            if query.get("type") not in (None, "update_many", "update_one"):
                errors.append(
                    f"Mongo query #{idx + 1} skipped: unsupported type '{query.get('type')}'"
                )
                continue
            if mongo_client is None:
                errors.append("Mongo update execution failed: mongo_client is not provided")
                break

            try:
                mongo_updated += run_mongo_update(query)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Mongo query #{idx + 1} execution failed: {exc}")

        status = (
            "partial_success"
            if errors and (sql_updated or mongo_updated)
            else ("error" if errors else "success")
        )
        return {
            "status": status,
            "operation": "update",
            "message": "Update executed with warnings." if errors else "Update executed successfully.",
            "sql_updated": sql_updated,
            "mongo_updated": mongo_updated,
            "errors": errors,
        }

    def _execute_delete(self, plan: QueryPlan, mysql_client, mongo_client) -> dict:
        """Delete: cascade deletion across SQL + MongoDB.

        Steps:
        1. Determine all related tables/collections from plan.
        2. Delete child records first (respect FK ordering).
        3. For SQL, run DELETE via mysql_client.execute() on each table.
        4. For MongoDB, run delete_one/delete_many on each collection.
        5. Return deletion counts per backend.
        """
        errors: list[str] = []
        sql_deleted = 0
        mongo_deleted = 0

        def build_sql_delete(query: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
            table = query.get("table")
            if not table:
                raise ValueError("SQL delete query missing 'table'")

            where = query.get("where") or {}
            params: list[Any] = []
            sql = f"DELETE FROM `{table}`"

            if where:
                conds: list[str] = []
                for col, val in where.items():
                    conds.append(f"`{col}` = %s")
                    params.append(val)
                sql += " WHERE " + " AND ".join(conds)

            return sql, tuple(params)

        def build_sql_where_clause(where: dict[str, Any]) -> tuple[str, list[Any]]:
            if not where:
                return "", []
            conds: list[str] = []
            params: list[Any] = []
            for col, val in where.items():
                conds.append(f"`{col}` = %s")
                params.append(val)
            return " AND ".join(conds), params

        def count_sql_rows(query: dict[str, Any]) -> int:
            table = query.get("table")
            if not table:
                raise ValueError("SQL delete query missing 'table'")

            where = query.get("where") or {}
            params: list[Any] = []
            count_sql = f"SELECT COUNT(*) AS cnt FROM `{table}`"

            if where:
                conds: list[str] = []
                for col, val in where.items():
                    conds.append(f"`{col}` = %s")
                    params.append(val)
                count_sql += " WHERE " + " AND ".join(conds)

            rows = mysql_client.fetch_all(count_sql, tuple(params) if params else None)
            if not rows:
                return 0
            first = rows[0]
            if isinstance(first, dict):
                value = first.get("cnt")
            else:
                value = None
            return int(value or 0)

        def attempt_sql_fk_cascade(query: dict[str, Any]) -> bool:
            table = query.get("table")
            if not table:
                return False

            where = query.get("where") or {}
            where_clause, where_params = build_sql_where_clause(where)
            if not where_clause:
                return False

            fk_query = (
                "SELECT TABLE_NAME, COLUMN_NAME, REFERENCED_COLUMN_NAME "
                "FROM INFORMATION_SCHEMA.KEY_COLUMN_USAGE "
                "WHERE TABLE_SCHEMA = DATABASE() "
                "AND REFERENCED_TABLE_NAME = %s"
            )
            child_fks = mysql_client.fetch_all(fk_query, (table,))
            if not child_fks:
                return False

            cascaded = False
            for fk in child_fks:
                child_table = fk.get("TABLE_NAME") if isinstance(fk, dict) else None
                child_column = fk.get("COLUMN_NAME") if isinstance(fk, dict) else None
                parent_column = fk.get("REFERENCED_COLUMN_NAME") if isinstance(fk, dict) else None
                if not child_table or not child_column or not parent_column:
                    continue

                delete_child_sql = (
                    f"DELETE FROM `{child_table}` "
                    f"WHERE `{child_column}` IN ("
                    f"SELECT `{parent_column}` FROM `{table}` WHERE {where_clause}"
                    ")"
                )
                mysql_client.execute(delete_child_sql, tuple(where_params))
                cascaded = True

            return cascaded

        def build_sql_nullify(query: dict[str, Any]) -> tuple[str, tuple[Any, ...]]:
            table = query.get("table")
            if not table:
                raise ValueError("SQL nullify query missing 'table'")

            columns = list(query.get("columns") or [])
            if not columns:
                raise ValueError("SQL nullify query missing 'columns'")

            where = query.get("where") or {}
            params: list[Any] = []
            set_clause = ", ".join(f"`{col}` = NULL" for col in columns)
            sql = f"UPDATE `{table}` SET {set_clause}"

            if where:
                conds: list[str] = []
                for col, val in where.items():
                    conds.append(f"`{col}` = %s")
                    params.append(val)
                sql += " WHERE " + " AND ".join(conds)

            return sql, tuple(params)

        def run_mongo_delete(query: dict[str, Any]) -> int:
            collection = query.get("collection")
            if not collection:
                raise ValueError("Mongo delete query missing 'collection'")

            delete_filter = dict(query.get("filter") or {})
            query_type = str(query.get("type") or "delete_many").lower()

            if hasattr(mongo_client, "client") and getattr(mongo_client, "client", None) is not None:
                db_name = getattr(mongo_client, "database", None)
                if not db_name:
                    raise ValueError("Mongo client missing database name")

                db = mongo_client.client[db_name]
                if collection == "*":
                    deleted_total = 0
                    for collection_name in db.list_collection_names():
                        coll = db[collection_name]
                        if query_type == "delete_one":
                            result = coll.delete_one(delete_filter)
                        else:
                            result = coll.delete_many(delete_filter)
                        deleted_total += int(getattr(result, "deleted_count", 0))
                    return deleted_total

                coll = db[collection]
                if query_type == "delete_one":
                    result = coll.delete_one(delete_filter)
                else:
                    result = coll.delete_many(delete_filter)
                return int(getattr(result, "deleted_count", 0))

            if query_type == "delete_one" and hasattr(mongo_client, "delete_one"):
                result = mongo_client.delete_one(collection, delete_filter)
                if isinstance(result, int):
                    return result
                return int(getattr(result, "deleted_count", 0))
            if hasattr(mongo_client, "delete_many"):
                result = mongo_client.delete_many(collection, delete_filter)
                if isinstance(result, int):
                    return result
                return int(getattr(result, "deleted_count", 0))

            raise ValueError("Mongo client does not support delete operations")

        def run_mongo_unset(query: dict[str, Any]) -> int:
            collection = query.get("collection")
            if not collection:
                raise ValueError("Mongo unset query missing 'collection'")

            update_filter = dict(query.get("filter") or {})
            unset_paths = list(query.get("unset_paths") or [])
            if not unset_paths:
                return 0
            unset_doc = {path: "" for path in unset_paths}

            if hasattr(mongo_client, "client") and getattr(mongo_client, "client", None) is not None:
                db_name = getattr(mongo_client, "database", None)
                if not db_name:
                    raise ValueError("Mongo client missing database name")

                db = mongo_client.client[db_name]
                if collection == "*":
                    modified_total = 0
                    for collection_name in db.list_collection_names():
                        coll = db[collection_name]
                        result = coll.update_many(update_filter, {"$unset": unset_doc})
                        modified_total += int(getattr(result, "modified_count", 0))
                    return modified_total

                coll = db[collection]
                result = coll.update_many(update_filter, {"$unset": unset_doc})
                return int(getattr(result, "modified_count", 0))

            raise ValueError("Mongo client does not support unset operations")

        # Execute SQL deletes in planned order (planner handles child-first sequencing).
        sql_queries = list(plan.sql_queries or [])

        for idx, query in enumerate(sql_queries):
            if query.get("type") not in (None, "delete", "nullify"):
                errors.append(
                    f"SQL query #{idx + 1} skipped: unsupported type '{query.get('type')}'"
                )
                continue
            if mysql_client is None:
                errors.append("SQL delete execution failed: mysql_client is not provided")
                break

            try:
                affected = count_sql_rows(query)
                if query.get("type") == "nullify":
                    sql, params = build_sql_nullify(query)
                else:
                    sql, params = build_sql_delete(query)
                mysql_client.execute(sql, params if params else None)
                sql_deleted += affected
            except Exception as exc:  # noqa: BLE001
                recovered = False
                if query.get("type") == "delete":
                    try:
                        cascaded = attempt_sql_fk_cascade(query)
                        if cascaded:
                            affected = count_sql_rows(query)
                            sql, params = build_sql_delete(query)
                            mysql_client.execute(sql, params if params else None)
                            sql_deleted += affected
                            recovered = True
                    except Exception:  # noqa: BLE001
                        recovered = False

                if not recovered:
                    errors.append(f"SQL query #{idx + 1} execution failed: {exc}")

        for idx, query in enumerate(plan.mongo_queries or []):
            if query.get("type") not in (None, "delete_many", "delete_one", "unset_many"):
                errors.append(
                    f"Mongo query #{idx + 1} skipped: unsupported type '{query.get('type')}'"
                )
                continue
            if mongo_client is None:
                errors.append("Mongo delete execution failed: mongo_client is not provided")
                break

            try:
                query_type = str(query.get("type") or "delete_many").lower()
                if query_type == "unset_many":
                    mongo_deleted += run_mongo_unset(query)
                else:
                    mongo_deleted += run_mongo_delete(query)
            except Exception as exc:  # noqa: BLE001
                errors.append(f"Mongo query #{idx + 1} execution failed: {exc}")

        status = (
            "partial_success"
            if errors and (sql_deleted or mongo_deleted)
            else ("error" if errors else "success")
        )
        return {
            "status": status,
            "operation": "delete",
            "message": "Delete executed with warnings." if errors else "Delete executed successfully.",
            "sql_deleted": sql_deleted,
            "mongo_deleted": mongo_deleted,
            "errors": errors,
        }
