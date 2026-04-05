from __future__ import annotations

from typing import Any

from src.a2.contracts import CrudOperation, FieldLocation
from src.a2.crud_engine import CrudEngine
from src.a2.query_planner import QueryPlanner

from .contracts import LogicalEntity


class LogicalReconstructor:
    """Reconstructs unified logical entities from split SQL+Mongo storage.

    All output is backend-agnostic — no table names, collection names,
    column references, or join keys are exposed to the caller.
    """

    def __init__(self, query_planner: QueryPlanner, crud_engine: CrudEngine):
        self.query_planner = query_planner
        self.crud_engine = crud_engine

    def list_entities(self, field_locations: list[FieldLocation]) -> list[str]:
        entities: set[str] = set()
        has_root_scalars = False

        for loc in field_locations:
            path = loc.field_path
            if "." in path:
                entities.add(path.split(".")[0])
            else:
                has_root_scalars = True

        if has_root_scalars:
            entities.add("root")

        return sorted(entities)

    def get_entity_instances(
        self,
        entity_name: str,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
        limit: int = 100,
        offset: int = 0,
    ) -> LogicalEntity:
        entity_fields = self._fields_for_entity(entity_name, field_locations)
        if not entity_fields:
            return LogicalEntity(entity_name=entity_name)

        plan = self.query_planner.build_plan(
            CrudOperation.READ,
            {"fields": entity_fields, "limit": limit, "offset": offset},
            field_locations,
        )
        result = self.crud_engine.execute(plan, mysql_client, mongo_client)
        records = result.get("records", [])

        clean_records = []
        for record in records:
            cleaned = self._clean_record(record, entity_name)
            if cleaned:
                clean_records.append(cleaned)

        clean_field_names = self._clean_field_names(entity_fields, entity_name)

        return LogicalEntity(
            entity_name=entity_name,
            fields=sorted(clean_field_names),
            instances=clean_records,
        )

    def get_all_data(
        self,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
        limit: int = 100,
    ) -> list[dict[str, Any]]:
        plan = self.query_planner.build_plan(
            CrudOperation.READ,
            {"limit": limit},
            field_locations,
        )
        result = self.crud_engine.execute(plan, mysql_client, mongo_client)
        return result.get("records", [])

    def get_table_stats(
        self,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
    ) -> dict[str, int]:
        stats: dict[str, int] = {}

        sql_tables: set[str] = set()
        mongo_collections: set[str] = set()
        for loc in field_locations:
            backend = loc.backend.lower()
            if backend in ("sql", "both"):
                sql_tables.add(loc.table_or_collection)
            if backend in ("mongo", "both"):
                mongo_collections.add(loc.table_or_collection)

        conn = getattr(mysql_client, "connection", None)
        if conn is not None:
            for table in sorted(sql_tables):
                try:
                    rows = mysql_client.fetch_all(f"SELECT COUNT(*) AS cnt FROM `{table}`")
                    stats[f"sql:{table}"] = int(rows[0].get("cnt", 0)) if rows else 0
                except Exception:
                    stats[f"sql:{table}"] = -1

        mongo_db = None
        if hasattr(mongo_client, "client") and mongo_client.client is not None:
            db_name = getattr(mongo_client, "database", None)
            if db_name:
                mongo_db = mongo_client.client[db_name]

        if mongo_db is not None:
            for coll_name in sorted(mongo_collections):
                try:
                    stats[f"mongo:{coll_name}"] = mongo_db[coll_name].count_documents({})
                except Exception:
                    stats[f"mongo:{coll_name}"] = -1

        return stats

    @staticmethod
    def _fields_for_entity(
        entity_name: str,
        field_locations: list[FieldLocation],
    ) -> list[str]:
        if entity_name == "root":
            return [loc.field_path for loc in field_locations if "." not in loc.field_path]
        return [
            loc.field_path
            for loc in field_locations
            if loc.field_path == entity_name or loc.field_path.startswith(f"{entity_name}.")
        ]

    @staticmethod
    def _clean_record(record: dict[str, Any], entity_name: str) -> dict[str, Any]:
        if entity_name == "root":
            return {k: v for k, v in record.items() if k != "_id"}

        if entity_name in record and isinstance(record[entity_name], dict):
            return record[entity_name]

        clean: dict[str, Any] = {}
        prefix = f"{entity_name}."
        for key, value in record.items():
            if key.startswith(prefix):
                clean[key[len(prefix):]] = value
            elif key == entity_name:
                if isinstance(value, dict):
                    return value
                clean[entity_name] = value
        return clean

    @staticmethod
    def _clean_field_names(
        field_paths: list[str],
        entity_name: str,
    ) -> list[str]:
        names: list[str] = []
        prefix = f"{entity_name}."
        for path in field_paths:
            if entity_name == "root":
                names.append(path)
            elif path.startswith(prefix):
                names.append(path[len(prefix):])
            else:
                names.append(path)
        return names
