from __future__ import annotations

import json
from typing import Any

from src.a2.contracts import FieldLocation
from src.a2.metadata_catalog import MetadataCatalog

from .contracts import SessionInfo


class SessionManager:
    """Manages dashboard session state: schema info, connection status, entity counts."""

    def __init__(self, metadata_catalog: MetadataCatalog, mysql_client, mongo_client):
        self.metadata_catalog = metadata_catalog
        self.mysql_client = mysql_client
        self.mongo_client = mongo_client

    def get_session_info(self) -> SessionInfo:
        schema = self._load_schema()
        return SessionInfo(
            schema_name=schema.get("schema_name", "unknown"),
            version=schema.get("version", "unknown"),
            root_entity=schema.get("root_entity", "unknown"),
            mysql_connected=self._is_mysql_connected(),
            mongo_connected=self._is_mongo_connected(),
            sql_tables=self._get_sql_tables(),
            mongo_collections=self._get_mongo_collections(),
            field_count=len(self.metadata_catalog.get_field_locations()),
        )

    def get_field_locations(self) -> list[FieldLocation]:
        return self.metadata_catalog.get_field_locations()

    def get_schema(self) -> dict[str, Any]:
        return self._load_schema()

    def get_sql_plan(self) -> dict[str, Any]:
        path = self.metadata_catalog.sql_plan_file
        if not path.exists():
            return {}
        with open(path, "r") as f:
            return json.load(f)

    def get_mongo_plan(self) -> dict[str, Any]:
        path = self.metadata_catalog.mongo_plan_file
        if not path.exists():
            return {}
        with open(path, "r") as f:
            return json.load(f)

    def _load_schema(self) -> dict[str, Any]:
        path = self.metadata_catalog.schema_file
        if not path.exists():
            return {}
        with open(path, "r") as f:
            return json.load(f)

    def _is_mysql_connected(self) -> bool:
        return getattr(self.mysql_client, "connection", None) is not None

    def _is_mongo_connected(self) -> bool:
        return getattr(self.mongo_client, "client", None) is not None

    def _get_sql_tables(self) -> list[str]:
        plan = self.get_sql_plan()
        return [t.get("table_name", "") for t in plan.get("tables", [])]

    def _get_mongo_collections(self) -> list[str]:
        plan = self.get_mongo_plan()
        return [c.get("collection_name", "") for c in plan.get("collections", [])]
