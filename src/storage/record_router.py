# ==============================================
# RecordRouter
# ==============================================
#
# PURPOSE:
#   Takes a normalized record and the classification decisions,
#   splits the record into SQL-bound and MongoDB-bound parts,
#   and writes each part to the appropriate backend.
#
# WHY THIS CLASS EXISTS:
#   A single incoming record may have some fields going to SQL
#   and others going to MongoDB. The linking fields (username,
#   sys_ingested_at) must go to BOTH. This class does that split
#   and coordinates the writes.
#
# CLASS: RecordRouter
# -------------------
#   Stateful — holds references to MySQLClient and MongoClient.
#
#   Constructor:
#   ------------
#   - __init__(mysql_client: MySQLClient, mongo_client: MongoClient)
#
#   Methods:
#   --------
#   - route_batch(
#         records: list[dict],
#         decisions: dict[str, PlacementDecision],
#         table_name: str = "records",
#         collection_name: str = "records"
#     ) -> RouteResult
#       For each record:
#         1. Split into sql_part and mongo_part using decisions
#         2. Batch insert sql_parts into MySQL
#         3. Batch insert mongo_parts into MongoDB
#       Returns a RouteResult with counts and errors.
#
#   - _split_record(
#         record: dict,
#         decisions: dict[str, PlacementDecision]
#     ) -> tuple[dict, dict]
#       Split one record into (sql_dict, mongo_dict).
#       Rules:
#         - Backend.SQL    → goes to sql_dict only
#         - Backend.MONGODB → goes to mongo_dict only
#         - Backend.BOTH   → goes to BOTH dicts
#         - Unknown field  → goes to mongo_dict (safe default)
#
# DATA CLASS: RouteResult
# -----------------------
#   Attributes:
#   -----------
#   - records_processed: int
#   - sql_inserts: int
#   - mongo_inserts: int
#   - errors: list[str]
#
# ==============================================
from dataclasses import dataclass, field
from typing import Any

from src.analysis.decision import Backend, PlacementDecision
@dataclass
class RouteResult:
    records_processed: int = 0
    sql_inserts: int = 0  # Actually upserts (insert or update)
    mongo_inserts: int = 0  # Actually upserts (insert or update)
    errors: list[str] = field(default_factory=list)

class RecordRouter:
    def __init__(self, mysql_client, mongo_client):
        self.mysql_client = mysql_client
        self.mongo_client = mongo_client

    def route_batch(self, records: list[dict], decisions : dict[str, PlacementDecision], table_name="records", collection_name="records") -> RouteResult:
        # For each record:
        #   1. Split into sql_part and mongo_part using decisions
        #   2. Batch upsert sql_parts into MySQL (insert or update on duplicate)
        #   3. Batch upsert mongo_parts into MongoDB (insert or update on duplicate)
        # Returns a RouteResult with counts and errors.
        result = RouteResult()
        sql_batch = []
        mongo_batch = []

        # Extract PRIMARY KEY field (not all unique fields) - used for upsert matching
        # Only the field marked as primary key should be used for duplicate detection
        primary_key_field = None
        primary_key_decision = None
        for field, decision in decisions.items():
            if decision.is_primary_key and decision.backend in (Backend.SQL, Backend.BOTH):
                primary_key_field = decision.sql_column_name or field
                primary_key_decision = decision
                break

        parent_key_record_field = (
            primary_key_decision.field_name
            if primary_key_decision is not None
            else primary_key_field
        )
        array_sql_batches: dict[str, list[dict[str, Any]]] = {}

        for record in records:
            try:
                sql_part, mongo_part, sql_array_part, split_errors = self._split_record(record, decisions)
                if sql_part:
                    sql_batch.append(sql_part)
                if mongo_part:
                    mongo_batch.append(mongo_part)

                for err in split_errors:
                    result.errors.append(err)

                for array_field, array_values in sql_array_part.items():
                    if primary_key_field is None or parent_key_record_field is None:
                        result.errors.append(
                            f"Skipping SQL array field '{array_field}': no SQL primary key available for parent linkage"
                        )
                        continue

                    parent_key_value = record.get(parent_key_record_field)
                    if parent_key_value is None or parent_key_value == "":
                        result.errors.append(
                            f"Skipping SQL array field '{array_field}': record is missing parent key '{parent_key_record_field}'"
                        )
                        continue

                    array_sql_batches.setdefault(array_field, []).append(
                        {
                            "parent_key": parent_key_value,
                            "values": array_values,
                        }
                    )
                result.records_processed += 1
            except Exception as e:
                result.errors.append(f"Error processing record {record}: {str(e)}")
        
        # For MongoDB, use the primary key if it goes to MongoDB, otherwise use first unique field
        # EXCLUDE timestamp fields from being used as upsert keys
        mongo_key_field = None
        if primary_key_field and primary_key_decision:
            # Check if primary key goes to MongoDB
            if primary_key_decision.backend in (Backend.MONGODB, Backend.BOTH):
                # Make sure it's not a timestamp field
                field_lower = primary_key_decision.field_name.lower()
                is_timestamp = any(x in field_lower for x in 
                                 ['timestamp', '_at', 'created', 'updated', 'ingested', 'time', 'date'])
                if not is_timestamp:
                    mongo_key_field = primary_key_decision.field_name
        
        # Fallback to first non-timestamp unique field in MongoDB if no primary key there
        if not mongo_key_field:
            for field, decision in decisions.items():
                if decision.is_unique and decision.backend in (Backend.MONGODB, Backend.BOTH):
                    # Exclude timestamp fields
                    field_lower = field.lower()
                    is_timestamp = any(x in field_lower for x in 
                                     ['timestamp', '_at', 'created', 'updated', 'ingested', 'time', 'date'])
                    if not is_timestamp:
                        mongo_key_field = field
                        break
        
        # Upsert batches (insert or update based on PRIMARY KEY only)
        if sql_batch:
            try:
                self.mysql_client.ensure_table(table_name, decisions)
                upserted_sql = self.mysql_client.insert_batch(
                    table_name, sql_batch, 
                    primary_key_field  # Use only PRIMARY KEY for upsert
                )
                result.sql_inserts += upserted_sql
            except Exception as e:
                result.errors.append(f"Error upserting SQL batch: {str(e)}")

        # SQL array normalization path: store SQL-routed arrays in dedicated child tables.
        if array_sql_batches:
            if primary_key_field is None:
                result.errors.append(
                    "Skipping SQL array persistence: no SQL primary key available for parent linkage"
                )
            else:
                parent_key_sql_type = (
                    primary_key_decision.sql_type
                    if primary_key_decision is not None and primary_key_decision.sql_type
                    else "VARCHAR(255)"
                )

                for array_field, payloads in array_sql_batches.items():
                    array_table = self._derive_array_table_name(table_name, array_field)
                    try:
                        self.mysql_client.ensure_array_table(
                            array_table,
                            primary_key_field,
                            parent_key_sql_type,
                        )
                        for payload in payloads:
                            inserted = self.mysql_client.replace_array_values(
                                array_table,
                                primary_key_field,
                                payload["parent_key"],
                                payload["values"],
                            )
                            result.sql_inserts += inserted
                    except Exception as e:
                        result.errors.append(
                            f"Error upserting SQL array field '{array_field}' into table '{array_table}': {str(e)}"
                        )

        if mongo_batch:
            try:
                self.mongo_client.ensure_indexes(collection_name, mongo_key_field)
                upserted_mongo = self.mongo_client.insert_batch(
                    collection_name, mongo_batch,
                    mongo_key_field  # Use primary key or first unique field
                )
                result.mongo_inserts += upserted_mongo
            except Exception as e:
                result.errors.append(f"Error upserting MongoDB batch: {str(e)}")
        return result

    def _split_record(self, record: dict, decisions: dict[str, PlacementDecision]) -> tuple[dict, dict, dict[str, list[Any]], list[str]]:
        # Split one record into (sql_dict, mongo_dict).
        # Rules:
        #   - Backend.SQL     → goes to sql_dict only
        #   - Backend.MONGODB → goes to mongo_dict only
        #   - Backend.BOTH    → goes to BOTH dicts
        #   - Unknown field   → goes to mongo_dict (safe default)
        sql_dict = {}
        mongo_dict = {}
        sql_array_dict: dict[str, list[Any]] = {}
        split_errors: list[str] = []
        
        # Identify linking fields that must be in both databases
        linking_fields = {"username", "sys_ingested_at", "t_stamp"}
        
        for field, value in record.items():
            decision = decisions.get(field)
            if decision is None:
                # Unknown field, default to MongoDB
                mongo_dict[field] = value
            elif decision.backend == Backend.SQL:
                if decision.canonical_type == "array":
                    if isinstance(value, list):
                        sql_array_dict[field] = value
                    else:
                        split_errors.append(
                            f"Expected list value for SQL array field '{field}', got {type(value).__name__}. Routing to MongoDB."
                        )
                        mongo_dict[field] = value
                    continue
                sql_field = decision.sql_column_name or field
                sql_dict[sql_field] = value
                # CRITICAL: If this is a linking field, also add to MongoDB
                if field in linking_fields:
                    mongo_dict[field] = value
            elif decision.backend == Backend.MONGODB:
                mongo_dict[field] = value
            elif decision.backend == Backend.BOTH:
                if decision.canonical_type == "array":
                    # Defensive path: preserve BOTH semantics if it ever occurs.
                    if isinstance(value, list):
                        sql_array_dict[field] = value
                        mongo_dict[field] = value
                    else:
                        split_errors.append(
                            f"Expected list value for BOTH array field '{field}', got {type(value).__name__}. Routing to MongoDB only."
                        )
                        mongo_dict[field] = value
                    continue
                sql_field = decision.sql_column_name or field
                sql_dict[sql_field] = value
                mongo_dict[field] = value
            else:
                raise ValueError(f"Invalid placement decision for field '{field}': {decision}")
        
        # ENSURE: If record went to SQL, it MUST also go to MongoDB with linking fields
        # This maintains cross-database join capability
        if (sql_dict or sql_array_dict) and not mongo_dict:
            # No MongoDB fields, but we need linking fields there
            for field in linking_fields:
                if field in record:
                    mongo_dict[field] = record[field]
        
        return sql_dict, mongo_dict, sql_array_dict, split_errors

    def _derive_array_table_name(self, root_table: str, field_path: str) -> str:
        """Generate deterministic array-child table name from root table + field path."""
        normalized = field_path.replace(".", "_")
        return f"{root_table}__arr__{normalized}"

    
