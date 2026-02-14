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
    sql_inserts: int = 0
    mongo_inserts: int = 0
    errors: list[str] = field(default_factory=list)

class RecordRouter:
    def __init__(self, mysql_client, mongo_client):
        self.mysql_client = mysql_client
        self.mongo_client = mongo_client

    def route_batch(self, records: list[dict], decisions : dict[str, PlacementDecision], table_name="records", collection_name="records") -> RouteResult:
        # For each record:
        #   1. Split into sql_part and mongo_part using decisions
        #   2. Batch insert sql_parts into MySQL
        #   3. Batch insert mongo_parts into MongoDB
        # Returns a RouteResult with counts and errors.
        result = RouteResult()
        sql_batch = []
        mongo_batch = []
        for record in records:
            try:
                sql_part, mongo_part = self._split_record(record, decisions)
                if sql_part:
                    sql_batch.append(sql_part)
                if mongo_part:
                    mongo_batch.append(mongo_part)
                result.records_processed += 1
            except Exception as e:
                result.errors.append(f"Error processing record {record}: {str(e)}")

        # Insert batches
        if sql_batch:
            try:
                self.mysql_client.ensure_table(table_name, decisions)
                inserted_sql = self.mysql_client.insert_batch(table_name, sql_batch)
                result.sql_inserts += inserted_sql
            except Exception as e:
                result.errors.append(f"Error inserting SQL batch: {str(e)}")
                print(mongo_batch)
        if mongo_batch:
            try:
                self.mongo_client.ensure_indexes(collection_name)
                inserted_mongo = self.mongo_client.insert_batch(collection_name, mongo_batch)
                print(mongo_batch)
                result.mongo_inserts += inserted_mongo
            except Exception as e:
                result.errors.append(f"Error inserting MongoDB batch: {str(e)}")
        return result

    def _split_record(self, record: dict, decisions: dict[str, PlacementDecision]) -> tuple[dict, dict]:
        # Split one record into (sql_dict, mongo_dict, buffer_dict).
        # Rules:
        #   - Backend.SQL    → goes to sql_dict only
        #   - Backend.MONGODB → goes to mongo_dict only
        #   - Backend.BOTH   → goes to BOTH dicts
        #   - Backend.BUFFER → goes to buffer_dict only
        #   - Unknown field  → goes to mongo_dict (safe default)
        sql_dict = {}
        mongo_dict = {}
        buffer_dict = {}
        for field, value in record.items():
            decision = decisions.get(field)
            if decision is None:
                # Unknown field, default to MongoDB
                mongo_dict[field] = value
            elif decision.backend == Backend.SQL:
                sql_dict[field] = value
            elif decision.backend == Backend.MONGODB:
                mongo_dict[field] = value
            elif decision.backend == Backend.BOTH:
                sql_dict[field] = value
                mongo_dict[field] = value
            elif decision.backend == Backend.BUFFER:
                buffer_dict[field] = value
            else:
                raise ValueError(f"Invalid placement decision for field '{field}': {decision}")
            if (buffer_dict !=  {}):
                mongo_dict["__buffer__"] = buffer_dict
        return sql_dict, mongo_dict

    
