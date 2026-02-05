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

pass
