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
        raise NotImplementedError("Implement read execution and result merging")

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
