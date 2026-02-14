# ==============================================
# Migrator
# ==============================================
#
# PURPOSE:
#   Handles migrations when schema conflicts are detected or when
#   field placement decisions change between backends.
#
# WHY THIS CLASS EXISTS:
#   1. When we discover that a field's type needs to change (e.g., zip_code
#      from INT to VARCHAR), we need to migrate existing data.
#   2. When a field's backend decision changes (e.g., MongoDB → SQL because
#      presence ratio increased), we need to move data between backends.
#
# CLASS: Migrator
# ---------------
#   Stateless coordinator that orchestrates migrations.
#
#   Methods:
#   --------
#   - migrate_field(
#         conflict: TypeConflict,
#         mysql_client: MySQLClient,
#         mongo_client: MongoClient,
#         table_name: str,
#         collection_name: str
#     ) -> dict
#       Execute migration for a single field type conflict.
#       Returns migration result with counts and status.
#
#   - migrate_backend(
#         field_name: str,
#         old_backend: Backend,
#         new_backend: Backend,
#         new_decision: PlacementDecision,
#         mysql_client: MySQLClient,
#         mongo_client: MongoClient,
#         table_name: str,
#         collection_name: str
#     ) -> dict
#       Execute migration when a field's backend changes.
#       Moves data from old backend to new backend.
#
# ==============================================

from typing import Dict, Any, List
from ..analysis.decision import TypeConflict, Backend, PlacementDecision


class Migrator:
    """Simple migration coordinator for handling type conflicts."""
    
    def migrate_field(
        self,
        conflict: TypeConflict,
        mysql_client,
        mongo_client,
        table_name: str = "records",
        collection_name: str = "records"
    ) -> Dict[str, Any]:
        """
        Execute migration for a field type conflict.
        
        Args:
            conflict: The detected type conflict
            mysql_client: Connected MySQLClient instance
            mongo_client: Connected MongoClient instance
            table_name: SQL table name
            collection_name: MongoDB collection name
            
        Returns:
            Dict with migration results:
            {
                "field": str,
                "backend": str,
                "old_type": str,
                "new_type": str,
                "records_migrated": int,
                "success": bool,
                "error": str (if failed)
            }
        """
        
        # Check if migration is possible
        if not conflict.can_widen:
            return {
                "field": conflict.field_name,
                "backend": conflict.stored_backend.value,
                "old_type": conflict.stored_type,
                "new_type": conflict.incoming_type,
                "records_migrated": 0,
                "success": False,
                "error": f"Cannot widen type: {conflict.reason}"
            }
        
        try:
            records_migrated = 0
            
            # Determine which backend to migrate
            if conflict.stored_backend == Backend.SQL:
                # Migrate SQL table
                new_sql_type = self._get_sql_type(conflict.widened_type)
                records_migrated = mysql_client.migrate_field_type(
                    table_name=table_name,
                    field_name=conflict.field_name,
                    old_type=conflict.stored_type,
                    new_type=conflict.widened_type,
                    new_sql_type=new_sql_type
                )
            
            elif conflict.stored_backend == Backend.MONGODB:
                # Migrate MongoDB collection
                records_migrated = mongo_client.migrate_field_type(
                    collection_name=collection_name,
                    field_name=conflict.field_name,
                    old_type=conflict.stored_type,
                    new_type=conflict.widened_type
                )
            
            elif conflict.stored_backend == Backend.BOTH:
                # Migrate both backends
                new_sql_type = self._get_sql_type(conflict.widened_type)
                sql_count = mysql_client.migrate_field_type(
                    table_name=table_name,
                    field_name=conflict.field_name,
                    old_type=conflict.stored_type,
                    new_type=conflict.widened_type,
                    new_sql_type=new_sql_type
                )
                mongo_count = mongo_client.migrate_field_type(
                    collection_name=collection_name,
                    field_name=conflict.field_name,
                    old_type=conflict.stored_type,
                    new_type=conflict.widened_type
                )
                records_migrated = sql_count + mongo_count
            
            return {
                "field": conflict.field_name,
                "backend": conflict.stored_backend.value,
                "old_type": conflict.stored_type,
                "new_type": conflict.widened_type,
                "records_migrated": records_migrated,
                "success": True
            }
            
        except Exception as e:
            return {
                "field": conflict.field_name,
                "backend": conflict.stored_backend.value,
                "old_type": conflict.stored_type,
                "new_type": conflict.incoming_type,
                "records_migrated": 0,
                "success": False,
                "error": str(e)
            }
    
    def _get_sql_type(self, canonical_type: str) -> str:
        """Map canonical type to SQL column type."""
        type_map = {
            "int": "BIGINT",
            "float": "DOUBLE",
            "bool": "BOOLEAN",
            "str": "VARCHAR(255)",
            "ip": "VARCHAR(45)",
            "uuid": "CHAR(36)",
            "datetime": "DATETIME"
        }
        return type_map.get(canonical_type, "TEXT")

    def migrate_backend(
        self,
        field_name: str,
        old_backend: Backend,
        new_backend: Backend,
        new_decision: PlacementDecision,
        mysql_client,
        mongo_client,
        table_name: str = "records",
        collection_name: str = "records"
    ) -> Dict[str, Any]:
        """
        Migrate a field's data when its backend decision changes.
        
        Args:
            field_name: Name of the field to migrate
            old_backend: Previous backend (SQL, MONGODB, or BOTH)
            new_backend: New backend (SQL, MONGODB, or BOTH)
            new_decision: The new PlacementDecision with type info
            mysql_client: Connected MySQLClient instance
            mongo_client: Connected MongoClient instance
            table_name: SQL table name
            collection_name: MongoDB collection name
            
        Returns:
            Dict with migration results
        """
        result = {
            "field": field_name,
            "old_backend": old_backend.value,
            "new_backend": new_backend.value,
            "records_migrated": 0,
            "success": True,
            "error": None
        }
        
        try:
            # Determine migration path
            if old_backend == Backend.MONGODB and new_backend == Backend.SQL:
                # MongoDB → SQL: Copy data to SQL, remove from MongoDB
                result["records_migrated"] = self._migrate_mongo_to_sql(
                    field_name, new_decision, mysql_client, mongo_client,
                    table_name, collection_name
                )
            
            elif old_backend == Backend.SQL and new_backend == Backend.MONGODB:
                # SQL → MongoDB: Copy data to MongoDB, remove from SQL
                result["records_migrated"] = self._migrate_sql_to_mongo(
                    field_name, new_decision, mysql_client, mongo_client,
                    table_name, collection_name
                )
            
            elif old_backend == Backend.MONGODB and new_backend == Backend.BOTH:
                # MongoDB → BOTH: Copy to SQL (keep in MongoDB)
                result["records_migrated"] = self._migrate_mongo_to_sql(
                    field_name, new_decision, mysql_client, mongo_client,
                    table_name, collection_name, remove_from_source=False
                )
            
            elif old_backend == Backend.SQL and new_backend == Backend.BOTH:
                # SQL → BOTH: Copy to MongoDB (keep in SQL)
                result["records_migrated"] = self._migrate_sql_to_mongo(
                    field_name, new_decision, mysql_client, mongo_client,
                    table_name, collection_name, remove_from_source=False
                )
            
            elif old_backend == Backend.BOTH and new_backend == Backend.SQL:
                # BOTH → SQL: Remove from MongoDB (already in SQL)
                result["records_migrated"] = self._remove_field_from_mongo(
                    field_name, mongo_client, collection_name
                )
            
            elif old_backend == Backend.BOTH and new_backend == Backend.MONGODB:
                # BOTH → MongoDB: Remove from SQL (already in MongoDB)
                result["records_migrated"] = self._remove_field_from_sql(
                    field_name, mysql_client, table_name
                )
            
            return result
            
        except Exception as e:
            result["success"] = False
            result["error"] = str(e)
            return result

    def _migrate_mongo_to_sql(
        self,
        field_name: str,
        decision: PlacementDecision,
        mysql_client,
        mongo_client,
        table_name: str,
        collection_name: str,
        remove_from_source: bool = True
    ) -> int:
        """Move field data from MongoDB to SQL."""
        # Get all MongoDB documents that have this field
        docs = mongo_client.find(collection_name, {field_name: {"$exists": True}})
        
        if not docs:
            return 0
        
        # Ensure the column exists in SQL
        mysql_client.ensure_table(table_name, {field_name: decision})
        
        # Update SQL records with the field values
        migrated = 0
        for doc in docs:
            if field_name in doc and "username" in doc and "sys_ingested_at" in doc:
                value = doc[field_name]
                username = doc["username"]
                sys_ingested_at = doc["sys_ingested_at"]
                
                # Update SQL record
                try:
                    query = f"UPDATE {table_name} SET {field_name} = %s WHERE username = %s AND sys_ingested_at = %s"
                    mysql_client.execute(query, (value, username, sys_ingested_at))
                    migrated += 1
                except Exception:
                    # Record might not exist in SQL, skip
                    pass
        
        mysql_client.connection.commit()
        
        # Remove field from MongoDB documents if requested
        if remove_from_source and migrated > 0:
            self._remove_field_from_mongo(field_name, mongo_client, collection_name)
        
        return migrated

    def _migrate_sql_to_mongo(
        self,
        field_name: str,
        decision: PlacementDecision,
        mysql_client,
        mongo_client,
        table_name: str,
        collection_name: str,
        remove_from_source: bool = True
    ) -> int:
        """Move field data from SQL to MongoDB."""
        # Get all SQL records that have this field (not NULL)
        query = f"SELECT username, sys_ingested_at, {field_name} FROM {table_name} WHERE {field_name} IS NOT NULL"
        try:
            rows = mysql_client.fetch_all(query)
        except Exception:
            return 0
        
        if not rows:
            return 0
        
        # Update MongoDB documents with the field values
        migrated = 0
        db = mongo_client.client[mongo_client.database]
        collection = db[collection_name]
        
        for row in rows:
            username = row.get("username")
            sys_ingested_at = row.get("sys_ingested_at")
            value = row.get(field_name)
            
            if username and sys_ingested_at and value is not None:
                # Update MongoDB document
                result = collection.update_one(
                    {"username": username, "sys_ingested_at": sys_ingested_at},
                    {"$set": {field_name: value}}
                )
                if result.modified_count > 0:
                    migrated += 1
        
        # Remove field from SQL if requested
        if remove_from_source and migrated > 0:
            self._remove_field_from_sql(field_name, mysql_client, table_name)
        
        return migrated

    def _remove_field_from_mongo(
        self,
        field_name: str,
        mongo_client,
        collection_name: str
    ) -> int:
        """Remove a field from all MongoDB documents."""
        db = mongo_client.client[mongo_client.database]
        collection = db[collection_name]
        
        result = collection.update_many(
            {field_name: {"$exists": True}},
            {"$unset": {field_name: ""}}
        )
        return result.modified_count

    def _remove_field_from_sql(
        self,
        field_name: str,
        mysql_client,
        table_name: str
    ) -> int:
        """Remove a column from SQL table by dropping it."""
        try:
            # Get count of records that had this field before dropping
            count_query = f"SELECT COUNT(*) as cnt FROM {table_name} WHERE {field_name} IS NOT NULL"
            result = mysql_client.fetch_all(count_query)
            affected_count = result[0]["cnt"] if result else 0
            
            # Drop the column from the table
            drop_query = f"ALTER TABLE {table_name} DROP COLUMN {field_name}"
            mysql_client.execute(drop_query)
            mysql_client.connection.commit()
            
            return affected_count
        except Exception as e:
            # Column might not exist or other error
            print(f"⚠ Could not drop column {field_name}: {e}")
            return 0
