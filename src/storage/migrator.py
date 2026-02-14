# ==============================================
# Migrator
# ==============================================
#
# PURPOSE:
#   Handles type migration when schema conflicts are detected.
#   Coordinates migration across SQL and MongoDB backends.
#
# WHY THIS CLASS EXISTS:
#   When we discover that a field's type needs to change (e.g., zip_code
#   from INT to VARCHAR), we need to:
#   1. Migrate all existing data
#   2. Update metadata with the new type
#   3. Update placement decisions
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
#       Execute migration for a single field conflict.
#       Returns migration result with counts and status.
#
# ==============================================

from typing import Dict, Any
from ..analysis.decision import TypeConflict, Backend


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
