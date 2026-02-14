# ==============================================
# MySQLClient
# ==============================================
#
# PURPOSE:
#   Manages MySQL connection and all SQL operations.
#   Creates tables dynamically, alters schemas when new fields
#   appear, and inserts records.
#
# WHY THIS CLASS EXISTS:
#   The system creates SQL tables ON THE FLY based on classification
#   decisions. There is no predefined schema. When the classifier
#   says "steps is BIGINT", this class creates the column.
#   When a new field gets promoted to SQL later, this class
#   ALTERs the table to add it.
#
# CLASS: MySQLClient
# ------------------
#   Stateful — holds connection to MySQL.
#
#   Constructor:
#   ------------
#   - __init__(host, port, user, password, database)
#       Store connection params. Don't connect yet.
#
#   Methods:
#   --------
#   - connect() -> None
#       Establish connection. Create database if it doesn't exist.
#
#   - disconnect() -> None
#       Close connection cleanly.
#
#   - ensure_table(
#         table_name: str,
#         decisions: dict[str, PlacementDecision]
#     ) -> None
#       Create table if it doesn't exist, or ALTER TABLE to add
#       new columns for any new SQL-bound fields.
#       Must always include: username, sys_ingested_at, t_stamp.
#       Primary key: auto-increment id.
#
#   - insert_batch(table_name: str, records: list[dict]) -> int
#       Insert multiple records. Return count inserted.
#       Handle missing fields gracefully (NULL).
#
#   - get_current_columns(table_name: str) -> dict[str, str]
#       Query INFORMATION_SCHEMA to get current column names and types.
#       Used to detect what columns need to be added.
#
#   - execute(query: str, params: tuple = None) -> None
#       Execute a raw SQL query (for flexibility).
#
#   - fetch_all(query: str, params: tuple = None) -> list[dict]
#       Execute SELECT and return rows as dicts.
#
#   Context Manager:
#   ----------------
#   - __enter__ / __exit__ for `with MySQLClient(...) as db:` usage.
#
# ==============================================

from typing import Any, Tuple, cast
import pymysql
import pymysql.cursors
from src.analysis.decision import PlacementDecision, Backend

class MySQLClient:
    def __init__(self, host, port, user, password, database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.connection = None
    def connect(self) -> None:
        # Establish connection to MySQL, create database if it doesn't exist
        self.connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
        )
        cursor = self.connection.cursor()
        cursor.execute(f"CREATE DATABASE IF NOT EXISTS {self.database}")
        cursor.execute(f"USE {self.database}")
        cursor.close()
    def disconnect(self) -> None:
        # Close connection cleanly
        if self.connection:
            self.connection.close()
            self.connection = None
    def ensure_table(self, table_name: str, decisions: dict[str, PlacementDecision]) -> None:
        # Create table if it doesn't exist, or ALTER TABLE to add new columns
        if self.connection is not None:
            cursor = self.connection.cursor()
            # Check if table exists
            cursor.execute(
                "SELECT COUNT(*) FROM INFORMATION_SCHEMA.TABLES "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (self.database, table_name)
            )
            cf = cursor.fetchone()
            if cf is None:
                raise RuntimeError("COUNT query returned no rows")
            if cf[0] == 0:
                # Table doesn't exist, create it
                columns_def: str = ""
                for field, decision in decisions.items():
                    if decision.backend in (Backend.SQL, Backend.BOTH):
                        is_nullable = "NULL" if decision.is_nullable else "NOT NULL"
                        dtype = decision.sql_type or "VARCHAR(255)"
                        is_unique = "UNIQUE" if decision.is_unique else ""
                        is_primary_key = "PRIMARY KEY" if decision.is_primary_key else ""
                        # Store required fields and types for table creation
                        columns_def = f"{columns_def}{field} {dtype} {is_nullable} {is_unique} {is_primary_key}, "
                columns_def = columns_def.rstrip(", ")
                create_query = f"CREATE TABLE {table_name}({columns_def})"
                print(create_query) # DEBUG: print the create query
                cursor.execute(create_query)
            else:
                # Table exists, check for missing columns and ALTER TABLE to add them
                cursor.execute(
                    "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS "
                    "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                    (self.database, table_name)
                )
                rows = cast(list[Tuple[Any, ...]], cursor.fetchall())
                existing_columns = set(row[0] for row in rows)
                for field, decision in decisions.items():
                    if decision.backend in (Backend.SQL, Backend.BOTH) and field not in existing_columns:
                        is_nullable = "NULL" if decision.is_nullable else "NOT NULL"
                        dtype = decision.sql_type or "VARCHAR(255)"
                        is_unique = "UNIQUE" if decision.is_unique else ""
                        alter_query = f"ALTER TABLE {table_name} ADD COLUMN {field} {dtype} {is_nullable} {is_unique}"
                        cursor.execute(alter_query)
            self.connection.commit()
            cursor.close()
        
    def insert_batch(self, table_name: str, records: list[dict], primary_key_field: str = None) -> int:
        # Insert or update multiple records (upsert), return count processed
        # primary_key_field: THE primary key field name for duplicate detection
        # Upsert matches only on PRIMARY KEY, not all unique fields
        if self.connection is not None and records:
            cursor = self.connection.cursor()
            upserted_count = 0
            
            for record in records:
                try:
                    columns = list(record.keys())
                    placeholders = ", ".join(['%s'] * len(columns))
                    column_names = ", ".join(columns)
                    
                    # Build ON DUPLICATE KEY UPDATE clause
                    # Update all columns EXCEPT the primary key itself
                    update_parts = []
                    for col in columns:
                        if col != primary_key_field:  # Don't update the primary key
                            update_parts.append(f"{col} = VALUES({col})")
                    
                    if primary_key_field and update_parts:  
                        # Primary key exists - do upsert
                        update_clause = ", ".join(update_parts)
                        query = (
                            f"INSERT INTO {table_name} ({column_names}) "
                            f"VALUES ({placeholders}) "
                            f"ON DUPLICATE KEY UPDATE {update_clause}"
                        )
                    else:
                        # No primary key or nothing to update - just insert
                        query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
                    
                    values = tuple(record.values())
                    cursor.execute(query, values)
                    self.connection.commit()
                    upserted_count += 1
                except Exception as e:
                    # Log error but continue with other records
                    print(f"✗ MySQL upsert failed: {str(e)[:100]}")
                    self.connection.rollback()
            cursor.close()
            return upserted_count
        else:
            return 0

    def get_current_columns(self, table_name: str) -> dict[str, str]:
        # Query INFORMATION_SCHEMA to get current column names and types
        if self.connection is not None:
            cursor = self.connection.cursor()
            cursor.execute(
                "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS "
                "WHERE TABLE_SCHEMA = %s AND TABLE_NAME = %s",
                (self.database, table_name)
            )
            # Each row is a tuple: (column_name, data_type)
            columns: dict[str, str] = {
                str(name): str(dtype)
                for name, dtype in cursor.fetchall()
            }
            cursor.close()
            return columns
        else:
            return {}
    def execute(self, query: str, params: tuple | None = None) -> None:
        # Execute a raw SQL query
        if self.connection is not None:
            cursor = self.connection.cursor()
            if params:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            self.connection.commit()
            cursor.close()
    def fetch_all(self, query: str, params: tuple | None = None) -> list[dict]:
        # Execute SELECT and return rows as dicts
        if self.connection is not None:
            cursor = self.connection.cursor(pymysql.cursors.DictCursor)
            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cast(list[dict[str, Any]], cursor.fetchall())
            cursor.close()
            return results
        else:
            return []
    def migrate_field_type(
        self, 
        table_name: str, 
        field_name: str, 
        old_type: str,
        new_type: str,
        new_sql_type: str
    ) -> int:
        """
        Migrate a field from one type to another.
        
        Args:
            table_name: Name of the table
            field_name: Name of the field to migrate
            old_type: Old canonical type (int, float, str, etc.)
            new_type: New canonical type to convert to
            new_sql_type: New SQL column type (VARCHAR(255), etc.)
            
        Returns:
            Number of records migrated
        """
        if not self.connection:
            raise RuntimeError("Not connected to MySQL")
        
        cursor = self.connection.cursor(pymysql.cursors.DictCursor)
        
        # Step 1: Fetch all records
        query = f"SELECT * FROM {table_name}"
        cursor.execute(query)
        records = cursor.fetchall()
        
        if not records:
            cursor.close()
            return 0
        
        # Step 2: Convert values
        converted_records = []
        for record in records:
            if field_name in record and record[field_name] is not None:
                old_value = record[field_name]
                # Convert to new type
                if new_type == "str":
                    record[field_name] = str(old_value)
                elif new_type == "float":
                    record[field_name] = float(old_value)
                elif new_type == "int":
                    record[field_name] = int(old_value)
            converted_records.append(record)
        
        # Step 3: ALTER TABLE
        alter_query = f"ALTER TABLE {table_name} MODIFY COLUMN {field_name} {new_sql_type}"
        cursor.execute(alter_query)
        self.connection.commit()
        
        # Step 4: Update all records with converted values
        # Use first available key field (prefer unique fields, fallback to any available)
        key_fields = []
        if converted_records:
            # Look for potential key fields (unique identifiers)
            first_record = converted_records[0]
            potential_keys = ['username', 'sys_ingested_at', 'id', 'uuid']
            for key in potential_keys:
                if key in first_record:
                    key_fields.append(key)
            # If no standard keys found, use all fields for WHERE clause
            if not key_fields:
                key_fields = list(first_record.keys())
        
        for record in converted_records:
            # Build UPDATE query with dynamic WHERE clause
            non_key_fields = [col for col in record.keys() if col not in key_fields]
            if non_key_fields:
                set_clause = ", ".join([f"{col} = %s" for col in non_key_fields])
                where_clause = " AND ".join([f"{key} = %s" for key in key_fields])
                update_query = f"UPDATE {table_name} SET {set_clause} WHERE {where_clause}"
                values = [record[col] for col in non_key_fields] + [record[key] for key in key_fields]
                cursor.execute(update_query, tuple(values))
        
        self.connection.commit()
        cursor.close()
        
        return len(converted_records)

    def __enter__(self):
        self.connect()
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
