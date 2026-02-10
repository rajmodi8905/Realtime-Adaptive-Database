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
import mysql.connector
from analysis.decision import PlacementDecision, Backend

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
        self.connection = mysql.connector.connect(
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
            cursor = self.connection.cursor(dictionary=False)
            # Always include linking fields
            required_fields = {
                "username": "VARCHAR(255)",
                "sys_ingested_at": "DATETIME",
                "t_stamp": "DATETIME"
            }
            # Add fields from decisions that are SQL-bound
            for field, decision in decisions.items():
                if decision.backend in (Backend.SQL, Backend.BOTH):
                    sql_type = decision.sql_type or "VARCHAR(255)"
                    required_fields[field] = sql_type
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
                columns_def = ", ".join(f"{name} {dtype}" for name, dtype in required_fields.items())
                create_query = f"CREATE TABLE {table_name} (id INT AUTO_INCREMENT PRIMARY KEY, {columns_def})"
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
                for field, dtype in required_fields.items():
                    if field not in existing_columns:
                        alter_query = f"ALTER TABLE {table_name} ADD COLUMN {field} {dtype}"
                        cursor.execute(alter_query)
            self.connection.commit()
            cursor.close()
        
    def insert_batch(self, table_name: str, records: list[dict]) -> int:
        # Insert multiple records, return count inserted
        if self.connection is not None and records:
            cursor = self.connection.cursor()
            # Get columns from first record (assuming all have same keys)
            columns = records[0].keys()
            placeholders = ", ".join(["%s"] * len(columns))
            column_names = ", ".join(columns)
            query = f"INSERT INTO {table_name} ({column_names}) VALUES ({placeholders})"
            values = [tuple(record[col] for col in columns) for record in records]
            cursor.executemany(query, values)
            self.connection.commit()
            inserted_count = cursor.rowcount
            cursor.close()
            return inserted_count
        else:
            return 0

    def get_current_columns(self, table_name: str) -> dict[str, str]:
        # Query INFORMATION_SCHEMA to get current column names and types
        if self.connection is not None:
            cursor = self.connection.cursor(buffered=True)
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
            cursor = self.connection.cursor(dictionary=True)
            if params is not None:
                cursor.execute(query, params)
            else:
                cursor.execute(query)
            results = cast(list[dict[str, Any]], cursor.fetchall())
            cursor.close()
            return results
        else:
            return []
    def __enter__(self):
        self.connect()
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()
