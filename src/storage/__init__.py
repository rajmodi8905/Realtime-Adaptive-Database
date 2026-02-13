# ==============================================
# TOPIC 3: STORAGE (MySQL + MongoDB)
# ==============================================
#
# This package handles all database operations:
# connecting, creating schemas dynamically, and inserting data.
#
# Modules:
# --------
# - mysql_client.py    → MySQL connection and operations
# - mongo_client.py    → MongoDB connection and operations
# - record_router.py   → Splits a record and routes parts to the right DB
# - migrator.py        → Handles type migration when conflicts detected
#
# ==============================================

from .mysql_client import MySQLClient
from .mongo_client import MongoClient
from .record_router import RecordRouter, RouteResult
from .migrator import Migrator

__all__ = [
    "MySQLClient",
    "MongoClient",
    "RecordRouter",
    "RouteResult",
    "Migrator"
]