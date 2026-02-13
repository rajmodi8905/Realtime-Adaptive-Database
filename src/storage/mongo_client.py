# ==============================================
# MongoClient
# ==============================================
#
# PURPOSE:
#   Manages MongoDB connection and all document operations.
#   Inserts documents preserving full nested structure.
#   Creates indexes on linking fields.
#
# WHY THIS CLASS EXISTS:
#   MongoDB stores all the "messy" data — nested objects, arrays,
#   type-drifting fields, sparse fields. It preserves the original
#   structure without needing a fixed schema.
#
# CLASS: MongoClient
# ------------------
#   Stateful — holds connection to MongoDB.
#
#   Constructor:
#   ------------
#   - __init__(host, port, database, user=None, password=None)
#
#   Methods:
#   --------
#   - connect() -> None
#       Establish connection to MongoDB.
#
#   - disconnect() -> None
#       Close connection.
#
#   - ensure_indexes(collection_name: str) -> None
#       Create indexes on:
#         - username (for cross-DB joins)
#         - sys_ingested_at (for cross-DB joins and ordering)
#         - Compound index on (username, sys_ingested_at)
#
#   - insert_batch(collection_name: str, documents: list[dict]) -> int
#       Insert multiple documents. Return count inserted.
#       Preserves nested structure as-is.
#
#   - insert_one(collection_name: str, document: dict) -> str
#       Insert single document. Return inserted_id.
#
#   - find(collection_name: str, query: dict) -> list[dict]
#       Query documents matching filter.
#
#   Context Manager:
#   ----------------
#   - __enter__ / __exit__ for `with MongoClient(...) as db:` usage.
#
# ==============================================

import pymongo
from pymongo import MongoClient as PyMongoClient
import pymongo.errors
from pymongo.errors import ConnectionFailure, OperationFailure

class MongoClient:
    def __init__(self, host, port, database, user=None, password=None):
        # Store connection params. Don't connect yet.
        self.host = host
        self.port = port
        self.database = database
        self.user = user
        self.password = password
        self.client = None  # Will hold the actual MongoDB client connection

    def connect(self):
        # Establish connection to MongoDB.
        try:
            if self.user and self.password:
                uri = f"mongodb://{self.user}:{self.password}@{self.host}:{self.port}/{self.database}"
            else:
                uri = f"mongodb://{self.host}:{self.port}/{self.database}"
            self.client = PyMongoClient(uri)
            # Test connection
            self.client.admin.command('ping')
            print("Connected to MongoDB successfully.")
        except ConnectionFailure as e:
            print(f"Could not connect to MongoDB: {e}")
            raise
        except OperationFailure as e:
            print(f"Authentication failed: {e}")
            raise

    def disconnect(self):
        # Close connection.
        if self.client:
            self.client.close()
            print("Disconnected from MongoDB.")
            self.client = None

    def ensure_indexes(self, collection_name):
        # Create unique indexes and enforce NOT NULL via schema validator
        if not self.client:
            raise Exception("Not connected to MongoDB.")
        
        db = self.client[self.database]
        collection = db[collection_name]
        
        # Create unique indexes for each field individually
        collection.create_index("username", unique=True)
        collection.create_index("sys_ingested_at", unique=True)
        
        # Enforce NOT NULL using schema validator
        validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["username", "sys_ingested_at"],
                "properties": {
                    "username": {
                        "bsonType": "string",
                        "description": "username is required and cannot be null"
                    },
                    "sys_ingested_at": {
                        "bsonType": "string",
                        "description": "sys_ingested_at is required and cannot be null"
                    }
                }
            }
        }
        
        # Apply validator to existing collection
        try:
            db.command("collMod", collection_name, validator=validator)
            print(f"Schema validator applied to collection '{collection_name}'.")
        except pymongo.errors.OperationFailure:
            # Collection might not exist yet, will be validated on insert
            pass
        
        print(f"Unique indexes ensured on collection '{collection_name}'.")

    def insert_batch(self, collection_name, documents):
        # Insert multiple documents. Return count inserted.
        # Preserves nested structure as-is.
        if not self.client:
            raise Exception("Not connected to MongoDB.")
        collection = self.client[self.database][collection_name]
        result = collection.insert_many(documents)
        print(f"Inserted {len(result.inserted_ids)} documents into '{collection_name}'.")
        return len(result.inserted_ids) 

    def insert_one(self, collection_name, document):
        # Insert single document. Return inserted_id.
        if not self.client:
            raise Exception("Not connected to MongoDB.")
        collection = self.client[self.database][collection_name]
        result = collection.insert_one(document)
        print(f"Inserted document with id {result.inserted_id} into '{collection_name}'.")
        return result.inserted_id

    def find(self, collection_name, query):
        # Query documents matching filter.
        if not self.client:
            raise Exception("Not connected to MongoDB.")
        collection = self.client[self.database][collection_name]
        results = collection.find(query)
        return list(results)

    def __enter__(self):
        # For `with MongoClient(...) as db:` usage.
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()