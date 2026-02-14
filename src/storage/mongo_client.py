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

    def ensure_indexes(self, collection_name, key_field: str = None):
        # Create indexes dynamically based on discovered key field
        if not self.client:
            raise Exception("Not connected to MongoDB.")
        
        db = self.client[self.database]
        collection = db[collection_name]
        
        # Drop existing indexes first (except _id) to avoid conflicts
        try:
            existing_indexes = collection.list_indexes()
            for idx in existing_indexes:
                if idx['name'] != '_id_':  # Never drop the _id index
                    collection.drop_index(idx['name'])
        except Exception as e:
            pass  # Collection might not exist yet
        
        # Create unique index on the key field if specified
        if key_field:
            collection.create_index(key_field, unique=True)
            print(f"Created unique index on '{key_field}' in '{collection_name}'.")
        
        # Create non-unique index on sys_ingested_at for time-based queries
        collection.create_index("sys_ingested_at", unique=False)
        
        # Enforce NOT NULL using schema validator only for required fields
        validator = {
            "$jsonSchema": {
                "bsonType": "object",
                "required": ["sys_ingested_at"],  # Only sys_ingested_at is always required
                "properties": {
                    "sys_ingested_at": {
                        "bsonType": "string",
                        "description": "sys_ingested_at is required and cannot be null"
                    }
                }
            }
        }
        
        # Add key field to required if specified
        if key_field:
            validator["$jsonSchema"]["required"].append(key_field)
            validator["$jsonSchema"]["properties"][key_field] = {
                "bsonType": "string",
                "description": f"{key_field} is required and cannot be null"
            }
        
        # Apply validator to existing collection
        try:
            db.command("collMod", collection_name, validator=validator)
            print(f"Schema validator applied to collection '{collection_name}'.")
        except pymongo.errors.OperationFailure:
            # Collection might not exist yet, will be validated on insert
            pass

    def insert_batch(self, collection_name, documents, key_field: str = None):
        # Insert or update multiple documents (upsert). Return count processed.
        # Preserves nested structure as-is.
        # key_field: THE field to use for duplicate detection (primary key or unique field)
        if not self.client:
            raise Exception("Not connected to MongoDB.")
        collection = self.client[self.database][collection_name]
        upserted_count = 0
        
        for doc in documents:
            try:
                if key_field and key_field in doc:
                    # Use update_one with upsert=True based on key field
                    key_value = doc.get(key_field)
                    if not key_value:
                        print(f"✗ MongoDB upsert skipped: Document missing {key_field}")
                        continue
                    
                    # Update entire document, or insert if not exists
                    result = collection.update_one(
                        {key_field: key_value},  # Filter by the key field
                        {'$set': doc},            # Update all fields
                        upsert=True               # Insert if doesn't exist
                    )
                    upserted_count += 1
                else:
                    # No key field - just insert (may fail on duplicate)
                    collection.insert_one(doc)
                    upserted_count += 1
                    
            except Exception as e:
                print(f"✗ MongoDB upsert failed: {str(e)[:100]}")
        
        if upserted_count > 0:
            print(f"Upserted {upserted_count} documents into '{collection_name}'.")
        return upserted_count 

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

    def migrate_field_type(
        self, 
        collection_name: str, 
        field_name: str, 
        old_type: str,
        new_type: str
    ) -> int:
        """
        Migrate a field from one type to another in MongoDB.
        
        Args:
            collection_name: Name of the collection
            field_name: Name of the field to migrate (can be nested with dots)
            old_type: Old canonical type (int, float, str, etc.)
            new_type: New canonical type to convert to
            
        Returns:
            Number of documents migrated
        """
        if not self.client:
            raise Exception("Not connected to MongoDB")
        
        collection = self.client[self.database][collection_name]
        
        # Find all documents that have this field
        query = {field_name: {"$exists": True}}
        documents = list(collection.find(query))
        
        if not documents:
            return 0
        
        # Convert and update each document
        updated_count = 0
        for doc in documents:
            # Handle nested fields (e.g., "metadata.sensor.version")
            field_parts = field_name.split(".")
            
            # Navigate to the field
            obj = doc
            for part in field_parts[:-1]:
                if part in obj:
                    obj = obj[part]
                else:
                    break
            
            # Get the field value
            last_part = field_parts[-1]
            if last_part in obj and obj[last_part] is not None:
                old_value = obj[last_part]
                
                # Convert to new type
                try:
                    if new_type == "str":
                        obj[last_part] = str(old_value)
                    elif new_type == "float":
                        obj[last_part] = float(old_value)
                    elif new_type == "int":
                        obj[last_part] = int(old_value)
                    
                    # Update document
                    collection.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {field_name: obj[last_part]}}
                    )
                    updated_count += 1
                except (ValueError, TypeError):
                    # Skip documents that can't be converted
                    continue
        
        return updated_count

    def __enter__(self):
        # For `with MongoClient(...) as db:` usage.
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.disconnect()