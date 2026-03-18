import json
from dataclasses import asdict
from pathlib import Path

from .contracts import CollectionPlan, FieldLocation, RelationshipPlan, SchemaRegistration, SqlTablePlan


class MetadataCatalog:
    """Central metadata manager for Assignment 2.

    Working model:
    - Stores schema registrations.
    - Stores SQL normalization output (table/relationship plans).
    - Stores Mongo decomposition output (embedding/reference decisions).
    - Stores field-to-storage routing map used by query planner.

    This module is the core enabler for metadata-driven CRUD generation.
    """

    def __init__(self, metadata_dir: str = "metadata"):
        """Initialize metadata catalog with storage directory.
        
        Args:
            metadata_dir: Directory to store metadata artifacts
        """
        self.metadata_dir = Path(metadata_dir)
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        
        # Define file paths for each artifact type
        self.schema_file = self.metadata_dir / "schema.json"
        self.sql_plan_file = self.metadata_dir / "sql_plan.json"
        self.mongo_plan_file = self.metadata_dir / "mongo_plan.json"
        self.field_locations_file = self.metadata_dir / "field_locations.json"

    def save_schema(self, registration: SchemaRegistration) -> None:
        """Persist schema registration and version metadata.
        
        Args:
            registration: SchemaRegistration containing user-defined schema
        """
        schema_dict = asdict(registration)
        with open(self.schema_file, 'w') as f:
            json.dump(schema_dict, f, indent=2)
        print(f"Saved schema registration to {self.schema_file}")

    def save_sql_plan(self, tables: list[SqlTablePlan], relationships: list[RelationshipPlan]) -> None:
        """Persist normalized relational planning artifacts.
        
        Args:
            tables: List of SqlTablePlan objects
            relationships: List of RelationshipPlan objects
        """
        plan_dict = {
            "tables": [asdict(table) for table in tables],
            "relationships": [asdict(rel) for rel in relationships]
        }
        with open(self.sql_plan_file, 'w') as f:
            json.dump(plan_dict, f, indent=2)
        print(f"Saved SQL plan ({len(tables)} tables, {len(relationships)} relationships) to {self.sql_plan_file}")

    def save_mongo_plan(self, collections: list[CollectionPlan]) -> None:
        """Persist Mongo embedding/reference planning artifacts.
        
        Args:
            collections: List of CollectionPlan objects
        """
        plan_dict = {
            "collections": [asdict(collection) for collection in collections]
        }
        with open(self.mongo_plan_file, 'w') as f:
            json.dump(plan_dict, f, indent=2)
        print(f"Saved MongoDB plan ({len(collections)} collections) to {self.mongo_plan_file}")

    def save_field_locations(self, mappings: list[FieldLocation]) -> None:
        """Persist field-level storage location metadata.
        
        Args:
            mappings: List of FieldLocation objects mapping field paths to storage
        """
        mappings_dict = {
            "field_locations": [asdict(mapping) for mapping in mappings]
        }
        with open(self.field_locations_file, 'w') as f:
            json.dump(mappings_dict, f, indent=2)
        print(f"Saved {len(mappings)} field location mappings to {self.field_locations_file}")

    def get_field_locations(self) -> list[FieldLocation]:
        """Return the current field routing map for query planning.
        
        Returns:
            List of FieldLocation objects, or empty list if file doesn't exist
        """
        if not self.field_locations_file.exists():
            return []
        
        with open(self.field_locations_file, 'r') as f:
            data = json.load(f)
        
        # Convert dictionaries back to FieldLocation objects
        locations = [
            FieldLocation(**mapping) 
            for mapping in data.get("field_locations", [])
        ]
        return locations
