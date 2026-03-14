from .contracts import ClassifiedField, CollectionPlan, SchemaRegistration


class MongoDecompositionEngine:
    """Phase 5B: MongoDB document decomposition strategy.

    Working model:
    - Use ClassifiedField list from A1 classification to know which
      paths are nested objects/arrays and their nesting characteristics.
    - Decide per path whether to embed or reference based on A1's
      observed stats (is_array, nesting_depth, parent_path).
    - Emit collection plans and reference links.
    - Execute collection creation on MongoDB.

    Suggested decision signals (available via ClassifiedField):
    - is_array / is_nested flags
    - nesting_depth (deep nesting → consider referencing)
    - parent_path (identify shared sub-documents)
    """

    def generate_collection_plans(
        self,
        registration: SchemaRegistration,
        classified_fields: list[ClassifiedField],
    ) -> list[CollectionPlan]:
        """Generate collection decomposition decisions for MongoDB.

        Uses classified_fields carrying A1's nesting analysis to decide
        embedding vs referencing without re-running classification.
        """
        raise NotImplementedError("Implement embed-vs-reference planner")

    def execute_collection_plans(self, collections: list[CollectionPlan], mongo_client) -> dict:
        """Create MongoDB collections and indexes from CollectionPlans.

        Steps:
        1. For each CollectionPlan, create the collection if it doesn't exist.
        2. For referenced paths, create the reference collections and set up
           indexes on the reference key fields.
        3. For embedded paths, no separate collection is needed (data stays
           inside the parent document).

        Args:
            collections: Collection plans from generate_collection_plans().
            mongo_client: Connected A1 MongoClient instance.

        Returns:
            Dict with collections_created count and any errors.
        """
        raise NotImplementedError("Implement MongoDB collection creation")
