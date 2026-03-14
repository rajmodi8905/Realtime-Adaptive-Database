from .contracts import ClassifiedField, RelationshipPlan, SchemaRegistration, SqlTablePlan


class SqlNormalizationEngine:
    """Phase 5A: Automated relational normalization strategy.

    Working model:
    1. Analyze schema + ClassifiedField list from A1 classification.
    2. Detect entities and one-to-many boundaries using nesting info
       (is_array, nesting_depth, parent_path) already computed by A1.
    3. Generate normalized tables with PK/FK/index strategy.
    4. Execute CREATE TABLE on MySQL using the generated plans.
    """

    def generate_table_plans(
        self,
        registration: SchemaRegistration,
        classified_fields: list[ClassifiedField],
    ) -> list[SqlTablePlan]:
        """Produce normalized table definitions from schema + A1 classification output.

        The classified_fields carry nesting information (is_array, parent_path,
        nesting_depth) so this engine can detect repeating groups and split them
        into child tables without re-running classification.
        """
        raise NotImplementedError("Implement relational decomposition")

    def generate_relationships(self, tables: list[SqlTablePlan]) -> list[RelationshipPlan]:
        """Generate FK relationships and cardinality metadata."""
        raise NotImplementedError("Implement relationship discovery")

    def execute_table_plans(self, tables: list[SqlTablePlan], relationships: list[RelationshipPlan], mysql_client) -> dict:
        """Execute CREATE TABLE statements on MySQL for each SqlTablePlan.

        Steps:
        1. Order tables so parent tables are created before children
           (using relationships to determine dependency order).
        2. For each table, build a CREATE TABLE statement from
           SqlTablePlan.columns, primary_key, foreign_keys, indexes.
        3. Execute each statement via mysql_client.execute().

        Args:
            tables: Normalized table plans from generate_table_plans().
            relationships: FK relationships from generate_relationships().
            mysql_client: Connected A1 MySQLClient instance.

        Returns:
            Dict with tables_created count and any errors.
        """
        raise NotImplementedError("Implement SQL table creation")
