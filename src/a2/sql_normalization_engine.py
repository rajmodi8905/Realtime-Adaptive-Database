from .contracts import ClassifiedField, RelationshipPlan, SchemaRegistration, SqlTablePlan


class SqlNormalizationEngine:
    """Phase 5A: Automated relational normalization strategy.

    Normalization approach
    ─────────────────────
    1NF – Eliminate repeating groups: every array field detected in the
          classified-fields list becomes a separate child table.
    2NF – Remove partial dependencies: each child table's non-key
          columns depend on the whole primary key, not just a subset.
    3NF – Remove transitive dependencies: 1:1 nested objects (non-array)
          are flattened into the parent table using dot-notation column
          names, avoiding extra tables that would introduce transitive
          dependencies through artificial join keys.

    Working model:
    1. Analyze schema + ClassifiedField list from A1 classification.
    2. Detect entities and one-to-many boundaries using nesting info
       (is_array, nesting_depth, parent_path) already computed by A1.
    3. Generate normalized tables with PK/FK/index strategy.
    4. Execute CREATE TABLE on MySQL using the generated plans.
    """

    # SQL type fallback when ClassifiedField.sql_type is None
    _TYPE_MAP: dict[str, str] = {
        "int": "BIGINT",
        "float": "DOUBLE",
        "bool": "BOOLEAN",
        "str": "VARCHAR(255)",
        "datetime": "DATETIME",
        "ip": "VARCHAR(45)",
        "uuid": "CHAR(36)",
    }

    def __init__(self) -> None:
        # Per-column constraint metadata populated by generate_table_plans,
        # consumed by _build_create_ddl inside execute_table_plans.
        self._column_meta: dict[str, dict[str, dict]] = {}

    # ================================================================== #
    #  PUBLIC API                                                         #
    # ================================================================== #

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

        # ── 1. Identify every array path (defines entity boundaries) ───
        array_paths: set[str] = set()
        for cf in classified_fields:
            if cf.is_array or cf.canonical_type == "array":
                array_paths.add(cf.field_path)

        # ── 2. Collect SQL-bound scalar fields (become table columns) ──
        sql_scalars = [
            cf
            for cf in classified_fields
            if cf.backend in ("SQL", "BOTH")
            and cf.canonical_type not in ("array", "object")
        ]

        # ── 3. Group scalars by their owning entity ────────────────────
        entity_fields: dict[str, list[ClassifiedField]] = {}
        for cf in sql_scalars:
            entity = self._find_owning_entity(cf.field_path, array_paths)
            entity_fields.setdefault(entity, []).append(cf)

        # ── 4. Register primitive-array entities (no scalar children) ──
        for ap in array_paths:
            if ap not in entity_fields:
                acf = next(
                    (c for c in classified_fields if c.field_path == ap), None
                )
                if acf and acf.backend in ("SQL", "BOTH"):
                    entity_fields[ap] = []

        # ── 5. Schema constraints ──────────────────────────────────────
        constraints = registration.constraints or {}
        unique_candidates = set(constraints.get("unique_candidates", []))
        not_null_fields = set(constraints.get("not_null", []))
        index_candidates: list[str] = list(
            constraints.get("index_candidates", [])
        )
        root_entity = registration.root_entity

        # ── 6. Sort entities: root first, then by nesting depth ────────
        sorted_entities = sorted(
            entity_fields.keys(),
            key=lambda e: (e.count(".") if e else -1, e),
        )

        # ── 7. Build SqlTablePlan per entity ───────────────────────────
        tables: list[SqlTablePlan] = []
        entity_pk: dict[str, str] = {}  # entity_path → pk column name
        self._column_meta.clear()

        for entity_path in sorted_entities:
            fields = entity_fields[entity_path]
            is_root = entity_path == ""
            table_name = self._derive_table_name(entity_path, root_entity)

            # ── columns ───────────────────────────────────────────────
            columns: dict[str, str] = {}
            col_meta: dict[str, dict] = {}
            pk = ""

            for cf in fields:
                col = self._derive_column_name(
                    cf.field_path, entity_path, is_root
                )
                ctype = cf.sql_type or self._TYPE_MAP.get(
                    cf.canonical_type, "TEXT"
                )
                columns[col] = ctype

                nullable = cf.is_nullable and cf.field_path not in not_null_fields
                col_meta[col] = {
                    "is_nullable": nullable,
                    "is_unique": cf.is_unique,
                    "is_primary_key": cf.is_primary_key,
                }

                if cf.is_primary_key:
                    pk = col

            # PK fallback: schema unique_candidates
            if not pk:
                pk = self._find_pk_from_candidates(
                    columns, unique_candidates, entity_path, is_root
                )

            # Primitive-array: add a 'value' column
            if not is_root and not columns:
                columns["value"] = "VARCHAR(255)"
                col_meta["value"] = {
                    "is_nullable": False,
                    "is_unique": False,
                    "is_primary_key": False,
                }

            # ── foreign key to parent ─────────────────────────────────
            foreign_keys: list[dict[str, str]] = []
            if not is_root:
                parent_entity = self._find_parent_entity(
                    entity_path, array_paths
                )
                parent_table = self._derive_table_name(
                    parent_entity, root_entity
                )
                parent_pk = entity_pk.get(parent_entity, "")

                if parent_pk:
                    parent_plan = next(
                        (t for t in tables if t.table_name == parent_table),
                        None,
                    )
                    fk_type = (
                        parent_plan.columns.get(parent_pk, "VARCHAR(255)")
                        if parent_plan
                        else "VARCHAR(255)"
                    )
                    if parent_pk not in columns:
                        columns[parent_pk] = fk_type
                        col_meta[parent_pk] = {
                            "is_nullable": False,
                            "is_unique": False,
                            "is_primary_key": False,
                        }

                    foreign_keys.append(
                        {
                            "column": parent_pk,
                            "references_table": parent_table,
                            "references_column": parent_pk,
                            "source_path": entity_path,
                        }
                    )

            # ── indexes ───────────────────────────────────────────────
            indexes: list[list[str]] = []
            for fk in foreign_keys:
                idx = [fk["column"]]
                if idx not in indexes:
                    indexes.append(idx)

            for ic in index_candidates:
                resolved = self._resolve_index_candidate(
                    ic, columns, entity_path, is_root
                )
                if resolved and [resolved] not in indexes:
                    indexes.append([resolved])

            # ── record PK for child tables to reference ───────────────
            entity_pk[entity_path] = pk
            self._column_meta[table_name] = col_meta

            tables.append(
                SqlTablePlan(
                    table_name=table_name,
                    columns=columns,
                    primary_key=pk,
                    foreign_keys=foreign_keys,
                    indexes=indexes,
                )
            )

        return tables

    def generate_relationships(
        self, tables: list[SqlTablePlan]
    ) -> list[RelationshipPlan]:
        """Generate FK relationships and cardinality metadata."""
        relationships: list[RelationshipPlan] = []
        for table in tables:
            for fk in table.foreign_keys:
                relationships.append(
                    RelationshipPlan(
                        parent_table=fk["references_table"],
                        child_table=table.table_name,
                        cardinality="one-to-many",
                        parent_key=fk["references_column"],
                        child_foreign_key=fk["column"],
                        source_path=fk.get(
                            "source_path",
                            f"{fk['references_table']}.{table.table_name}",
                        ),
                    )
                )
        return relationships

    def execute_table_plans(
        self,
        tables: list[SqlTablePlan],
        relationships: list[RelationshipPlan],
        mysql_client,
    ) -> dict:
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
        ordered = self._topological_sort(tables, relationships)

        created = 0
        errors: list[str] = []

        for table in ordered:
            ddl = self._build_create_ddl(table)
            try:
                mysql_client.execute(ddl)
                created += 1
            except Exception as exc:
                errors.append(f"{table.table_name}: {exc}")

        return {"tables_created": created, "errors": errors}

    # ================================================================== #
    #  PRIVATE HELPERS                                                    #
    # ================================================================== #

    @staticmethod
    def _find_owning_entity(field_path: str, array_paths: set[str]) -> str:
        """Return the deepest array ancestor of *field_path*, or '' (root).

        Walks up the dot-path from the immediate parent towards the root
        and returns the first (deepest) segment that is a known array.
        """
        parts = field_path.split(".")
        for i in range(len(parts) - 1, 0, -1):
            candidate = ".".join(parts[:i])
            if candidate in array_paths:
                return candidate
        return ""

    @staticmethod
    def _find_parent_entity(entity_path: str, array_paths: set[str]) -> str:
        """Return the nearest array ancestor above *entity_path*, or '' (root)."""
        parts = entity_path.split(".")
        for i in range(len(parts) - 1, 0, -1):
            candidate = ".".join(parts[:i])
            if candidate in array_paths:
                return candidate
        return ""

    @staticmethod
    def _derive_table_name(entity_path: str, root_entity: str) -> str:
        """Map an entity path to a SQL table name."""
        if entity_path == "":
            return root_entity
        # Use the last segment of the path as the table name
        return entity_path.rsplit(".", 1)[-1]

    @staticmethod
    def _derive_column_name(
        field_path: str, entity_path: str, is_root: bool
    ) -> str:
        """Derive column name from a field path.

        Root table  → full dot-path   (e.g. ``device.model``)
        Child table → stripped prefix  (e.g. ``text`` from ``post.comments.text``)
        """
        if is_root:
            return field_path
        prefix = entity_path + "."
        if field_path.startswith(prefix):
            return field_path[len(prefix):]
        return field_path

    @staticmethod
    def _find_pk_from_candidates(
        columns: dict[str, str],
        unique_candidates: set[str],
        entity_path: str,
        is_root: bool,
    ) -> str:
        """Pick a primary key from schema unique_candidates."""
        for uc in unique_candidates:
            if is_root:
                if uc in columns:
                    return uc
            else:
                prefix = entity_path + "."
                if uc.startswith(prefix):
                    stripped = uc[len(prefix):]
                    if stripped in columns:
                        return stripped
                # Also check direct match (e.g. FK column name)
                if uc in columns:
                    return uc
        return ""

    @staticmethod
    def _resolve_index_candidate(
        candidate: str,
        columns: dict[str, str],
        entity_path: str,
        is_root: bool,
    ) -> str | None:
        """Resolve a schema index_candidate to a column name in this table."""
        if is_root:
            return candidate if candidate in columns else None
        prefix = entity_path + "."
        if candidate.startswith(prefix):
            stripped = candidate[len(prefix):]
            if stripped in columns:
                return stripped
        if candidate in columns:
            return candidate
        return None

    def _build_create_ddl(self, table: SqlTablePlan) -> str:
        """Build a ``CREATE TABLE IF NOT EXISTS`` DDL statement."""
        meta = self._column_meta.get(table.table_name, {})
        parts: list[str] = []

        for col_name, col_type in table.columns.items():
            cm = meta.get(col_name, {})
            tokens = [f"  `{col_name}`", col_type]

            if not cm.get("is_nullable", True):
                tokens.append("NOT NULL")
            if cm.get("is_unique", False) and col_name != table.primary_key:
                tokens.append("UNIQUE")

            parts.append(" ".join(tokens))

        if table.primary_key:
            parts.append(f"  PRIMARY KEY (`{table.primary_key}`)")

        for fk in table.foreign_keys:
            parts.append(
                f"  FOREIGN KEY (`{fk['column']}`) "
                f"REFERENCES `{fk['references_table']}`(`{fk['references_column']}`)"
            )

        for idx in table.indexes:
            idx_cols = ", ".join(f"`{c}`" for c in idx)
            idx_name = "_".join(idx).replace(".", "_") + "_idx"
            parts.append(f"  INDEX `{idx_name}` ({idx_cols})")

        body = ",\n".join(parts)
        return (
            f"CREATE TABLE IF NOT EXISTS `{table.table_name}` (\n"
            f"{body}\n"
            f")"
        )

    @staticmethod
    def _topological_sort(
        tables: list[SqlTablePlan],
        relationships: list[RelationshipPlan],
    ) -> list[SqlTablePlan]:
        """Sort tables so that parent tables are created before children (Kahn's algorithm)."""
        table_map = {t.table_name: t for t in tables}
        children_of: dict[str, list[str]] = {}
        in_degree: dict[str, int] = {t.table_name: 0 for t in tables}

        for rel in relationships:
            children_of.setdefault(rel.parent_table, []).append(
                rel.child_table
            )
            if rel.child_table in in_degree:
                in_degree[rel.child_table] += 1

        queue = [name for name, deg in in_degree.items() if deg == 0]
        ordered: list[SqlTablePlan] = []

        while queue:
            name = queue.pop(0)
            if name in table_map:
                ordered.append(table_map[name])
            for child in children_of.get(name, []):
                in_degree[child] -= 1
                if in_degree[child] == 0:
                    queue.append(child)

        # Append any remaining (shouldn't happen in well-formed input)
        seen = {t.table_name for t in ordered}
        for t in tables:
            if t.table_name not in seen:
                ordered.append(t)

        return ordered
