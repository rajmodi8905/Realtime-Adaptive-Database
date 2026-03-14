from .contracts import CrudOperation, FieldLocation, QueryPlan


class QueryPlanner:
    """Phase 6: Metadata-driven query plan generator.

    Working model:
    - Input: operation request JSON + field location metadata.
    - Output: backend-specific SQL/Mongo query plans + merge strategy.

    The planner stays backend-agnostic and only emits plans.
    Execution is delegated to CrudEngine.
    """

    def build_plan(
        self,
        operation: CrudOperation,
        payload: dict,
        field_locations: list[FieldLocation],
    ) -> QueryPlan:
        """Generate a query plan for create/read/update/delete."""
        raise NotImplementedError("Implement metadata-to-query planning")
