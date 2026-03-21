from src.analysis.decision import Backend, PlacementDecision
from src.storage.record_router import RecordRouter


class FakeMySQLClient:
    def __init__(self) -> None:
        self.main_rows = []
        self.array_table_calls = []
        self.array_replace_calls = []

    def ensure_table(self, table_name: str, decisions: dict) -> None:
        return None

    def insert_batch(self, table_name: str, records: list[dict], primary_key_field: str | None = None) -> int:
        self.main_rows.extend(records)
        return len(records)

    def ensure_array_table(
        self,
        table_name: str,
        parent_key_column: str,
        parent_key_sql_type: str,
        value_sql_type: str = "TEXT",
    ) -> None:
        self.array_table_calls.append((table_name, parent_key_column, parent_key_sql_type, value_sql_type))

    def replace_array_values(
        self,
        table_name: str,
        parent_key_column: str,
        parent_key_value,
        values: list,
    ) -> int:
        self.array_replace_calls.append((table_name, parent_key_column, parent_key_value, list(values)))
        return len(values)


class FakeMongoClient:
    def __init__(self) -> None:
        self.docs = []

    def ensure_indexes(self, collection_name: str, key_field: str | None = None) -> None:
        return None

    def insert_batch(self, collection_name: str, documents: list[dict], key_field: str | None = None) -> int:
        self.docs.extend(documents)
        return len(documents)


def _decisions() -> dict[str, PlacementDecision]:
    return {
        "username": PlacementDecision(
            field_name="username",
            backend=Backend.BOTH,
            sql_type="VARCHAR(255)",
            sql_column_name="username",
            canonical_type="str",
            is_nullable=False,
            is_unique=True,
            is_primary_key=True,
            reason="pk",
        ),
        "sys_ingested_at": PlacementDecision(
            field_name="sys_ingested_at",
            backend=Backend.BOTH,
            sql_type="VARCHAR(255)",
            sql_column_name="sys_ingested_at",
            canonical_type="str",
            is_nullable=False,
            reason="link",
        ),
        "age": PlacementDecision(
            field_name="age",
            backend=Backend.SQL,
            sql_type="BIGINT",
            sql_column_name="age",
            canonical_type="int",
            is_nullable=True,
            reason="scalar",
        ),
        "tags": PlacementDecision(
            field_name="tags",
            backend=Backend.SQL,
            sql_type="TEXT",
            sql_column_name="tags",
            canonical_type="array",
            is_nullable=True,
            reason="array-sql",
        ),
    }


def test_router_sends_sql_arrays_to_child_tables_without_mongo_copy() -> None:
    mysql_client = FakeMySQLClient()
    mongo_client = FakeMongoClient()
    router = RecordRouter(mysql_client, mongo_client)

    records = [
        {
            "username": "u1",
            "sys_ingested_at": "2026-01-01T00:00:00Z",
            "age": 30,
            "tags": ["a", "b"],
        },
        {
            "username": "u2",
            "sys_ingested_at": "2026-01-01T00:00:01Z",
            "age": 31,
            "tags": ["x"],
        },
    ]

    result = router.route_batch(records, _decisions())

    assert result.records_processed == 2
    assert result.mongo_inserts == 2

    # Main SQL rows should not contain raw array field.
    assert all("tags" not in row for row in mysql_client.main_rows)

    # Mongo docs should not carry SQL-only array field.
    assert all("tags" not in doc for doc in mongo_client.docs)

    # Array values should be persisted in dedicated child table.
    assert mysql_client.array_table_calls
    child_table_name = mysql_client.array_table_calls[0][0]
    assert child_table_name == "records__arr__tags"

    assert len(mysql_client.array_replace_calls) == 2
    assert mysql_client.array_replace_calls[0][2] == "u1"
    assert mysql_client.array_replace_calls[0][3] == ["a", "b"]
    assert mysql_client.array_replace_calls[1][2] == "u2"
    assert mysql_client.array_replace_calls[1][3] == ["x"]
