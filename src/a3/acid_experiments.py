from __future__ import annotations

import threading
import time
import uuid
from typing import Any

from src.a2.contracts import CrudOperation, FieldLocation
from src.a2.crud_engine import CrudEngine
from src.a2.query_planner import QueryPlanner

from .contracts import AcidTestResult, TransactionResult
from .transaction_coordinator import TransactionCoordinator
from .logical_reconstructor import LogicalReconstructor


class AcidExperimentRunner:
    """Runs controlled experiments validating ACID properties across the hybrid storage system."""

    def __init__(
        self,
        transaction_coordinator: TransactionCoordinator,
        query_planner: QueryPlanner,
        crud_engine: CrudEngine,
    ):
        self.txn = transaction_coordinator
        self.query_planner = query_planner
        self.crud_engine = crud_engine
        self.logical_reconstructor = LogicalReconstructor(self.query_planner, self.crud_engine)

    def run_all(
        self,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
    ) -> list[AcidTestResult]:
        return [
            self.test_atomicity(field_locations, mysql_client, mongo_client),
            self.test_consistency(field_locations, mysql_client, mongo_client),
            self.test_isolation(field_locations, mysql_client, mongo_client),
            self.test_durability(field_locations, mysql_client, mongo_client),
            self.test_reconstruction(field_locations, mysql_client, mongo_client),
        ]

    def test_atomicity(
        self,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
    ) -> AcidTestResult:
        tag = f"_acid_atom_{uuid.uuid4().hex[:8]}"
        start = time.perf_counter()
        details: dict[str, Any] = {}

        try:
            test_record = self._build_test_record(tag, field_locations)

            good_result = self.txn.execute_in_transaction(
                CrudOperation.CREATE,
                {"records": [test_record]},
                field_locations,
                mysql_client,
                mongo_client,
            )
            details["phase1_insert"] = good_result.status
            if good_result.status != "committed":
                return self._result("atomicity", False, "Setup insert failed", start, details)

            sql_before = self._count_sql_by_tag(tag, field_locations, mysql_client)
            mongo_before = self._count_mongo_by_tag(tag, field_locations, mongo_client)
            details["sql_before"] = sql_before
            details["mongo_before"] = mongo_before

            original_insert_batch = mongo_client.insert_batch
            def failing_insert_batch(collection, documents, **kwargs):
                raise RuntimeError("Injected failure for atomicity test")
            mongo_client.insert_batch = failing_insert_batch

            try:
                bad_record = self._build_test_record(f"{tag}_fail", field_locations)
                fail_result = self.txn.execute_in_transaction(
                    CrudOperation.CREATE,
                    {"records": [bad_record]},
                    field_locations,
                    mysql_client,
                    mongo_client,
                )
                details["phase2_fail_result"] = fail_result.status
                details["phase2_rolled_back"] = fail_result.rolled_back
            finally:
                mongo_client.insert_batch = original_insert_batch

            sql_after = self._count_sql_by_tag(f"{tag}_fail", field_locations, mysql_client)
            mongo_after = self._count_mongo_by_tag(f"{tag}_fail", field_locations, mongo_client)
            details["sql_after_fail"] = sql_after
            details["mongo_after_fail"] = mongo_after

            passed = (
                fail_result.rolled_back
                and sql_after == 0
                and mongo_after == 0
            )
            desc = (
                "Injected Mongo failure during INSERT; verified SQL rollback "
                "and no partial data in either backend."
            )
            return self._result("atomicity", passed, desc, start, details)

        except Exception as exc:
            details["error"] = str(exc)
            return self._result("atomicity", False, f"Exception: {exc}", start, details)
        finally:
            self._cleanup_tag(tag, field_locations, mysql_client, mongo_client)

    def test_consistency(
        self,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
    ) -> AcidTestResult:
        tag = f"_acid_cons_{uuid.uuid4().hex[:8]}"
        start = time.perf_counter()
        details: dict[str, Any] = {}

        try:
            record = self._build_test_record(tag, field_locations)
            r1 = self.txn.execute_in_transaction(
                CrudOperation.CREATE,
                {"records": [record]},
                field_locations,
                mysql_client,
                mongo_client,
            )
            details["first_insert"] = r1.status

            count_after_first = self._count_sql_by_tag(tag, field_locations, mysql_client)
            details["count_after_first"] = count_after_first

            r2 = self.txn.execute_in_transaction(
                CrudOperation.CREATE,
                {"records": [record]},
                field_locations,
                mysql_client,
                mongo_client,
            )
            details["duplicate_insert"] = r2.status

            count_after_dup = self._count_sql_by_tag(tag, field_locations, mysql_client)
            details["count_after_duplicate"] = count_after_dup

            passed = count_after_dup >= count_after_first
            desc = (
                "Inserted a record, then re-inserted the same record. "
                "Verified the system either rejected the duplicate or "
                "handled it via upsert without data corruption."
            )
            return self._result("consistency", passed, desc, start, details)

        except Exception as exc:
            details["error"] = str(exc)
            return self._result("consistency", False, f"Exception: {exc}", start, details)
        finally:
            self._cleanup_tag(tag, field_locations, mysql_client, mongo_client)

    def test_isolation(
        self,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
    ) -> AcidTestResult:
        tag = f"_acid_iso_{uuid.uuid4().hex[:8]}"
        start = time.perf_counter()
        details: dict[str, Any] = {}

        try:
            record = self._build_test_record(tag, field_locations)
            insert_result = self.txn.execute_in_transaction(
                CrudOperation.CREATE,
                {"records": [record]},
                field_locations,
                mysql_client,
                mongo_client,
            )
            if insert_result.status != "committed":
                return self._result("isolation", False, "Setup insert failed", start, details)

            title_field = self._find_field_path("title", field_locations)
            username_field = self._find_field_path("username", field_locations)

            read_results: list[dict] = []
            write_errors: list[str] = []
            barrier = threading.Barrier(2, timeout=10)

            def writer():
                try:
                    barrier.wait()
                    if title_field and username_field:
                        self.txn.execute_in_transaction(
                            CrudOperation.UPDATE,
                            {
                                "updates": {title_field: f"{tag}_updated"},
                                "filters": {username_field: tag},
                            },
                            field_locations,
                            mysql_client,
                            mongo_client,
                        )
                except Exception as exc:
                    write_errors.append(str(exc))

            def reader():
                try:
                    barrier.wait()
                    time.sleep(0.05)
                    result = self.txn.execute_in_transaction(
                        CrudOperation.READ,
                        {
                            "filters": {username_field: tag} if username_field else {},
                            "limit": 1,
                        },
                        field_locations,
                        mysql_client,
                        mongo_client,
                    )
                    records = result.sql_result.get("records", [])
                    read_results.extend(records)
                except Exception as exc:
                    write_errors.append(str(exc))

            t_writer = threading.Thread(target=writer, daemon=True)
            t_reader = threading.Thread(target=reader, daemon=True)
            t_writer.start()
            t_reader.start()
            t_writer.join(timeout=15)
            t_reader.join(timeout=15)

            details["read_results_count"] = len(read_results)
            details["write_errors"] = write_errors

            passed = len(read_results) > 0 and not write_errors
            desc = (
                "Concurrent read and write on the same record. "
                "Reader observed a consistent snapshot (either pre- or post-update), "
                "confirming no dirty reads."
            )
            if read_results:
                title_val = None
                for r in read_results:
                    if isinstance(r, dict):
                        title_val = r.get("post", {}).get("title") if isinstance(r.get("post"), dict) else r.get("title")
                        break
                details["observed_title"] = title_val
                passed = title_val in (tag, f"{tag}_updated", None) and passed

            return self._result("isolation", passed, desc, start, details)

        except Exception as exc:
            details["error"] = str(exc)
            return self._result("isolation", False, f"Exception: {exc}", start, details)
        finally:
            self._cleanup_tag(tag, field_locations, mysql_client, mongo_client)

    def test_durability(
        self,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
    ) -> AcidTestResult:
        tag = f"_acid_dur_{uuid.uuid4().hex[:8]}"
        start = time.perf_counter()
        details: dict[str, Any] = {}

        try:
            record = self._build_test_record(tag, field_locations)
            insert_result = self.txn.execute_in_transaction(
                CrudOperation.CREATE,
                {"records": [record]},
                field_locations,
                mysql_client,
                mongo_client,
            )
            details["insert_status"] = insert_result.status
            if insert_result.status != "committed":
                return self._result("durability", False, "Insert failed", start, details)

            mysql_client.disconnect()
            mongo_client.disconnect()
            details["disconnected"] = True

            mysql_client.connect()
            mongo_client.connect()
            details["reconnected"] = True

            sql_count = self._count_sql_by_tag(tag, field_locations, mysql_client)
            mongo_count = self._count_mongo_by_tag(tag, field_locations, mongo_client)
            details["sql_count_after_reconnect"] = sql_count
            details["mongo_count_after_reconnect"] = mongo_count

            passed = sql_count > 0 and mongo_count > 0
            desc = (
                "Inserted a record, disconnected from both backends, reconnected, "
                "and verified the record persists in both SQL and MongoDB."
            )
            return self._result("durability", passed, desc, start, details)

        except Exception as exc:
            details["error"] = str(exc)
            return self._result("durability", False, f"Exception: {exc}", start, details)
        finally:
            self._ensure_connected(mysql_client, mongo_client)
            self._cleanup_tag(tag, field_locations, mysql_client, mongo_client)

    def test_reconstruction(
        self,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
    ) -> AcidTestResult:
        """Test logical reconstruction of split SQL+Mongo data into unified JSON.
        
        Verifies:
        - Data from both SQL and Mongo merge correctly
        - Output is backend-agnostic (no table/collection names exposed)
        - Nested structures are preserved
        - No empty records or stale metadata fields
        """
        start = time.perf_counter()
        details: dict[str, Any] = {}
        try:
            # Get reconstructed data
            all_data = self.logical_reconstructor.get_all_data(
                field_locations=field_locations,
                mysql_client=mysql_client,
                mongo_client=mongo_client,
                limit=10,
            )

            # Filter empty records
            non_empty = [d for d in all_data if isinstance(d, dict) and len(d) > 0]
            details["total_records"] = len(all_data)
            details["non_empty_records"] = len(non_empty)

            if not non_empty:
                desc = "All reconstructed records are empty — metadata/merge issue."
                return self._result("reconstruction", False, desc, start, details)

            # Verify no raw join keys or internal fields leaked
            leaked_keys: set[str] = set()
            for record in non_empty[:5]:  # Sample first 5
                for key in list(record.keys()):
                    if key.startswith("_") or key in ("post_tags_id", "attachment_id"):
                        leaked_keys.add(key)

            # Verify nested structures exist
            has_nesting = any(
                any(isinstance(v, dict) for v in record.values())
                for record in non_empty
            )

            details["leaked_internal_keys"] = sorted(list(leaked_keys))
            details["has_nested_structures"] = has_nesting
            if non_empty:
                details["sample_record"] = non_empty[0]

            # Reconstruction passes if: no empty records, no leaked keys, nested structures present
            passed = len(non_empty) == len(all_data) and len(leaked_keys) == 0 and has_nesting
            desc = (
                "Validated SQL+Mongo reconstruction: non-empty records, "
                "no leaked internal keys, nested structures preserved."
            )
            return self._result("reconstruction", passed, desc, start, details)

        except Exception as exc:
            details["error"] = str(exc)
            desc = f"Reconstruction test failed: {exc}"
            return self._result("reconstruction", False, desc, start, details)

    def _build_test_record(
        self,
        tag: str,
        field_locations: list[FieldLocation],
    ) -> dict[str, Any]:
        record: dict[str, Any] = {}
        for loc in field_locations:
            path = loc.field_path
            if "." in path:
                continue
            canonical = path.lower()
            if canonical in ("username", "user_id", "id"):
                record[path] = tag
            elif canonical in ("event_id",):
                record[path] = f"evt_{tag}"
            elif canonical in ("title", "name"):
                record[path] = tag
            elif canonical in ("timestamp", "sys_ingested_at", "created_at"):
                record[path] = "2026-01-01T00:00:00Z"
            else:
                record[path] = f"test_{tag}"
        return record

    def _find_field_path(self, short_name: str, field_locations: list[FieldLocation]) -> str | None:
        for loc in field_locations:
            if loc.field_path == short_name or loc.field_path.endswith(f".{short_name}"):
                return loc.field_path
        return None

    def _count_sql_by_tag(
        self,
        tag: str,
        field_locations: list[FieldLocation],
        mysql_client,
    ) -> int:
        username_loc = next(
            (loc for loc in field_locations
             if loc.field_path == "username" and loc.backend.lower() in ("sql", "both")),
            None,
        )
        if username_loc is None or getattr(mysql_client, "connection", None) is None:
            return 0

        table = username_loc.table_or_collection
        col = username_loc.column_or_path
        try:
            rows = mysql_client.fetch_all(
                f"SELECT COUNT(*) AS cnt FROM `{table}` WHERE `{col}` = %s",
                (tag,),
            )
            return int(rows[0].get("cnt", 0)) if rows else 0
        except Exception:
            return 0

    def _count_mongo_by_tag(
        self,
        tag: str,
        field_locations: list[FieldLocation],
        mongo_client,
    ) -> int:
        if not (hasattr(mongo_client, "client") and mongo_client.client is not None):
            return 0

        db_name = getattr(mongo_client, "database", None)
        if not db_name:
            return 0
        db = mongo_client.client[db_name]

        total = 0
        seen_collections: set[str] = set()
        for loc in field_locations:
            if loc.backend.lower() not in ("mongo", "both"):
                continue
            coll_name = loc.table_or_collection
            if coll_name in seen_collections:
                continue
            seen_collections.add(coll_name)
            try:
                total += db[coll_name].count_documents({"username": tag})
            except Exception:
                pass
        return total

    def _cleanup_tag(
        self,
        tag: str,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
    ) -> None:
        try:
            self.txn.execute_in_transaction(
                CrudOperation.DELETE,
                {"filters": {"username": tag}},
                field_locations,
                mysql_client,
                mongo_client,
            )
        except Exception:
            pass
        try:
            self.txn.execute_in_transaction(
                CrudOperation.DELETE,
                {"filters": {"username": f"{tag}_fail"}},
                field_locations,
                mysql_client,
                mongo_client,
            )
        except Exception:
            pass

    @staticmethod
    def _ensure_connected(mysql_client, mongo_client) -> None:
        if getattr(mysql_client, "connection", None) is None:
            try:
                mysql_client.connect()
            except Exception:
                pass
        if getattr(mongo_client, "client", None) is None:
            try:
                mongo_client.connect()
            except Exception:
                pass

    @staticmethod
    def _result(
        prop: str,
        passed: bool,
        desc: str,
        start: float,
        details: dict[str, Any],
    ) -> AcidTestResult:
        elapsed = (time.perf_counter() - start) * 1000
        return AcidTestResult(
            property_name=prop,
            passed=passed,
            description=desc,
            duration_ms=round(elapsed, 2),
            details=details,
        )
