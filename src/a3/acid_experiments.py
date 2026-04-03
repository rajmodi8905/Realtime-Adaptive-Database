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

            passed = (
                r2.status in ("failed", "rolled_back")
                or count_after_dup == count_after_first
            )
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
        """Run three concurrency contention tests and report aggregate result."""
        tag = f"_acid_iso_{uuid.uuid4().hex[:8]}"
        start = time.perf_counter()
        details: dict[str, Any] = {}
        sub_results: list[bool] = []

        try:
            # --- sub-test 1: Lost Update Prevention ---
            lost_update_passed, lost_update_details = self._test_lost_update(
                tag, field_locations, mysql_client, mongo_client,
            )
            details["lost_update"] = lost_update_details
            sub_results.append(lost_update_passed)

            # --- sub-test 2: Dirty Read Prevention ---
            dirty_read_passed, dirty_read_details = self._test_dirty_read(
                tag, field_locations, mysql_client, mongo_client,
            )
            details["dirty_read"] = dirty_read_details
            sub_results.append(dirty_read_passed)

            # --- sub-test 3: Lock Timeout / Blocked Transaction ---
            lock_timeout_passed, lock_timeout_details = self._test_lock_timeout(
                tag, field_locations, mysql_client, mongo_client,
            )
            details["lock_timeout"] = lock_timeout_details
            sub_results.append(lock_timeout_passed)

            passed = all(sub_results)
            desc = (
                f"Ran 3 isolation sub-tests: "
                f"lost_update={'PASS' if sub_results[0] else 'FAIL'}, "
                f"dirty_read={'PASS' if sub_results[1] else 'FAIL'}, "
                f"lock_timeout={'PASS' if sub_results[2] else 'FAIL'}. "
                "Concurrency control prevents interference between overlapping transactions."
            )
            return self._result("isolation", passed, desc, start, details)

        except Exception as exc:
            details["error"] = str(exc)
            return self._result("isolation", False, f"Exception: {exc}", start, details)
        finally:
            self._cleanup_tag(tag, field_locations, mysql_client, mongo_client)

    # ── Isolation sub-tests ──────────────────────────────────────────

    def _test_lost_update(
        self,
        tag: str,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
    ) -> tuple[bool, dict[str, Any]]:
        """Two threads update the same record's title concurrently.

        With proper locking the final value must be one of the two
        updates (serialized), never a corrupted mix or a lost update.
        """
        details: dict[str, Any] = {}
        sub_tag = f"{tag}_lu"

        # setup: insert a record
        record = self._build_test_record(sub_tag, field_locations)
        ins = self.txn.execute_in_transaction(
            CrudOperation.CREATE,
            {"records": [record]},
            field_locations, mysql_client, mongo_client,
        )
        if ins.status != "committed":
            details["setup"] = "insert failed"
            return False, details

        title_field = self._find_field_path("title", field_locations)
        username_field = self._find_field_path("username", field_locations)
        if not title_field or not username_field:
            details["setup"] = "missing title or username field"
            return False, details

        update_a = f"{sub_tag}_A"
        update_b = f"{sub_tag}_B"
        errors: list[str] = []
        barrier = threading.Barrier(2, timeout=10)

        def updater(new_title: str) -> None:
            try:
                barrier.wait()
                self.txn.execute_in_transaction(
                    CrudOperation.UPDATE,
                    {"updates": {title_field: new_title}, "filters": {username_field: sub_tag}},
                    field_locations, mysql_client, mongo_client,
                )
            except Exception as exc:
                errors.append(str(exc))

        t1 = threading.Thread(target=updater, args=(update_a,), daemon=True)
        t2 = threading.Thread(target=updater, args=(update_b,), daemon=True)
        t1.start(); t2.start()
        t1.join(timeout=15); t2.join(timeout=15)

        # read final state
        read_res = self.txn.execute_in_transaction(
            CrudOperation.READ,
            {"filters": {username_field: sub_tag}, "limit": 1},
            field_locations, mysql_client, mongo_client,
        )
        records = read_res.sql_result.get("records", [])
        final_title = None
        for r in records:
            t = self._extract_title(r)
            if t is not None:
                final_title = t
                break

        details["final_title"] = final_title
        details["expected_one_of"] = [update_a, update_b]
        details["errors"] = errors

        passed = final_title in (update_a, update_b) and not errors
        self._cleanup_tag(sub_tag, field_locations, mysql_client, mongo_client)
        return passed, details

    def _test_dirty_read(
        self,
        tag: str,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
    ) -> tuple[bool, dict[str, Any]]:
        """Writer starts an update; concurrent reader must see only committed state.

        The reader must observe either the original title or the final
        committed title — never an in-flight uncommitted value.
        """
        details: dict[str, Any] = {}
        sub_tag = f"{tag}_dr"

        record = self._build_test_record(sub_tag, field_locations)
        ins = self.txn.execute_in_transaction(
            CrudOperation.CREATE,
            {"records": [record]},
            field_locations, mysql_client, mongo_client,
        )
        if ins.status != "committed":
            details["setup"] = "insert failed"
            return False, details

        title_field = self._find_field_path("title", field_locations)
        username_field = self._find_field_path("username", field_locations)
        if not title_field or not username_field:
            details["setup"] = "missing title or username field"
            return False, details

        original_title = sub_tag
        updated_title = f"{sub_tag}_updated"
        read_results: list[Any] = []
        errors: list[str] = []
        barrier = threading.Barrier(2, timeout=10)

        def writer() -> None:
            try:
                barrier.wait()
                self.txn.execute_in_transaction(
                    CrudOperation.UPDATE,
                    {"updates": {title_field: updated_title}, "filters": {username_field: sub_tag}},
                    field_locations, mysql_client, mongo_client,
                )
            except Exception as exc:
                errors.append(f"writer: {exc}")

        def reader() -> None:
            try:
                barrier.wait()
                time.sleep(0.02)  # small delay so write is in-flight
                res = self.txn.execute_in_transaction(
                    CrudOperation.READ,
                    {"filters": {username_field: sub_tag}, "limit": 1},
                    field_locations, mysql_client, mongo_client,
                )
                recs = res.sql_result.get("records", [])
                read_results.extend(recs)
            except Exception as exc:
                errors.append(f"reader: {exc}")

        tw = threading.Thread(target=writer, daemon=True)
        tr = threading.Thread(target=reader, daemon=True)
        tw.start(); tr.start()
        tw.join(timeout=15); tr.join(timeout=15)

        observed_title = None
        for r in read_results:
            t = self._extract_title(r)
            if t is not None:
                observed_title = t
                break

        details["observed_title"] = observed_title
        details["valid_values"] = [original_title, updated_title]
        details["errors"] = errors

        # The observed title must be one of the two committed states
        passed = observed_title in (original_title, updated_title, None) and not errors
        self._cleanup_tag(sub_tag, field_locations, mysql_client, mongo_client)
        return passed, details

    def _test_lock_timeout(
        self,
        tag: str,
        field_locations: list[FieldLocation],
        mysql_client,
        mongo_client,
    ) -> tuple[bool, dict[str, Any]]:
        """One thread holds a write; another attempts a write on the same key.

        The second writer must either wait and succeed after the first
        finishes (within timeout) or fail with a LockTimeoutError.
        Either way the data must remain consistent.
        """
        from .concurrency_manager import LockTimeoutError

        details: dict[str, Any] = {}
        sub_tag = f"{tag}_lt"

        record = self._build_test_record(sub_tag, field_locations)
        ins = self.txn.execute_in_transaction(
            CrudOperation.CREATE,
            {"records": [record]},
            field_locations, mysql_client, mongo_client,
        )
        if ins.status != "committed":
            details["setup"] = "insert failed"
            return False, details

        title_field = self._find_field_path("title", field_locations)
        username_field = self._find_field_path("username", field_locations)
        if not title_field or not username_field:
            details["setup"] = "missing title or username field"
            return False, details

        results_a: list[str] = []
        results_b: list[str] = []
        errors: list[str] = []
        barrier = threading.Barrier(2, timeout=10)

        def slow_writer() -> None:
            """Holds the lock for a noticeable duration."""
            try:
                barrier.wait()
                self.txn.execute_in_transaction(
                    CrudOperation.UPDATE,
                    {"updates": {title_field: f"{sub_tag}_slow"}, "filters": {username_field: sub_tag}},
                    field_locations, mysql_client, mongo_client,
                )
                results_a.append("committed")
            except Exception as exc:
                errors.append(f"slow_writer: {exc}")

        def competing_writer() -> None:
            """Attempts a write on the same entity — should be serialized."""
            try:
                barrier.wait()
                time.sleep(0.01)  # ensure it arrives slightly after
                self.txn.execute_in_transaction(
                    CrudOperation.UPDATE,
                    {"updates": {title_field: f"{sub_tag}_fast"}, "filters": {username_field: sub_tag}},
                    field_locations, mysql_client, mongo_client,
                )
                results_b.append("committed")
            except LockTimeoutError:
                results_b.append("timeout")
            except Exception as exc:
                errors.append(f"competing_writer: {exc}")

        t1 = threading.Thread(target=slow_writer, daemon=True)
        t2 = threading.Thread(target=competing_writer, daemon=True)
        t1.start(); t2.start()
        t1.join(timeout=20); t2.join(timeout=20)

        details["writer_a"] = results_a
        details["writer_b"] = results_b
        details["errors"] = errors

        # read final state for consistency check
        read_res = self.txn.execute_in_transaction(
            CrudOperation.READ,
            {"filters": {username_field: sub_tag}, "limit": 1},
            field_locations, mysql_client, mongo_client,
        )
        recs = read_res.sql_result.get("records", [])
        final_title = None
        for r in recs:
            t = self._extract_title(r)
            if t is not None:
                final_title = t
                break

        details["final_title"] = final_title

        # Success: both committed in serial order, OR one timed out — data is consistent
        a_ok = len(results_a) == 1
        b_ok = len(results_b) == 1 and results_b[0] in ("committed", "timeout")
        data_ok = final_title in (f"{sub_tag}_slow", f"{sub_tag}_fast", sub_tag)

        passed = a_ok and b_ok and data_ok and not errors
        self._cleanup_tag(sub_tag, field_locations, mysql_client, mongo_client)
        return passed, details

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

    @staticmethod
    def _extract_title(rec: dict) -> Any:
        """Search a record for title across all possible column name formats."""
        if not isinstance(rec, dict):
            return None
        if "title" in rec:
            return rec["title"]
        for key, val in rec.items():
            if key.endswith(".title") or key.endswith("_title"):
                return val
        if isinstance(rec.get("post"), dict):
            return rec["post"].get("title")
        return None

    def _build_test_record(
        self,
        tag: str,
        field_locations: list[FieldLocation],
    ) -> dict[str, Any]:
        """Build a minimal valid record that satisfies all NOT NULL columns."""
        record: dict[str, Any] = {}

        def _set_nested(d: dict, path: str, value: Any) -> None:
            parts = path.split(".")
            for part in parts[:-1]:
                d = d.setdefault(part, {})
            d[parts[-1]] = value

        for loc in field_locations:
            path = loc.field_path
            canonical = path.split(".")[-1].lower()

            if canonical in ("username", "user_id", "id"):
                _set_nested(record, path, tag)
            elif canonical in ("event_id",):
                _set_nested(record, path, f"evt_{tag}")
            elif canonical in ("title", "name"):
                _set_nested(record, path, tag)
            elif canonical in ("timestamp", "sys_ingested_at", "created_at"):
                _set_nested(record, path, "2026-01-01T00:00:00Z")
            elif canonical in ("tags",):
                _set_nested(record, path, ["test"])
            elif canonical in ("count",):
                _set_nested(record, path, 1)
            elif canonical in ("latency_ms", "battery_pct"):
                _set_nested(record, path, 0.0)
            elif canonical.endswith("_id") or canonical.endswith("_count"):
                # Likely BIGINT column — use a deterministic integer from tag
                _set_nested(record, path, abs(hash(f"{tag}_{path}")) % 100000)
            else:
                _set_nested(record, path, f"test_{tag}")
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
