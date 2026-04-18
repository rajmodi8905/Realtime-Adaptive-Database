"""Metrics collection for query monitoring and performance benchmarking.

Thread-safe, bounded, in-memory metrics store that tracks:
- Query latency histograms
- Throughput counters
- Error rates
- Lock wait times
- Operation breakdowns
"""

from __future__ import annotations

import threading
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Any, Optional


@dataclass
class MetricPoint:
    timestamp: float
    operation: str
    duration_ms: float
    status: str  # "success" | "error"
    lock_wait_ms: float = 0.0
    entity: str = ""


class MetricsCollector:
    """Thread-safe metrics collector with bounded rolling window."""

    def __init__(self, max_points: int = 2000, window_seconds: float = 3600):
        self._lock = threading.Lock()
        self._points: deque[MetricPoint] = deque(maxlen=max_points)
        self._window = window_seconds
        self._total_queries = 0
        self._total_errors = 0
        self._start_time = time.time()

    def record(
        self,
        operation: str,
        duration_ms: float,
        status: str = "success",
        lock_wait_ms: float = 0.0,
        entity: str = "",
    ) -> None:
        point = MetricPoint(
            timestamp=time.time(),
            operation=operation,
            duration_ms=duration_ms,
            status=status,
            lock_wait_ms=lock_wait_ms,
            entity=entity,
        )
        with self._lock:
            self._points.append(point)
            self._total_queries += 1
            if status == "error":
                self._total_errors += 1

    def get_snapshot(self) -> dict[str, Any]:
        """Return current metrics snapshot for API consumption."""
        now = time.time()
        with self._lock:
            # Filter to window
            window_points = [p for p in self._points if now - p.timestamp <= self._window]

            if not window_points:
                return self._empty_snapshot(now)

            durations = [p.duration_ms for p in window_points]
            lock_waits = [p.lock_wait_ms for p in window_points]
            errors = sum(1 for p in window_points if p.status == "error")
            successes = len(window_points) - errors

            # Operation breakdown
            ops = {}
            for p in window_points:
                if p.operation not in ops:
                    ops[p.operation] = {"count": 0, "avg_ms": 0, "errors": 0, "total_ms": 0}
                ops[p.operation]["count"] += 1
                ops[p.operation]["total_ms"] += p.duration_ms
                if p.status == "error":
                    ops[p.operation]["errors"] += 1
            for k in ops:
                if ops[k]["count"] > 0:
                    ops[k]["avg_ms"] = round(ops[k]["total_ms"] / ops[k]["count"], 2)
                del ops[k]["total_ms"]

            # Time series (buckets of 30s)
            bucket_size = 30
            ts_buckets = {}
            for p in window_points:
                bucket = int(p.timestamp // bucket_size) * bucket_size
                if bucket not in ts_buckets:
                    ts_buckets[bucket] = {"queries": 0, "errors": 0, "total_ms": 0}
                ts_buckets[bucket]["queries"] += 1
                ts_buckets[bucket]["total_ms"] += p.duration_ms
                if p.status == "error":
                    ts_buckets[bucket]["errors"] += 1

            time_series = []
            for ts in sorted(ts_buckets.keys()):
                b = ts_buckets[ts]
                time_series.append({
                    "timestamp": ts,
                    "queries": b["queries"],
                    "errors": b["errors"],
                    "avg_ms": round(b["total_ms"] / b["queries"], 2) if b["queries"] > 0 else 0,
                })

            sorted_dur = sorted(durations)
            p50 = sorted_dur[len(sorted_dur) // 2] if sorted_dur else 0
            p95 = sorted_dur[int(len(sorted_dur) * 0.95)] if len(sorted_dur) > 1 else p50
            p99 = sorted_dur[int(len(sorted_dur) * 0.99)] if len(sorted_dur) > 1 else p95

            uptime = now - self._start_time

            return {
                "window_seconds": self._window,
                "total_queries": self._total_queries,
                "total_errors": self._total_errors,
                "window": {
                    "count": len(window_points),
                    "successes": successes,
                    "errors": errors,
                    "error_rate": round(errors / len(window_points) * 100, 2) if window_points else 0,
                    "avg_latency_ms": round(sum(durations) / len(durations), 2),
                    "p50_latency_ms": round(p50, 2),
                    "p95_latency_ms": round(p95, 2),
                    "p99_latency_ms": round(p99, 2),
                    "min_latency_ms": round(min(durations), 2),
                    "max_latency_ms": round(max(durations), 2),
                    "avg_lock_wait_ms": round(sum(lock_waits) / len(lock_waits), 2) if lock_waits else 0,
                    "throughput_qps": round(len(window_points) / min(self._window, uptime), 2) if uptime > 0 else 0,
                },
                "operations": ops,
                "time_series": time_series[-60:],  # Last 60 buckets (30min at 30s buckets)
            }

    def _empty_snapshot(self, now: float) -> dict[str, Any]:
        uptime = now - self._start_time
        return {
            "window_seconds": self._window,
            "total_queries": self._total_queries,
            "total_errors": self._total_errors,
            "window": {
                "count": 0, "successes": 0, "errors": 0, "error_rate": 0,
                "avg_latency_ms": 0, "p50_latency_ms": 0, "p95_latency_ms": 0,
                "p99_latency_ms": 0, "min_latency_ms": 0, "max_latency_ms": 0,
                "avg_lock_wait_ms": 0,
                "throughput_qps": 0,
            },
            "operations": {},
            "time_series": [],
        }

    def reset(self) -> None:
        with self._lock:
            self._points.clear()
            self._total_queries = 0
            self._total_errors = 0
            self._start_time = time.time()


class BenchmarkRunner:
    """Run comparative benchmarks: logical-path vs direct-path."""

    def __init__(self, pipeline):
        self._pipeline = pipeline
        self._results: list[dict[str, Any]] = []
        self._lock = threading.Lock()

    # ── helpers: introspect pipeline schema ───────────────────────────────────

    def _get_field_locations(self):
        """Return the current list of FieldLocation objects from the pipeline."""
        return self._pipeline._get_field_locations()

    def _get_existing_username(self) -> str:
        """Fetch a real username that exists in the data for use in filters."""
        try:
            fl = self._get_field_locations()
            sql_tables = set(
                loc.table_or_collection for loc in fl if loc.backend.lower() in ("sql", "both")
            )
            main_table = "event" if "event" in sql_tables else (list(sql_tables)[0] if sql_tables else None)
            if main_table:
                rows = self._pipeline._mysql_client.fetch_all(
                    f"SELECT `username` FROM `{main_table}` LIMIT 1"
                )
                if rows and isinstance(rows[0], dict):
                    val = rows[0].get("username")
                    if val:
                        return str(val)
        except Exception:
            pass
        return "bench_user_1"

    def _get_schema_field_map(self) -> dict[str, Any]:
        """Build a map of field_path → FieldLocation for the current schema."""
        return {loc.field_path: loc for loc in self._get_field_locations()}

    def _get_updatable_sql_field(self) -> tuple[str, str] | None:
        """Find a non-key, non-id SQL field suitable for update benchmarks.
        Returns (field_path, column_or_path) or None."""
        fl = self._get_field_locations()
        # Prefer a simple string field that isn't an ID or key
        skip_cols = {"username", "event_id", "sys_ingested_at", "timestamp"}
        for loc in fl:
            if loc.backend.lower() not in ("sql", "both"):
                continue
            if loc.field_path in skip_cols:
                continue
            if "id" in loc.column_or_path.lower():
                continue
            # Found a good candidate (e.g. post.title, device.model, metrics.signal_quality)
            return (loc.field_path, loc.column_or_path)
        return None

    # ── Schema-aware record generation ───────────────────────────────────────

    def _make_create_query(self) -> dict[str, Any]:
        """Generate a CREATE query with a full schema-aware record.

        Reads field locations from the pipeline metadata and generates
        proper values for every field, including nested objects, so that
        SQL inserts don't fail due to missing NOT NULL columns.
        """
        import random as _rng

        uid = f"bench_{_rng.randint(10000, 99999)}"
        ts = time.strftime("%Y-%m-%dT%H:%M:%S")

        fl = self._get_field_locations()
        field_map = {loc.field_path: loc for loc in fl}

        # Build a complete record covering ALL schema fields
        record: dict[str, Any] = {}

        for field_path, loc in sorted(field_map.items()):
            # Generate appropriate value based on field path heuristics
            value = self._generate_field_value(field_path, uid, ts)
            if value is not None:
                self._set_nested(record, field_path, value)

        # Ensure required top-level fields are always present
        record.setdefault("username", uid)
        record.setdefault("event_id", f"evt_{uid}")
        record.setdefault("timestamp", ts)

        return {"operation": "create", "records": [record]}

    def _generate_field_value(self, field_path: str, uid: str, ts: str) -> Any:
        """Generate a synthetic value for a given field path."""
        import random as _rng

        fp = field_path.lower()

        # Known field patterns
        if fp == "username":
            return uid
        if fp == "event_id":
            return f"evt_{uid}"
        if fp == "timestamp":
            return ts
        if fp == "sys_ingested_at":
            return ts

        # Device fields
        if fp == "device.device_id":
            return f"dev_{uid}"
        if fp == "device.model":
            return _rng.choice(["iPhone 15", "Galaxy S24", "Pixel 8", "OnePlus 12"])
        if fp == "device.firmware":
            return f"v{_rng.randint(1, 9)}.{_rng.randint(0, 9)}.{_rng.randint(0, 9)}"

        # Post fields
        if fp == "post.post_id":
            return f"post_{uid}"
        if fp == "post.title":
            return f"Benchmark post {uid}"
        if fp == "post.content":
            return f"Benchmark test content for {uid}"

        # Post sub-arrays (tags, attachments, comments)
        if fp.startswith("post.tags"):
            if fp == "post.tags" or fp == "post.tags.tags_value":
                return None  # Will be handled as array below
            return None
        if fp.startswith("post.attachments"):
            return None  # Will be handled as struct
        if fp.startswith("post.comments"):
            return None  # Complex nested — skip for benchmark records

        # Metrics fields
        if fp == "metrics.latency_ms":
            return round(_rng.uniform(10, 500), 2)
        if fp == "metrics.battery_pct":
            return _rng.randint(10, 100)
        if fp == "metrics.signal_quality":
            return _rng.choice(["excellent", "good", "fair", "poor"])

        # Device sensors (mongo embedded)
        if fp == "device.sensors":
            return None  # Complex nested mongo field

        # Generic fallback for unknown string fields
        if "id" in fp:
            return f"{fp.replace('.', '_')}_{uid}"
        if "time" in fp or "date" in fp:
            return ts

        return f"bench_val_{_rng.randint(1, 999)}"

    @staticmethod
    def _set_nested(obj: dict, dotted_path: str, value: Any) -> None:
        """Set a value in a nested dict using dot notation."""
        parts = dotted_path.split(".")
        current = obj
        for part in parts[:-1]:
            current = current.setdefault(part, {})
            if not isinstance(current, dict):
                return  # Can't nest further
        current[parts[-1]] = value

    # ── Main benchmark entrypoint ────────────────────────────────────────────

    def run_benchmark(self, config: dict[str, Any]) -> dict[str, Any]:
        """Run a benchmark trial with the given configuration."""
        b_type = config.get("type", "standard")
        scenario = config.get("scenario")
        warmup = config.get("warmup", 2)
        iterations = config.get("iterations", 10)
        label = config.get("label", f"Benchmark @ {time.strftime('%H:%M:%S')}")
        mode = config.get("mode", "read")

        if b_type == "comparative" and scenario:
            return self._run_comparative_benchmark(scenario, warmup, iterations, label, config)

        if mode == "custom_query":
            return self._run_custom_query_benchmark(config, warmup, iterations, label)

        # For create mode, we must generate a fresh query each iteration to avoid
        # duplicate primary key errors.  For other modes, build once and reuse.
        is_create = mode == "create"

        # Build queries list based on mode (used directly for non-create modes)
        queries = self._build_mode_queries(mode, iterations)
        if not queries:
            return {"label": label, "error": f"Failed to build queries for mode: {mode}"}

        # Warmup
        for _ in range(warmup):
            warmup_q = self._build_mode_queries(mode, 1) if is_create else queries[:min(len(queries), 3)]
            for q in warmup_q:
                try:
                    self._pipeline.execute_query(q)
                except Exception:
                    pass

        # Pre-compute execution breakdown keys
        breakdowns = {"metadata_lookup_ms": 0, "query_plan_ms": 0, "sql_ms": 0, "mongo_ms": 0, "merge_ms": 0}
        try:
            from src.a2.contracts import CrudOperation
            q0 = queries[0]
            op = q0.get("operation", "read")
            payload = {k: v for k, v in q0.items() if k != "operation"}
            plan = self._pipeline.a2.preview_plan(CrudOperation(op), payload)
            planned_queries = {"sql": plan.sql_queries, "mongo": plan.mongo_queries}
        except Exception:
            planned_queries = {"sql": [], "mongo": []}

        # Timed runs
        latencies = []
        errors = 0
        op_counts = {}

        for _ in range(iterations):
            # For create, generate a fresh query with unique IDs each iteration
            iter_queries = self._build_mode_queries(mode, 1) if is_create else queries
            for q in iter_queries:
                op_name = q.get("operation", "read")
                op_counts[op_name] = op_counts.get(op_name, 0) + 1
                t0 = time.perf_counter()
                try:
                    res = self._pipeline.execute_query(q)
                    latencies.append((time.perf_counter() - t0) * 1000)
                    timings = res.get("timings", {})
                    for k in breakdowns:
                        breakdowns[k] += timings.get(k, 0)
                except Exception:
                    latencies.append((time.perf_counter() - t0) * 1000)
                    errors += 1

        if not latencies:
            return {"label": label, "error": "No queries executed"}

        for k in breakdowns:
            breakdowns[k] = round(breakdowns[k] / max(1, len(latencies)), 6)

        sorted_lat = sorted(latencies)
        result = {
            "label": label,
            "timestamp": time.time(),
            "timestamp_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "config": {
                "mode": mode,
                "queries": len(queries),
                "warmup": warmup,
                "iterations": iterations,
                "user_query": queries[0] if queries else {},
            },
            "results": {
                "total_runs": len(latencies),
                "errors": errors,
                "avg_ms": round(sum(latencies) / len(latencies), 2),
                "min_ms": round(min(latencies), 2),
                "max_ms": round(max(latencies), 2),
                "p50_ms": round(sorted_lat[len(sorted_lat) // 2], 2),
                "p95_ms": round(sorted_lat[int(len(sorted_lat) * 0.95)], 2) if len(sorted_lat) > 1 else round(sorted_lat[0], 2),
                "p99_ms": round(sorted_lat[int(len(sorted_lat) * 0.99)], 2) if len(sorted_lat) > 1 else round(sorted_lat[0], 2),
                "throughput_qps": round(len(latencies) / (sum(latencies) / 1000), 2) if sum(latencies) > 0 else 0,
                "avg_breakdown_ms": breakdowns,
                "planned_queries": planned_queries,
                "op_counts": op_counts,
            },
        }

        with self._lock:
            self._results.append(result)
            if len(self._results) > 50:
                self._results = self._results[-50:]

        return result

    # ── Schema-aware query builders ──────────────────────────────────────────

    def _build_mode_queries(self, mode: str, count: int = 1) -> list[dict[str, Any]]:
        """Build a list of queries for a single-mode benchmark using schema metadata."""
        if mode == "read":
            return self._build_read_queries()
        elif mode == "create":
            return [self._make_create_query()]
        elif mode == "update":
            return self._build_update_queries()
        elif mode == "delete":
            return self._build_delete_queries()
        return self._build_read_queries()

    def _build_read_queries(self) -> list[dict[str, Any]]:
        """Build schema-aware read queries using real field names."""
        username = self._get_existing_username()
        return [{"operation": "read", "filters": {"username": username}}]

    def _build_update_queries(self) -> list[dict[str, Any]]:
        """Build schema-aware update queries targeting real fields."""
        import random as _rng

        username = self._get_existing_username()
        updatable = self._get_updatable_sql_field()

        if updatable:
            field_path, col = updatable
            # Generate a sensible update value
            if "title" in field_path.lower():
                new_val = f"Bench Updated {_rng.randint(1, 9999)}"
            elif "model" in field_path.lower():
                new_val = _rng.choice(["BenchPhone X", "BenchTab Pro", "BenchWatch 3"])
            elif "firmware" in field_path.lower():
                new_val = f"bench_fw_{_rng.randint(1, 99)}"
            elif "quality" in field_path.lower():
                new_val = _rng.choice(["excellent", "good", "fair"])
            else:
                new_val = f"bench_updated_{_rng.randint(1, 9999)}"
            return [{
                "operation": "update",
                "filters": {"username": username},
                "updates": {field_path: new_val},
            }]
        else:
            # Fallback: update with a filter but no specific field
            return [{
                "operation": "update",
                "filters": {"username": username},
                "updates": {"metrics.signal_quality": "benchmark_updated"},
            }]

    def _build_delete_queries(self) -> list[dict[str, Any]]:
        """Build actual delete benchmark: create a temp record then delete it.

        This creates a temporary benchmark record, then returns a delete query
        targeting that same record so we measure real DELETE latency.
        """
        import random as _rng

        # First, create a temporary record to delete
        temp_uid = f"bench_del_{_rng.randint(100000, 999999)}"
        create_query = self._make_create_query()
        # Override the username to our temp uid so we can target it for deletion
        if create_query.get("records") and len(create_query["records"]) > 0:
            create_query["records"][0]["username"] = temp_uid
            create_query["records"][0]["event_id"] = f"evt_{temp_uid}"

        # Insert the temp record via the pipeline
        try:
            self._pipeline.execute_query(create_query)
        except Exception:
            pass

        # Return a delete query targeting the temp record
        return [{
            "operation": "delete",
            "filters": {"username": temp_uid},
        }]

    # ── Custom query benchmark ───────────────────────────────────────────────

    def _run_custom_query_benchmark(
        self, config: dict[str, Any], warmup: int, iterations: int, label: str
    ) -> dict[str, Any]:
        """Run a user-supplied query through the framework and return latency breakdown."""
        user_query = config.get("custom_query")
        if not user_query or not isinstance(user_query, dict):
            return {"label": label, "error": "No custom_query provided"}

        # Ensure operation field exists
        if "operation" not in user_query:
            user_query["operation"] = "read"

        # Make sure users follow the schema in custom queries:
        # If they specify a username filter (like "user_1"), replace it with a real one
        # so the query doesn't return instantly on an empty set.
        filters = user_query.get("filters", {})
        if "username" in filters:
            filters["username"] = self._get_existing_username()

        # Warmup
        for _ in range(warmup):
            try:
                self._pipeline.execute_query(user_query)
            except Exception:
                pass

        breakdowns = {"metadata_lookup_ms": 0, "query_plan_ms": 0, "sql_ms": 0, "mongo_ms": 0, "merge_ms": 0}

        # Get planned queries for display
        try:
            from src.a2.contracts import CrudOperation
            op_str = user_query.get("operation", "read")
            payload = {k: v for k, v in user_query.items() if k != "operation"}
            plan = self._pipeline.a2.preview_plan(CrudOperation(op_str), payload)
            planned_queries = {"sql": plan.sql_queries, "mongo": plan.mongo_queries}
        except Exception:
            planned_queries = {"sql": [], "mongo": []}

        latencies = []
        errors = 0
        op_name = user_query.get("operation", "read")

        for _ in range(iterations):
            t0 = time.perf_counter()
            try:
                res = self._pipeline.execute_query(user_query)
                latencies.append((time.perf_counter() - t0) * 1000)
                timings = res.get("timings", {})
                for k in breakdowns:
                    breakdowns[k] += timings.get(k, 0)
            except Exception:
                latencies.append((time.perf_counter() - t0) * 1000)
                errors += 1

        if not latencies:
            return {"label": label, "error": "No queries executed"}

        for k in breakdowns:
            breakdowns[k] = round(breakdowns[k] / max(1, len(latencies)), 6)

        sorted_lat = sorted(latencies)
        result = {
            "label": label,
            "timestamp": time.time(),
            "timestamp_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "config": {
                "mode": "custom_query",
                "queries": 1,
                "warmup": warmup,
                "iterations": iterations,
                "user_query": user_query,
            },
            "results": {
                "total_runs": len(latencies),
                "errors": errors,
                "avg_ms": round(sum(latencies) / len(latencies), 2),
                "min_ms": round(min(latencies), 2),
                "max_ms": round(max(latencies), 2),
                "p50_ms": round(sorted_lat[len(sorted_lat) // 2], 2),
                "p95_ms": round(sorted_lat[int(len(sorted_lat) * 0.95)], 2) if len(sorted_lat) > 1 else round(sorted_lat[0], 2),
                "p99_ms": round(sorted_lat[int(len(sorted_lat) * 0.99)], 2) if len(sorted_lat) > 1 else round(sorted_lat[0], 2),
                "throughput_qps": round(len(latencies) / (sum(latencies) / 1000), 2) if sum(latencies) > 0 else 0,
                "avg_breakdown_ms": breakdowns,
                "planned_queries": planned_queries,
                "op_counts": {op_name: len(latencies)},
            },
        }

        with self._lock:
            self._results.append(result)
            if len(self._results) > 50:
                self._results = self._results[-50:]

        return result

    # ── Comparative benchmark ────────────────────────────────────────────────

    def _run_comparative_benchmark(
        self, scenario: str, warmup: int, iterations: int, label: str,
        config: dict[str, Any] | None = None,
    ) -> dict:
        field_locs = self._pipeline._get_field_locations()
        sql_tables = set(loc.table_or_collection for loc in field_locs if loc.backend.lower() in ("sql", "both"))
        mongo_colls = set(loc.table_or_collection for loc in field_locs if loc.backend.lower() in ("mongo", "mongodb"))

        logical_latencies = []
        direct_latencies = []
        logical_errors = 0
        direct_errors = 0

        # Default logical query and direct function
        logical_query = {"operation": "read", "filters": {}}

        def direct_fn():
            pass

        if scenario == "retrieve_users_sql":
            table = list(sql_tables)[0] if sql_tables else "event"
            existing_username = self._get_existing_username()
            logical_query = {"operation": "read", "filters": {"username": existing_username}}

            def direct_fn():
                sql = f"SELECT * FROM `{table}` WHERE `username` = %s LIMIT 100"
                try:
                    res = self._pipeline._mysql_client.fetch_all(sql, (existing_username,))
                    if not res:
                        pass
                except Exception:
                    pass

        elif scenario == "access_nested_mongo":
            coll = list(mongo_colls)[0] if mongo_colls else "events"
            existing_username = self._get_existing_username()
            logical_query = {"operation": "read", "filters": {"username": existing_username}}

            def direct_fn():
                db_name = self._pipeline._mongo_client.database
                client = self._pipeline._mongo_client.client
                # Fully evaluate cursor to measure true retrieval latency
                try:
                    res = list(client[db_name][coll].find({"username": existing_username}).limit(100))
                    if not res:
                        pass
                except Exception:
                    pass

        elif scenario == "update_multi_entity":
            # Find a real updatable SQL field and a real username for filtering
            updatable = self._get_updatable_sql_field()
            existing_username = self._get_existing_username()

            if updatable:
                update_field_path, update_col = updatable
            else:
                update_field_path = "metrics.signal_quality"
                update_col = "metrics.signal_quality"

            import random as _rng
            bench_val = f"comp_bench_{_rng.randint(1000, 9999)}"

            # Logical path: update via the framework with filter + real field
            # This goes through: metadata lookup → query planning → concurrency lock
            # → SQL update → Mongo sync → transaction commit
            logical_query = {
                "operation": "update",
                "filters": {"username": existing_username},
                "updates": {update_field_path: bench_val},
            }

            # Direct path: ONLY do the raw SQL update — the minimal equivalent.
            # This isolates the framework's overhead (metadata lookup, query planning,
            # concurrency control, transaction management, Mongo sync).
            main_table = "event" if "event" in sql_tables else (list(sql_tables)[0] if sql_tables else None)

            def direct_fn():
                # Direct SQL update — raw single-statement execution
                if main_table:
                    sql = f"UPDATE `{main_table}` SET `{update_col}` = %s WHERE `username` = %s"
                    try:
                        self._pipeline._mysql_client.execute(sql, (bench_val, existing_username))
                    except Exception:
                        pass

        elif scenario == "custom_query":
            # Custom comparative benchmark from user-supplied query
            user_custom = (config or {}).get("custom_query", {})
            if not user_custom:
                return {"label": label, "error": "No custom_query provided for comparative benchmark"}

            logical_query = dict(user_custom)
            if "operation" not in logical_query:
                logical_query["operation"] = "read"

            # For custom queries, direct path reads directly from SQL + Mongo
            filters = logical_query.get("filters", {})
            main_table = "event" if "event" in sql_tables else (list(sql_tables)[0] if sql_tables else None)

            def direct_fn():
                if main_table:
                    where_parts = []
                    params = []
                    for k, v in filters.items():
                        where_parts.append(f"`{k}` = %s")
                        params.append(v)
                    where_clause = " AND ".join(where_parts) if where_parts else "1=1"
                    sql = f"SELECT * FROM `{main_table}` WHERE {where_clause} LIMIT 100"
                    try:
                        self._pipeline._mysql_client.fetch_all(sql, tuple(params) if params else None)
                    except Exception:
                        pass
                if mongo_colls:
                    coll_name = list(mongo_colls)[0]
                    db_name = self._pipeline._mongo_client.database
                    client = self._pipeline._mongo_client.client
                    try:
                        list(client[db_name][coll_name].find(filters).limit(100))
                    except Exception:
                        pass

        else:
            return {"label": label, "error": f"Unknown comparative scenario: {scenario}"}

        breakdowns = {"metadata_lookup_ms": 0, "query_plan_ms": 0, "sql_ms": 0, "mongo_ms": 0, "merge_ms": 0}
        try:
            from src.a2.contracts import CrudOperation
            op = logical_query.get("operation", "read")
            payload = {k: v for k, v in logical_query.items() if k != "operation"}
            plan = self._pipeline.a2.preview_plan(CrudOperation(op), payload)
            planned_queries = {"sql": plan.sql_queries, "mongo": plan.mongo_queries}
        except Exception:
            planned_queries = {"sql": [], "mongo": []}

        def logical_fn():
            res = self._pipeline.execute_query(logical_query)
            if res.get("status") == "error":
                raise RuntimeError(str(res.get("errors")))
            return res

        # Warmup executes without recording
        for _ in range(warmup):
            try:
                logical_fn()
            except Exception:
                pass
            try:
                direct_fn()
            except Exception:
                pass

        # Interleave metrics to avoid network bursts favoring one over the other
        for _ in range(iterations):
            # Logical run
            t0 = time.perf_counter()
            try:
                res = logical_fn()
                logical_latencies.append((time.perf_counter() - t0) * 1000)
                timings = res.get("timings", {})
                for k in breakdowns:
                    breakdowns[k] += timings.get(k, 0)
            except Exception:
                logical_errors += 1

            # Direct run
            t0 = time.perf_counter()
            try:
                direct_fn()
                direct_latencies.append((time.perf_counter() - t0) * 1000)
            except Exception:
                direct_errors += 1

        if not logical_latencies:
            logical_latencies = [0]
        if not direct_latencies:
            direct_latencies = [0]

        avg_logical = sum(logical_latencies) / len(logical_latencies)
        avg_direct = sum(direct_latencies) / len(direct_latencies)

        for k in breakdowns:
            breakdowns[k] = round(breakdowns[k] / max(1, len(logical_latencies)), 6)

        result = {
            "label": label,
            "timestamp": time.time(),
            "timestamp_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "config": {
                "type": "comparative",
                "scenario": scenario,
                "iterations": iterations,
                "warmup": warmup,
                "logical_query": logical_query,
            },
            "results": {
                "overhead_ms": round(avg_logical - avg_direct, 2),
                "overhead_pct": round(((avg_logical - avg_direct) / avg_direct) * 100, 2) if avg_direct > 0 else 0,
                "avg_breakdown_ms": breakdowns,
                "planned_queries": planned_queries,
                "logical": {
                    "avg_ms": round(avg_logical, 2),
                    "errors": logical_errors,
                    "latencies": [round(l, 2) for l in logical_latencies]
                },
                "direct": {
                    "avg_ms": round(avg_direct, 2),
                    "errors": direct_errors,
                    "latencies": [round(l, 2) for l in direct_latencies]
                },
            }
        }

        with self._lock:
            self._results.append(result)
            if len(self._results) > 50:
                self._results = self._results[-50:]

        return result

    def get_results(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._results)
