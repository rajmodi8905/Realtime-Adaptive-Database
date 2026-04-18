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

    def run_benchmark(self, config: dict[str, Any]) -> dict[str, Any]:
        """Run a benchmark trial with the given configuration."""
        b_type = config.get("type", "standard")
        scenario = config.get("scenario")
        warmup = config.get("warmup", 2)
        iterations = config.get("iterations", 10)
        label = config.get("label", f"Benchmark @ {time.strftime('%H:%M:%S')}")
        mode = config.get("mode", "read")

        if b_type == "comparative" and scenario:
            return self._run_comparative_benchmark(scenario, warmup, iterations, label)

        # Build queries list based on mode
        if mode == "custom":
            mix = config.get("mix", {"read": 10})
            queries = self._build_custom_mix_queries(mix)
            if not queries:
                return {"label": label, "error": "Empty custom mix"}
        else:
            queries = self._build_mode_queries(mode, iterations)
            if not queries:
                return {"label": label, "error": f"Failed to build queries for mode: {mode}"}

        # Warmup
        warmup_queries = queries[:min(len(queries), 3)]
        for _ in range(warmup):
            for q in warmup_queries:
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

        # Timed runs — for non-custom modes, repeat queries `iterations` times
        latencies = []
        errors = 0
        op_counts = {}

        if mode == "custom":
            # Custom: run each query once (already sized by mix)
            import random as _rng
            shuffled = list(queries)
            _rng.shuffle(shuffled)
            for q in shuffled:
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
        else:
            for _ in range(iterations):
                for q in queries:
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
            breakdowns[k] = round(breakdowns[k] / max(1, len(latencies)), 2)

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
                "mix": config.get("mix") if mode == "custom" else None,
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

    def _build_mode_queries(self, mode: str, count: int = 1) -> list[dict[str, Any]]:
        """Build a list of queries for a single-mode benchmark."""
        if mode == "read":
            return [{"operation": "read", "filters": {}}]
        elif mode == "create":
            return [self._make_create_query()]
        elif mode == "update":
            return [{"operation": "update", "filters": {}, "updates": {"_bench_flag": "updated"}}]
        elif mode == "delete":
            # Create a temp record first, then delete it
            return [{"operation": "read", "filters": {}}]  # fallback to read for safety
        return [{"operation": "read", "filters": {}}]

    def _build_custom_mix_queries(self, mix: dict[str, int]) -> list[dict[str, Any]]:
        """Build an interleaved list of queries from a mix spec like {read: 10, create: 5}."""
        queries: list[dict[str, Any]] = []
        for op, count in mix.items():
            if not isinstance(count, int) or count <= 0:
                continue
            for _ in range(count):
                if op == "read":
                    queries.append({"operation": "read", "filters": {}})
                elif op == "create":
                    queries.append(self._make_create_query())
                elif op == "update":
                    queries.append({"operation": "update", "filters": {}, "updates": {"_bench_flag": "updated"}})
                elif op == "delete":
                    # For delete benchmarks, run a read instead to avoid data loss
                    queries.append({"operation": "read", "filters": {}})
        return queries

    def _make_create_query(self) -> dict[str, Any]:
        """Generate a sample CREATE query using a minimal synthetic record."""
        import random as _rng
        uid = f"bench_{_rng.randint(10000, 99999)}"
        record = {
            "username": uid,
            "event_id": f"evt_{uid}",
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "event_type": "benchmark_test",
            "post": {"content": "Benchmark test record", "tags": ["bench"]},
            "device": {"device_id": f"dev_{uid}", "type": "test"},
        }
        return {"operation": "create", "records": [record]}

    def _run_comparative_benchmark(self, scenario: str, warmup: int, iterations: int, label: str) -> dict:
        field_locs = self._pipeline._get_field_locations()
        sql_tables = set(loc.table_or_collection for loc in field_locs if loc.backend.lower() == "sql")
        mongo_colls = set(loc.table_or_collection for loc in field_locs if loc.backend.lower() in ("mongo", "mongodb"))

        logical_latencies = []
        direct_latencies = []
        logical_errors = 0
        direct_errors = 0

        # Define the functions for logical vs direct
        logical_query = {"operation": "read", "filters": {}}
        
        def direct_fn():
            pass

        if scenario == "retrieve_users_sql":
            table = list(sql_tables)[0] if sql_tables else "event"
            def direct_fn():
                # Direct SQL
                res = self._pipeline._mysql_client.fetch_all(f"SELECT * FROM `{table}` LIMIT 100")
                if not res: pass

        elif scenario == "access_nested_mongo":
            coll = list(mongo_colls)[0] if mongo_colls else "events"
            def direct_fn():
                db_name = self._pipeline._mongo_client.database
                client = self._pipeline._mongo_client.client
                # Fully evaluate cursor to measure true retrieval latency
                res = list(client[db_name][coll].find().limit(100))
                if not res: pass

        elif scenario == "update_multi_entity":
            sql_fields = [f.field_path for f in field_locs if f.backend.lower() == "sql" and "id" not in f.column_or_path.lower()]
            logical_query = {
                "operation": "update",
                "filters": {},
                "updates": {}
            }
            if sql_fields:
                logical_query["updates"][sql_fields[0]] = "bench_update_val"
            
            def direct_fn():
                if sql_tables:
                    table = list(sql_tables)[0]
                    sql = f"UPDATE `{table}` SET `{sql_fields[0] if sql_fields else '_sync'}` = '1' LIMIT 50"
                    try:
                        self._pipeline._mysql_client.execute(sql)
                    except:
                        pass # Ignore if column doesn't exist
                if mongo_colls:
                    coll = list(mongo_colls)[0]
                    db_name = self._pipeline._mongo_client.database
                    client = self._pipeline._mongo_client.client
                    client[db_name][coll].update_many({}, {"$set": {"_bench_sync": 1}})
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
            try: logical_fn()
            except Exception: pass
            try: direct_fn()
            except Exception: pass

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
            breakdowns[k] = round(breakdowns[k] / max(1, len(logical_latencies)), 2)

        result = {
            "label": label,
            "timestamp": time.time(),
            "timestamp_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "config": {
                "type": "comparative",
                "scenario": scenario,
                "iterations": iterations,
                "warmup": warmup
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
                "overhead_ms": round(avg_logical - avg_direct, 2),
                "overhead_pct": round((avg_logical / avg_direct - 1) * 100, 2) if avg_direct > 0 else 0
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
        with self._lock:
            return list(self._results)
