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
        queries = config.get("queries", [
            {"operation": "read", "filters": {}},
        ])
        warmup = config.get("warmup", 2)
        iterations = config.get("iterations", 10)
        label = config.get("label", f"Benchmark @ {time.strftime('%H:%M:%S')}")

        # Warmup
        for _ in range(warmup):
            for q in queries:
                try:
                    self._pipeline.execute_query(q)
                except Exception:
                    pass

        # Timed runs
        latencies = []
        errors = 0
        for _ in range(iterations):
            for q in queries:
                t0 = time.monotonic()
                try:
                    self._pipeline.execute_query(q)
                    latencies.append((time.monotonic() - t0) * 1000)
                except Exception:
                    latencies.append((time.monotonic() - t0) * 1000)
                    errors += 1

        if not latencies:
            return {"label": label, "error": "No queries executed"}

        sorted_lat = sorted(latencies)
        result = {
            "label": label,
            "timestamp": time.time(),
            "timestamp_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
            "config": {
                "queries": len(queries),
                "warmup": warmup,
                "iterations": iterations,
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
            },
        }

        with self._lock:
            self._results.append(result)
            # Keep last 50 benchmark runs
            if len(self._results) > 50:
                self._results = self._results[-50:]

        return result

    def get_results(self) -> list[dict[str, Any]]:
        with self._lock:
            return list(self._results)
