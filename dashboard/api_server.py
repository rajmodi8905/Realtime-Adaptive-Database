#!/usr/bin/env python3
"""Logical Dashboard — FastAPI backend.

Thin API layer over Assignment3Pipeline. Serves the static frontend
and exposes REST + SSE endpoints for session, entities, CRUD,
ACID tests, and query execution.

Usage:
    python -m dashboard.api_server
"""

from __future__ import annotations

import asyncio
import copy
import io
import json
import logging
import random
import secrets
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import HTMLResponse, JSONResponse, StreamingResponse
from fastapi.staticfiles import StaticFiles

# ── project imports ───────────────────────────────────────────────────────────
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from src.a2.contracts import CrudOperation, SchemaRegistration
from src.a3.orchestrator import Assignment3Pipeline
from src.config import get_config
from src.persistence.metadata_store import MetadataStore

logger = logging.getLogger("dashboard")

# ── globals ───────────────────────────────────────────────────────────────────
pipeline: Assignment3Pipeline | None = None
_registration: SchemaRegistration | None = None
_lock = threading.Lock()

SCHEMA_PATH = Path(__file__).resolve().parent.parent / "schemas" / "assignment2_schema.template.json"
STATIC_DIR = Path(__file__).resolve().parent / "static"
METADATA_DIR = Path(__file__).resolve().parent.parent / "metadata"

# ── user session store (in-memory, thread-safe) ──────────────────────────────
_user_sessions: dict[str, dict[str, Any]] = {}   # token → session dict
_user_sessions_lock = threading.Lock()

# ── ingestion latency store ──────────────────────────────────────────────────
_last_bootstrap_timings: dict[str, Any] | None = None


# ── helpers ───────────────────────────────────────────────────────────────────

def _ok(data: Any = None) -> dict:
    return {"success": True, "data": data, "error": None}


def _err(msg: str, status: int = 400) -> JSONResponse:
    return JSONResponse(
        {"success": False, "data": None, "error": msg},
        status_code=status,
    )


# ── Response sanitizer ────────────────────────────────────────────────────────
# Strip storage-internal keys from UI-consumed payloads.
_FORBIDDEN_KEYS = frozenset({
    "sql_queries", "mongo_queries", "sql_result", "mongo_result",
    "sql_plan", "mongo_plan", "table_name", "table", "collection_name",
    "collection", "index_name", "_id", "lock_key",
})


def _sanitize(obj: Any, *, _depth: int = 0) -> Any:
    """Recursively strip forbidden storage-internal keys."""
    if _depth > 20:
        return obj
    if isinstance(obj, dict):
        return {
            k: _sanitize(v, _depth=_depth + 1)
            for k, v in obj.items()
            if k not in _FORBIDDEN_KEYS
        }
    if isinstance(obj, (list, tuple)):
        return [_sanitize(item, _depth=_depth + 1) for item in obj]
    return obj


def _ok_sanitized(data: Any = None) -> dict:
    """Return a sanitized success response for UI-consumed endpoints."""
    return {"success": True, "data": _sanitize(data), "error": None}


def _load_registration() -> SchemaRegistration:
    data = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return SchemaRegistration(
        schema_name=data["schema_name"],
        version=data["version"],
        root_entity=data["root_entity"],
        json_schema=data["json_schema"],
        constraints=data.get("constraints", {}),
    )


def _registration_from_payload(payload: dict[str, Any]) -> SchemaRegistration:
    if not isinstance(payload, dict):
        raise ValueError("Schema payload must be a JSON object")

    json_schema = payload.get("json_schema")
    if json_schema is None and isinstance(payload.get("schema"), dict):
        json_schema = payload.get("schema")

    # Allow passing plain JSON schema directly.
    if json_schema is None and payload.get("type") == "object":
        json_schema = payload

    if not isinstance(json_schema, dict):
        raise ValueError("Schema payload must include a valid 'json_schema' object")

    return SchemaRegistration(
        schema_name=str(payload.get("schema_name") or "custom_schema"),
        version=str(payload.get("version") or "1.0.0"),
        root_entity=str(payload.get("root_entity") or "root"),
        json_schema=json_schema,
        constraints=payload.get("constraints") or {},
    )


def _enrich_records(records: list[dict]) -> list[dict]:
    """Add nested structures (attachments + sensors) — mirrors run_pipeline.py."""
    for i, record in enumerate(records):
        eid = record.get("event_id", f"e{i}")
        did = record.get("device", {}).get("device_id", f"d{i}")
        record["post"]["attachments"] = [
            {"attachment_id": f"att_{eid}_1", "file_type": "image"},
            {"attachment_id": f"att_{eid}_2", "file_type": "video"},
        ]
        record["device"]["sensors"] = [
            {
                "sensor_id": f"sen_{did}_1",
                "type": "temperature",
                "readings": [
                    {"timestamp": record.get("timestamp", ""), "value": round(random.uniform(20, 40), 1)},
                    {"timestamp": record.get("timestamp", ""), "value": round(random.uniform(20, 40), 1)},
                ],
            },
            {
                "sensor_id": f"sen_{did}_2",
                "type": "humidity",
                "readings": [
                    {"timestamp": record.get("timestamp", ""), "value": round(random.uniform(30, 90), 1)},
                ],
            },
        ]
    return records


def _ensure_pipeline() -> Assignment3Pipeline:
    global pipeline
    if pipeline is None:
        raise RuntimeError("Pipeline not initialised. Run /api/bootstrap first.")
    return pipeline


def _reset_backend_data(p: Assignment3Pipeline) -> None:
    """Clear all SQL tables and Mongo collections for a deterministic bootstrap snapshot."""
    p.ensure_connected()

    mysql_client = p.a2.a1_pipeline._mysql_client
    mysql_client.execute("SET FOREIGN_KEY_CHECKS=0")
    try:
        rows = mysql_client.fetch_all("SHOW TABLES")
        table_names: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                continue
            for value in row.values():
                if value:
                    table_names.append(str(value))
                    break
        for table_name in table_names:
            mysql_client.execute(f"DROP TABLE IF EXISTS `{table_name}`")
    finally:
        mysql_client.execute("SET FOREIGN_KEY_CHECKS=1")

    mongo_client = p.a2.a1_pipeline._mongo_client
    mongo_raw_client = getattr(mongo_client, "client", None)
    db_name = getattr(mongo_client, "database", None)
    if mongo_raw_client is not None and db_name:
        db = mongo_raw_client[db_name]
        for coll_name in db.list_collection_names():
            db.drop_collection(coll_name)


def _run_bootstrap_with_records(
    registration: SchemaRegistration,
    records: list[dict[str, Any]],
) -> dict[str, Any]:
    global pipeline, _registration, _last_bootstrap_timings

    t_total_start = time.perf_counter()

    # Cleanup old pipeline
    if pipeline is not None:
        try:
            pipeline.close()
        except Exception:
            pass
        pipeline = None

    cfg = get_config()
    MetadataStore(cfg.metadata_dir).clear()

    _registration = registration
    pipeline = Assignment3Pipeline(config=cfg)

    # Phase 1: Register schema
    t0 = time.perf_counter()
    pipeline.a2.register_schema(registration)
    t_schema = (time.perf_counter() - t0) * 1000

    # Phase 2: Ingest/classify from provided records
    t0 = time.perf_counter()
    pipeline.a2.run_ingestion(records)
    t_ingestion = (time.perf_counter() - t0) * 1000

    # Phase 3: Build storage strategy
    t0 = time.perf_counter()
    pipeline.a2.build_storage_strategy(registration)
    t_storage = (time.perf_counter() - t0) * 1000

    # Phase 4: Reset backend + rebuild
    t0 = time.perf_counter()
    _reset_backend_data(pipeline)
    pipeline.a2.build_storage_strategy(registration)
    t_reset = (time.perf_counter() - t0) * 1000

    # Phase 5: Insert records via transactional CRUD
    t0 = time.perf_counter()
    result = pipeline.execute_transactional(
        CrudOperation.CREATE, {"records": records}
    )
    t_insert = (time.perf_counter() - t0) * 1000

    t_total = (time.perf_counter() - t_total_start) * 1000

    _last_bootstrap_timings = {
        "timestamp": time.time(),
        "timestamp_iso": time.strftime("%Y-%m-%dT%H:%M:%S"),
        "record_count": len(records),
        "schema_name": registration.schema_name,
        "total_ms": round(t_total, 2),
        "phases": {
            "schema_registration_ms": round(t_schema, 2),
            "data_ingestion_ms": round(t_ingestion, 2),
            "storage_strategy_ms": round(t_storage, 2),
            "backend_reset_rebuild_ms": round(t_reset, 2),
            "transactional_insert_ms": round(t_insert, 2),
        },
    }
    logger.info("Bootstrap timings: %s", _last_bootstrap_timings)

    session = pipeline.get_session_info()
    return {
        "create_status": result.status,
        "session": asdict(session),
        "timings": _last_bootstrap_timings,
    }


def _resolve_custom_linking_field(
    registration: SchemaRegistration,
    records: list[dict[str, Any]],
) -> str:
    if all(str(record.get("username", "")).strip() for record in records):
        return "username"

    constraints = registration.constraints or {}
    configured = constraints.get("linking_field")
    if isinstance(configured, str) and configured and all(str(record.get(configured, "")).strip() for record in records):
        return configured

    unique_candidates = constraints.get("unique_candidates") or []
    for candidate in unique_candidates:
        if not isinstance(candidate, str) or "." in candidate:
            continue
        if all(str(record.get(candidate, "")).strip() for record in records):
            return candidate

    heuristic_candidates = [
        "customer_id",
        "order_id",
        "user_id",
        "id",
    ]
    for candidate in heuristic_candidates:
        if all(str(record.get(candidate, "")).strip() for record in records):
            return candidate

    raise ValueError(
        "Could not infer a linking key for custom schema. Provide top-level 'username' in each record, "
        "or set constraints.linking_field to a top-level field present in all records."
    )


def _normalize_custom_records_for_pipeline(
    registration: SchemaRegistration,
    records: list[dict[str, Any]],
) -> tuple[SchemaRegistration, list[dict[str, Any]], str]:
    linking_field = _resolve_custom_linking_field(registration, records)

    normalized_records: list[dict[str, Any]] = []
    for index, record in enumerate(records):
        normalized = dict(record)
        if "username" not in normalized or not str(normalized.get("username", "")).strip():
            source_value = normalized.get(linking_field)
            if source_value is None or str(source_value).strip() == "":
                raise ValueError(
                    f"Record at index {index} is missing linking field '{linking_field}' needed to derive username"
                )
            normalized["username"] = str(source_value)
        normalized_records.append(normalized)

    normalized_registration = copy.deepcopy(registration)
    schema = normalized_registration.json_schema or {}
    properties = schema.setdefault("properties", {})
    if "username" not in properties:
        properties["username"] = {"type": "string"}

    constraints = dict(normalized_registration.constraints or {})
    not_null = list(constraints.get("not_null") or [])
    if "username" not in not_null:
        not_null.append("username")
    constraints["not_null"] = not_null
    normalized_registration.constraints = constraints

    return normalized_registration, normalized_records, linking_field


# ── lifespan ──────────────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(app: FastAPI):
    yield
    global pipeline
    if pipeline is not None:
        pipeline.close()
        pipeline = None


# ── FastAPI app ───────────────────────────────────────────────────────────────

app = FastAPI(title="Logical Dashboard", lifespan=lifespan)

# CORS for dev mode (Vite dev server on port 5173)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR), html=True), name="static")


# ── Root → serve index.html ──────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    index = STATIC_DIR / "index.html"
    return HTMLResponse(index.read_text(encoding="utf-8"))


# ══════════════════════════════════════════════════════════════════════════════
#  AUTH ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

@app.post("/api/auth/login")
async def auth_login(request: Request):
    """Register a user session. Returns a session token."""
    try:
        body = await request.json()
        username = (body.get("username") or "").strip()
        if not username:
            return _err("Username is required", 400)

        token = secrets.token_hex(16)
        client_host = request.client.host if request.client else "unknown"
        user_agent = request.headers.get("user-agent", "unknown")

        session_entry = {
            "token": token,
            "username": username,
            "login_time": time.time(),
            "ip": client_host,
            "user_agent": user_agent,
        }

        with _user_sessions_lock:
            _user_sessions[token] = session_entry

        logger.info("User '%s' logged in from %s (token=%s…)", username, client_host, token[:8])
        return _ok({"token": token, "username": username})
    except Exception as exc:
        return _err(str(exc), 500)


@app.post("/api/auth/logout")
async def auth_logout(request: Request):
    """Remove a user session by token."""
    try:
        body = await request.json()
        token = (body.get("token") or "").strip()
        if not token:
            return _err("Token is required", 400)

        with _user_sessions_lock:
            removed = _user_sessions.pop(token, None)

        if removed:
            logger.info("User '%s' logged out (token=%s…)", removed["username"], token[:8])
            return _ok({"logged_out": True})
        return _err("Session not found", 404)
    except Exception as exc:
        return _err(str(exc), 500)


@app.get("/api/auth/sessions")
async def auth_sessions():
    """Return all currently active user sessions."""
    try:
        now = time.time()
        with _user_sessions_lock:
            sessions = []
            for s in _user_sessions.values():
                duration_s = now - s["login_time"]
                sessions.append({
                    "username": s["username"],
                    "login_time": s["login_time"],
                    "login_time_iso": time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(s["login_time"])),
                    "duration_seconds": round(duration_s),
                    "duration_display": _fmt_duration(duration_s),
                    "ip": s["ip"],
                    "user_agent": s["user_agent"],
                })
        return _ok({"count": len(sessions), "sessions": sessions})
    except Exception as exc:
        return _err(str(exc), 500)


def _fmt_duration(seconds: float) -> str:
    """Format seconds into human-readable duration."""
    s = int(seconds)
    if s < 60:
        return f"{s}s"
    if s < 3600:
        return f"{s // 60}m {s % 60}s"
    h = s // 3600
    m = (s % 3600) // 60
    return f"{h}h {m}m"


@app.get("/api/ingestion/timings")
async def ingestion_timings():
    """Return latest bootstrap pipeline timing breakdown."""
    if _last_bootstrap_timings is None:
        return _err("No bootstrap has been run yet", 404)
    return _ok(_last_bootstrap_timings)


# ══════════════════════════════════════════════════════════════════════════════
#  API ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

# ── Bootstrap ─────────────────────────────────────────────────────────────────

@app.post("/api/bootstrap")
async def bootstrap(request: Request):
    body = await request.json()
    record_count = body.get("record_count", 100)
    try:
        with _lock:
            registration = _load_registration()
            generator_pipeline = Assignment3Pipeline(config=get_config())
            records = generator_pipeline.a2.generate_records(record_count, registration)
            generator_pipeline.close()
            records = _enrich_records(records)
            summary = _run_bootstrap_with_records(registration, records)
            return _ok({
                "message": f"Bootstrapped with {record_count} records",
                **summary,
            })
    except Exception as exc:
        logger.exception("Bootstrap failed")
        return _err(f"Bootstrap failed: {exc}", 500)


@app.post("/api/bootstrap/custom")
async def bootstrap_custom(request: Request):
    body = await request.json()
    try:
        schema_payload = body.get("schema")
        records = body.get("records")

        if not isinstance(records, list) or not all(isinstance(r, dict) for r in records):
            return _err("'records' must be a JSON array of objects", 400)
        if not records:
            return _err("'records' cannot be empty", 400)

        registration = _registration_from_payload(schema_payload)
        registration, records, linking_field = _normalize_custom_records_for_pipeline(registration, records)

        with _lock:
            summary = _run_bootstrap_with_records(registration, records)
            return _ok({
                "message": f"Custom bootstrap completed with {len(records)} records",
                "linking_field": linking_field,
                **summary,
            })
    except ValueError as exc:
        return _err(str(exc), 400)
    except Exception as exc:
        logger.exception("Custom bootstrap failed")
        return _err(f"Custom bootstrap failed: {exc}", 500)


# ── Session ───────────────────────────────────────────────────────────────────

@app.get("/api/session")
async def get_session():
    try:
        p = _ensure_pipeline()
        info = p.get_session_info()
        return _ok_sanitized(asdict(info))
    except Exception as exc:
        return _err(str(exc))


# ── Schema Plan ──────────────────────────────────────────────────────────────

@app.get("/api/schema/plan")
async def get_schema_plan():
    """Return sql_plan, mongo_plan and field_locations from metadata directory.
    Read-only file access — no pipeline required."""
    try:
        sql_file   = METADATA_DIR / "sql_plan.json"
        mongo_file = METADATA_DIR / "mongo_plan.json"
        fields_file = METADATA_DIR / "field_locations.json"
        if not sql_file.exists() or not mongo_file.exists():
            return _err("Schema plans not found — bootstrap the database first", 404)
        sql_plan   = json.loads(sql_file.read_text(encoding="utf-8"))
        mongo_plan = json.loads(mongo_file.read_text(encoding="utf-8"))
        field_locs = json.loads(fields_file.read_text(encoding="utf-8")) if fields_file.exists() else {}
        return _ok({
            "sql": sql_plan,
            "mongo": mongo_plan,
            "fields": field_locs.get("field_locations", []),
        })
    except Exception as exc:
        logger.exception("schema/plan failed")
        return _err(str(exc), 500)


# ── Entities ──────────────────────────────────────────────────────────────────

@app.get("/api/entities")
async def list_entities():
    try:
        p = _ensure_pipeline()
        return _ok(p.list_entities())
    except Exception as exc:
        return _err(str(exc))


@app.get("/api/entities/all")
async def all_entity_data(limit: int = Query(100, ge=1, le=1000)):
    try:
        p = _ensure_pipeline()
        data = p.get_all_data(limit=limit)
        return _ok(data)
    except Exception as exc:
        return _err(str(exc))


@app.get("/api/entities/{name}")
async def get_entity(name: str, limit: int = Query(50, ge=1, le=1000), offset: int = Query(0, ge=0)):
    try:
        p = _ensure_pipeline()
        entity = p.get_entity_data(name, limit=limit, offset=offset)
        return _ok_sanitized(asdict(entity))
    except Exception as exc:
        return _err(str(exc))


# ── Stats ─────────────────────────────────────────────────────────────────────

@app.get("/api/stats")
async def get_stats():
    try:
        p = _ensure_pipeline()
        return _ok(p.get_stats())
    except Exception as exc:
        return _err(str(exc))


# ── CRUD ──────────────────────────────────────────────────────────────────────

@app.post("/api/crud")
async def execute_crud(request: Request):
    try:
        p = _ensure_pipeline()
        body = await request.json()
        op_str = body.pop("operation", "read").lower()
        operation = CrudOperation(op_str)
        result = p.execute_transactional(operation, body)
        return _ok(asdict(result))
    except Exception as exc:
        return _err(str(exc))


# ── Query ─────────────────────────────────────────────────────────────────────

@app.post("/api/query/preview")
async def preview_query(request: Request):
    try:
        p = _ensure_pipeline()
        body = await request.json()
        plan = p.preview_query(body)
        return _ok(plan)
    except Exception as exc:
        return _err(str(exc))


@app.post("/api/query/execute")
async def execute_query(request: Request):
    try:
        p = _ensure_pipeline()
        body = await request.json()
        result = p.execute_query(body)
        return _ok_sanitized(result)
    except Exception as exc:
        return _err(str(exc))


# ── Query History ─────────────────────────────────────────────────────────────

@app.get("/api/query/history")
async def list_query_history(page: int = Query(1, ge=1), limit: int = Query(50, ge=1, le=200)):
    try:
        p = _ensure_pipeline()
        return _ok(p.query_history.list(page=page, limit=limit))
    except Exception as exc:
        return _err(str(exc))


@app.get("/api/query/history/{entry_id}")
async def get_query_history_entry(entry_id: str):
    try:
        p = _ensure_pipeline()
        entry = p.query_history.get(entry_id)
        if entry is None:
            return _err(f"History entry not found: {entry_id}", 404)
        return _ok(entry.to_dict())
    except Exception as exc:
        return _err(str(exc))


@app.post("/api/query/history/delete")
async def delete_query_history(request: Request):
    try:
        p = _ensure_pipeline()
        body = await request.json()
        entry_id = body.get("id")
        if not entry_id:
            return _err("Missing 'id' in request body")
        removed = p.query_history.delete(entry_id)
        if not removed:
            return _err(f"Entry not found: {entry_id}", 404)
        return _ok({"deleted": entry_id})
    except Exception as exc:
        return _err(str(exc))


@app.post("/api/query/history/clear")
async def clear_query_history():
    try:
        p = _ensure_pipeline()
        count = p.query_history.clear()
        return _ok({"cleared": count})
    except Exception as exc:
        return _err(str(exc))


# ── Evidence Export ───────────────────────────────────────────────────────────

@app.get("/api/evidence/export")
async def export_evidence():
    """Export comprehensive evidence package for demo/submission."""
    try:
        p = _ensure_pipeline()
        evidence = {
            "exported_at": __import__("time").strftime("%Y-%m-%dT%H:%M:%S"),
            "session": asdict(p.get_session_info()),
            "query_history": {
                "stats": p.query_history.get_stats(),
                "recent": p.query_history.list(page=1, limit=20),
            },
            "metrics": p.metrics.get_snapshot(),
            "benchmarks": p.benchmark_runner.get_results(),
            "entities": p.session_manager.list_entity_names(),
        }
        return _ok(evidence)
    except Exception as exc:
        return _err(str(exc))


# ── Metrics & Monitoring ──────────────────────────────────────────────────────

@app.get("/api/metrics")
async def get_metrics():
    try:
        p = _ensure_pipeline()
        return _ok(p.metrics.get_snapshot())
    except Exception as exc:
        return _err(str(exc))


@app.post("/api/metrics/reset")
async def reset_metrics():
    try:
        p = _ensure_pipeline()
        p.metrics.reset()
        return _ok({"reset": True})
    except Exception as exc:
        return _err(str(exc))


# ── Benchmarks ────────────────────────────────────────────────────────────────

REPO_ROOT = Path(__file__).resolve().parent.parent


def _resolve_k6_script(script_name: str) -> Path:
    script = (script_name or "load_test.js").strip()
    candidate = Path(script)
    if not candidate.is_absolute():
        candidate = (REPO_ROOT / candidate).resolve()
    else:
        candidate = candidate.resolve()

    if REPO_ROOT not in candidate.parents and candidate != REPO_ROOT:
        raise ValueError("k6 script must be inside repository workspace")
    if not candidate.exists() or not candidate.is_file():
        raise ValueError(f"k6 script not found: {script}")
    if candidate.suffix.lower() != ".js":
        raise ValueError("k6 script must be a .js file")
    return candidate


def _safe_metric(summary: dict[str, Any], metric: str, field: str, default: float = 0.0) -> float:
    metrics = summary.get("metrics") or {}
    metric_data = metrics.get(metric) or {}
    value = metric_data.get(field)
    if value is None and isinstance(metric_data.get("values"), dict):
        value = metric_data["values"].get(field)
    try:
        return float(value)
    except (TypeError, ValueError):
        return float(default)


def _run_k6_process(cmd: list[str], cwd: str) -> subprocess.CompletedProcess[str]:
    return subprocess.run(
        cmd,
        cwd=cwd,
        capture_output=True,
        text=True,
        timeout=600,
    )

@app.post("/api/benchmark/run")
async def run_benchmark(request: Request):
    try:
        p = _ensure_pipeline()
        body = await request.json()
        import asyncio
        loop = asyncio.get_event_loop()
        result = await loop.run_in_executor(None, lambda: p.benchmark_runner.run_benchmark(body))
        return _ok(result)
    except Exception as exc:
        return _err(str(exc))


@app.get("/api/benchmark/results")
async def get_benchmark_results():
    try:
        p = _ensure_pipeline()
        return _ok(p.benchmark_runner.get_results())
    except Exception as exc:
        return _err(str(exc))


@app.post("/api/benchmark/k6")
async def run_k6_benchmark(request: Request):
    try:
        body = await request.json()
        if not isinstance(body, dict):
            return _err("Invalid payload", status=400)

        if shutil.which("k6") is None:
            return _err("k6 is not installed or not in PATH", status=500)

        script_path = _resolve_k6_script(str(body.get("script", "load_test.js")))
        vus = max(1, int(body.get("vus", 10)))
        duration = str(body.get("duration", "30s")).strip() or "30s"

        with tempfile.NamedTemporaryFile(prefix="k6-summary-", suffix=".json", delete=False) as tmp:
            summary_path = Path(tmp.name)

        cmd = [
            "k6",
            "run",
            "--vus",
            str(vus),
            "--duration",
            duration,
            "--summary-export",
            str(summary_path),
            str(script_path),
        ]

        loop = asyncio.get_running_loop()
        completed = await loop.run_in_executor(None, lambda: _run_k6_process(cmd, str(REPO_ROOT)))

        if not summary_path.exists():
            return _err("k6 summary export not generated", status=500)

        summary = json.loads(summary_path.read_text(encoding="utf-8"))
        try:
            summary_path.unlink(missing_ok=True)
        except Exception:
            pass

        response = {
            "script": str(script_path.relative_to(REPO_ROOT)),
            "vus": vus,
            "duration": duration,
            "throughput_ops_per_sec": _safe_metric(summary, "successful_operations", "rate"),
            "successful_operations": _safe_metric(summary, "successful_operations", "count"),
            "failed_operations": _safe_metric(summary, "failed_operations", "count"),
            "http_reqs_per_sec": _safe_metric(summary, "http_reqs", "rate"),
            "operation_success_rate": _safe_metric(
                summary,
                "operation_success_rate",
                "value",
                default=_safe_metric(summary, "checks", "value"),
            ),
            "lock_wait_ms_avg": _safe_metric(summary, "lock_wait_ms", "avg"),
            "lock_wait_ms_p95": _safe_metric(summary, "lock_wait_ms", "p(95)"),
            "coordination_overhead_ms_avg": _safe_metric(summary, "coordination_overhead_ms", "avg"),
            "coordination_overhead_ms_p95": _safe_metric(summary, "coordination_overhead_ms", "p(95)"),
            "exit_code": completed.returncode,
        }
        if completed.returncode != 0:
            response["warning"] = (completed.stderr or completed.stdout or "k6 exited with non-zero code")[-1000:]

        return _ok(response)
    except subprocess.TimeoutExpired:
        return _err("k6 execution timed out", status=504)
    except ValueError as exc:
        return _err(str(exc), status=400)
    except Exception as exc:
        logger.exception("Failed to run k6 benchmark")
        return _err(str(exc), status=500)


# ── ACID Tests ────────────────────────────────────────────────────────────────

ACID_PROPERTIES = ["atomicity", "consistency", "isolation", "durability", "reconstruction"]


@app.post("/api/acid/run/{property_name}")
async def run_acid_test(property_name: str):
    if property_name not in ACID_PROPERTIES:
        return _err(f"Unknown property: {property_name}. Valid: {ACID_PROPERTIES}")
    try:
        p = _ensure_pipeline()
        result = p.run_acid_experiment(property_name)
        return _ok(asdict(result))
    except Exception as exc:
        return _err(str(exc))


@app.post("/api/acid/run-all")
async def run_all_acid():
    try:
        p = _ensure_pipeline()
        results = p.run_acid_experiments()
        return _ok([asdict(r) for r in results])
    except Exception as exc:
        return _err(str(exc))


# ── ACID SSE Streaming ───────────────────────────────────────────────────────

def _capture_logs_and_run(func, *args) -> tuple[list[dict], Any]:
    """Run func while capturing log output, return (log_entries, result)."""
    log_buffer: list[dict] = []
    handler = logging.Handler()
    handler.emit = lambda record: log_buffer.append({
        "time": time.strftime("%H:%M:%S"),
        "level": record.levelname,
        "msg": record.getMessage(),
        "logger": record.name,
    })
    handler.setLevel(logging.DEBUG)

    root_logger = logging.getLogger()
    root_logger.addHandler(handler)
    old_level = root_logger.level
    root_logger.setLevel(logging.DEBUG)
    try:
        result = func(*args)
        return log_buffer, result
    finally:
        root_logger.removeHandler(handler)
        root_logger.setLevel(old_level)


@app.get("/api/acid/stream/{property_name}")
async def stream_acid_test(property_name: str):
    if property_name not in ACID_PROPERTIES:
        return _err(f"Unknown property: {property_name}")

    async def event_generator():
        try:
            p = _ensure_pipeline()
            yield f"data: {json.dumps({'type': 'start', 'test': property_name})}\n\n"

            loop = asyncio.get_event_loop()
            log_buffer, result = await loop.run_in_executor(
                None,
                lambda: _capture_logs_and_run(p.run_acid_experiment, property_name),
            )

            for entry in log_buffer:
                yield f"data: {json.dumps({'type': 'log', **entry})}\n\n"

            yield f"data: {json.dumps({'type': 'result', 'result': asdict(result)})}\n\n"
            yield f"data: {json.dumps({'type': 'done'})}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


@app.get("/api/acid/stream-all")
async def stream_all_acid():
    async def event_generator():
        try:
            p = _ensure_pipeline()
            yield f"data: {json.dumps({'type': 'start', 'test': 'all'})}\n\n"

            for prop in ACID_PROPERTIES:
                yield f"data: {json.dumps({'type': 'test_start', 'test': prop})}\n\n"

                loop = asyncio.get_event_loop()
                log_buffer, result = await loop.run_in_executor(
                    None,
                    lambda prop=prop: _capture_logs_and_run(p.run_acid_experiment, prop),
                )

                for entry in log_buffer:
                    yield f"data: {json.dumps({'type': 'log', 'test': prop, **entry})}\n\n"

                yield f"data: {json.dumps({'type': 'result', 'test': prop, 'result': asdict(result)})}\n\n"

            yield f"data: {json.dumps({'type': 'done'})}\n\n"
        except Exception as exc:
            yield f"data: {json.dumps({'type': 'error', 'message': str(exc)})}\n\n"

    return StreamingResponse(event_generator(), media_type="text/event-stream")


# ── Schema info (for frontend form building) ─────────────────────────────────

@app.get("/api/schema")
async def get_schema():
    try:
        if _registration is not None:
            return _ok({
                "schema_name": _registration.schema_name,
                "version": _registration.version,
                "root_entity": _registration.root_entity,
                "json_schema": _registration.json_schema,
                "constraints": _registration.constraints,
            })
        # Fall back to file
        data = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
        return _ok(data)
    except Exception as exc:
        return _err(str(exc))


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    uvicorn.run(
        "dashboard.api_server:app",
        host="0.0.0.0",
        port=8080,
        reload=False,
        log_level="info",
    )
