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
import io
import json
import logging
import random
import sys
import threading
import time
import traceback
from contextlib import asynccontextmanager
from dataclasses import asdict
from pathlib import Path
from typing import Any

from fastapi import FastAPI, Query, Request
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


# ── helpers ───────────────────────────────────────────────────────────────────

def _ok(data: Any = None) -> dict:
    return {"success": True, "data": data, "error": None}


def _err(msg: str, status: int = 400) -> JSONResponse:
    return JSONResponse(
        {"success": False, "data": None, "error": msg},
        status_code=status,
    )


def _load_registration() -> SchemaRegistration:
    data = json.loads(SCHEMA_PATH.read_text(encoding="utf-8"))
    return SchemaRegistration(
        schema_name=data["schema_name"],
        version=data["version"],
        root_entity=data["root_entity"],
        json_schema=data["json_schema"],
        constraints=data.get("constraints", {}),
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

# Serve static files
app.mount("/static", StaticFiles(directory=str(STATIC_DIR)), name="static")


# ── Root → serve index.html ──────────────────────────────────────────────────

@app.get("/", response_class=HTMLResponse)
async def root():
    index = STATIC_DIR / "index.html"
    return HTMLResponse(index.read_text(encoding="utf-8"))


# ══════════════════════════════════════════════════════════════════════════════
#  API ENDPOINTS
# ══════════════════════════════════════════════════════════════════════════════

# ── Bootstrap ─────────────────────────────────────────────────────────────────

@app.post("/api/bootstrap")
async def bootstrap(request: Request):
    global pipeline, _registration
    body = await request.json()
    record_count = body.get("record_count", 100)
    try:
        with _lock:
            # Cleanup old pipeline
            if pipeline is not None:
                try:
                    pipeline.close()
                except Exception:
                    pass
                pipeline = None

            cfg = get_config()
            MetadataStore(cfg.metadata_dir).clear()

            _registration = _load_registration()
            pipeline = Assignment3Pipeline(config=cfg)

            # Phase 1: Register schema
            pipeline.a2.register_schema(_registration)

            # Phase 2: Generate + enrich + ingest
            records = pipeline.a2.generate_records(record_count, _registration)
            records = _enrich_records(records)
            pipeline.a2.run_ingestion(records)

            # Phase 3: Build storage strategy
            pipeline.a2.build_storage_strategy(_registration)

            # Phase 4: Insert records via transactional CRUD
            result = pipeline.execute_transactional(
                CrudOperation.CREATE, {"records": records}
            )

            session = pipeline.get_session_info()
            return _ok({
                "message": f"Bootstrapped with {record_count} records",
                "create_status": result.status,
                "session": asdict(session),
            })
    except Exception as exc:
        logger.exception("Bootstrap failed")
        return _err(f"Bootstrap failed: {exc}", 500)


# ── Session ───────────────────────────────────────────────────────────────────

@app.get("/api/session")
async def get_session():
    try:
        p = _ensure_pipeline()
        info = p.get_session_info()
        return _ok(asdict(info))
    except Exception as exc:
        return _err(str(exc))


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
        return _ok(asdict(entity))
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
        return _ok(result)
    except Exception as exc:
        return _err(str(exc))


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
