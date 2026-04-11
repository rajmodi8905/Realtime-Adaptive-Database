"""Query History — bounded in-memory store with file-backed persistence.

Records every query preview/execute through the dashboard and persists
them to a JSON-lines file for restart resilience.  History is bounded
at ``max_entries`` to prevent unbounded growth.
"""

from __future__ import annotations

import hashlib
import json
import logging
import threading
import time
import uuid
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_DEFAULT_MAX_ENTRIES = 500
_DEFAULT_HISTORY_FILE = "query_history.jsonl"


@dataclass
class QueryHistoryEntry:
    id: str
    timestamp: float
    timestamp_iso: str
    operation: str
    payload: dict
    status: str  # "success" | "error" | "preview"
    duration_ms: float
    result_summary: dict  # row_count, error, etc.
    payload_digest: str  # SHA-256 of payload for dedup/search

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, d: dict[str, Any]) -> "QueryHistoryEntry":
        return cls(**{k: v for k, v in d.items() if k in cls.__dataclass_fields__})


class QueryHistoryStore:
    """Thread-safe, bounded, file-backed query history."""

    def __init__(
        self,
        persistence_dir: Optional[Path] = None,
        max_entries: int = _DEFAULT_MAX_ENTRIES,
    ):
        self._lock = threading.Lock()
        self._entries: list[QueryHistoryEntry] = []
        self._max = max_entries
        self._file: Optional[Path] = None

        if persistence_dir is not None:
            persistence_dir.mkdir(parents=True, exist_ok=True)
            self._file = persistence_dir / _DEFAULT_HISTORY_FILE
            self._load_from_disk()

    # ── Public API ────────────────────────────────────────────────────────────

    def record(
        self,
        operation: str,
        payload: dict,
        status: str,
        duration_ms: float,
        result_summary: Optional[dict] = None,
    ) -> QueryHistoryEntry:
        """Record a new query execution in history."""
        now = time.time()
        entry = QueryHistoryEntry(
            id=uuid.uuid4().hex[:12],
            timestamp=now,
            timestamp_iso=time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime(now)),
            operation=operation,
            payload=payload,
            status=status,
            duration_ms=round(duration_ms, 2),
            result_summary=result_summary or {},
            payload_digest=self._digest(payload),
        )

        with self._lock:
            self._entries.insert(0, entry)  # newest first
            # Trim oldest
            if len(self._entries) > self._max:
                self._entries = self._entries[: self._max]
            self._persist_append(entry)

        logger.debug("History recorded: %s %s [%s] %.1fms", entry.id, operation, status, duration_ms)
        return entry

    def list(self, page: int = 1, limit: int = 50) -> dict[str, Any]:
        """Return paginated history list."""
        with self._lock:
            total = len(self._entries)
            start = (page - 1) * limit
            end = start + limit
            items = [e.to_dict() for e in self._entries[start:end]]
        return {
            "items": items,
            "total": total,
            "page": page,
            "limit": limit,
            "total_pages": max(1, -(-total // limit)),
        }

    def get(self, entry_id: str) -> Optional[QueryHistoryEntry]:
        """Get a single history entry by ID."""
        with self._lock:
            for e in self._entries:
                if e.id == entry_id:
                    return e
        return None

    def delete(self, entry_id: str) -> bool:
        """Delete a single history entry."""
        with self._lock:
            before = len(self._entries)
            self._entries = [e for e in self._entries if e.id != entry_id]
            removed = len(self._entries) < before
            if removed:
                self._persist_full()
        return removed

    def clear(self) -> int:
        """Clear all history. Returns count of removed entries."""
        with self._lock:
            count = len(self._entries)
            self._entries.clear()
            self._persist_full()
        return count

    def get_stats(self) -> dict[str, Any]:
        """Return aggregate stats about query history."""
        with self._lock:
            total = len(self._entries)
            if total == 0:
                return {"total": 0, "success": 0, "error": 0, "avg_duration_ms": 0}
            success = sum(1 for e in self._entries if e.status == "success")
            error = sum(1 for e in self._entries if e.status == "error")
            avg_dur = sum(e.duration_ms for e in self._entries) / total
        return {
            "total": total,
            "success": success,
            "error": error,
            "preview": total - success - error,
            "avg_duration_ms": round(avg_dur, 2),
        }

    # ── Persistence ───────────────────────────────────────────────────────────

    def _load_from_disk(self) -> None:
        if self._file is None or not self._file.exists():
            return
        try:
            entries = []
            with open(self._file, "r", encoding="utf-8") as f:
                for line in f:
                    line = line.strip()
                    if line:
                        entries.append(QueryHistoryEntry.from_dict(json.loads(line)))
            # Keep bounded
            self._entries = entries[: self._max]
            logger.info("Loaded %d history entries from disk", len(self._entries))
        except Exception as exc:
            logger.warning("Failed to load query history: %s", exc)

    def _persist_append(self, entry: QueryHistoryEntry) -> None:
        if self._file is None:
            return
        try:
            with open(self._file, "a", encoding="utf-8") as f:
                f.write(json.dumps(entry.to_dict()) + "\n")
        except Exception as exc:
            logger.warning("Failed to persist history entry: %s", exc)

    def _persist_full(self) -> None:
        """Rewrite full history file (used after delete/clear)."""
        if self._file is None:
            return
        try:
            with open(self._file, "w", encoding="utf-8") as f:
                for e in self._entries:
                    f.write(json.dumps(e.to_dict()) + "\n")
        except Exception as exc:
            logger.warning("Failed to rewrite history file: %s", exc)

    # ── Helpers ───────────────────────────────────────────────────────────────

    @staticmethod
    def _digest(payload: dict) -> str:
        raw = json.dumps(payload, sort_keys=True, default=str)
        return hashlib.sha256(raw.encode()).hexdigest()[:16]
