"""Per-entity read/write lock manager for transaction isolation.

Provides shared (read) and exclusive (write) locks keyed by a logical
entity identifier derived from query payloads.  All locking is
in-process using threading primitives — no distributed coordination
is required.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Optional

logger = logging.getLogger(__name__)


class LockTimeoutError(Exception):
    """Raised when a lock cannot be acquired within the configured timeout."""

    def __init__(self, key: str, timeout: float):
        self.key = key
        self.timeout = timeout
        super().__init__(
            f"Could not acquire lock for key '{key}' within {timeout}s"
        )


class _ReadWriteLock:
    """Single read/write lock with shared-read / exclusive-write semantics.

    Multiple readers can hold the lock concurrently.  A writer blocks
    until all readers and any prior writer release.  Readers block while
    a writer holds the lock.
    """

    def __init__(self) -> None:
        self._cond = threading.Condition(threading.Lock())
        self._readers: int = 0
        self._writer: bool = False
        self._write_waiter: int = 0  # pending writers — gives write priority

    # -- shared (read) -------------------------------------------------

    def acquire_read(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        with self._cond:
            while self._writer or self._write_waiter > 0:
                remaining = deadline - time.monotonic()
                if remaining <= 0:
                    return False
                self._cond.wait(timeout=remaining)
            self._readers += 1
            return True

    def release_read(self) -> None:
        with self._cond:
            self._readers = max(0, self._readers - 1)
            if self._readers == 0:
                self._cond.notify_all()

    # -- exclusive (write) ---------------------------------------------

    def acquire_write(self, timeout: float) -> bool:
        deadline = time.monotonic() + timeout
        with self._cond:
            self._write_waiter += 1
            try:
                while self._writer or self._readers > 0:
                    remaining = deadline - time.monotonic()
                    if remaining <= 0:
                        return False
                    self._cond.wait(timeout=remaining)
                self._writer = True
                return True
            finally:
                self._write_waiter -= 1

    def release_write(self) -> None:
        with self._cond:
            self._writer = False
            self._cond.notify_all()


class ConcurrencyManager:
    """Manages per-entity read/write locks for transaction isolation.

    Usage::

        mgr = ConcurrencyManager(default_timeout=5.0)
        key = mgr.extract_lock_key(operation, payload)
        mgr.acquire(key, exclusive=True)
        try:
            ...  # do transactional work
        finally:
            mgr.release(key, exclusive=True)
    """

    _GLOBAL_KEY = "__global__"

    def __init__(self, default_timeout: float = 5.0) -> None:
        self.default_timeout = default_timeout
        self._locks: dict[str, _ReadWriteLock] = {}
        self._map_lock = threading.Lock()  # protects _locks dict itself

    # -- public API ----------------------------------------------------

    def acquire(
        self,
        key: str,
        exclusive: bool = True,
        timeout: Optional[float] = None,
    ) -> None:
        """Acquire a shared or exclusive lock for *key*.

        Raises ``LockTimeoutError`` if the lock is not obtained within
        *timeout* seconds.
        """
        t = timeout if timeout is not None else self.default_timeout
        rw = self._get_or_create(key)

        if exclusive:
            ok = rw.acquire_write(t)
        else:
            ok = rw.acquire_read(t)

        if not ok:
            raise LockTimeoutError(key, t)

        mode = "EXCLUSIVE" if exclusive else "SHARED"
        logger.debug("Lock acquired [%s] key=%s", mode, key)

    def release(self, key: str, exclusive: bool = True) -> None:
        """Release a previously acquired lock."""
        rw = self._locks.get(key)
        if rw is None:
            return

        if exclusive:
            rw.release_write()
        else:
            rw.release_read()

        mode = "EXCLUSIVE" if exclusive else "SHARED"
        logger.debug("Lock released [%s] key=%s", mode, key)

    # -- key extraction ------------------------------------------------

    @staticmethod
    def extract_lock_key(operation_value: str, payload: dict) -> str:
        """Derive a logical lock key from the query payload.

        Heuristic priority:
        1. Explicit ``filters`` with an identity field (username, user_id, id, event_id)
        2. First record in ``records`` list with an identity field
        3. ``updates`` dict with an identity field
        4. Fall back to a global lock key
        """
        identity_fields = ("username", "user_id", "id", "event_id")

        # 1. filters
        filters = payload.get("filters") or {}
        for field in identity_fields:
            val = filters.get(field)
            if val is not None:
                return f"entity:{field}={val}"

        # 2. records (CREATE payloads)
        records = payload.get("records") or []
        if records and isinstance(records[0], dict):
            for field in identity_fields:
                val = records[0].get(field)
                if val is not None:
                    return f"entity:{field}={val}"

        # 3. updates
        updates = payload.get("updates") or {}
        for field in identity_fields:
            val = updates.get(field)
            if val is not None:
                return f"entity:{field}={val}"

        # 4. global fallback
        return ConcurrencyManager._GLOBAL_KEY

    # -- internals -----------------------------------------------------

    def _get_or_create(self, key: str) -> _ReadWriteLock:
        rw = self._locks.get(key)
        if rw is not None:
            return rw
        with self._map_lock:
            # double-checked locking
            if key not in self._locks:
                self._locks[key] = _ReadWriteLock()
            return self._locks[key]
