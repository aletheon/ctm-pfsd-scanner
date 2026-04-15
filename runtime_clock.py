"""
runtime_clock.py — Monotonic tick counter for the CTM-PFSD execution kernel.

Constitution §7 (Determinism Law): tick_index is the canonical time axis.
1 tick = 1 service dispatch. Monotonic — never rewinds, never skips.
Thread-safe. Persists across server restarts.

§48 boundary: stdlib only (json, os, pathlib, threading).
"""
from __future__ import annotations

import json
import os
import threading
from pathlib import Path


class RuntimeClock:
    """
    Monotonic, persistent, thread-safe tick counter.

    tick_index starts at 0 on first creation and only ever increases.
    session_count increments by 1 on every __init__ (every server load).
    All writes are atomic: .tmp → os.replace().
    """

    def __init__(self, path: str) -> None:
        self._path = path
        self._lock = threading.RLock()

        with self._lock:
            data = self._load_raw()
            self._tick_index    = data["tick_index"]
            self._session_count = data["session_count"] + 1
            self._save()

    # ── Public API ─────────────────────────────────────────────────────────

    def advance(self) -> int:
        """
        Atomically increment tick_index by exactly 1, persist, and return
        the new value. Called once per service dispatch — no more, no less.
        """
        with self._lock:
            self._tick_index += 1
            self._save()
            return self._tick_index

    def current(self) -> int:
        """Return current tick_index without advancing. Read-only."""
        with self._lock:
            return self._tick_index

    def session_count(self) -> int:
        """
        Return number of times this clock file has been loaded
        (i.e. server sessions since the clock was first created).
        """
        with self._lock:
            return self._session_count

    def status(self) -> dict:
        """
        Return a snapshot of clock state:
        { "tick_index": int, "session_count": int, "clock_path": str }
        """
        with self._lock:
            return {
                "tick_index":    self._tick_index,
                "session_count": self._session_count,
                "clock_path":    self._path,
            }

    # ── Private ────────────────────────────────────────────────────────────

    def _load_raw(self) -> dict:
        """
        Read tick_index and session_count from disk.
        Returns {"tick_index": 0, "session_count": 0} if file is absent
        or malformed — never raises.
        """
        try:
            with open(self._path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if (isinstance(data, dict)
                    and isinstance(data.get("tick_index"), int)
                    and isinstance(data.get("session_count"), int)
                    and data["tick_index"] >= 0
                    and data["session_count"] >= 0):
                return data
        except (FileNotFoundError, json.JSONDecodeError, OSError, KeyError):
            pass
        return {"tick_index": 0, "session_count": 0}

    def _save(self) -> None:
        """
        Atomically write current state to disk.
        Writes to path+".tmp" then os.replace(tmp, path).
        Called under self._lock — do not acquire again.
        """
        tmp = self._path + ".tmp"
        Path(tmp).parent.mkdir(parents=True, exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(
                {"tick_index": self._tick_index,
                 "session_count": self._session_count},
                f,
                separators=(",", ":"),
            )
        os.replace(tmp, self._path)
