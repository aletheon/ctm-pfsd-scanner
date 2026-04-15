"""
state_store.py — Zone 2/3 in-memory state accumulator.

CTM-PFSD Code Governance Spec v1.6 §3c

Receives state_delta dicts from service invocations and
accumulates them into a running state snapshot.

INVARIANTS (enforced):
  Zone 5 never reads StateStore — import boundary
  State is accumulated, not replaced (merge semantics)
  Thread-safe (RLock)
  Append-only delta log per session
  get_snapshot() always returns a copy — never the internal dict

§48 boundary: stdlib only (threading, copy).
"""
from __future__ import annotations

import copy
import threading
from typing import Optional


class StateStore:
    """
    Thread-safe, merge-semantics state accumulator.
    write-once-per-tick, read-any-time.
    """

    def __init__(self) -> None:
        self._state:  dict  = {}
        self._deltas: list  = []
        self._lock:   threading.RLock = threading.RLock()

    def commit_delta(
        self,
        delta:   dict,
        tick:    int,
        service: str,
    ) -> None:
        """
        Merge delta into current state (shallow — delta keys overwrite).
        Appends {tick, service, delta} to the delta log.
        Silently ignores empty delta dicts.
        Thread-safe.
        """
        if not delta:
            return
        with self._lock:
            self._state.update(delta)
            self._deltas.append({
                "tick":    tick,
                "service": service,
                "delta":   delta,
            })

    def get_snapshot(self) -> dict:
        """
        Returns a shallow copy of the current accumulated state.
        Thread-safe. Never returns the internal dict.
        """
        with self._lock:
            return copy.copy(self._state)

    def get_delta_count(self) -> int:
        """Returns number of deltas committed."""
        with self._lock:
            return len(self._deltas)

    def status(self) -> dict:
        """
        Returns a summary of current store state.
        {
          "state_keys":   list of current state key names,
          "delta_count":  int,
          "last_tick":    int | null,
          "last_service": str | null,
        }
        """
        with self._lock:
            last_tick:    Optional[int] = None
            last_service: Optional[str] = None
            if self._deltas:
                last = self._deltas[-1]
                last_tick    = last["tick"]
                last_service = last["service"]
            return {
                "state_keys":   list(self._state.keys()),
                "delta_count":  len(self._deltas),
                "last_tick":    last_tick,
                "last_service": last_service,
            }
