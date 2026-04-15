"""
intent_queue.py — CodeChangeIntent and IntentQueue for the CTM-PFSD execution kernel.

Constitution §7 (Determinism Law): every intent carries a content-addressed
intent_id derived from its diff_hash + session_id.
CTM-PFSD Spec v1.6 §5 (CodeChangeIntent), SovereignClaw §1.5 / §2.1.

§48 boundary: stdlib only (uuid, hashlib, time, dataclasses, typing,
              enum, threading, collections).
"""
from __future__ import annotations

import hashlib
import heapq
import threading
import uuid
from typing import Optional


# ── Namespace for UUID v5 intent_id derivation ─────────────────────────────

_INTENT_NS = uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


# ── Exceptions ─────────────────────────────────────────────────────────────

class IntentMutationError(RuntimeError):
    """E-INT-001 IntentMutation — raised on any field write after __init__."""


# ── CodeChangeIntent ───────────────────────────────────────────────────────

class CodeChangeIntent:
    """
    Immutable, content-addressed intent describing a proposed code change.

    All fields are set once in __init__ via object.__setattr__.
    Any subsequent write raises IntentMutationError (E-INT-001).
    tick_created is set to 0 at creation and written exactly once
    by IntentQueue.push() — only the queue may set it.
    """

    __slots__ = (
        "intent_id",
        "tick_created",
        "ttl_ticks",
        "priority",
        "preemptible",
        "change_type",
        "scope",
        "actor",
        "owner_policy",
        "intent_origin",
        "files_changed",
        "diff_summary",
        "change_description",
        "diff_hash",
        "model_id",
        "prompt_hash",
        "session_id",
        "human_reviewed",
    )

    def __init__(
        self,
        *,
        intent_id:          str,
        tick_created:       int,
        ttl_ticks:          int,
        priority:           int,
        preemptible:        bool,
        change_type:        str,
        scope:              str,
        actor:              str,
        owner_policy:       str,
        intent_origin:      str,
        files_changed:      tuple,
        diff_summary:       str,
        change_description: str,
        diff_hash:          str,
        model_id:           str,
        prompt_hash:        str,
        session_id:         str,
        human_reviewed:     bool,
    ) -> None:
        object.__setattr__(self, "intent_id",          intent_id)
        object.__setattr__(self, "tick_created",       tick_created)
        object.__setattr__(self, "ttl_ticks",          ttl_ticks)
        object.__setattr__(self, "priority",           priority)
        object.__setattr__(self, "preemptible",        preemptible)
        object.__setattr__(self, "change_type",        change_type)
        object.__setattr__(self, "scope",              scope)
        object.__setattr__(self, "actor",              actor)
        object.__setattr__(self, "owner_policy",       owner_policy)
        object.__setattr__(self, "intent_origin",      intent_origin)
        object.__setattr__(self, "files_changed",      tuple(files_changed))
        object.__setattr__(self, "diff_summary",       diff_summary)
        object.__setattr__(self, "change_description", change_description)
        object.__setattr__(self, "diff_hash",          diff_hash)
        object.__setattr__(self, "model_id",           model_id)
        object.__setattr__(self, "prompt_hash",        prompt_hash)
        object.__setattr__(self, "session_id",         session_id)
        object.__setattr__(self, "human_reviewed",     human_reviewed)

    def __setattr__(self, name: str, value: object) -> None:
        raise IntentMutationError(
            f"E-INT-001 IntentMutation — field '{name}' is immutable after creation"
        )

    def __repr__(self) -> str:
        return (
            f"CodeChangeIntent(intent_id={self.intent_id!r}, "
            f"change_type={self.change_type!r}, "
            f"priority={self.priority}, "
            f"tick_created={self.tick_created})"
        )

    @classmethod
    def make(
        cls,
        change_type:        str,
        scope:              str,
        actor:              str,
        owner_policy:       str,
        intent_origin:      str,
        files_changed:      tuple,
        diff_summary:       str,
        change_description: str,
        diff_hash:          str,
        model_id:           str  = "unknown",
        prompt_hash:        Optional[str] = None,
        session_id:         Optional[str] = None,
        human_reviewed:     bool = False,
        priority:           int  = 1,
        preemptible:        bool = True,
        ttl_ticks:          int  = 300,
    ) -> "CodeChangeIntent":
        """
        Convenience constructor. Derives intent_id as UUID v5 from
        diff_hash + session_id. tick_created is set to 0 (queue sets it).
        """
        session_id  = session_id  or str(uuid.uuid4())
        prompt_hash = prompt_hash or hashlib.sha256(b"").hexdigest()
        intent_id   = str(uuid.uuid5(_INTENT_NS, f"{diff_hash}:{session_id}"))
        return cls(
            intent_id          = intent_id,
            tick_created       = 0,
            ttl_ticks          = ttl_ticks,
            priority           = priority,
            preemptible        = preemptible,
            change_type        = change_type,
            scope              = scope,
            actor              = actor,
            owner_policy       = owner_policy,
            intent_origin      = intent_origin,
            files_changed      = tuple(files_changed),
            diff_summary       = diff_summary,
            change_description = change_description,
            diff_hash          = diff_hash,
            model_id           = model_id,
            prompt_hash        = prompt_hash,
            session_id         = session_id,
            human_reviewed     = human_reviewed,
        )


# ── IntentQueue ────────────────────────────────────────────────────────────

class IntentQueue:
    """
    In-memory priority queue for CodeChangeIntent objects.

    Ordering: priority DESC, tick_created ASC (ties broken by insertion order).
    Thread-safe via RLock.
    """

    def __init__(self) -> None:
        self._lock: threading.RLock = threading.RLock()
        self._heap: list            = []   # heap entries: (-priority, tick, seq, intent)
        self._seq:  int             = 0    # monotonic insertion counter for tie-breaking

    def push(self, intent: "CodeChangeIntent", clock) -> "CodeChangeIntent":
        """
        Set intent.tick_created = clock.current(), then enqueue.
        Only the queue may write tick_created after creation.
        Returns the intent with tick_created set.
        """
        with self._lock:
            object.__setattr__(intent, "tick_created", clock.current())
            seq = self._seq
            self._seq += 1
            heapq.heappush(
                self._heap,
                (-intent.priority, intent.tick_created, seq, intent),
            )
            return intent

    def pop(self) -> Optional["CodeChangeIntent"]:
        """
        Remove and return the highest-priority intent
        (priority DESC, tick_created ASC for ties).
        Returns None if the queue is empty.
        """
        with self._lock:
            if not self._heap:
                return None
            _, _, _, intent = heapq.heappop(self._heap)
            return intent

    def expire(self, current_tick: int) -> list:
        """
        Remove and return all intents whose TTL has elapsed:
          (current_tick - intent.tick_created) >= intent.ttl_ticks

        Caller must emit a FAILURE_RESIDUAL for each expired intent.
        """
        with self._lock:
            expired   = []
            remaining = []
            for entry in self._heap:
                _, _, _, intent = entry
                if (current_tick - intent.tick_created) >= intent.ttl_ticks:
                    expired.append(intent)
                else:
                    remaining.append(entry)
            heapq.heapify(remaining)
            self._heap = remaining
            return expired

    def depth(self) -> int:
        """Return number of intents currently queued."""
        with self._lock:
            return len(self._heap)

    def peek(self) -> Optional["CodeChangeIntent"]:
        """Return the next intent without removing it. Returns None if empty."""
        with self._lock:
            if not self._heap:
                return None
            return self._heap[0][3]
