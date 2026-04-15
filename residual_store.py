"""
residual_store.py — Episodic memory layer for Zone 5.

File-based, append-only, newline-delimited JSON (.jsonl).
Identical pattern to pic_chain.py.

Zone boundaries:
  WRITE: Zone 2 only (server.py _orchestrate() and /approve).
  READ:  Zone 5 only (bdh_kernel.py, distillation workers).
  Zone 3 services NEVER write here directly.
  Zone 5 NEVER writes here — read-only access only.

§48 boundary: stdlib only (json, uuid, os, pathlib, time, datetime).
"""
from __future__ import annotations

import json
import uuid
from pathlib import Path
from datetime import datetime, timezone

_REQUIRED_FIELDS = frozenset({
    "residual_id",
    "intent_id",
    "tick",
    "source_type",
    "service",
    "outcome",
    "exclude_from_learning",
})


# ── Public API ─────────────────────────────────────────────────────────────

def make_entry(
    intent_id:            str,
    tick:                 int,
    source_type:          str,
    project_name:         str,
    service:              str,
    outcome:              str,
    policy_id:            str | None = None,
    delta_magnitude:      float      = 0.0,
    graph_hash:           str | None = None,
    rule_id:              str | None = None,
    diff_hash:            str | None = None,
) -> dict:
    """
    Build a validated residual entry dict.
    Does NOT write to file — call append() to persist.

    Sets:
      residual_id          = "RES-" + uuid4 hex[:12]
      exclude_from_learning = (outcome == "FAILED")
      safety_trigger       = null
      timestamp            = ISO 8601 UTC now
      rule_id              = None for SCAN/APPROVE; set when a rule fires (Path A)
      diff_hash            = None for SCAN/APPROVE; set when governed execution
                             produces a real diff
    """
    return {
        "residual_id":           "RES-" + uuid.uuid4().hex[:12],
        "intent_id":             intent_id,
        "tick":                  tick,
        "source_type":           source_type,
        "project_name":          project_name,
        "policy_id":             policy_id,
        "service":               service,
        "outcome":               outcome,
        "delta_magnitude":       float(delta_magnitude),
        "exclude_from_learning": (outcome == "FAILED"),
        "safety_trigger":        None,
        "graph_hash":            graph_hash,
        "rule_id":               rule_id,
        "diff_hash":             diff_hash,
        "timestamp":             datetime.now(timezone.utc).strftime(
                                     "%Y-%m-%dT%H:%M:%SZ"),
    }


def append(entry: dict, path: str) -> dict:
    """
    Validate required fields and append one JSON line to the store file.
    Creates the file and any parent directories if needed.
    Returns the entry.

    Raises ValueError if any required field is missing.
    """
    missing = _REQUIRED_FIELDS - set(entry.keys())
    if missing:
        raise ValueError(
            f"Residual entry missing required fields: {sorted(missing)}"
        )

    Path(path).parent.mkdir(parents=True, exist_ok=True)

    with open(path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, separators=(",", ":")) + "\n")

    return entry


def read_window(path: str, limit: int = 500) -> list[dict]:
    """
    Read the most recent `limit` entries from the store file.
    Returns a list of dicts, most recent last.
    Returns [] if the file does not exist.

    §48: Zone 5 calls this. Zone 2 never reads.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            lines = [l.rstrip("\n") for l in f if l.strip()]
    except FileNotFoundError:
        return []

    tail = lines[-limit:] if len(lines) > limit else lines

    entries: list[dict] = []
    for line in tail:
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError:
            pass   # skip malformed lines — do not raise
    return entries


def entry_count(path: str) -> int:
    """
    Return the total number of entries in the store file.
    Reads line count only — does not parse JSON.
    Returns 0 if the file does not exist.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            return sum(1 for line in f if line.strip())
    except FileNotFoundError:
        return 0
