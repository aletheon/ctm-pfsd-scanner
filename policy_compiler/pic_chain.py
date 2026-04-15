"""
policy_compiler.pic_chain — Append-only PIC Chain ledger.

File format: newline-delimited JSON (.jsonl).
One entry per line. Never modify or delete existing entries.
Create file and parent directories if they do not exist.

§48 boundary: stdlib only (json, uuid, os, pathlib, datetime).
"""
from __future__ import annotations
import json
import uuid
import os
from pathlib import Path
from datetime import datetime, timezone


# ── Public API ─────────────────────────────────────────────────────────────

def get_head_hash(pic_chain_path: str) -> str:
    """
    Returns the graph_hash of the last entry in the chain.
    Returns "sha256:genesis" if the file does not exist or is empty.
    Reads only a tail chunk — does not load the entire file.
    """
    try:
        with open(pic_chain_path, "rb") as f:
            f.seek(0, 2)                  # seek to end
            size = f.tell()
            if size == 0:
                return "sha256:genesis"

            # Read the last ≤8 KB chunk and find the last non-empty line.
            chunk_size = min(8192, size)
            f.seek(size - chunk_size)
            chunk = f.read(chunk_size)

            # Split on newlines; take the last non-empty stripped line.
            for raw_line in reversed(chunk.split(b"\n")):
                line = raw_line.strip()
                if line:
                    entry = json.loads(line)
                    return entry.get("graph_hash", "sha256:genesis")

    except (FileNotFoundError, json.JSONDecodeError, OSError, ValueError):
        pass

    return "sha256:genesis"


def append_entry(serialised_graph: dict,
                 project_name: str,
                 pic_chain_path: str) -> dict:
    """
    Builds a PIC Chain entry from serialised_graph, appends it to the
    chain file, and returns the entry dict.

    Creates the file and any parent directories if needed.
    """
    prev_hash = get_head_hash(pic_chain_path)

    meta      = serialised_graph.get("compiler_metadata") or {}
    nodes     = serialised_graph.get("nodes") or []

    pic_id = "PIC-" + uuid.uuid4().hex[:12]
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")

    entry: dict = {
        "pic_id":               pic_id,
        "timestamp":            timestamp,
        "mutation_type":        "COMPILE_GRAPH",
        "graph_hash":           serialised_graph.get("graph_hash", ""),
        "prev_hash":            prev_hash,
        "compiler_version":     meta.get("compiler_version", ""),
        "policy_graph_version": meta.get("policy_graph_version", ""),
        "project_name":         project_name,
        "namespace":            serialised_graph.get("namespace"),
        "node_counts": {
            "policies": sum(1 for n in nodes if n.get("type") == "P"),
            "rules":    sum(1 for n in nodes if n.get("type") == "R"),
            "services": sum(1 for n in nodes if n.get("type") == "S"),
        },
    }

    # Ensure parent directory exists.
    path = Path(pic_chain_path)
    path.parent.mkdir(parents=True, exist_ok=True)

    with open(pic_chain_path, "a", encoding="utf-8") as f:
        f.write(json.dumps(entry, separators=(",", ":")) + "\n")

    return entry


def verify_chain(pic_chain_path: str) -> dict:
    """
    Reads all entries and verifies prev_hash linkage throughout the chain.

    Returns:
      {
        "valid":       bool,
        "entry_count": int,
        "head_hash":   str,
        "errors":      [str],
      }
    If file does not exist or is empty: returns valid with entry_count 0.
    """
    _GENESIS = "sha256:genesis"

    try:
        with open(pic_chain_path, "r", encoding="utf-8") as f:
            lines = [l.strip() for l in f if l.strip()]
    except FileNotFoundError:
        return {"valid": True, "entry_count": 0,
                "head_hash": _GENESIS, "errors": []}

    if not lines:
        return {"valid": True, "entry_count": 0,
                "head_hash": _GENESIS, "errors": []}

    chain_errors: list[str] = []
    entries: list[dict] = []

    for i, line in enumerate(lines):
        try:
            entries.append(json.loads(line))
        except json.JSONDecodeError as exc:
            chain_errors.append(f"Line {i + 1}: JSON parse error — {exc}")

    # Verify linkage.
    for idx in range(1, len(entries)):
        expected = entries[idx - 1].get("graph_hash", "")
        actual   = entries[idx].get("prev_hash", "")
        if expected != actual:
            chain_errors.append(
                f"Entry {idx + 1} ('{entries[idx].get('pic_id', '?')}') "
                f"prev_hash '{actual}' != entry {idx} graph_hash '{expected}'"
            )

    head_hash = entries[-1].get("graph_hash", _GENESIS) if entries else _GENESIS

    return {
        "valid":       len(chain_errors) == 0,
        "entry_count": len(entries),
        "head_hash":   head_hash,
        "errors":      chain_errors,
    }
