"""
bdh_store.py — Persisted BDH pathway registry.

Written by distillation_runner.py (Zone 5) between sessions.
Read by server.py (Zone 2) as a read-only snapshot.
Zone 2 NEVER writes here. Zone 5 NEVER reads from server.py.

§48 boundary: stdlib only (json, os, pathlib).
"""
from __future__ import annotations

import json
import os
from pathlib import Path


def load(path: str) -> dict:
    """
    Read the persisted pathway registry from a JSON file.

    Returns a dict of { pathway_id: entry_dict }.
    Returns {} if the file is absent or malformed — never raises.
    """
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        if isinstance(data, dict):
            return data
        return {}
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return {}


def save(pathways: dict, path: str) -> None:
    """
    Atomically write the pathway registry to a JSON file.

    Writes to path+".tmp" then os.replace(tmp, path) to ensure
    readers never see a partial write.

    pathways must be a dict of { pathway_id: entry_dict }.
    """
    tmp = path + ".tmp"
    Path(tmp).parent.mkdir(parents=True, exist_ok=True)
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(pathways, f, separators=(",", ":"))
    os.replace(tmp, path)
