"""
graph_store.py — Live compiled PolicyGraph store.

Holds the currently active compiled graph in memory.
Supports hot-reload: calling load() replaces the current graph atomically.
History is NOT kept here — the PIC Chain (pic_chain.py) is the immutable ledger.

This store does NOT compile. It does NOT validate source.
It accepts a pre-compiled, pre-validated CompileResult dict
(output of PolicyCompiler.compile() where is_valid_ctm_graph == True).

§48 boundary: stdlib only (threading, time).
Zone 4 purity: no imports from server.py, scaffold_generator,
               gap_classifier, or any Zone 3 file.
"""
from __future__ import annotations

import threading
import time
from typing import Optional


class GraphStore:
    """
    Thread-safe in-memory store for exactly one compiled PolicyGraph.
    Hot-reload replaces the current graph atomically under an RLock.
    """

    def __init__(self) -> None:
        self._graph:      Optional[dict] = None
        self._loaded_at:  Optional[int]  = None
        self._load_count: int             = 0
        self._lock:       threading.RLock = threading.RLock()

    # ── Public API ─────────────────────────────────────────────────────────

    def load(self, compiled_graph: dict) -> None:
        """
        Validate and store a compiled graph.

        Raises ValueError if:
          - is_valid_ctm_graph is not True
          - "graph" key is absent
          - graph["graph_hash"] is absent or does not start with "sha256:"
        """
        if not compiled_graph.get("is_valid_ctm_graph"):
            raise ValueError("Cannot load graph with is_valid_ctm_graph: false")

        inner = compiled_graph.get("graph")
        if inner is None:
            raise ValueError("compiled_graph missing valid graph_hash")

        graph_hash = inner.get("graph_hash", "")
        if not isinstance(graph_hash, str) or not graph_hash.startswith("sha256:"):
            raise ValueError("compiled_graph missing valid graph_hash")

        with self._lock:
            self._graph      = compiled_graph
            self._loaded_at  = int(time.time())
            self._load_count += 1

    def get(self) -> Optional[dict]:
        """Return the current compiled graph dict, or None."""
        with self._lock:
            return self._graph

    def get_graph_hash(self) -> Optional[str]:
        """Return the graph_hash of the loaded graph, or None."""
        with self._lock:
            if self._graph is None:
                return None
            return self._graph.get("graph", {}).get("graph_hash")

    def is_loaded(self) -> bool:
        """True if a compiled graph is currently held."""
        with self._lock:
            return self._graph is not None

    def status(self) -> dict:
        """
        Return a status snapshot:
        {
          "is_loaded":   bool,
          "graph_hash":  str | null,
          "namespace":   str | null,
          "loaded_at":   int | null,
          "load_count":  int,
          "node_counts": {"policies": int, "rules": int, "services": int} | null,
        }
        """
        with self._lock:
            if self._graph is None:
                return {
                    "is_loaded":   False,
                    "graph_hash":  None,
                    "namespace":   None,
                    "loaded_at":   None,
                    "load_count":  self._load_count,
                    "node_counts": None,
                }

            inner = self._graph.get("graph") or {}
            nodes = inner.get("nodes") or []

            node_counts = {
                "policies": sum(1 for n in nodes if n.get("type") == "P"),
                "rules":    sum(1 for n in nodes if n.get("type") == "R"),
                "services": sum(1 for n in nodes if n.get("type") == "S"),
            }

            return {
                "is_loaded":   True,
                "graph_hash":  inner.get("graph_hash"),
                "namespace":   inner.get("namespace"),
                "loaded_at":   self._loaded_at,
                "load_count":  self._load_count,
                "node_counts": node_counts,
            }
