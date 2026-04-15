"""
historical_context_service.py — Zone 3, the sole memory gateway.

CTM-PFSD Code Governance Spec v1.6 §3c
SovereignClaw spec §15.1, §16, §19
CTM_PFSD_MVP_Implementation_Spec_v3_5.md §15

"HistoricalContextService — The Sole Memory Gateway"

Queries MemoryGraph and ResidualStore to provide governed
context during execution. Pure function — no persistent
internal state. Deterministic: same inputs → same outputs (§32).

simulation_safe: true
No S→S calls.

§48 boundary: stdlib only (json, hashlib, os, collections)
              + config + residual_store (read_window only).
              Reads MEMORY_GRAPH_PATH from disk directly.
              Does NOT import memory_graph_builder.
"""
from __future__ import annotations

import json
import os
from collections import Counter
from typing import Optional

import config
from residual_store import read_window


class HistoricalContextService:
    """
    The sole memory gateway at execution time.
    query() never raises — on any error returns a safe empty delta.
    """

    def query(
        self,
        query_type:         str,
        horizon_ticks:      int,
        residual_window_id: str,
        memory_graph_hash:  str,
    ) -> dict:
        """
        Dispatch to the appropriate query handler.
        Returns a state_delta-compatible dict.
        Never raises.
        """
        try:
            if query_type == "pattern":
                return self._query_pattern(
                    horizon_ticks, residual_window_id, memory_graph_hash)
            if query_type == "session":
                return self._query_session(
                    horizon_ticks, residual_window_id, memory_graph_hash)
            if query_type == "episode":
                return self._query_episode(
                    horizon_ticks, residual_window_id, memory_graph_hash)
            if query_type == "concept":
                return self._query_concept(
                    horizon_ticks, residual_window_id, memory_graph_hash)
            return {
                "query_type": query_type,
                "error":      f"Unknown query_type: {query_type}",
                "confidence": 0.0,
            }
        except Exception as e:
            return {
                "query_type": query_type,
                "error":      str(e),
                "confidence": 0.0,
            }

    # ── Pattern query ─────────────────────────────────────────────────────

    def _query_pattern(
        self,
        horizon_ticks:      int,
        residual_window_id: str,
        memory_graph_hash:  str,
    ) -> dict:
        memory_graph  = self._load_memory_graph()
        concept_nodes = memory_graph.get("concept_nodes", [])
        active_nodes  = [n for n in concept_nodes
                         if n.get("status") == "ACTIVE"]

        weights = [n.get("weight", 0.0) for n in active_nodes]
        confidence = sum(weights) / len(weights) if weights else 0.0

        entries = read_window(
            config.RESIDUAL_STORE_PATH,
            limit=max(horizon_ticks, 1),
        )
        oldest_tick = min((e.get("tick", 0) for e in entries), default=0)

        return {
            "query_type":           "pattern",
            "relevant_patterns":    [n["concept_id"] for n in active_nodes],
            "concept_clusters":     [n["concept_id"] for n in active_nodes],
            "confidence":           round(float(confidence), 6),
            "oldest_residual_tick": oldest_tick,
            "memory_graph_hash":    memory_graph.get("construction_hash"),
        }

    # ── Session query ─────────────────────────────────────────────────────

    def _query_session(
        self,
        horizon_ticks:      int,
        residual_window_id: str,
        memory_graph_hash:  str,
    ) -> dict:
        limit   = max(horizon_ticks, 1) if horizon_ticks else 200
        entries = read_window(config.RESIDUAL_STORE_PATH, limit=limit)

        execute_entries = [e for e in entries
                           if e.get("source_type") == "EXECUTE"]
        total   = len(execute_entries)
        failed  = sum(1 for e in execute_entries
                      if e.get("outcome") == config.OUTCOME_FAILED)
        error_rate = round(failed / total, 6) if total else 0.0

        svc_counter: Counter = Counter(
            e.get("service", "") for e in execute_entries if e.get("service")
        )
        most_frequent: Optional[str] = (
            svc_counter.most_common(1)[0][0] if svc_counter else None
        )
        session_start = min(
            (e.get("tick", 0) for e in execute_entries), default=0
        )

        return {
            "query_type":            "session",
            "commands_this_session": total,
            "error_rate":            error_rate,
            "most_frequent_service": most_frequent,
            "session_start_tick":    session_start,
        }

    # ── Episode query ─────────────────────────────────────────────────────

    def _query_episode(
        self,
        horizon_ticks:      int,
        residual_window_id: str,
        memory_graph_hash:  str,
    ) -> dict:
        limit   = max(horizon_ticks, 1) if horizon_ticks else 200
        entries = read_window(config.RESIDUAL_STORE_PATH, limit=limit)

        execute_entries = [e for e in entries
                           if e.get("source_type") == "EXECUTE"]
        total    = len(execute_entries)
        failed   = sum(1 for e in execute_entries
                       if e.get("outcome") == config.OUTCOME_FAILED)
        services = len({e.get("service", "") for e in execute_entries
                        if e.get("service")})
        oldest   = min((e.get("tick", 0) for e in execute_entries), default=0)
        success_rate = round((total - failed) / total, 6) if total else 0.0

        summary = (
            f"{total} executions, {services} services, "
            f"{failed} failures in last {limit} ticks"
        )

        return {
            "query_type":           "episode",
            "episode_summary":      summary,
            "confidence":           success_rate,
            "oldest_residual_tick": oldest,
        }

    # ── Concept query ─────────────────────────────────────────────────────

    def _query_concept(
        self,
        horizon_ticks:      int,
        residual_window_id: str,
        memory_graph_hash:  str,
    ) -> dict:
        """residual_window_id is used as concept_id to look up."""
        memory_graph  = self._load_memory_graph()
        concept_nodes = memory_graph.get("concept_nodes", [])

        for node in concept_nodes:
            if node.get("concept_id") == residual_window_id:
                return {
                    "query_type": "concept",
                    "concept_id": node["concept_id"],
                    "services":   node.get("services", []),
                    "weight":     node.get("weight", 0.0),
                    "status":     node.get("status"),
                    "found":      True,
                }

        return {
            "query_type": "concept",
            "concept_id": None,
            "services":   [],
            "weight":     0.0,
            "status":     None,
            "found":      False,
        }

    # ── Helpers ───────────────────────────────────────────────────────────

    def _load_memory_graph(self) -> dict:
        """
        Load MemoryGraph from disk. Returns {} if absent or malformed.
        Never raises.
        """
        path = config.MEMORY_GRAPH_PATH
        try:
            if not os.path.exists(path):
                return {}
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            if isinstance(data, dict):
                return data
            return {}
        except (json.JSONDecodeError, OSError):
            return {}
