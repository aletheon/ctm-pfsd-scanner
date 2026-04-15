"""
memory_graph_builder.py — Zone 5 offline MemoryGraph construction.

CTM-PFSD Code Governance Spec v1.6 §3c
SovereignClaw spec §15, §18

Reads ResidualStore + BDH pathway registry.
Clusters services into ConceptNodes based on BDH co-occurrence.
Produces a weighted ConceptNode graph persisted to MEMORY_GRAPH_PATH.

Three-tier memory architecture:
  Layer 1 — PolicyGraph    OPERATIONAL
  Layer 2 — MemoryGraph    THIS FILE — offline concept graph
  Layer 3 — ResidualStore  OPERATIONAL

Zone 5 rules (§48 boundary):
  Imports: stdlib only + config + residual_store + bdh_store.
  Never imports from server.py, Zone 3, or Zone 4 files.
  Never writes to ResidualStore.
  Only side effect: writes to MEMORY_GRAPH_PATH.
  Deterministic: same inputs → same construction_hash (§15.3 INVARIANT 3).
"""
from __future__ import annotations

import hashlib
import json
import os
import time
from collections import Counter
from datetime import datetime, timezone

import config
from residual_store import read_window
from bdh_store import load as bdh_load


# ── Union-Find ─────────────────────────────────────────────────────────────

class _UnionFind:
    def __init__(self) -> None:
        self._parent: dict[str, str] = {}

    def find(self, x: str) -> str:
        if x not in self._parent:
            self._parent[x] = x
        while self._parent[x] != x:
            self._parent[x] = self._parent[self._parent[x]]
            x = self._parent[x]
        return x

    def union(self, a: str, b: str) -> None:
        ra, rb = self.find(a), self.find(b)
        if ra != rb:
            self._parent[rb] = ra


# ── MemoryGraphBuilder ─────────────────────────────────────────────────────

class MemoryGraphBuilder:
    """
    Offline MemoryGraph construction between sessions.
    build() is deterministic — same inputs produce the same
    construction_hash (§15.3 INVARIANT 3).
    """

    _ACTIVE_STATUSES = frozenset(["STABLE", "ELEVATED", "FORMALISED"])
    _RESIDUAL_LIMIT  = 2000
    _ACTIVE_WEIGHT   = 0.3
    _ACTIVE_OBS      = 3

    def build(self, session_id: str) -> dict:
        """
        Build a MemoryGraph from ResidualStore + BDH pathway registry.
        Persists atomically to MEMORY_GRAPH_PATH.
        Returns the full MemoryGraph dict.
        """
        t_start = time.time()

        # ── Step 1 — Read ResidualStore ────────────────────────────────────
        entries = read_window(config.RESIDUAL_STORE_PATH,
                              limit=self._RESIDUAL_LIMIT)
        eligible = [
            e for e in entries
            if not e.get("exclude_from_learning")
            and e.get("source_type") != "SCAN"
        ]

        residual_window_id = hashlib.sha256(
            "".join(sorted(e["residual_id"] for e in eligible)).encode()
        ).hexdigest()[:16]

        # ── Step 2 — Read BDH pathway registry ────────────────────────────
        all_pathways = bdh_load(config.BDH_STORE_PATH)
        pathways = {
            pid: entry
            for pid, entry in all_pathways.items()
            if entry.get("status") in self._ACTIVE_STATUSES
        }

        bdh_snapshot_hash = hashlib.sha256(
            "".join(sorted(pathways.keys())).encode()
        ).hexdigest()[:16]

        # ── Step 3 — Build service co-occurrence index from residuals ──────
        # Group residuals by intent_id to find services that fired together.
        intent_services: dict[str, list[str]] = {}
        intent_policy:   dict[str, list[str]] = {}

        for e in eligible:
            iid     = e.get("intent_id", "")
            svc     = e.get("service", "")
            pol     = e.get("policy_id") or ""
            if iid and svc:
                intent_services.setdefault(iid, []).append(svc)
            if iid and pol:
                intent_policy.setdefault(iid, []).append(pol)

        # service → most common policy_id across all residuals
        service_policy_counts: dict[str, Counter] = {}
        for e in eligible:
            svc = e.get("service", "")
            pol = e.get("policy_id") or ""
            if svc:
                service_policy_counts.setdefault(svc, Counter())
                if pol:
                    service_policy_counts[svc][pol] += 1

        service_policy_map: dict[str, str] = {
            svc: counter.most_common(1)[0][0]
            for svc, counter in service_policy_counts.items()
            if counter
        }

        # ── Step 4 — Cluster into ConceptNodes via union-find ──────────────
        uf = _UnionFind()
        # Each service is its own element; merge via pathways
        for entry in pathways.values():
            si = entry.get("service_i", "")
            sj = entry.get("service_j", "")
            if si and sj:
                uf.union(si, sj)

        # Group pathway entries by their union-find root
        cluster_pathways: dict[str, list[dict]] = {}
        for pid, entry in pathways.items():
            si = entry.get("service_i", "")
            sj = entry.get("service_j", "")
            if not (si and sj):
                continue
            root = uf.find(si)
            cluster_pathways.setdefault(root, []).append(
                {"pid": pid, "entry": entry}
            )

        # Collect all services per cluster root
        cluster_services: dict[str, set[str]] = {}
        for entry in pathways.values():
            si = entry.get("service_i", "")
            sj = entry.get("service_j", "")
            if not (si and sj):
                continue
            root = uf.find(si)
            cluster_services.setdefault(root, set()).update([si, sj])

        # Build ConceptNode for each cluster
        concept_nodes: list[dict] = []

        for root, pw_items in cluster_pathways.items():
            services = sorted(cluster_services.get(root, set()))
            if not services:
                continue

            pathway_ids = [item["pid"] for item in pw_items]
            weights     = [item["entry"].get("coupling_weight", 0.0)
                           for item in pw_items]
            obs_list    = [item["entry"].get("observations", 0)
                           for item in pw_items]

            mean_weight       = sum(weights) / len(weights) if weights else 0.0
            total_obs         = sum(obs_list)

            # cluster_label: most common policy_id for services in cluster
            policy_counter: Counter = Counter()
            for svc in services:
                pol = service_policy_map.get(svc, "")
                if pol:
                    policy_counter[pol] += 1
            cluster_label = (policy_counter.most_common(1)[0][0]
                             if policy_counter else "")

            concept_id = hashlib.sha256(
                "".join(services).encode()
            ).hexdigest()[:16]

            status = (
                "ACTIVE"
                if mean_weight >= self._ACTIVE_WEIGHT
                and total_obs  >= self._ACTIVE_OBS
                else "WEAK"
            )

            concept_nodes.append({
                "concept_id":        concept_id,
                "services":          services,
                "pathway_ids":       pathway_ids,
                "weight":            round(mean_weight, 6),
                "observation_count": total_obs,
                "cluster_label":     cluster_label,
                "status":            status,
            })

        # Sort for determinism
        concept_nodes.sort(key=lambda n: n["concept_id"])

        # ── Step 5 — Compute construction_hash ────────────────────────────
        build_params = json.dumps({
            "residual_limit": self._RESIDUAL_LIMIT,
            "min_status":     sorted(self._ACTIVE_STATUSES),
            "active_weight":  self._ACTIVE_WEIGHT,
            "active_obs":     self._ACTIVE_OBS,
        }, sort_keys=True, separators=(",", ":"))

        construction_hash = hashlib.sha256(
            (residual_window_id + bdh_snapshot_hash + build_params).encode()
        ).hexdigest()

        # ── Step 6 — Assemble and persist ─────────────────────────────────
        build_duration_ms = int((time.time() - t_start) * 1000)

        memory_graph = {
            "schema_version":     "1.0",
            "construction_hash":  construction_hash,
            "built_at":           datetime.now(timezone.utc).strftime(
                                      "%Y-%m-%dT%H:%M:%SZ"),
            "residual_window_id": residual_window_id,
            "bdh_snapshot_hash":  bdh_snapshot_hash,
            "concept_nodes":      concept_nodes,
            "concept_count":      len(concept_nodes),
            "active_count":       sum(1 for n in concept_nodes
                                      if n["status"] == "ACTIVE"),
            "edge_count":         sum(len(n["pathway_ids"])
                                      for n in concept_nodes),
            "build_duration_ms":  build_duration_ms,
        }

        # Atomic write
        tmp = config.MEMORY_GRAPH_PATH + ".tmp"
        os.makedirs(os.path.dirname(tmp) or ".", exist_ok=True)
        with open(tmp, "w", encoding="utf-8") as f:
            json.dump(memory_graph, f, separators=(",", ":"))
        os.replace(tmp, config.MEMORY_GRAPH_PATH)

        return memory_graph
