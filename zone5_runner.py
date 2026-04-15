"""
zone5_runner.py — Zone 5 distillation runner for CTM-PFSD scanner.

Reads ResidualStore, updates BDH pathway registry, persists to bdh_store.
Called between sessions by operators via POST /run-distillation.
NEVER called during a request-response cycle.

Zone 5 rules:
  - Reads ResidualStore — never writes to it
  - Writes bdh_store — never reads from server.py
  - Never imports from server.py or any Zone 3 file
  - Never modifies the PolicyGraph
  - Produces no side effects except bdh_store.save()

§48 boundary: imports only config, stdlib, residual_store,
              bdh_kernel, bdh_store.
"""
from __future__ import annotations

import time
from dataclasses import dataclass

import config
from residual_store import read_window
from bdh_kernel     import BDHKernel, PathwayEntry
from bdh_store      import load as bdh_load, save as bdh_save


@dataclass
class _Residual:
    """Minimal residual object satisfying BDHKernel.update() contract."""
    intent_id:    str
    service:      str
    owner_policy: str
    outcome:      str
    tick:         int


class Zone5Runner:
    """
    Zone 5 distillation runner.

    run(session_id) executes the full 7-step distillation sequence
    and returns a result dict. Idempotent — safe to call repeatedly.
    """

    def run(self, session_id: str) -> dict:
        t_start = time.monotonic()

        # Step 1 — Load existing BDH pathway state
        existing = bdh_load(config.BDH_STORE_PATH)

        # Step 2 — Read residuals from ResidualStore
        limit   = config.DISTILLATION_MIN_OBSERVATIONS * 100
        entries = read_window(config.RESIDUAL_STORE_PATH, limit=limit)
        residuals_read = len(entries)

        # Filter: exclude entries where exclude_from_learning is True,
        # or source_type == "SCAN" (scan residuals are structural observations,
        # not governed execution outcomes — BDH learns from EXECUTE/APPROVE only)
        scan_excluded = sum(1 for e in entries
                            if e.get("source_type") == "SCAN")
        eligible = [
            e for e in entries
            if not e.get("exclude_from_learning")
            and e.get("source_type") != "SCAN"
        ]

        # Step 3 — Build residual objects for BDH
        residuals = [
            _Residual(
                intent_id    = e["intent_id"],
                service      = e["service"],
                owner_policy = e.get("policy_id") or e["project_name"],
                outcome      = e["outcome"],
                tick         = e["tick"],
            )
            for e in eligible
        ]

        # Step 4 — Restore existing BDH state into a fresh kernel
        bdh = BDHKernel()
        for pid, e in existing.items():
            bdh._pathways[pid] = PathwayEntry(
                pathway_id        = pid,
                service_i         = e["service_i"],
                service_j         = e["service_j"],
                intent_context    = e.get("context_label", ""),
                coupling_weight   = e["coupling_weight"],
                observations      = e["observations"],
                positive_outcomes = 0,
                last_reinforced   = 0,
                ticks_below_prune = 0,
                status            = e["status"],
            )

        # Step 5 — Update BDH with new residuals
        bdh.update(residuals)

        # Step 6 — Save updated pathway registry
        updated = {
            pid: {
                "pathway_id":      p.pathway_id,
                "service_i":       p.service_i,
                "service_j":       p.service_j,
                "context_label":   p.intent_context,
                "coupling_weight": p.coupling_weight,
                "observations":    p.observations,
                "status":          p.status,
            }
            for pid, p in bdh._pathways.items()
            if p.status != config.BDH_STATUS_PRUNED
        }
        bdh_save(updated, config.BDH_STORE_PATH)

        # Step 7 — Return result dict
        return {
            "session_id":              session_id,
            "residuals_read":          residuals_read,
            "residuals_scan_excluded": scan_excluded,
            "residuals_used":          len(residuals),
            "pathways_before":  len(existing),
            "pathways_after":   len(updated),
            "stable_count":     sum(
                1 for p in updated.values()
                if p["status"] == config.BDH_STATUS_STABLE
            ),
            "elevated_count":   sum(
                1 for p in updated.values()
                if p["status"] == config.BDH_STATUS_ELEVATED
            ),
            "formalised_count": sum(
                1 for p in updated.values()
                if p["status"] == config.BDH_STATUS_FORMALISED
            ),
            "run_duration_ms":  int((time.monotonic() - t_start) * 1000),
        }
