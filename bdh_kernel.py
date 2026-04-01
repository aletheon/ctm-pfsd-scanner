"""
BDH Kernel (§13) — Behaviour-Dependency Hebbian kernel.

Tracks service-pair coupling weights derived from residual co-activation
patterns across intent lifecycles.

§48 boundary invariant: this file imports nothing from runtime/, services/,
policies/, or interception/ — only config and stdlib.

Hebbian update rule (§13.1):
  Δw = BDH_LEARNING_RATE × a_i × a_j × outcome_signal
  a_i = a_j = 1.0  (both services active in the same intent lifecycle)
  outcome_signal: +1.0 SUCCESS, -1.0 FAILED, 0.0 neutral (REJECTED/EXPIRED)
  Decay: |w| × (1 - BDH_DECAY_RATE)^tick_gap applied before each update.
  Clamp: coupling_weight ∈ [-1.0, +1.0]

Status transitions (§13.5):
  FORMING  → STABLE     weight > BDH_STABLE_WEIGHT_THRESHOLD
                        AND observations >= BDH_STABLE_MIN_OBSERVATIONS
  STABLE   → ELEVATED   via BDHKernel.elevate(pathway_id) — called by CTM
  Any      → PRUNED     |weight| < BDH_PRUNE_WEIGHT_THRESHOLD
                        AND ticks_since_last_reinforced > BDH_PRUNE_TICK_WINDOW
                        AND status not in (ELEVATED, FORMALISED)
  FORMALISED            exempt from pruning permanently
"""

from __future__ import annotations

import uuid
from dataclasses import dataclass, field

import config


# ── PathwayEntry (§13) ────────────────────────────────────────────────────────

@dataclass
class PathwayEntry:
    pathway_id:        str
    service_i:         str
    service_j:         str
    intent_context:    str    # owner_policy of the shared lifecycle
    coupling_weight:   float  # Hebbian weight, clamped to [-1.0, +1.0]
    observations:      int
    positive_outcomes: int
    last_reinforced:   int    # tick index of last Hebbian update
    ticks_below_prune: int    # accumulated ticks spent below prune threshold
    status:            str    # FORMING|STABLE|DECAYING|ELEVATED|FORMALISED|PRUNED


# ── Helpers ───────────────────────────────────────────────────────────────────

def _make_pathway_id(service_i: str, service_j: str, intent_context: str) -> str:
    """UUID v5 — canonical (sorted) service pair + intent_context."""
    s_a, s_b = sorted([service_i, service_j])
    ns = uuid.UUID(config.BDH_PATHWAY_NAMESPACE)
    return str(uuid.uuid5(ns, f"{s_a}|{s_b}|{intent_context}"))


# ── BDHKernel ─────────────────────────────────────────────────────────────────

class BDHKernel:
    """
    Hebbian service-pair coupling tracker (§13).

    §48 boundary: imports only config and stdlib — no runtime engine classes.
    """

    def __init__(self):
        self._pathways: dict[str, PathwayEntry] = {}

    # ── Public API ────────────────────────────────────────────────────────

    def update(self, residuals: list) -> list:
        """
        Process residuals grouped by intent_id (lifecycle).
        For each lifecycle, update or create PathwayEntries for all
        service pairs, apply decay, and apply status transitions.
        Returns all non-PRUNED pathways.
        """
        # Group by intent_id (each group = one lifecycle)
        by_intent: dict[str, list] = {}
        for r in residuals:
            by_intent.setdefault(r.intent_id, []).append(r)

        for group in by_intent.values():
            self._process_lifecycle(group)

        return [p for p in self._pathways.values()
                if p.status != config.BDH_STATUS_PRUNED]

    def get_stable_pathways(self) -> list:
        """Return pathways with status STABLE or ELEVATED."""
        return [
            p for p in self._pathways.values()
            if p.status in (config.BDH_STATUS_STABLE, config.BDH_STATUS_ELEVATED)
        ]

    def elevate(self, pathway_id: str) -> None:
        """CTM marks a pathway ELEVATED (§13.5)."""
        if pathway_id in self._pathways:
            self._pathways[pathway_id].status = config.BDH_STATUS_ELEVATED

    # ── Private ───────────────────────────────────────────────────────────

    def _process_lifecycle(self, group: list) -> None:
        """Apply one Hebbian update per unique service pair in this lifecycle."""
        # Determine outcome signal for the whole lifecycle
        outcomes = {r.outcome for r in group}
        if config.OUTCOME_SUCCESS in outcomes:
            signal = 1.0
        elif config.OUTCOME_FAILED in outcomes:
            signal = -1.0
        else:
            signal = 0.0

        current_tick   = max(r.tick for r in group)
        intent_context = group[0].owner_policy

        # Unique services present — sorted for deterministic pair ordering
        services = sorted({r.service for r in group})

        for i in range(len(services)):
            for j in range(i + 1, len(services)):
                s_i, s_j = services[i], services[j]
                pid = _make_pathway_id(s_i, s_j, intent_context)
                self._update_pathway(pid, s_i, s_j, intent_context,
                                     current_tick, signal)

    def _update_pathway(
        self,
        pid:            str,
        s_i:            str,
        s_j:            str,
        intent_context: str,
        current_tick:   int,
        signal:         float,
    ) -> None:
        if pid not in self._pathways:
            self._pathways[pid] = PathwayEntry(
                pathway_id        = pid,
                service_i         = s_i,
                service_j         = s_j,
                intent_context    = intent_context,
                coupling_weight   = 0.0,
                observations      = 0,
                positive_outcomes = 0,
                last_reinforced   = current_tick,
                ticks_below_prune = 0,
                status            = config.BDH_STATUS_FORMING,
            )

        p = self._pathways[pid]

        # Apply decay since last reinforcement (§13.1)
        tick_gap = current_tick - p.last_reinforced
        if tick_gap > 0:
            p.coupling_weight *= (1 - config.BDH_DECAY_RATE) ** tick_gap

        # Hebbian update: Δw = η × 1.0 × 1.0 × signal
        delta = config.BDH_LEARNING_RATE * 1.0 * 1.0 * signal
        p.coupling_weight = max(-1.0, min(1.0, p.coupling_weight + delta))

        p.observations += 1
        if signal > 0.0:
            p.positive_outcomes += 1
        p.last_reinforced = current_tick

        # Status transitions (§13.5)
        self._transition_status(p, current_tick)

    def persist(self, path: str) -> None:
        """Write pathway registry to JSON."""
        import os
        import json
        import dataclasses
        os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
        with open(path, "w") as f:
            json.dump([dataclasses.asdict(p) for p in self._pathways.values()], f, indent=2)

    def load(self, path: str) -> None:
        """Load pathway registry from JSON."""
        import json
        with open(path) as f:
            data = json.load(f)
        self._pathways = {}
        for d in data:
            p = PathwayEntry(**d)
            self._pathways[p.pathway_id] = p

    def _transition_status(self, p: PathwayEntry, current_tick: int) -> None:
        # FORMING → STABLE
        if p.status == config.BDH_STATUS_FORMING:
            if (p.coupling_weight > config.BDH_STABLE_WEIGHT_THRESHOLD
                    and p.observations >= config.BDH_STABLE_MIN_OBSERVATIONS):
                p.status = config.BDH_STATUS_STABLE
                return

        # PRUNED (exempt: ELEVATED, FORMALISED)
        if p.status not in (config.BDH_STATUS_ELEVATED, config.BDH_STATUS_FORMALISED):
            ticks_since = current_tick - p.last_reinforced
            if (abs(p.coupling_weight) < config.BDH_PRUNE_WEIGHT_THRESHOLD
                    and ticks_since > config.BDH_PRUNE_TICK_WINDOW):
                p.status = config.BDH_STATUS_PRUNED
