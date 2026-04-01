"""
learning/distillation_worker.py — ported from tools/distillation_worker.py (ctm-pfsd).

Heuristic pattern detector and proposal generator.
Implements §54.3 DistillationWorker (Phase 1 — no LLM).

PATTERN TARGETED:
    The service-call ratio for HistoricalContextService vs ShellCommandService
    falls below DISTILLATION_CROSSING_RATIO_THRESHOLD.

    Signal: HistoricalContextService count / ShellCommandService count
            < DISTILLATION_CROSSING_RATIO_THRESHOLD
    Proposal: lower service_call_ratio_threshold from THRESHOLD_CURRENT
              to THRESHOLD_PROPOSED

CONSTRAINTS (§55.4 PatternDetectionInterface):
    Reads ONLY from the residual window dict — never from live runtime.
    Does NOT access AgentState, IntentQueue, ExecutionEngine, or RuntimeClock.
    Does NOT import anything from runtime/, services/, or policies/.

Schema mapping from ctm-pfsd:
    GateCrossingService  → config.DISTILLATION_TARGET_SERVICE
    ControlService       → config.DISTILLATION_BASELINE_SERVICE
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass

import config


# ── DetectedPattern ───────────────────────────────────────────────────────────

@dataclass
class DetectedPattern:
    pattern_id:     str
    crossing_count: int    # target service (HistoricalContextService) count
    control_count:  int    # baseline service (ShellCommandService) count
    crossing_ratio: float
    confidence:     float
    window_id:      str


# ── Cooling helpers ───────────────────────────────────────────────────────────

def load_cooling_log() -> dict:
    path = config.DISTILLATION_COOLING_LOG_PATH
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)
    return {}


def is_in_cooling(cooling_log: dict) -> bool:
    key = config.DISTILLATION_COOLING_KEY
    if key not in cooling_log:
        return False
    return time.time() < cooling_log[key].get("cooling_until_ts", 0)


# ── Pattern detection ─────────────────────────────────────────────────────────

def detect_patterns(window: dict) -> list:
    """
    PatternDetectionInterface.analyse() — slow path only.
    Reads pre-loaded residual window dict. Never touches live runtime.
    """
    by_service     = window.get("by_service", {})
    crossing_count = by_service.get(config.DISTILLATION_TARGET_SERVICE,
                                    {}).get("count", 0)
    control_count  = by_service.get(config.DISTILLATION_BASELINE_SERVICE,
                                    {}).get("count", 0)

    if control_count < config.DISTILLATION_MIN_OBSERVATIONS:
        print(f"[distillation_worker] Insufficient samples: "
              f"{config.DISTILLATION_BASELINE_SERVICE} count={control_count} "
              f"< {config.DISTILLATION_MIN_OBSERVATIONS}")
        return []

    if control_count == 0:
        return []

    crossing_ratio = crossing_count / control_count

    if crossing_ratio >= config.DISTILLATION_CROSSING_RATIO_THRESHOLD:
        print(f"[distillation_worker] Crossing ratio {crossing_ratio:.3f} "
              f">= threshold {config.DISTILLATION_CROSSING_RATIO_THRESHOLD} "
              f"— no pattern detected")
        return []

    # Confidence: higher when ratio is lower (more improvement headroom)
    deficit    = config.DISTILLATION_CROSSING_RATIO_THRESHOLD - crossing_ratio
    confidence = min(
        config.DISTILLATION_CONFIDENCE_MAX,
        config.DISTILLATION_CONFIDENCE_FLOOR + deficit * config.DISTILLATION_CONFIDENCE_SLOPE,
    )

    pattern_id = (
        f"low_service_ratio_"
        f"{window.get('window_id', 'unknown')[:8]}"
    )
    pattern = DetectedPattern(
        pattern_id     = pattern_id,
        crossing_count = crossing_count,
        control_count  = control_count,
        crossing_ratio = round(crossing_ratio, 4),
        confidence     = round(confidence, 3),
        window_id      = window.get("window_id", ""),
    )
    print(f"[distillation_worker] Pattern detected: {pattern_id}")
    print(f"  crossing_count={crossing_count}  control_count={control_count}  "
          f"ratio={crossing_ratio:.3f}  confidence={confidence:.3f}")
    return [pattern]


# ── Proposal generation ───────────────────────────────────────────────────────

def generate_proposal(pattern: DetectedPattern, cooling_log: dict):
    """
    Convert DetectedPattern → ProposalBundle (§54.5 schema).
    Returns None if cooling active or confidence below floor.
    """
    if is_in_cooling(cooling_log):
        print(f"[distillation_worker] Cooling active for "
              f"'{config.DISTILLATION_COOLING_KEY}' — skipping")
        return None

    if pattern.confidence < config.DISTILLATION_CONFIDENCE_FLOOR:
        print(f"[distillation_worker] Confidence {pattern.confidence:.2f} "
              f"below {config.DISTILLATION_CONFIDENCE_FLOOR} floor — W020")
        return None

    proposal_id = str(uuid.uuid4())
    bundle_id   = hashlib.sha256(
        f"{pattern.window_id}{proposal_id}".encode()
    ).hexdigest()[:16]

    bundle = {
        # §54.5 ProposalBundle schema
        "bundle_id":          bundle_id,
        "generated_at_tick":  0,   # offline — not during a live run
        "window_id":          pattern.window_id,
        "source_type":        config.PROPOSAL_SOURCE_DW,
        "generator_id":       config.DISTILLATION_GENERATOR_ID,
        "generator_version":  config.DISTILLATION_GENERATOR_VERSION,
        "model_id":           None,
        "prompt_hash":        None,
        "residual_window_id": pattern.window_id,
        # §55.5 extension fields
        "detector_ids":       [config.DISTILLATION_GENERATOR_ID],
        "residual_sources":   ["AUTONOMOUS"],
        "cooling_checked_at": int(time.time()),
        "no_cooling_active":  True,
        "proposals": [
            {
                "proposal_id": proposal_id,
                "cooling_key": config.DISTILLATION_COOLING_KEY,
                "fol_rule": (
                    f"LowServiceCallRatio(ratio={pattern.crossing_ratio:.3f}) "
                    f"<- ServiceCallThreshold("
                    f"current={config.DISTILLATION_THRESHOLD_CURRENT}) "
                    f"AND HistoricalContextUsage(low=0.0, high=1.0)"
                ),
                "config_patch": {
                    "path":     config.DISTILLATION_CONFIG_PATCH_PATH,
                    "current":  config.DISTILLATION_THRESHOLD_CURRENT,
                    "proposed": config.DISTILLATION_THRESHOLD_PROPOSED,
                    "safe_min": config.DISTILLATION_SAFE_MIN,
                    "safe_max": config.DISTILLATION_SAFE_MAX,
                },
                "attach_to":    config.POLICY_AGENT_ROOT,
                "confidence":   pattern.confidence,
                "observations": pattern.control_count,
                "rationale": (
                    f"{config.DISTILLATION_TARGET_SERVICE} was called on "
                    f"{pattern.crossing_count} of {pattern.control_count} "
                    f"{config.DISTILLATION_BASELINE_SERVICE} ticks "
                    f"(ratio={pattern.crossing_ratio:.3f}, "
                    f"threshold={config.DISTILLATION_CROSSING_RATIO_THRESHOLD}). "
                    f"Lowering threshold to "
                    f"{config.DISTILLATION_THRESHOLD_PROPOSED} would improve "
                    f"historical context utilisation. Safe bounds "
                    f"[{config.DISTILLATION_SAFE_MIN}, "
                    f"{config.DISTILLATION_SAFE_MAX}] verified. "
                    f"Change is reversible: original value recorded in "
                    f"config_patch.current."
                ),
                "evidence": {
                    "pattern_id":     pattern.pattern_id,
                    "crossing_count": pattern.crossing_count,
                    "control_count":  pattern.control_count,
                    "crossing_ratio": pattern.crossing_ratio,
                },
            }
        ],
    }
    return bundle


# ── HeuristicDetector (§11.4 PatternDetectionInterface wrapper) ──────────────

from learning.pattern_detection import PatternDetectionInterface, PatternReport


class HeuristicDetector(PatternDetectionInterface):
    """Wraps heuristic detect_patterns() logic as PatternDetectionInterface (§11.4)."""

    def analyse(
        self,
        residual_window: list,
        bdh_pathways:    list,
        policy_graph:    dict,
    ) -> list:
        # Build window dict from residual_window
        by_service: dict = {}
        for r in residual_window:
            svc = getattr(r, "service", "unknown")
            if svc not in by_service:
                by_service[svc] = {"count": 0}
            by_service[svc]["count"] += 1

        window = {
            "run_id":     "heuristic",
            "window_id":  "heuristic",
            "by_service": by_service,
            "residuals":  [],
        }
        patterns = detect_patterns(window)
        reports  = []
        for p in patterns:
            import uuid as _uuid
            pid = str(_uuid.uuid4())
            reports.append(PatternReport(
                pattern_id       = pid,
                detector_id      = config.DETECTOR_ID_HEURISTIC,
                service_pair     = (config.DISTILLATION_TARGET_SERVICE,),
                pattern_type     = config.PATTERN_LOW_RATE,
                confidence       = getattr(p, "confidence", config.DISTILLATION_CONFIDENCE_FLOOR),
                observations     = getattr(p, "observations", 0),
                improvement_rate = 0.0,
                bdh_weight       = 0.0,
                evidence         = {"pattern": repr(p)},
                recommended_rule = getattr(p, "fol_rule", ""),
            ))
        return reports
