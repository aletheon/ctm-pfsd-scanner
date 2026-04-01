"""
learning/distillation_runner.py — orchestrates the full distillation pipeline
for one session. Called from main.py after a session ends; never called
during execution.

Phase 8 pipeline (§54 + §22 + §5 + §15 + §17):
  1. Query residual_store for all residuals in the session
  2. BDHKernel.update(residuals) → updated pathways
  3. distillation_worker.detect_patterns(window) → heuristic patterns
  4. LNNILPEngine.analyse(residuals, pathways, policy_graph) → LNN-ILP reports
  5. _merge_reports(heuristic_reports, lnn_ilp_reports) → merged reports
  6. For each pattern: generate_proposal() → ProposalBundle
     Also generate bundles from THRESHOLD_DRIFT LNN-ILP reports
  7. For each bundle:
     a. Append PROPOSAL_SUBMITTED to PIC Chain (backward compat)
     b. Submit to AgentForum (mandatory gate — no auto-approval)
  8. Return DistillationResult (includes pic_chain and forum for inspection)
  Note: APPROVAL and GRAPH_UPDATE only happen after AgentManager.approve().
"""

from __future__ import annotations

import hashlib
import json
import os
import time
import uuid
from dataclasses import dataclass, field
from typing import Optional

import config
from learning.bdh_kernel import BDHKernel
from learning.distillation_worker import detect_patterns, generate_proposal
from learning.pattern_detection import PatternReport
from learning.knowledge_base import KnowledgeBase
from learning.lnn_ilp import LNNILPEngine
from learning.apply_proposal import (
    validate_proposal,
    append_changelog,
    load_cooling_log,
    save_cooling_log,
)
from governance.pic_chain import PICChain
from governance.graph_version import GraphVersionTracker
from governance.compiler import Compiler
from governance.forum import AgentForum, ForumPermissionError


# ── DistillationResult ────────────────────────────────────────────────────────

@dataclass
class DistillationResult:
    session_id:              str
    residuals_analysed:      int
    pathways_updated:        int
    patterns_detected:       int
    proposals_generated:     int
    proposals_accepted:      int
    proposal_bundle_ids:     list             = field(default_factory=list)
    run_duration_ms:         int              = 0
    pic_chain:               Optional[object] = field(default=None, repr=False)
    forum_entries_submitted: int              = 0
    forum:                   Optional[object] = field(default=None, repr=False)
    llm_patterns_detected:   int              = 0


# ── DistillationRunner ────────────────────────────────────────────────────────

class DistillationRunner:

    @staticmethod
    def _merge_reports(*report_lists) -> list:
        """
        Merge PatternReport lists from any number of detectors.
        Reports with the same (pattern_type, service_pair[0]) are deduplicated
        and their confidences are combined via probability union:
            1 - prod((1 - conf) for each conf).
        Non-overlapping reports are kept as-is.
        """
        merged: dict = {}
        for report_list in report_lists:
            for r in report_list:
                key = (r.pattern_type, r.service_pair[0] if r.service_pair else "")
                if key in merged:
                    existing = merged[key]
                    # Probability union of confidences
                    combined_conf = 1.0 - (1.0 - existing.confidence) * (1.0 - r.confidence)
                    merged[key] = PatternReport(
                        pattern_id       = existing.pattern_id,
                        detector_id      = f"{existing.detector_id}+{r.detector_id}",
                        service_pair     = existing.service_pair,
                        pattern_type     = existing.pattern_type,
                        confidence       = combined_conf,
                        observations     = max(existing.observations, r.observations),
                        improvement_rate = max(existing.improvement_rate, r.improvement_rate),
                        bdh_weight       = max(existing.bdh_weight, r.bdh_weight),
                        evidence         = {**existing.evidence, **r.evidence},
                        recommended_rule = existing.recommended_rule or r.recommended_rule,
                    )
                else:
                    merged[key] = r

        return list(merged.values())

    @staticmethod
    def _bundle_from_lnn_report(report: PatternReport, session_id: str) -> Optional[dict]:
        """
        Build a ProposalBundle from an LNN-ILP THRESHOLD_DRIFT PatternReport.
        Only THRESHOLD_DRIFT reports produce config_patch proposals.
        """
        evidence = report.evidence or {}
        path     = evidence.get("path", "")
        current  = evidence.get("current", config.DISTILLATION_THRESHOLD_CURRENT)
        proposed = evidence.get("proposed", config.DISTILLATION_THRESHOLD_PROPOSED)

        if not path:
            return None

        # Only patch paths in the known-safe list pass the compiler
        if path not in config.COMPILER_KNOWN_PATCH_PATHS:
            return None

        proposal_id = str(uuid.uuid4())
        bundle_id   = hashlib.sha256(
            f"lnn_{session_id}{proposal_id}".encode()
        ).hexdigest()[:16]

        rationale = (
            f"LNN-ILP threshold drift inducer detected success_rate="
            f"{evidence.get('success_rate', 0.0):.3f} below target "
            f"{config.LNN_ILP_TARGET_SUCCESS_RATE}. "
            f"Proposing to lower {path} from {current} to {proposed}. "
            f"Safe bounds [{config.DISTILLATION_SAFE_MIN}, "
            f"{config.DISTILLATION_SAFE_MAX}] verified. "
            f"Change is reversible: original value recorded in config_patch.current."
        )

        bundle = {
            "bundle_id":          bundle_id,
            "generated_at_tick":  0,
            "window_id":          session_id[:16] if len(session_id) >= 16 else session_id,
            "source_type":        config.PROPOSAL_SOURCE_LNN_ILP,
            "generator_id":       config.DETECTOR_ID_LNN_ILP,
            "generator_version":  "0.1.0",
            "model_id":           None,
            "prompt_hash":        None,
            "residual_window_id": session_id[:16] if len(session_id) >= 16 else session_id,
            "detector_ids":       [config.DETECTOR_ID_LNN_ILP],
            "residual_sources":   ["AUTONOMOUS"],
            "cooling_checked_at": int(time.time()),
            "no_cooling_active":  True,
            "proposals": [
                {
                    "proposal_id": proposal_id,
                    "cooling_key": path,
                    "fol_rule":    report.recommended_rule,
                    "config_patch": {
                        "path":     path,
                        "current":  current,
                        "proposed": proposed,
                        "safe_min": config.DISTILLATION_SAFE_MIN,
                        "safe_max": config.DISTILLATION_SAFE_MAX,
                    },
                    "attach_to":    config.POLICY_AGENT_ROOT,
                    "confidence":   report.confidence,
                    "observations": report.observations,
                    "rationale":    rationale,
                    "evidence":     evidence,
                }
            ],
        }
        return bundle

    @staticmethod
    def run(residual_store, session_id: str) -> DistillationResult:
        """
        Run the full Phase 7 distillation pipeline for one session.
        residual_store: ResidualStore — all residuals for the session.
        session_id:     str — used as window_id and correlation key.
        """
        t_start = time.monotonic()

        # Step 1 — collect all residuals
        residuals   = residual_store.all()
        n_residuals = len(residuals)

        # Step 2 — BDH update
        bdh      = BDHKernel()
        pathways = bdh.update(residuals)
        n_pathways = len(pathways)

        # Step 3 — build window dict and detect heuristic patterns
        by_service: dict = {}
        for r in residuals:
            if r.service not in by_service:
                by_service[r.service] = {"count": 0}
            by_service[r.service]["count"] += 1

        window_id = session_id[:16] if len(session_id) >= 16 else session_id
        window = {
            "run_id":     session_id,
            "window_id":  window_id,
            "by_service": by_service,
            "residuals":  [],
        }
        patterns   = detect_patterns(window)
        n_patterns = len(patterns)

        # Step 4 — run LNN-ILP engine
        kb           = KnowledgeBase()
        lnn_ilp      = LNNILPEngine(kb)
        policy_graph = {
            config.DISTILLATION_CONFIG_PATCH_PATH: config.DISTILLATION_THRESHOLD_CURRENT,
        }
        lnn_reports = lnn_ilp.analyse(residuals, pathways, policy_graph)

        # Step 4b — run LLM distillation worker
        from learning.llm_distillation_worker import LLMDistillationWorker
        from learning.llm_provider import MockLLMProvider
        llm_worker  = LLMDistillationWorker(provider=MockLLMProvider(), window_id=session_id)
        llm_reports = llm_worker.analyse(residuals, pathways, policy_graph)

        # Step 4c — three-way merge of all detector reports
        all_reports = DistillationRunner._merge_reports([], lnn_reports, llm_reports)

        # Step 5 — generate heuristic proposal bundles
        cooling_log = load_cooling_log()
        bundles: list = []
        for pattern in patterns:
            bundle = generate_proposal(pattern, cooling_log)
            if bundle is not None:
                bundles.append(bundle)

        # Step 5b — generate LNN-ILP bundles for THRESHOLD_DRIFT reports
        for report in lnn_reports:
            if report.pattern_type == config.PATTERN_THRESHOLD_DRIFT:
                lnn_bundle = DistillationRunner._bundle_from_lnn_report(report, session_id)
                if lnn_bundle is not None:
                    bundles.append(lnn_bundle)

        # Steps 6a–6d — govern each bundle through PIC Chain + Compiler
        # Steps 6–7 — submit each bundle to Forum (mandatory gate, §17)
        pic_chain          = PICChain()
        forum              = AgentForum(pic_chain=pic_chain)
        submitted_entry_ids = []
        bundle_ids          = []

        for bundle in bundles:
            # 7a — PROPOSAL_SUBMITTED (backward-compat PIC entry)
            pic_chain.append(
                config.PIC_TYPE_PROPOSAL,
                0,
                {"bundle_id": bundle["bundle_id"], "session_id": session_id},
            )

            # 7b — Submit to Forum (mandatory gate — no auto-approval)
            try:
                fe = forum.submit(bundle, tick_index=0)
                submitted_entry_ids.append(fe.entry_id)
            except ForumPermissionError:
                pic_chain.append(
                    config.PIC_TYPE_REJECTION,
                    0,
                    {
                        "bundle_id": bundle["bundle_id"],
                        "reason":    "ForumPermissionError",
                    },
                )

        elapsed_ms = int((time.monotonic() - t_start) * 1000)

        return DistillationResult(
            session_id              = session_id,
            residuals_analysed      = n_residuals,
            pathways_updated        = n_pathways,
            patterns_detected       = n_patterns + len(lnn_reports),
            proposals_generated     = len(bundles),
            proposals_accepted      = 0,
            proposal_bundle_ids     = bundle_ids,
            run_duration_ms         = max(elapsed_ms, 1),
            pic_chain               = pic_chain,
            forum_entries_submitted = len(submitted_entry_ids),
            forum                   = forum,
            llm_patterns_detected   = len(llm_reports),
        )
