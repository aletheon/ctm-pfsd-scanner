"""
render_services.py — Zone 3 Forum Layout Panel render services.

CTM_PFSD_MVP_Implementation_Spec_v3_7.md §9
CTM_PFSD_Forum_Layout_Panel_Spec_Step8plus.md §6

Three pure render services. Return structured JSON payloads —
never HTML. No persistent state. OSError caught.

Standard render payload shape (all three services):
{
  "panel":          str,
  "render_type":    str,
  "render_title":   str,
  "data":           dict,
  "interactions":   list,
  "lifecycle_step": int,
  "member_id":      str | null
}

§48 boundary: stdlib only (json, os) + config + governance_tools.
Pure functions. No S→S calls.
"""
from __future__ import annotations

import json
import os
from typing import Optional

import config
from governance_tools import diff_graphs, trace_authority


# ── GovernanceDiffRenderService ────────────────────────────────────────────

class GovernanceDiffRenderService:
    """
    Renders a structured diff between two compiled graph dicts.
    panel: "governance_diff"
    """

    def render(
        self,
        graph_a:   dict,
        graph_b:   dict,
        member_id: Optional[str] = None,
    ) -> dict:
        try:
            data = diff_graphs(graph_a, graph_b)
            return {
                "panel":          "governance_diff",
                "render_type":    "diff",
                "render_title":   "Governance Diff",
                "data":           data,
                "interactions":   [
                    {
                        "label":          "Run new diff",
                        "message_type":   "@command",
                        "command":        "run_governance_diff",
                        "permission":     "submit_intent",
                        "target_service": "GovernanceDiffService",
                        "params":         ["source_a", "source_b"],
                    }
                ],
                "lifecycle_step": 4,
                "member_id":      member_id,
            }
        except Exception as e:
            return {
                "panel":          "governance_diff",
                "render_type":    "diff",
                "render_title":   "Governance Diff",
                "data":           {
                    "error":     str(e),
                    "identical": True,
                    "summary":   "Diff unavailable",
                },
                "interactions":   [],
                "lifecycle_step": 4,
                "member_id":      member_id,
            }


# ── CapabilityClosureRenderService ─────────────────────────────────────────

class CapabilityClosureRenderService:
    """
    Renders the authority chain for a service label.
    panel: "capability_closure"
    """

    def render(
        self,
        graph:        dict,
        policy_label: str,
        member_id:    Optional[str] = None,
    ) -> dict:
        try:
            data = trace_authority(graph, policy_label)
            return {
                "panel":          "capability_closure",
                "render_type":    "chain",
                "render_title":   "Capability Closure",
                "data":           data,
                "interactions":   [
                    {
                        "label":          "Inspect service",
                        "message_type":   "@command",
                        "command":        "trace_authority",
                        "permission":     "submit_intent",
                        "target_service": "CapabilityClosureService",
                        "params":         ["service_label"],
                    }
                ],
                "lifecycle_step": 4,
                "member_id":      member_id,
            }
        except Exception as e:
            return {
                "panel":          "capability_closure",
                "render_type":    "chain",
                "render_title":   "Capability Closure",
                "data":           {
                    "error":            str(e),
                    "found":            False,
                    "authority_chains": [],
                },
                "interactions":   [],
                "lifecycle_step": 4,
                "member_id":      member_id,
            }


# ── PicChainRenderService ──────────────────────────────────────────────────

class PicChainRenderService:
    """
    Renders the last N PIC Chain entries as a timeline payload.
    Shows both COMPILE_GRAPH and CODE_CHANGE_COMMITTED entries.
    panel: "pic_chain"
    """

    def render(
        self,
        last_n:    int              = 20,
        member_id: Optional[str]   = None,
    ) -> dict:
        total_in_file = 0
        entries: list = []
        try:
            with open(config.PIC_CHAIN_PATH, "r", encoding="utf-8") as f:
                lines = [l.strip() for l in f if l.strip()]
            total_in_file = len(lines)
            for line in lines:
                try:
                    entries.append(json.loads(line))
                except (json.JSONDecodeError, ValueError):
                    pass
            entries = entries[-last_n:]
        except OSError:
            entries       = []
            total_in_file = 0

        return {
            "panel":          "pic_chain",
            "render_type":    "timeline",
            "render_title":   "PIC Chain",
            "data":           {
                "entries":       entries,
                "entry_count":   len(entries),
                "total_in_file": total_in_file,
                "last_n":        last_n,
            },
            "interactions":   [],
            "lifecycle_step": 4,
            "member_id":      member_id,
        }
