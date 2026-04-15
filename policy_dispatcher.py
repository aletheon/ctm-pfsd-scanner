"""
policy_dispatcher.py — Policy Graph Dispatcher for Path A execution.

CTM-PFSD Spec v1.6 §3.2, §3.3 (Path A: P → R → S)
SovereignClaw §2.1 canonical tick loop phases 4–6
Constitution §1 (Two Execution Paths — Path A)

Walks a compiled PolicyGraph and dispatches a CodeChangeIntent
against matching P nodes, yielding RuleFirings in rule_order_index order.

Zone 2 contract:
  - Does NOT invoke services
  - Does NOT write residuals
  - Does NOT modify the PolicyGraph
  - Zone 2 (server.py) consumes RuleFirings and does all of the above

§48 boundary: stdlib only (uuid, hashlib, dataclasses, typing, time).
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


# ── RuleFiring ─────────────────────────────────────────────────────────────

@dataclass(frozen=True)
class RuleFiring:
    """Immutable record of one rule evaluated during a dispatch."""
    policy_id:        str
    policy_label:     str
    rule_id:          str
    rule_label:       str
    service_label:    str
    rule_order_index: int
    when_condition:   Optional[str]
    condition_met:    bool


# ── PolicyDispatcher ───────────────────────────────────────────────────────

class PolicyDispatcher:
    """
    Stateless dispatcher. dispatch() may be called concurrently —
    it holds no mutable state.
    """

    def dispatch(self, graph: dict, intent) -> list:
        """
        Walk the compiled graph and return a RuleFiring for every R node
        owned by policies that match the intent's actor.

        graph   — compile_result["graph"] (inner graph dict)
        intent  — CodeChangeIntent instance

        Returns all RuleFirings (condition_met=True and condition_met=False).
        Caller decides what to do with non-firing rules.
        """
        nodes = graph.get("nodes", [])
        edges = graph.get("edges", [])

        # Step 1 — Build node lookups
        node_by_id: dict = {n["id"]: n for n in nodes}
        p_ids = [n["id"] for n in nodes if n.get("type") == "P"]

        # Step 2 — Build edge lookups
        p_contains_r: dict = {}   # { p_id: [r_id, ...] }
        r_targets_s:  dict = {}   # { r_id: s_id }
        p_contains_p_edges = []

        for edge in edges:
            etype = edge.get("type", "")
            frm   = edge.get("from", "")
            to    = edge.get("to", "")
            if etype == "P_CONTAINS_R":
                p_contains_r.setdefault(frm, []).append(to)
            elif etype == "R_TARGETS_S":
                r_targets_s[frm] = to
            elif etype == "P_CONTAINS_P":
                p_contains_p_edges.append(edge)

        # Step 3 — Topological sort of P nodes (roots first via P_CONTAINS_P)
        ordered_p_ids = _topo_sort_p_nodes(p_ids, p_contains_p_edges)

        firings: list = []

        for p_id in ordered_p_ids:
            p_node = node_by_id.get(p_id)
            if p_node is None:
                continue

            # Check actor scope match
            if not _policy_matches_intent(p_node, intent):
                continue

            # Step 4 — Collect and sort R nodes for this policy
            r_ids = p_contains_r.get(p_id, [])
            r_nodes = [node_by_id[r_id] for r_id in r_ids if r_id in node_by_id]
            r_nodes.sort(
                key=lambda n: n.get("rule_order_index") or 0
            )

            # Step 5 — Evaluate each R node
            for r_node in r_nodes:
                when_condition = r_node.get("when") or None
                rule_order     = r_node.get("rule_order_index") or 0

                # Resolve target S node
                s_id    = r_targets_s.get(r_node["id"])
                s_node  = node_by_id.get(s_id) if s_id else None
                s_label = s_node["label"] if s_node else ""

                condition_met = _eval_condition(when_condition, intent)

                firings.append(RuleFiring(
                    policy_id        = p_id,
                    policy_label     = p_node.get("label", ""),
                    rule_id          = r_node["id"],
                    rule_label       = r_node.get("label", ""),
                    service_label    = s_label,
                    rule_order_index = rule_order,
                    when_condition   = when_condition,
                    condition_met    = condition_met,
                ))

        return firings


# ── Helpers ────────────────────────────────────────────────────────────────

def _topo_sort_p_nodes(p_ids: list, p_contains_p_edges: list) -> list:
    """
    Kahn's topological sort over P nodes using P_CONTAINS_P edges.
    Nodes with no incoming edges (roots) are processed first.
    Disconnected nodes are appended after the sorted sequence.
    """
    p_id_set = set(p_ids)

    children:  dict = {pid: [] for pid in p_ids}
    in_degree: dict = {pid: 0  for pid in p_ids}

    for edge in p_contains_p_edges:
        parent = edge.get("from", "")
        child  = edge.get("to",   "")
        if parent in p_id_set and child in p_id_set:
            children[parent].append(child)
            in_degree[child] += 1

    # Seed with zero-in-degree nodes, stable-sorted for determinism
    queue = sorted(pid for pid in p_ids if in_degree[pid] == 0)
    order = []

    while queue:
        pid = queue.pop(0)
        order.append(pid)
        for child in sorted(children[pid]):
            in_degree[child] -= 1
            if in_degree[child] == 0:
                queue.append(child)

    # Append any nodes not reached (cycles / disconnected)
    seen = set(order)
    for pid in p_ids:
        if pid not in seen:
            order.append(pid)

    return order


def _policy_matches_intent(p_node: dict, intent) -> bool:
    """
    Return True if the intent's actor is in scope for this policy.

    Match conditions (any one sufficient):
      1. intent.actor is in policy actor_scope list
      2. policy meta.is_root == True  (root always matches)
      3. intent.actor == "Codebase"   (root actor matches all policies)

    NOTE: actor_scope is a top-level field on the P node (set by
    graph_builder.py), not nested inside meta. meta only carries
    is_root and similar flags.
    """
    meta = p_node.get("meta", {})

    if meta.get("is_root"):
        return True

    actor = getattr(intent, "actor", "")
    if actor == "Codebase":
        return True

    # actor_scope lives at the top level of the node, not in meta
    actor_scope = p_node.get("actor_scope") or []
    return actor in actor_scope


def _eval_condition(condition: Optional[str], intent) -> bool:
    """
    Evaluate a when_condition string against a CodeChangeIntent.

    Stage A: handles null, == comparisons, and IN membership.
    Unknown condition patterns → True (conservative; full evaluation Stage B).

    Special case: when intent.actor == "Codebase", actor field conditions
    are always satisfied (root actor bypasses actor-scoped restrictions).
    """
    if not condition:
        return True

    cond = condition.strip()

    # ── intent.X IN [A,B,...] ─────────────────────────────────────────────
    upper = cond.upper()
    if " IN " in upper:
        in_pos = upper.index(" IN ")
        lhs = cond[:in_pos].strip()
        rhs = cond[in_pos + 4:].strip()

        field_val = _resolve_intent_field(lhs, intent)
        if field_val is None:
            return True  # unknown field — conservative

        # Codebase root actor bypasses actor conditions
        lhs_field = lhs[7:] if lhs.startswith("intent.") else ""
        if lhs_field == "actor" and str(field_val) == "Codebase":
            return True

        if rhs.startswith("[") and rhs.endswith("]"):
            members = [
                s.strip().strip("'\"")
                for s in rhs[1:-1].split(",")
                if s.strip()
            ]
            return str(field_val) in members

        return True  # malformed rhs — conservative

    # ── intent.X == Y ─────────────────────────────────────────────────────
    if "==" in cond:
        parts = cond.split("==", 1)
        lhs   = parts[0].strip()
        rhs   = parts[1].strip().strip("'\"")

        field_val = _resolve_intent_field(lhs, intent)
        if field_val is None:
            return True  # unknown field — conservative

        # Codebase root actor bypasses actor conditions
        lhs_field = lhs[7:] if lhs.startswith("intent.") else ""
        if lhs_field == "actor" and str(field_val) == "Codebase":
            return True

        return str(field_val) == rhs

    # Unknown operator — conservative
    return True


def _resolve_intent_field(lhs: str, intent) -> Optional[str]:
    """
    Resolve "intent.fieldname" to its value on the intent object.
    Returns None if the expression is not in "intent.X" form.

    Normalises spaces around dots emitted by the lexer
    (e.g. "intent . change_type" → "intent.change_type").
    """
    lhs = lhs.strip().replace(" . ", ".").replace(" .", ".").replace(". ", ".")
    if not lhs.startswith("intent."):
        return None
    field_name = lhs[7:]
    val = getattr(intent, field_name, None)
    if val is None:
        return None
    return str(val)
