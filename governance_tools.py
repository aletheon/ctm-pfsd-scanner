"""
governance_tools.py — Zone 4 read-only forensic tools.

CTM-PFSD Code Governance Spec v1.6 §18 Step 8
CTM_PFSD_MVP_Implementation_Spec_v3_6.md §17

Three pure functions operating on compiled graph dicts.
All read-only. No writes. No governance mutations.

Confirmed node schema (from test_node_schema.py + dispatcher):
  P: top-level actor_scope, capability_closure; meta.is_root
  R: top-level when, rule_order_index, depends_on — NO meta
  S: top-level label, endpoint, etc — NO meta
  Edges: {from, to, type}  types: P_CONTAINS_P, P_CONTAINS_R, R_TARGETS_S

§48 boundary: stdlib only (json, hashlib).
No imports from Zone 3, Zone 5, or server.py.
No imports from any service file.
"""
from __future__ import annotations


# ── diff_graphs ────────────────────────────────────────────────────────────

def diff_graphs(graph_a: dict, graph_b: dict) -> dict:
    """
    Compare two compiled inner graph dicts.
    Returns a structured diff showing added/removed policies, rules,
    services, and edges. Uses node labels for human-readable output.
    """
    try:
        # Index nodes by label per type
        def _index(graph: dict, node_type: str) -> dict:
            return {
                n["label"]: n
                for n in graph.get("nodes", [])
                if n.get("type") == node_type
            }

        policies_a = _index(graph_a, "P")
        policies_b = _index(graph_b, "P")
        rules_a    = _index(graph_a, "R")
        rules_b    = _index(graph_b, "R")
        services_a = _index(graph_a, "S")
        services_b = _index(graph_b, "S")

        # Build id→label lookup for edge display
        def _id_to_label(graph: dict) -> dict:
            return {n["id"]: n.get("label", n["id"])
                    for n in graph.get("nodes", [])}

        id_label_a = _id_to_label(graph_a)
        id_label_b = _id_to_label(graph_b)

        def _edge_set(graph: dict, id_label: dict) -> set:
            result = set()
            for e in graph.get("edges", []):
                frm  = id_label.get(e.get("from", ""), e.get("from", ""))
                to   = id_label.get(e.get("to",   ""), e.get("to",   ""))
                etype = e.get("type", "")
                result.add((frm, to, etype))
            return result

        edges_a = _edge_set(graph_a, id_label_a)
        edges_b = _edge_set(graph_b, id_label_b)

        policies_added   = sorted(set(policies_b) - set(policies_a))
        policies_removed = sorted(set(policies_a) - set(policies_b))
        rules_added      = sorted(set(rules_b)    - set(rules_a))
        rules_removed    = sorted(set(rules_a)    - set(rules_b))
        services_added   = sorted(set(services_b) - set(services_a))
        services_removed = sorted(set(services_a) - set(services_b))

        raw_edges_added   = sorted(edges_b - edges_a,
                                   key=lambda t: (t[0], t[1], t[2]))
        raw_edges_removed = sorted(edges_a - edges_b,
                                   key=lambda t: (t[0], t[1], t[2]))

        edges_added   = [{"from": f, "to": t, "type": et}
                         for f, t, et in raw_edges_added]
        edges_removed = [{"from": f, "to": t, "type": et}
                         for f, t, et in raw_edges_removed]

        identical = (
            not policies_added and not policies_removed and
            not rules_added    and not rules_removed    and
            not services_added and not services_removed and
            not edges_added    and not edges_removed
        )

        # Build summary string
        parts = []
        if rules_added:      parts.append(f"{len(rules_added)} rule(s) added")
        if rules_removed:    parts.append(f"{len(rules_removed)} rule(s) removed")
        if policies_added:   parts.append(f"{len(policies_added)} policy/ies added")
        if policies_removed: parts.append(f"{len(policies_removed)} policy/ies removed")
        if services_added:   parts.append(f"{len(services_added)} service(s) added")
        if services_removed: parts.append(f"{len(services_removed)} service(s) removed")
        summary = ", ".join(parts) if parts else "no changes"

        return {
            "graph_hash_a":      graph_a.get("graph_hash"),
            "graph_hash_b":      graph_b.get("graph_hash"),
            "identical":         identical,
            "policies_added":    policies_added,
            "policies_removed":  policies_removed,
            "rules_added":       rules_added,
            "rules_removed":     rules_removed,
            "services_added":    services_added,
            "services_removed":  services_removed,
            "edges_added":       edges_added,
            "edges_removed":     edges_removed,
            "summary":           summary,
        }
    except Exception as e:
        return {"error": str(e), "identical": False}


# ── trace_authority ────────────────────────────────────────────────────────

def trace_authority(graph: dict, service_label: str) -> dict:
    """
    Trace the full authority chain for a service.
    Answers: "who has authority over ServiceX and why does it fire?"

    Uses confirmed R node schema: when (top-level), rule_order_index
    (top-level), depends_on (top-level), no meta dict.
    Uses confirmed P node schema: actor_scope (top-level),
    capability_closure (top-level), meta.is_root.
    """
    try:
        nodes      = graph.get("nodes", [])
        edges      = graph.get("edges", [])
        node_by_id = {n["id"]: n for n in nodes}

        # Find the S node with matching label
        s_node = next(
            (n for n in nodes
             if n.get("type") == "S" and n.get("label") == service_label),
            None,
        )
        if s_node is None:
            return {
                "service_label":           service_label,
                "found":                   False,
                "authority_chains":        [],
                "policies_with_authority": [],
                "total_rules_targeting":   0,
            }

        s_id = s_node["id"]

        # Find all R nodes that target this S node via R_TARGETS_S
        r_ids_targeting = {
            e["from"]
            for e in edges
            if e.get("type") == "R_TARGETS_S" and e.get("to") == s_id
        }

        if not r_ids_targeting:
            return {
                "service_label":           service_label,
                "found":                   True,
                "authority_chains":        [],
                "policies_with_authority": [],
                "total_rules_targeting":   0,
            }

        # Build P_CONTAINS_R reverse map: r_id → p_id
        r_to_p: dict[str, str] = {
            e["to"]: e["from"]
            for e in edges
            if e.get("type") == "P_CONTAINS_R"
        }

        authority_chains = []
        policies_seen    = set()

        for r_id in sorted(r_ids_targeting):
            r_node = node_by_id.get(r_id)
            if r_node is None:
                continue

            p_id   = r_to_p.get(r_id)
            p_node = node_by_id.get(p_id) if p_id else None
            if p_node is None:
                continue

            # R node fields — all top-level, no meta
            when_raw  = r_node.get("when")
            # Normalise lexer-spaced dots for display
            when_cond = None
            if when_raw:
                when_cond = (when_raw
                             .replace(" . ", ".")
                             .replace(" .", ".")
                             .replace(". ", "."))

            rule_order = r_node.get("rule_order_index") or 0
            depends_on = r_node.get("depends_on") or None

            # P node fields
            meta         = p_node.get("meta", {})
            actor_scope  = p_node.get("actor_scope") or []
            cap_closure  = p_node.get("capability_closure") or []
            is_root      = bool(meta.get("is_root"))
            in_closure   = service_label in cap_closure

            policies_seen.add(p_node.get("label", ""))

            authority_chains.append({
                "policy_label":          p_node.get("label", ""),
                "actor_scope":           actor_scope,
                "is_root_policy":        is_root,
                "in_capability_closure": in_closure,
                "rule_label":            r_node.get("label", ""),
                "when_condition":        when_cond,
                "rule_order_index":      rule_order,
                "depends_on":            depends_on,
            })

        # Sort by rule_order_index for deterministic output
        authority_chains.sort(key=lambda c: c["rule_order_index"])

        return {
            "service_label":           service_label,
            "found":                   True,
            "authority_chains":        authority_chains,
            "policies_with_authority": sorted(policies_seen),
            "total_rules_targeting":   len(authority_chains),
        }
    except Exception as e:
        return {
            "service_label":         service_label,
            "found":                 False,
            "error":                 str(e),
            "authority_chains":      [],
            "total_rules_targeting": 0,
        }


# ── detect_ungoverned ──────────────────────────────────────────────────────

def detect_ungoverned(graph: dict, scanned_services: list) -> dict:
    """
    Compare scanned service labels against the compiled graph.
    Returns which scanned services have no P→R→S governance path.
    """
    try:
        governed_services = {
            n["label"]
            for n in graph.get("nodes", [])
            if n.get("type") == "S"
        }

        total      = len(scanned_services)
        governed   = sorted(s for s in scanned_services
                            if s in governed_services)
        ungoverned = sorted(s for s in scanned_services
                            if s not in governed_services)

        coverage = round(len(governed) / total, 6) if total else 0.0

        return {
            "total_scanned":       total,
            "governed_count":      len(governed),
            "ungoverned_count":    len(ungoverned),
            "governed_services":   governed,
            "ungoverned_services": ungoverned,
            "governance_coverage": coverage,
        }
    except Exception as e:
        return {"error": str(e), "total_scanned": 0}
