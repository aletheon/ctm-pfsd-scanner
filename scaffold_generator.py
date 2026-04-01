"""
MVP Distiller Adapter (ScaffoldGenerator)

Scans Python source via AST and produces a DistilledGraph scaffold.

Output contract:
  - schema_version: "1.1"
  - is_valid_ctm_graph: always False
  - All nodes marked is_governance_gap: True, with typed gaps[] registry
  - R nodes have rule_type: AUTO_INFERRED
  - Edge types: "P_CONTAINS_P", "P_CONTAINS_R", "R_TARGETS_S" — never "P→R→S"
  - Top-level: paths_represented, future_edges_supported, forum_notice
  - summary.gaps_total derived from typed gaps registry

§48 boundary: imports only stdlib — no runtime engine classes.
Zone 3 purity: scan() returns nodes + edges only.
Orchestration (BDH, GapClassifier) is Zone 2 responsibility (server.py).
"""
from __future__ import annotations
import ast
import re

DISCLAIMER = (
    "This output is NOT a valid CTM-PFSD PolicyGraph. It is a scaffold "
    "representing inferred services (HOW layer only). Governance layers — "
    "WHY (Policy authority) and WHAT (Rule declarations, conditions, "
    "depends_on ordering) — are intentionally absent and are marked as "
    "governance gaps requiring human authorship."
)


class ScaffoldGenerator:

    def scan(self, project_name: str, files: list) -> dict:
        nodes  = []
        edges  = []
        root_id = _node_id("P", "root_" + _slugify(project_name))

        root_slug = _slugify(project_name)

        # ── Root P node ───────────────────────────────────────
        nodes.append(_make_node(
            id_      = root_id,
            type_    = "P",
            label    = _to_pascal("root_" + root_slug) + "Policy",
            meta     = {
                "is_root":              True,
                "is_virtual_root":      True,
                "actor_scope":          None,
                "file":                 None,
                "module_name":          project_name,
                "is_inferred_boundary": True,
                "inference_source":     "project_root",
                "confidence":           0.7,
            },
            gap_reason = (
                "Root policy authority and actor_scope require human authorship. "
                "This root node is virtual — it does not imply a real unified "
                "governance authority. Multi-domain systems may have multiple "
                "top-level policies or no single root."
            ),
            gaps = [
                {"gap_id": f"policy_ownership_{root_slug}", "category": "POLICY_OWNERSHIP",
                 "severity": "BLOCKING", "layer": "WHY",
                 "description": "Root policy authority requires human authorship"},
                {"gap_id": f"actor_scope_{root_slug}", "category": "ACTOR_SCOPE",
                 "severity": "BLOCKING", "layer": "WHY",
                 "description": "actor_scope requires human authorship"},
            ],
        ))

        services_count = 0
        rules_count    = 0

        for file_entry in files:
            path     = file_entry.get("path", "")
            content  = file_entry.get("content", "")
            filename = path.split("/")[-1]

            if not content.strip() or len(content) > 500_000:
                continue
            if filename.startswith("test_") or filename == "conftest.py":
                continue

            module_name = filename.replace(".py", "")
            p_id        = _node_id("P", module_name)
            p_label     = _to_pascal(module_name) + "Policy"

            p_slug = _slugify(module_name)

            # ── Module P node ──────────────────────────────────
            nodes.append(_make_node(
                id_      = p_id,
                type_    = "P",
                label    = p_label,
                meta     = {
                    "is_root":              False,
                    "is_virtual_root":      False,
                    "file":                 path,
                    "module_name":          module_name,
                    "actor_scope":          None,
                    "is_inferred_boundary": True,
                    "inference_source":     "file_boundary",
                    "confidence":           0.6,
                },
                gap_reason = (
                    "actor_scope and policy authority require human authorship. "
                    "IMPORTANT: file boundary ≠ policy boundary. One file may map "
                    "to multiple policies; multiple files may share one policy. "
                    "Human governance authorship decides the real policy scope."
                ),
                gaps = [
                    {"gap_id": f"actor_scope_{p_slug}", "category": "ACTOR_SCOPE",
                     "severity": "BLOCKING", "layer": "WHY",
                     "description": "actor_scope requires human authorship"},
                    {"gap_id": f"policy_ownership_{p_slug}", "category": "POLICY_OWNERSHIP",
                     "severity": "BLOCKING", "layer": "WHY",
                     "description": "Policy authority requires human authorship"},
                    {"gap_id": f"forum_assignment_{p_slug}", "category": "FORUM_ASSIGNMENT",
                     "severity": "ADVISORY", "layer": "WHY",
                     "description": "Forum assignment requires human governance authorship"},
                    {"gap_id": f"manager_assignment_{p_slug}", "category": "MANAGER_ASSIGNMENT",
                     "severity": "ADVISORY", "layer": "WHY",
                     "description": "Manager assignment requires human governance authorship"},
                ],
            ))

            # P_CONTAINS_P edge: root → module P
            edges.append({"from": root_id, "to": p_id,
                          "type": "P_CONTAINS_P", "label": "P_CONTAINS_P"})

            # ── Parse services from file ───────────────────────
            svcs = _parse_services(path, content)

            for svc in svcs:
                slug    = _slugify(svc["source_name"])
                rs_slug = module_name + "_" + slug
                r_id    = _node_id("R", rs_slug)
                s_id    = _node_id("S", rs_slug)

                # R node (auto-inferred rule)
                nodes.append(_make_node(
                    id_      = r_id,
                    type_    = "R",
                    label    = "AutoRule_" + svc["name"],
                    meta     = {
                        "rule_type":              "AUTO_INFERRED",
                        "when_condition":         None,
                        "depends_on":             None,
                        "rule_inference_strategy":"one_to_one_service_mapping",
                        "rule_identity_source":   "service_mapping_v1",
                        "confidence":             0.8,
                    },
                    gap_reason = (
                        "Rule condition (when), depends_on, and scope require "
                        "human authorship"
                    ),
                    gaps = [
                        {"gap_id": f"rule_condition_{_slugify(rs_slug)}", "category": "RULE_CONDITION",
                         "severity": "BLOCKING", "layer": "WHAT",
                         "description": "Rule condition (when) requires human authorship"},
                        {"gap_id": f"rule_ordering_{_slugify(rs_slug)}", "category": "RULE_ORDERING",
                         "severity": "ADVISORY", "layer": "WHAT",
                         "description": "depends_on ordering requires human authorship"},
                    ],
                ))

                # S node (inferred service)
                nodes.append(_make_node(
                    id_      = s_id,
                    type_    = "S",
                    label    = svc["name"],
                    meta     = {
                        "source_name":               svc["source_name"],
                        "source_type":               svc["type"],
                        "parameters":                svc["parameters"],
                        "returns":                   svc["returns"],
                        "line_number":               svc["line_number"],
                        "simulation_safe":           None,
                        "service_detection_strategy":"ast_function_class_v1",
                        "is_shared_candidate":       False,
                    },
                    gap_reason = (
                        "Service schema, endpoint, and simulation_safe "
                        "require human authorship"
                    ),
                    gaps = [
                        {"gap_id": f"service_schema_{_slugify(rs_slug)}", "category": "SERVICE_SCHEMA",
                         "severity": "ADVISORY", "layer": "HOW",
                         "description": "Service schema and endpoint require human authorship"},
                        {"gap_id": f"simulation_safe_{_slugify(rs_slug)}", "category": "SIMULATION_SAFE",
                         "severity": "ADVISORY", "layer": "HOW",
                         "description": "simulation_safe flag requires human authorship"},
                    ],
                ))

                # P_CONTAINS_R edge
                edges.append({"from": p_id, "to": r_id,
                              "type": "P_CONTAINS_R", "label": "P_CONTAINS_R"})

                # R_TARGETS_S edge  (NOT "P→R→S" — always atomic)
                edges.append({"from": r_id, "to": s_id,
                              "type": "R_TARGETS_S", "label": "R_TARGETS_S"})

                rules_count    += 1
                services_count += 1

        return {
            "schema_version":         "1.1",
            "is_valid_ctm_graph":     False,
            "disclaimer":             DISCLAIMER,
            "project_name":           project_name,
            "root_policy_id":         root_id,
            "paths_represented":      ["P→R→S"],
            "future_edges_supported":  ["P→R→F", "B→G→middleware→S", "B→G→middleware→F"],
            "forum_notice": (
                "Forums (F) and Path B execution are not shown in scaffold output. "
                "Path B has two flows: @command flow (B→G→middleware→S) and "
                "forum post flow (B→G→middleware→F). In both flows permission "
                "validation occurs in middleware — not inside the forum. "
                "Only Path A (P→R→S) chains are represented here. "
                "Forum and middleware assignments require human governance authorship."
            ),
            "summary": {
                "policies_count":  len([n for n in nodes if n["type"] == "P"]),
                "rules_count":     rules_count,
                "services_count":  services_count,
                "gaps_total":      sum(len(n.get("gaps", [])) for n in nodes),
            },
            "nodes":         nodes,
            "edges":         edges,
        }


# ── AST parsing ────────────────────────────────────────────────────────────

def _parse_services(path: str, content: str) -> list:
    try:
        tree = ast.parse(content, filename=path)
    except SyntaxError:
        return []

    services = []
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            if node.name.startswith("_"):
                continue
            params = [a.arg for a in node.args.args
                      if a.arg not in ("self", "cls")]
            returns = "unknown"
            if node.returns:
                try:
                    returns = ast.unparse(node.returns)
                except Exception:
                    pass
            services.append({
                "name":        _to_pascal(node.name) + "Service",
                "source_name": node.name,
                "type":        "function",
                "parameters":  params,
                "returns":     returns,
                "line_number": node.lineno,
            })
        elif isinstance(node, ast.ClassDef):
            if node.name.startswith("_"):
                continue
            services.append({
                "name":        node.name + "Service",
                "source_name": node.name,
                "type":        "class",
                "parameters":  [],
                "returns":     "unknown",
                "line_number": node.lineno,
            })
    return services


# ── Helpers ────────────────────────────────────────────────────────────────

def _make_node(id_: str, type_: str, label: str,
               meta: dict, gap_reason: str, gaps: list = None) -> dict:
    return {
        "id":                id_,
        "type":              type_,
        "label":             label,
        "meta":              meta,
        "is_governance_gap": True,
        "gap_reason":        gap_reason,
        "gaps":              gaps or [],
    }

def _node_id(type_: str, name: str) -> str:
    return (type_.lower() + "_" + _slugify(name))[:64]

def _slugify(name: str) -> str:
    return re.sub(r"[^a-z0-9_]", "_", name.lower()).strip("_")

def _to_pascal(name: str) -> str:
    parts = re.split(r"[_\-\s\.]+", name)
    return "".join(p.capitalize() for p in parts if p)
