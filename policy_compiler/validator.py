"""
policy_compiler.validator — Stage 3–5 semantic validation (Checks 1–17 + Stages 4–5).

INPUT:  graph: dict  (from graph_builder.build())
OUTPUT: dict  (ValidationResult — errors, warnings[, checks_run])

validate_1_6  — Checks 1–6:
  1  DAG Integrity          E003 CycleDetected
  2  Legal Edge Table        E004 IllegalEdge
  3  Execution Path          E010 ServiceCallsService
  4  Governance Boundary     E011 UngovernedLearningSystem
  5  Root Axiom              E008 MultipleRootPolicies / E038 MissingRootPolicy
  6  Unreachable Policy      E037 UnreachablePolicyNode

validate_7_12 — Checks 7–12:
  7  Policy Inheritance Cycle  E036 PolicyGraphCycleDetected
  8  Monotonicity              W001 InertPolicyBranch (Stage-A placeholder)
  9  Service Schema Presence   E006 MissingServiceSchema
  10 Service Return Contract   E014 MissingReturnContract
  11 State Schema Ownership    E015 UnownedStateSchema / E016 StateSchemaConflict
  12 Permission Integrity      E007 PermissionViolation

validate_13_17 — Checks 13–17:
  13 Rule Dependency Acyclicity  E017 RuleDependencyCycle / E018 UnresolvedRuleDependency
  14 Result Type Checking        E019 ResultTypeMismatch
  15 Rule Condition Purity       E043 ImpureRuleCondition
  16 Service Signature Match     E040 ServiceSignatureMismatch
  17 Determinism in Learning Path E041 NonDeterministicServiceInLearningPath
                                  W013 NonDeterministicPattern

validate_stage4 — Stage 4 (Authority Chain):
  4-1 Actor Scope Declaration     E031 MissingActorScope
  4-2 Actor Scope Undeclared      E032 ActorScopeUndeclared
  4-3 Service Actor Scope         W015 ActorScopeUnchecked (advisory)
  4-4 Simulation Safety           E045 SimulationUnsafeService

validate_stage5 — Stage 5 (Capability Closure):
  Computes capability_closure on every P node (in-place)
  Assigns rule_order_index on every R node (in-place)
  W017 ActorScopeOverlap (sibling overlap)
  [E005 deferred to Stage C — W001 remains as Stage B placeholder]

§48 boundary: stdlib only.
Zone 3 purity: no imports from any project file except policy_compiler.
"""
from __future__ import annotations
from collections import deque
import re as _re

# ── Legal edge table ───────────────────────────────────────────────────────

# All edge types that the grammar can produce.
_LEGAL_EDGE_TYPES: frozenset[str] = frozenset({
    "P_CONTAINS_R",
    "P_CONTAINS_P",
    "P_CONTAINS_F",
    "P_BINDS_M",
    "R_TARGETS_S",
    "R_TARGETS_P",
    "R_TARGETS_F",
    "M_HOLDS_N",
    "B_HOLDS_N",
    "F_CONTAINS_B",
})

# Forbidden (from_type, to_type) pairs — catches structural violations
# regardless of the edge-type label.
_FORBIDDEN_PAIRS: frozenset[tuple[str, str]] = frozenset({
    ("S", "S"),   # E010 — handled separately with its own code
    ("S", "P"),
    ("S", "R"),
    ("S", "M"),
    ("S", "B"),
    ("R", "M"),
    ("R", "B"),
})

# Labels that must be covered by at least one policy.
_LEARNING_LABELS: frozenset[str] = frozenset({
    "BDH", "CTM", "LNN",
})


# ── Public API ─────────────────────────────────────────────────────────────

def validate(graph: dict) -> dict:
    """
    Run Stage 3 checks 1–6 against a PolicyGraph dict.

    Returns:
      {
        "errors":      [{"code", "name", "message", "node_id", "stage": 3}, ...],
        "warnings":    [{"code", "name", "message", "node_id", "stage": 3}, ...],
        "checks_run":  [1, 2, 3, 4, 5, 6],
      }

    Errors do not halt subsequent checks — all six checks always run.
    """
    errors:   list[dict] = []
    warnings: list[dict] = []

    nodes: list[dict] = graph.get("nodes", [])
    edges: list[dict] = graph.get("edges", [])

    # Pre-build shared indexes used across multiple checks.
    node_by_id: dict[str, dict] = {n["id"]: n for n in nodes}

    _check_1_dag(edges, node_by_id, errors)
    _check_2_legal_edges(edges, node_by_id, errors)
    _check_3_execution_path(edges, node_by_id, errors)
    _check_4_governance_boundary(nodes, edges, node_by_id, errors)
    _check_5_root_axiom(nodes, edges, errors)
    _check_6_unreachable_policies(nodes, edges, errors)

    return {
        "errors":     errors,
        "warnings":   warnings,
        "checks_run": [1, 2, 3, 4, 5, 6],
    }


# ── Check 1 — DAG Integrity ────────────────────────────────────────────────

def _check_1_dag(edges: list[dict], node_by_id: dict[str, dict],
                 errors: list[dict]) -> None:
    """
    Kahn's algorithm over the full edge set.
    Any cycle → E003 CycleDetected (one error, no node_id).
    """
    # Build adjacency and in-degree over known node IDs only.
    known_ids = set(node_by_id)

    adj:      dict[str, list[str]] = {nid: [] for nid in known_ids}
    in_degree: dict[str, int]      = {nid: 0  for nid in known_ids}

    for edge in edges:
        frm = edge["from"]
        to  = edge["to"]
        if frm in known_ids and to in known_ids:
            adj[frm].append(to)
            in_degree[to] += 1

    queue = deque(nid for nid in known_ids if in_degree[nid] == 0)
    visited = 0

    while queue:
        nid = queue.popleft()
        visited += 1
        for neighbour in adj[nid]:
            in_degree[neighbour] -= 1
            if in_degree[neighbour] == 0:
                queue.append(neighbour)

    if visited < len(known_ids):
        errors.append(_make_error(
            "E003", "CycleDetected",
            "Policy graph contains a cycle — DAG invariant violated",
            node_id=None,
        ))


# ── Check 2 — Legal Edge Table ─────────────────────────────────────────────

def _check_2_legal_edges(edges: list[dict], node_by_id: dict[str, dict],
                         errors: list[dict]) -> None:
    """
    Each edge must:
      (a) have a type in _LEGAL_EDGE_TYPES          → E004 IllegalEdge
      (b) not connect a forbidden (from_type, to_type) pair  → E004 IllegalEdge
    """
    for edge in edges:
        etype = edge.get("type", "")
        frm   = edge["from"]
        to    = edge["to"]

        if etype not in _LEGAL_EDGE_TYPES:
            errors.append(_make_error(
                "E004", "IllegalEdge",
                f"Edge type '{etype}' is not a legal edge type",
                node_id=frm,
            ))
            continue

        f_node = node_by_id.get(frm)
        t_node = node_by_id.get(to)
        if f_node is None or t_node is None:
            continue

        pair = (f_node["type"], t_node["type"])
        if pair in _FORBIDDEN_PAIRS:
            errors.append(_make_error(
                "E004", "IllegalEdge",
                (f"Edge from '{f_node.get('label', frm)}' ({f_node['type']}) "
                 f"to '{t_node.get('label', to)}' ({t_node['type']}) "
                 f"via '{etype}' is not permitted"),
                node_id=frm,
            ))


# ── Check 3 — Execution Path Constraint ───────────────────────────────────

def _check_3_execution_path(edges: list[dict], node_by_id: dict[str, dict],
                            errors: list[dict]) -> None:
    """
    A Service node must not directly target another Service node.
    (S→S via any edge type) → E010 ServiceCallsService.
    """
    for edge in edges:
        f_node = node_by_id.get(edge["from"])
        t_node = node_by_id.get(edge["to"])
        if f_node is None or t_node is None:
            continue
        if f_node["type"] == "S" and t_node["type"] == "S":
            errors.append(_make_error(
                "E010", "ServiceCallsService",
                (f"Service '{f_node.get('label', edge['from'])}' directly targets "
                 f"service '{t_node.get('label', edge['to'])}' — "
                 "service-to-service calls are forbidden"),
                node_id=f_node["id"],
            ))


# ── Check 4 — Governance Boundary ─────────────────────────────────────────

def _check_4_governance_boundary(nodes: list[dict], edges: list[dict],
                                 node_by_id: dict[str, dict],
                                 errors: list[dict]) -> None:
    """
    Any service whose label contains a learning-system keyword (BDH, CTM, LNN)
    must be targeted by at least one R node that belongs to a P node.
    If no such rule targets it → E011 UngovernedLearningSystem.
    """
    # Find S nodes whose labels contain a learning-system keyword.
    learning_services = [
        n for n in nodes
        if n["type"] == "S"
        and any(kw in n.get("label", "") for kw in _LEARNING_LABELS)
    ]
    if not learning_services:
        return

    # Build a set of service IDs that are targeted by at least one R node.
    governed_svc_ids: set[str] = set()
    for edge in edges:
        if edge.get("type") == "R_TARGETS_S":
            r_node = node_by_id.get(edge["from"])
            if r_node and r_node["type"] == "R":
                governed_svc_ids.add(edge["to"])

    for svc in learning_services:
        if svc["id"] not in governed_svc_ids:
            errors.append(_make_error(
                "E011", "UngovernedLearningSystem",
                (f"Service '{svc.get('label', svc['id'])}' appears to be a "
                 "learning-system component but has no governing policy rule"),
                node_id=svc["id"],
            ))


# ── Check 5 — Root Axiom ───────────────────────────────────────────────────

def _check_5_root_axiom(nodes: list[dict], edges: list[dict],
                        errors: list[dict]) -> None:
    """
    Exactly one P node must be a root (no inbound P_CONTAINS_P edge).
      0 roots → E038 MissingRootPolicy
      2+ roots → E008 MultipleRootPolicies
    """
    p_ids: set[str] = {n["id"] for n in nodes if n["type"] == "P"}
    child_ids: set[str] = {
        e["to"] for e in edges if e.get("type") == "P_CONTAINS_P"
    }
    root_ids = p_ids - child_ids

    root_count = len(root_ids)

    if root_count == 0:
        errors.append(_make_error(
            "E038", "MissingRootPolicy",
            "No root policy found — policy graph has no entry point",
            node_id=None,
        ))
    elif root_count > 1:
        errors.append(_make_error(
            "E008", "MultipleRootPolicies",
            f"Multiple root policies found ({root_count}) — only one root is allowed",
            node_id=None,
        ))


# ── Check 6 — Unreachable Policy Nodes ────────────────────────────────────

def _check_6_unreachable_policies(nodes: list[dict], edges: list[dict],
                                  errors: list[dict]) -> None:
    """
    BFS from each root P node via P_CONTAINS_P edges.
    Any P node not reached → E037 UnreachablePolicyNode.
    """
    p_nodes: dict[str, dict] = {n["id"]: n for n in nodes if n["type"] == "P"}
    if not p_nodes:
        return

    child_ids: set[str] = {
        e["to"] for e in edges if e.get("type") == "P_CONTAINS_P"
    }
    root_ids = set(p_nodes) - child_ids

    # Build P→P adjacency (parent → children via P_CONTAINS_P)
    p_adj: dict[str, list[str]] = {pid: [] for pid in p_nodes}
    for edge in edges:
        if edge.get("type") == "P_CONTAINS_P":
            frm = edge["from"]
            to  = edge["to"]
            if frm in p_adj:
                p_adj[frm].append(to)

    # BFS from all roots
    visited: set[str] = set()
    queue = deque(root_ids)
    while queue:
        nid = queue.popleft()
        if nid in visited:
            continue
        visited.add(nid)
        for child in p_adj.get(nid, []):
            if child not in visited:
                queue.append(child)

    for pid, p_node in p_nodes.items():
        if pid not in visited:
            errors.append(_make_error(
                "E037", "UnreachablePolicyNode",
                f"Policy '{p_node.get('label', pid)}' is not reachable from the root policy",
                node_id=pid,
            ))


# ── Public API — Checks 7–12 ──────────────────────────────────────────────

_GOVERNANCE_PERMISSIONS: frozenset[str] = frozenset({
    "can_create_rule",
    "can_delete_rule",
    "can_bind_policy",
    "can_register_service",
    "add_manager",
})


def validate_7_12(graph: dict) -> dict:
    """
    Run Stage 3 checks 7–12 against a PolicyGraph dict.

    Returns:
      {
        "errors":   [{"code", "name", "message", "node_id", "stage": 3}, ...],
        "warnings": [{"code", "name", "message", "node_id", "stage": 3}, ...],
      }

    All six checks always run regardless of earlier failures.
    """
    errors:   list[dict] = []
    warnings: list[dict] = []

    nodes: list[dict] = graph.get("nodes", [])
    edges: list[dict] = graph.get("edges", [])

    node_by_id: dict[str, dict] = {n["id"]: n for n in nodes}

    _check_7_inheritance_cycle(nodes, errors)
    _check_8_monotonicity(nodes, edges, node_by_id, warnings)
    _check_9_service_schema(nodes, errors)
    _check_10_return_contract(nodes, errors)
    _check_11_state_ownership(nodes, graph, errors)
    _check_12_permission_integrity(nodes, errors)

    return {"errors": errors, "warnings": warnings}


# ── Check 7 — Policy Inheritance Cycle (E036) ─────────────────────────────

def _check_7_inheritance_cycle(nodes: list[dict], errors: list[dict]) -> None:
    """
    DFS on the P→extends chain.
    Any node appearing twice in a single ancestry walk → E036.
    """
    # Build label→node map and parent map for P nodes.
    p_by_label: dict[str, dict] = {}
    for n in nodes:
        if n["type"] == "P":
            p_by_label[n["label"]] = n

    # parent_label[label] = extends label (or None for roots)
    parent_label: dict[str, str | None] = {
        label: n.get("extends")
        for label, n in p_by_label.items()
    }

    reported: set[str] = set()

    for start_label in p_by_label:
        visited_walk: set[str] = set()
        current = start_label
        while current is not None:
            if current in visited_walk:
                if current not in reported:
                    reported.add(current)
                    node = p_by_label.get(current)
                    errors.append(_make_error(
                        "E036", "PolicyGraphCycleDetected",
                        f"Policy inheritance cycle detected involving '{current}'",
                        node_id=node["id"] if node else None,
                    ))
                break
            visited_walk.add(current)
            current = parent_label.get(current)  # None if root or unknown


# ── Check 8 — Monotonicity (W001, Stage-A placeholder) ────────────────────

def _check_8_monotonicity(nodes: list[dict], edges: list[dict],
                          node_by_id: dict[str, dict],
                          warnings: list[dict]) -> None:
    """
    For every P→P (P_CONTAINS_P) edge:
      child services \ parent services ≠ ∅  → W001 InertPolicyBranch.

    Only direct R_TARGETS_S edges are considered (not recursive closure —
    that is Stage B's job).
    """
    # Build direct service label sets per P node (own rules only).
    def direct_services(p_id: str) -> set[str]:
        svcs: set[str] = set()
        for e in edges:
            if e.get("type") == "P_CONTAINS_R" and e["from"] == p_id:
                r_id = e["to"]
                for e2 in edges:
                    if e2.get("type") == "R_TARGETS_S" and e2["from"] == r_id:
                        svc = node_by_id.get(e2["to"])
                        if svc:
                            svcs.add(svc["label"])
        return svcs

    for edge in edges:
        if edge.get("type") != "P_CONTAINS_P":
            continue
        parent_id = edge["from"]
        child_id  = edge["to"]

        child_node  = node_by_id.get(child_id)
        parent_node = node_by_id.get(parent_id)
        if child_node is None or parent_node is None:
            continue

        child_svcs  = direct_services(child_id)
        parent_svcs = direct_services(parent_id)

        for svc in sorted(child_svcs - parent_svcs):
            warnings.append(_make_warning(
                "W001", "InertPolicyBranch",
                (f"Policy '{child_node['label']}' references service '{svc}' not "
                 f"declared in parent '{parent_node['label']}' — monotonicity "
                 "cannot be fully verified until Stage B"),
                node_id=child_id,
            ))


# ── Check 9 — Service Schema Presence (E006) ──────────────────────────────

def _check_9_service_schema(nodes: list[dict], errors: list[dict]) -> None:
    for n in nodes:
        if n["type"] != "S":
            continue
        schema = n.get("schema")
        if schema is None or schema == {}:
            errors.append(_make_error(
                "E006", "MissingServiceSchema",
                f"Service '{n['label']}' has no declared schema",
                node_id=n["id"],
            ))


# ── Check 10 — Service Return Contract (E014) ─────────────────────────────

def _check_10_return_contract(nodes: list[dict], errors: list[dict]) -> None:
    for n in nodes:
        if n["type"] != "S":
            continue
        schema = n.get("schema") or {}
        for method_name, method_def in schema.items():
            returns = method_def.get("returns") if isinstance(method_def, dict) else None
            if not returns:
                errors.append(_make_error(
                    "E014", "MissingReturnContract",
                    f"Service '{n['label']}.{method_name}' has no return type declared",
                    node_id=n["id"],
                ))


# ── Check 11 — State Schema Ownership (E015, E016) ────────────────────────

def _check_11_state_ownership(nodes: list[dict], graph: dict,
                              errors: list[dict]) -> None:
    state_registry: dict = graph.get("state_registry") or {}
    if not state_registry:
        return

    # Build ownership map: schema_name → [policy_label, ...]
    schema_owners: dict[str, list[str]] = {}
    for n in nodes:
        if n["type"] != "P":
            continue
        ss = n.get("state_schema")
        if ss:
            schema_owners.setdefault(ss, []).append(n["label"])

    for schema_name in state_registry:
        owners = schema_owners.get(schema_name, [])
        if len(owners) == 0:
            errors.append(_make_error(
                "E015", "UnownedStateSchema",
                f"State schema '{schema_name}' is declared but not owned by any policy",
                node_id=None,
            ))
        elif len(owners) > 1:
            labels = ", ".join(owners)
            errors.append(_make_error(
                "E016", "StateSchemaConflict",
                f"State schema '{schema_name}' is claimed by multiple policies: {labels}",
                node_id=None,
            ))
        else:
            # Exactly one owner — update owner_policy in place.
            state_registry[schema_name]["owner_policy"] = owners[0]


# ── Check 12 — Permission Integrity (E007) ────────────────────────────────

def _check_12_permission_integrity(nodes: list[dict],
                                   errors: list[dict]) -> None:
    for n in nodes:
        if n["type"] == "M":
            if not n.get("permissions"):
                errors.append(_make_error(
                    "E007", "PermissionViolation",
                    f"Manager '{n['label']}' has no permissions declared",
                    node_id=n["id"],
                ))

        elif n["type"] == "B":
            for perm in (n.get("permissions") or []):
                if perm in _GOVERNANCE_PERMISSIONS:
                    errors.append(_make_error(
                        "E007", "PermissionViolation",
                        (f"Member '{n['label']}' holds governance permission "
                         f"'{perm}' — members may not hold governance permissions"),
                        node_id=n["id"],
                    ))


# ── Error / warning factories ──────────────────────────────────────────────

def _make_error(code: str, name: str, message: str,
                node_id: str | None) -> dict:
    return {
        "code":    code,
        "name":    name,
        "message": message,
        "node_id": node_id,
        "stage":   3,
    }


def _make_warning(code: str, name: str, message: str,
                  node_id: str | None) -> dict:
    return {
        "code":    code,
        "name":    name,
        "message": message,
        "node_id": node_id,
        "stage":   3,
    }


# ══════════════════════════════════════════════════════════════════════════
# Public API — Checks 13–17
# ══════════════════════════════════════════════════════════════════════════

# Regex patterns for Check 14 and 15.
_RE_RESULT_FIELD   = _re.compile(r'result\("([^"]+)"\)\.(\w+)')
_RE_SERVICE_CALL   = _re.compile(r'[A-Z][A-Za-z0-9]*Service\s*\(')
_RE_STATE_WRITE    = _re.compile(r'state\.[A-Za-z]+\.[A-Za-z]+\s*=(?!=)')

# Non-deterministic name patterns for W013.
_NONDETERMINISM_PATTERNS = ("random", "uuid4", "timestamp", "clock", "datetime")


def validate_13_17(graph: dict) -> dict:
    """
    Run Stage 3 checks 13–17 against a PolicyGraph dict.

    Returns:
      {
        "errors":   [{"code", "name", "message", "node_id", "stage": 3}, ...],
        "warnings": [{"code", "name", "message", "node_id", "stage": 3}, ...],
      }

    All five checks always run regardless of earlier failures.
    """
    errors:   list[dict] = []
    warnings: list[dict] = []

    nodes: list[dict] = graph.get("nodes", [])
    edges: list[dict] = graph.get("edges", [])

    node_by_id: dict[str, dict] = {n["id"]: n for n in nodes}

    _check_13_rule_dep_acyclicity(nodes, edges, node_by_id, errors)
    _check_14_result_type(nodes, graph, errors)
    _check_15_condition_purity(nodes, errors)
    _check_16_service_signature(nodes, edges, node_by_id, graph, errors)
    _check_17_determinism(nodes, edges, node_by_id, errors, warnings)

    return {"errors": errors, "warnings": warnings}


# ── Check 13 — Rule Dependency Acyclicity (E017, E018) ────────────────────

def _check_13_rule_dep_acyclicity(nodes: list[dict], edges: list[dict],
                                   node_by_id: dict[str, dict],
                                   errors: list[dict]) -> None:
    """
    Per P node: collect its R children, check depends_on for
    unresolved references (E018) and cycles (E017).
    """
    # Build P → [R node ids] map via P_CONTAINS_R edges.
    p_rules: dict[str, list[str]] = {}
    for edge in edges:
        if edge.get("type") == "P_CONTAINS_R":
            p_rules.setdefault(edge["from"], []).append(edge["to"])

    for p_id, r_ids in p_rules.items():
        p_node = node_by_id.get(p_id)
        p_label = p_node["label"] if p_node else p_id

        r_nodes = [node_by_id[rid] for rid in r_ids if rid in node_by_id
                   and node_by_id[rid]["type"] == "R"]

        rule_by_label: dict[str, dict] = {r["label"]: r for r in r_nodes}

        # E018 — unresolved depends_on
        for r_node in r_nodes:
            dep = r_node.get("depends_on")
            if dep and dep not in rule_by_label:
                errors.append(_make_error(
                    "E018", "UnresolvedRuleDependency",
                    (f"Rule '{r_node['label']}' depends_on '{dep}' which does not "
                     f"exist in policy '{p_label}'"),
                    node_id=r_node["id"],
                ))

        # E017 — dependency cycle (DFS)
        reported: set[str] = set()

        def _dfs_cycle(start_label: str) -> None:
            stack: list[str] = []
            on_stack: set[str] = set()

            def _visit(label: str) -> bool:
                if label not in rule_by_label:
                    return False          # dangling dep — already flagged E018
                if label in on_stack:
                    if label not in reported:
                        reported.add(label)
                        node = rule_by_label[label]
                        errors.append(_make_error(
                            "E017", "RuleDependencyCycle",
                            (f"Rule dependency cycle detected in policy '{p_label}' "
                             f"involving rule '{label}'"),
                            node_id=node["id"],
                        ))
                    return True
                if label in stack:
                    return False          # already fully explored, no cycle from here
                stack.append(label)
                on_stack.add(label)
                dep = rule_by_label[label].get("depends_on")
                if dep:
                    _visit(dep)
                on_stack.discard(label)
                return False

            _visit(start_label)

        for label in rule_by_label:
            _dfs_cycle(label)


# ── Check 14 — Result Type Checking (E019) ────────────────────────────────

def _check_14_result_type(nodes: list[dict], graph: dict,
                          errors: list[dict]) -> None:
    """
    Scan R node 'when' strings for result("Svc.method").field patterns
    and verify each field exists in the service's output schema.
    """
    service_registry: dict = graph.get("service_registry") or {}

    for n in nodes:
        if n["type"] != "R":
            continue
        when = n.get("when") or ""
        for match in _RE_RESULT_FIELD.finditer(when):
            svc_method = match.group(1)   # "ServiceName.method"
            field_name = match.group(2)   # "fieldName"

            parts = svc_method.split(".", 1)
            if len(parts) != 2:
                continue
            svc_name, method_name = parts

            svc_entry = service_registry.get(svc_name)
            if svc_entry is None:
                errors.append(_make_error(
                    "E019", "ResultTypeMismatch",
                    (f"result('{svc_method}').{field_name} — "
                     f"service '{svc_name}' not found in service registry"),
                    node_id=n["id"],
                ))
                continue

            output_schema = (svc_entry.get("output_schema") or {}).get(method_name)
            if output_schema is None:
                errors.append(_make_error(
                    "E019", "ResultTypeMismatch",
                    (f"result('{svc_method}').{field_name} — "
                     f"method '{method_name}' not found in {svc_name} output schema"),
                    node_id=n["id"],
                ))
                continue

            # Skip field check if output schema is empty (Stage B will enforce).
            if not output_schema:
                continue

            if field_name not in output_schema:
                errors.append(_make_error(
                    "E019", "ResultTypeMismatch",
                    (f"result('{svc_method}').{field_name} — "
                     f"field '{field_name}' not found in {svc_name}.{method_name} "
                     f"output schema"),
                    node_id=n["id"],
                ))


# ── Check 15 — Rule Condition Purity (E043) ───────────────────────────────

def _check_15_condition_purity(nodes: list[dict], errors: list[dict]) -> None:
    """
    Flag R nodes whose 'when' condition contains:
      - a service invocation: PascalCaseService(
      - a state write:        state.Schema.field = (not ==)
    """
    for n in nodes:
        if n["type"] != "R":
            continue
        when = n.get("when") or ""
        if not when:
            continue

        if _RE_SERVICE_CALL.search(when) or _RE_STATE_WRITE.search(when):
            errors.append(_make_error(
                "E043", "ImpureRuleCondition",
                (f"Rule '{n['label']}' when condition contains impure expression: "
                 "service invocation or state write not permitted in conditions"),
                node_id=n["id"],
            ))


# ── Check 16 — Service Signature Match (E040) ─────────────────────────────

def _check_16_service_signature(nodes: list[dict], edges: list[dict],
                                 node_by_id: dict[str, dict],
                                 graph: dict, errors: list[dict]) -> None:
    """
    For each R node targeting a service, verify all required params are present.
    """
    service_registry: dict = graph.get("service_registry") or {}

    for n in nodes:
        if n["type"] != "R":
            continue
        if n.get("target_type") != "service":
            continue

        target_label  = n.get("target_label") or ""   # e.g. "ScanService"
        target_method = n.get("target_method") or ""  # e.g. "scan"

        svc_entry = service_registry.get(target_label)
        if svc_entry is None:
            continue  # E002 already covers unknown services

        input_schema: dict = svc_entry.get("input_schema") or {}
        method_params = input_schema.get(target_method)
        if method_params is None:
            errors.append(_make_error(
                "E040", "ServiceSignatureMismatch",
                (f"Rule '{n['label']}' calls {target_label}.{target_method} "
                 f"but method '{target_method}' not found in service schema"),
                node_id=n["id"],
            ))
            continue

        actual_params: dict = n.get("target_params") or {}

        # method_params is a list of {name, type, required} dicts
        for param in (method_params if isinstance(method_params, list) else []):
            if param.get("required") and param["name"] not in actual_params:
                errors.append(_make_error(
                    "E040", "ServiceSignatureMismatch",
                    (f"Rule '{n['label']}' calls {target_label}.{target_method} "
                     f"with missing required parameter '{param['name']}'"),
                    node_id=n["id"],
                ))


# ── Check 17 — Determinism in Learning Path (E041, W013) ──────────────────

def _check_17_determinism(nodes: list[dict], edges: list[dict],
                           node_by_id: dict[str, dict],
                           errors: list[dict], warnings: list[dict]) -> None:
    """
    E041: R node targets an S node with allow_nondeterminism == True.
    W013: S node name pattern suggests non-determinism but flag not set.
    """
    # Build R → S target map from R_TARGETS_S edges.
    r_to_s: dict[str, str] = {}
    for edge in edges:
        if edge.get("type") == "R_TARGETS_S":
            r_to_s[edge["from"]] = edge["to"]

    for n in nodes:
        if n["type"] == "R":
            s_id = r_to_s.get(n["id"])
            if s_id is None:
                continue
            s_node = node_by_id.get(s_id)
            if s_node is None:
                continue
            annotations = s_node.get("annotations") or {}
            if annotations.get("allow_nondeterminism") is True:
                errors.append(_make_error(
                    "E041", "NonDeterministicServiceInLearningPath",
                    (f"Rule '{n['label']}' targets service '{s_node['label']}' "
                     "annotated @allow_nondeterminism — non-deterministic services "
                     "must not appear in the residual learning path"),
                    node_id=n["id"],
                ))

        elif n["type"] == "S":
            annotations = n.get("annotations") or {}
            if annotations.get("allow_nondeterminism") is True:
                continue  # explicitly declared — E041 covers this via R node
            label_lower = n["label"].lower()
            if any(pat in label_lower for pat in _NONDETERMINISM_PATTERNS):
                warnings.append(_make_warning(
                    "W013", "NonDeterministicPattern",
                    (f"Service '{n['label']}' may be non-deterministic "
                     "(name pattern suggests randomness/time dependency). "
                     "Annotate with allow_nondeterminism: true if intentional."),
                    node_id=n["id"],
                ))


# ══════════════════════════════════════════════════════════════════════════
# Public API — Stage 4: Authority Chain Validation
# ══════════════════════════════════════════════════════════════════════════

def validate_stage4(graph: dict) -> dict:
    """
    Run Stage 4 authority-chain checks.

    Returns { "errors": [...], "warnings": [...] }
    All four checks always run.
    """
    errors:   list[dict] = []
    warnings: list[dict] = []

    nodes: list[dict] = graph.get("nodes", [])
    edges: list[dict] = graph.get("edges", [])
    actor_hierarchy: dict = graph.get("actor_hierarchy") or {}

    node_by_id: dict[str, dict] = {n["id"]: n for n in nodes}

    _s4_check_1_actor_scope_declared(nodes, errors)
    _s4_check_2_actor_scope_valid(nodes, actor_hierarchy, errors)
    _s4_check_3_service_actor_binding(nodes, edges, node_by_id, warnings)
    _s4_check_4_simulation_safety(nodes, edges, node_by_id, errors)

    return {"errors": errors, "warnings": warnings}


# ── Stage 4, Check 4-1 — Actor Scope Declaration (E031) ───────────────────

def _s4_check_1_actor_scope_declared(nodes: list[dict],
                                      errors: list[dict]) -> None:
    for n in nodes:
        if n["type"] != "P":
            continue
        scope = n.get("actor_scope")
        if scope is None or scope == []:
            errors.append(_make_error(
                "E031", "MissingActorScope",
                f"Policy '{n['label']}' does not declare actor_scope",
                node_id=n["id"],
            ))


# ── Stage 4, Check 4-2 — Actor Scope References Valid Actors (E032) ───────

def _s4_check_2_actor_scope_valid(nodes: list[dict],
                                   actor_hierarchy: dict,
                                   errors: list[dict]) -> None:
    for n in nodes:
        if n["type"] != "P":
            continue
        for actor_name in (n.get("actor_scope") or []):
            if actor_name not in actor_hierarchy:
                errors.append(_make_error(
                    "E032", "ActorScopeUndeclared",
                    (f"Policy '{n['label']}' actor_scope references '{actor_name}' "
                     "which is not declared in the actors block"),
                    node_id=n["id"],
                ))


# ── Stage 4, Check 4-3 — Service Actor Scope (W015, advisory) ────────────

def _s4_check_3_service_actor_binding(nodes: list[dict], edges: list[dict],
                                       node_by_id: dict[str, dict],
                                       warnings: list[dict]) -> None:
    """
    Advisory: P has actor_scope but targeted S has no actor binding declared.
    Runtime enforces E033; we emit W015 here as a heads-up.
    """
    p_to_r: dict[str, list[str]] = {}
    r_to_s: dict[str, str]       = {}
    for edge in edges:
        if edge.get("type") == "P_CONTAINS_R":
            p_to_r.setdefault(edge["from"], []).append(edge["to"])
        elif edge.get("type") == "R_TARGETS_S":
            r_to_s[edge["from"]] = edge["to"]

    for n in nodes:
        if n["type"] != "P":
            continue
        scope = n.get("actor_scope") or []
        if not scope:
            continue   # E031 already owns this

        for r_id in p_to_r.get(n["id"], []):
            s_id = r_to_s.get(r_id)
            if s_id is None:
                continue
            s_node = node_by_id.get(s_id)
            if s_node is None:
                continue
            annotations = s_node.get("annotations") or {}
            if "actor_scope" not in annotations and "actor_binding" not in annotations:
                warnings.append(_make_warning(
                    "W015", "ActorScopeUnchecked",
                    (f"Policy '{n['label']}' has actor_scope {scope} but "
                     f"service '{s_node['label']}' declares no actor binding — "
                     "runtime will enforce E033"),
                    node_id=n["id"],
                ))
                break   # one warning per P node is enough


# ── Stage 4, Check 4-4 — Simulation Safety (E045) ─────────────────────────

def _s4_check_4_simulation_safety(nodes: list[dict], edges: list[dict],
                                   node_by_id: dict[str, dict],
                                   errors: list[dict]) -> None:
    """
    Simulation P nodes (label contains 'Simulation' or 'Sim') may only
    reference services annotated simulation_safe: true.
    """
    p_to_r: dict[str, list[str]] = {}
    r_to_s: dict[str, str]       = {}
    for edge in edges:
        if edge.get("type") == "P_CONTAINS_R":
            p_to_r.setdefault(edge["from"], []).append(edge["to"])
        elif edge.get("type") == "R_TARGETS_S":
            r_to_s[edge["from"]] = edge["to"]

    for p_node in nodes:
        if p_node["type"] != "P":
            continue
        label = p_node["label"]
        if "Simulation" not in label and "Sim" not in label:
            continue

        for r_id in p_to_r.get(p_node["id"], []):
            r_node = node_by_id.get(r_id)
            if r_node is None:
                continue
            s_id = r_to_s.get(r_id)
            if s_id is None:
                continue
            s_node = node_by_id.get(s_id)
            if s_node is None:
                continue
            annotations = s_node.get("annotations") or {}
            if annotations.get("simulation_safe") is not True:
                errors.append(_make_error(
                    "E045", "SimulationUnsafeService",
                    (f"Simulation policy '{label}' rule '{r_node['label']}' "
                     f"references service '{s_node['label']}' which is not "
                     "declared simulation_safe: true"),
                    node_id=r_id,
                ))


# ══════════════════════════════════════════════════════════════════════════
# Public API — Stage 5: Capability Closure Generation
# ══════════════════════════════════════════════════════════════════════════

def validate_stage5(graph: dict) -> dict:
    """
    Stage 5: compute capability_closure, assign rule_order_index,
    and emit W017 actor scope overlap warnings.
    E005 monotonicity upgrade deferred to Stage C.

    MUTATES graph["nodes"] in-place (adds capability_closure to P nodes
    and rule_order_index to R nodes).

    Returns { "errors": [...], "warnings": [...] }
    """
    errors:   list[dict] = []
    warnings: list[dict] = []

    nodes: list[dict] = graph.get("nodes", [])
    edges: list[dict] = graph.get("edges", [])

    node_by_id: dict[str, dict] = {n["id"]: n for n in nodes}

    # ── Build supporting indexes ──────────────────────────────────────────
    p_to_r:          dict[str, list[str]] = {}
    child_to_parent: dict[str, str]       = {}
    r_to_s:          dict[str, str]       = {}

    for edge in edges:
        t = edge.get("type")
        if t == "P_CONTAINS_R":
            p_to_r.setdefault(edge["from"], []).append(edge["to"])
        elif t == "P_CONTAINS_P":
            child_to_parent[edge["to"]] = edge["from"]
        elif t == "R_TARGETS_S":
            r_to_s[edge["from"]] = edge["to"]

    p_nodes: list[dict] = [n for n in nodes if n["type"] == "P"]

    # ── own_services per P (direct R→S only) ─────────────────────────────
    own_svcs: dict[str, set[str]] = {}
    for p_node in p_nodes:
        svcs: set[str] = set()
        for r_id in p_to_r.get(p_node["id"], []):
            s_id = r_to_s.get(r_id)
            if s_id:
                s_node = node_by_id.get(s_id)
                if s_node:
                    svcs.add(s_node["label"])
        own_svcs[p_node["id"]] = svcs

    # ── Topological order of P nodes (roots first) ────────────────────────
    p_children: dict[str, list[str]] = {n["id"]: [] for n in p_nodes}
    for child_id, parent_id in child_to_parent.items():
        if parent_id in p_children:
            p_children[parent_id].append(child_id)

    in_deg: dict[str, int] = {n["id"]: 0 for n in p_nodes}
    for child_id in child_to_parent:
        if child_id in in_deg:
            in_deg[child_id] += 1

    topo_queue: deque = deque(pid for pid in in_deg if in_deg[pid] == 0)
    topo_order: list[str] = []
    while topo_queue:
        pid = topo_queue.popleft()
        topo_order.append(pid)
        for child_id in p_children.get(pid, []):
            in_deg[child_id] -= 1
            if in_deg[child_id] == 0:
                topo_queue.append(child_id)

    for p_node in p_nodes:
        if p_node["id"] not in topo_order:
            topo_order.append(p_node["id"])

    # ── Compute capability_closure in topo order ──────────────────────────
    closure: dict[str, set[str]] = {}

    for pid in topo_order:
        parent_id  = child_to_parent.get(pid)
        parent_cls = closure.get(parent_id, set()) if parent_id else set()
        closure[pid] = own_svcs.get(pid, set()) | parent_cls
        p_node = node_by_id.get(pid)
        if p_node:
            p_node["capability_closure"] = sorted(closure[pid])

    # ── Rule order index assignment ───────────────────────────────────────
    for p_node in p_nodes:
        r_ids = p_to_r.get(p_node["id"], [])
        r_nodes_here = [node_by_id[rid] for rid in r_ids
                        if rid in node_by_id and node_by_id[rid]["type"] == "R"]
        if not r_nodes_here:
            continue

        label_to_node:  dict[str, dict]      = {r["label"]: r for r in r_nodes_here}
        dep_in_deg:     dict[str, int]        = {r["label"]: 0 for r in r_nodes_here}
        dep_children:   dict[str, list[str]]  = {r["label"]: [] for r in r_nodes_here}

        for r_node in r_nodes_here:
            dep = r_node.get("depends_on")
            if dep and dep in dep_in_deg:
                dep_in_deg[r_node["label"]] += 1
                dep_children[dep].append(r_node["label"])

        dep_queue: deque = deque(
            r["label"] for r in r_nodes_here if dep_in_deg[r["label"]] == 0
        )
        idx = 0
        while dep_queue:
            lbl = dep_queue.popleft()
            node = label_to_node[lbl]
            node["rule_order_index"] = idx
            idx += 1
            for child_lbl in dep_children.get(lbl, []):
                dep_in_deg[child_lbl] -= 1
                if dep_in_deg[child_lbl] == 0:
                    dep_queue.append(child_lbl)

        for r_node in r_nodes_here:
            if r_node.get("rule_order_index") is None:
                r_node["rule_order_index"] = idx
                idx += 1

    # ── W017 Actor Scope Overlap ──────────────────────────────────────────
    siblings_by_parent: dict = {}
    for p_node in p_nodes:
        parent_id = child_to_parent.get(p_node["id"])
        siblings_by_parent.setdefault(parent_id, []).append(p_node)

    for parent_id, siblings in siblings_by_parent.items():
        if len(siblings) < 2:
            continue
        for i in range(len(siblings)):
            for j in range(i + 1, len(siblings)):
                a = siblings[i]
                b = siblings[j]
                scope_a = set(a.get("actor_scope") or [])
                scope_b = set(b.get("actor_scope") or [])
                overlap = scope_a & scope_b
                if overlap:
                    warnings.append(_make_warning(
                        "W017", "ActorScopeOverlap",
                        (f"Sibling policies '{a['label']}' and '{b['label']}' "
                         f"have overlapping actor_scope: {sorted(overlap)}"),
                        node_id=a["id"],
                    ))

    return {"errors": errors, "warnings": warnings}
