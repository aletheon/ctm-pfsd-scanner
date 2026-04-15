"""
policy_compiler.graph_builder — builds a PolicyGraph from a parsed AST.

INPUT:  ast:          dict  (from parser.parse())
        project_name: str   (namespace fallback when ast["namespace"] is null)
OUTPUT: dict  (GraphResult — nodes, edges, and three indexes)

Two-pass construction:
  Pass 1  — create all node dicts, populate a label→node lookup table.
  Pass 2  — build all edges (references resolved against pass-1 lookup).
  Pass 3  — build actor hierarchy, state registry, and service registry.

§48 boundary: stdlib only (uuid).
Zone 3 purity: no imports from any project file except policy_compiler.
"""
from __future__ import annotations
import uuid as _uuid

# ── UUID helpers ───────────────────────────────────────────────────────────

# DNS OID namespace — used for all content-addressed UUIDs in this compiler.
_NAMESPACE_OID = _uuid.UUID("6ba7b810-9dad-11d1-80b4-00c04fd430c8")


def _make_uuid(fqn: str) -> str:
    """Deterministic UUID v5 from a fully-qualified name string."""
    return str(_uuid.uuid5(_NAMESPACE_OID, fqn))


# ── Public error type ──────────────────────────────────────────────────────

class BuildError(Exception):
    """
    Raised when a reference cannot be resolved during graph construction.

    Attributes:
      code    — error code string ("E002", "E020", "E021")
      node_id — UUID of the node that owns the bad reference, or None
    """

    def __init__(self, message: str, code: str,
                 node_id: str | None = None) -> None:
        super().__init__(message)
        self.message = message
        self.code    = code
        self.node_id = node_id

    def __str__(self) -> str:
        return self.message


# ── Public API ─────────────────────────────────────────────────────────────

def build(ast: dict, project_name: str) -> dict:
    """
    Build a PolicyGraph from a parsed PolicyScript AST.

    Returns:
      {
        "namespace":        str,
        "nodes":            [node, ...],
        "edges":            [edge, ...],
        "actor_hierarchy":  dict,
        "state_registry":   dict,
        "service_registry": dict,
      }

    Raises BuildError(code="E002") if a rule target cannot be resolved
    to any known service or policy node.
    """
    ns = ast.get("namespace") or project_name

    nodes: list[dict] = []

    # by_key lookup: ("P", name) | ("S", name) | ("M", name) | ("B", name) → node
    by_key: dict[tuple[str, str], dict] = {}

    # Rules are tracked per policy for the edge pass.
    rules_by_policy: dict[str, list[dict]] = {}

    # ── Pass 1A: Service nodes ─────────────────────────────────────────────
    for svc_name, svc_data in ast.get("services", {}).items():
        fqn = f"{ns}.{svc_name}"
        uid = _make_uuid(fqn)
        node: dict = {
            "id":          uid,
            "type":        "S",
            "label":       svc_name,
            "fqn":         fqn,
            "namespace":   ns,
            "endpoint":    svc_data.get("endpoint"),
            "schema":      svc_data.get("schema", {}),
            "annotations": svc_data.get("annotations", {}),
            "bound_rules": [],
        }
        nodes.append(node)
        by_key[("S", svc_name)] = node

    # ── Pass 1B: Policy and Rule nodes ─────────────────────────────────────
    for pol_name, pol_data in ast.get("policies", {}).items():
        fqn = f"{ns}.{pol_name}"
        uid = _make_uuid(fqn)

        rule_uuids: list[str] = []
        rule_nodes: list[dict] = []

        for rule_data in pol_data.get("rules", []):
            r_fqn = f"{ns}.{pol_name}.{rule_data['name']}"
            r_uid = _make_uuid(r_fqn)

            target = rule_data["target"]
            target_type, target_method = _classify_target(target, ast)

            r_node: dict = {
                "id":               r_uid,
                "type":             "R",
                "label":            rule_data["name"],
                "fqn":              r_fqn,
                "namespace":        ns,
                "when":             rule_data.get("when"),
                "depends_on":       rule_data.get("depends_on"),
                "target_type":      target_type,
                "target_label":     target,
                "target_method":    target_method,
                "target_params":    rule_data.get("params"),
                "rule_order_index": None,   # assigned by Stage 3
            }
            rule_nodes.append(r_node)
            rule_uuids.append(r_uid)
            nodes.append(r_node)

        rules_by_policy[pol_name] = rule_nodes

        p_node: dict = {
            "id":           uid,
            "type":         "P",
            "label":        pol_name,
            "fqn":          fqn,
            "namespace":    ns,
            "actor_scope":  pol_data.get("actor_scope", []),
            "state_schema": pol_data.get("state"),
            "extends":      pol_data.get("extends"),
            "rules":        rule_uuids,
            "meta":         {"is_root": pol_data.get("extends") is None},
        }
        nodes.append(p_node)
        by_key[("P", pol_name)] = p_node

    # ── Pass 1C: Manager nodes ─────────────────────────────────────────────
    for mgr_name, mgr_data in ast.get("managers", {}).items():
        fqn = f"{ns}.{mgr_name}"
        uid = _make_uuid(fqn)
        node = {
            "id":          uid,
            "type":        "M",
            "label":       mgr_name,
            "fqn":         fqn,
            "namespace":   ns,
            "permissions": mgr_data.get("permissions", []),
        }
        nodes.append(node)
        by_key[("M", mgr_name)] = node

    # ── Pass 1D: Member nodes ──────────────────────────────────────────────
    for mem_name, mem_data in ast.get("members", {}).items():
        fqn = f"{ns}.{mem_name}"
        uid = _make_uuid(fqn)
        node = {
            "id":          uid,
            "type":        "B",
            "label":       mem_name,
            "fqn":         fqn,
            "namespace":   ns,
            "permissions": mem_data.get("permissions", []),
        }
        nodes.append(node)
        by_key[("B", mem_name)] = node

    # ── Pass 2: Edge construction ──────────────────────────────────────────
    edges: list[dict] = []

    for pol_name, pol_data in ast.get("policies", {}).items():
        p_node = by_key[("P", pol_name)]

        # P_CONTAINS_P — parent → child via extends
        extends = pol_data.get("extends")
        if extends:
            if ("P", extends) not in by_key:
                raise BuildError(
                    f"E002 UnknownReference: policy '{extends}' not found "
                    f"(referenced via 'extends' in '{pol_name}')",
                    "E002",
                    p_node["id"],
                )
            parent_node = by_key[("P", extends)]
            edges.append({
                "from": parent_node["id"],
                "to":   p_node["id"],
                "type": "P_CONTAINS_P",
            })

        # P_CONTAINS_R + R_TARGETS_*
        for r_node in rules_by_policy.get(pol_name, []):
            edges.append({
                "from": p_node["id"],
                "to":   r_node["id"],
                "type": "P_CONTAINS_R",
            })

            t_type  = r_node["target_type"]
            t_label = r_node["target_label"]

            if t_type == "service":
                # Service name is the part before the first dot (or the whole label)
                svc_name = t_label.split(".", 1)[0] if "." in t_label else t_label
                if ("S", svc_name) not in by_key:
                    raise BuildError(
                        f"E002 UnknownReference: service '{svc_name}' not found "
                        f"(referenced in rule '{r_node['label']}' of policy '{pol_name}')",
                        "E002",
                        r_node["id"],
                    )
                s_node = by_key[("S", svc_name)]
                edges.append({
                    "from": r_node["id"],
                    "to":   s_node["id"],
                    "type": "R_TARGETS_S",
                })

            elif t_type == "policy":
                if ("P", t_label) not in by_key:
                    raise BuildError(
                        f"E002 UnknownReference: policy '{t_label}' not found "
                        f"(referenced in rule '{r_node['label']}' of policy '{pol_name}')",
                        "E002",
                        r_node["id"],
                    )
                target_p = by_key[("P", t_label)]
                edges.append({
                    "from": r_node["id"],
                    "to":   target_p["id"],
                    "type": "R_TARGETS_P",
                })

            elif t_type == "forum":
                # Forum nodes are not declared in source — edge emitted symbolically.
                # Stage 3 will validate forum reachability.
                edges.append({
                    "from": r_node["id"],
                    "to":   _make_uuid(f"{ns}.{t_label}"),
                    "type": "R_TARGETS_F",
                })

    # P_BINDS_M — heuristic: only when policy name == manager name exactly.
    for mgr_name in ast.get("managers", {}):
        if ("P", mgr_name) in by_key and ("M", mgr_name) in by_key:
            edges.append({
                "from": by_key[("P", mgr_name)]["id"],
                "to":   by_key[("M", mgr_name)]["id"],
                "type": "P_BINDS_M",
            })

    # ── Pass 3: Build indexes ──────────────────────────────────────────────
    actor_hierarchy  = _build_actor_hierarchy(ast.get("actors", {}), ns, parent=None)
    state_registry   = _build_state_registry(ast.get("states", {}))
    service_registry = _build_service_registry(ast.get("services", {}), ns)

    return {
        "namespace":        ns,
        "nodes":            nodes,
        "edges":            edges,
        "actor_hierarchy":  actor_hierarchy,
        "state_registry":   state_registry,
        "service_registry": service_registry,
    }


# ── Target classification ──────────────────────────────────────────────────

def _classify_target(target: str, ast: dict) -> tuple[str, str | None]:
    """
    Classify a rule target string into (target_type, target_method).

    Rules:
      '.' in target  → "service",  method = part after first dot
      target in policies → "policy", method = None
      otherwise      → "service" (unresolved — E002 raised in edge pass)
    """
    if "." in target:
        method = target.split(".", 1)[1]
        return "service", method
    if target in ast.get("policies", {}):
        return "policy", None
    # Unresolved — treat as service; E002 fires in the edge pass
    return "service", None


# ── Index builders ─────────────────────────────────────────────────────────

def _build_actor_hierarchy(actor_dict: dict, ns: str,
                           parent: str | None) -> dict:
    """
    Flatten a nested actor dict (from AST) into a flat lookup:
      { actor_name: { id, parent, children } }

    Recursively processes children, propagating the parent name downward.
    """
    result: dict = {}
    for name, data in actor_dict.items():
        fqn          = f"{ns}.actor.{name}"
        uid          = _make_uuid(fqn)
        children_raw = data.get("children", {})
        result[name] = {
            "id":       uid,
            "parent":   parent,
            "children": list(children_raw.keys()),
        }
        # Recurse — children inherit this node as their parent
        sub = _build_actor_hierarchy(children_raw, ns, parent=name)
        result.update(sub)
    return result


def _build_state_registry(states: dict) -> dict:
    """
    Build the State Schema Registry from ast["states"].
    owner_policy is null here — assigned by Stage 3 check 11.
    """
    registry: dict = {}
    for schema_name, schema_data in states.items():
        registry[schema_name] = {
            "fields":       schema_data.get("fields", {}),
            "owner_policy": None,   # Stage 3 fills this
        }
    return registry


def _build_service_registry(services: dict, ns: str) -> dict:
    """
    Build the Service Registry from ast["services"].
    Separates input_schema (params) from output_schema (return type)
    per method. bound_rules is empty — populated by Stage 3.
    """
    registry: dict = {}
    for svc_name, svc_data in services.items():
        fqn = f"{ns}.{svc_name}"
        uid = _make_uuid(fqn)

        input_schema:  dict = {}
        output_schema: dict = {}

        for method_name, method_data in svc_data.get("schema", {}).items():
            input_schema[method_name] = [
                {"name": p["name"], "type": p["type"], "required": True}
                for p in method_data.get("params", [])
            ]
            output_schema[method_name] = {
                "type": method_data.get("returns", "unknown")
            }

        registry[svc_name] = {
            "service_uuid":  uid,
            "fqn":           fqn,
            "input_schema":  input_schema,
            "output_schema": output_schema,
            "annotations":   svc_data.get("annotations", {}),
            "bound_rules":   [],
        }
    return registry
