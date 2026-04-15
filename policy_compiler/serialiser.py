"""
policy_compiler.serialiser — Stage 6 (Residual Schema) and Stage 7 (Serialisation).

ZONE 3 CONSTRAINT:
  This module ONLY detects and describes — it never infers, completes,
  defaults, or suggests governance values. Governance fields (intent_origin,
  owner_policy, actor, safety_trigger) are runtime-populated. The schema
  records their expected TYPES only.

§48 boundary: stdlib only (json, hashlib, datetime).
"""
from __future__ import annotations
import json
import hashlib
from datetime import datetime, timezone

_COMPILER_VERSION      = "0.8.0-stage-c"
_SPEC_VERSION          = "CTM-PFSD-v0.8"
_POLICY_GRAPH_VERSION  = "0.1.0"
_MIN_RUNTIME_VERSION   = "0.8.0"
_INTENT_SCHEMA_VERSION = "1.0"

# Required fields: type declarations only — no values, no defaults.
_REQUIRED_FIELDS: dict[str, str] = {
    "residual_id":            "uuid",
    "intent_id":              "uuid",
    "intent_origin":          "string",
    "owner_policy":           "string",
    "actor":                  "string",
    "tick":                   "int",
    "safety_trigger":         "string|null",
    "exclude_from_learning":  "bool",
    "delta_magnitude":        "float",
}

# Fields an intent declaration must contain for E046 to NOT fire.
_INTENT_REQUIRED_FIELDS: frozenset[str] = frozenset({
    "intent_origin", "owner_policy", "actor",
})


# ── Stage 6 ────────────────────────────────────────────────────────────────

def generate_residual_schema(graph: dict) -> dict:
    """
    Stage 6 — Residual Schema Registration.

    Describes field TYPES only. Never fills, infers, or defaults values.
    Returns { "schema": dict, "error": None } on success,
            { "schema": None, "error": error_dict } on E046 failure.

    E046 fires ONLY when graph["intents"] is non-empty AND any intent
    declaration is missing required structural fields.
    Absent intents: valid — schema with type-only entries returned.
    """
    nodes = graph.get("nodes", [])

    # Detect health-monitored services (describe only — no inference).
    health_keys: list[str] = sorted(
        n["label"] for n in nodes
        if n["type"] == "S"
        and (n.get("annotations") or {}).get("health_monitor") is True
    )

    # E046 guard: malformed intent declarations only.
    intents: dict = graph.get("intents") or {}
    for intent_name, intent_data in intents.items():
        if not isinstance(intent_data, dict):
            return {
                "schema": None,
                "error": _make_e046(
                    f"Intent '{intent_name}' declaration is malformed — expected dict"
                ),
            }
        missing = _INTENT_REQUIRED_FIELDS - set(intent_data.keys())
        if missing:
            sorted_missing = sorted(missing)
            return {
                "schema": None,
                "error": _make_e046(
                    f"Intent '{intent_name}' declaration is missing required "
                    f"structural fields: {sorted_missing}"
                ),
            }

    schema: dict = {
        "required_fields":     dict(_REQUIRED_FIELDS),
        "service_health_keys": health_keys,
    }
    return {"schema": schema, "error": None}


def _make_e046(message: str) -> dict:
    return {
        "code":    "E046",
        "name":    "ResidualSchemaMismatch",
        "message": message,
        "node_id": None,
        "stage":   6,
    }


# ── Stage 7 ────────────────────────────────────────────────────────────────

def serialise(graph: dict, project_name: str) -> dict:
    """
    Stage 7 — Serialisation.

    Returns a serialised_graph dict ready for PIC Chain commit.
    Applies canonical ordering, computes graph_hash (SHA-256),
    and assembles the full output payload.
    """
    # ── Step 7-1: Canonical ordering (§24.4) ─────────────────────────────
    nodes = graph.get("nodes", [])

    p_nodes = sorted([n for n in nodes if n["type"] == "P"], key=lambda n: n["id"])
    r_nodes = sorted([n for n in nodes if n["type"] == "R"], key=lambda n: n["id"])
    s_nodes = sorted([n for n in nodes if n["type"] == "S"], key=lambda n: n["id"])
    other_nodes = sorted(
        [n for n in nodes if n["type"] not in ("P", "R", "S")],
        key=lambda n: (n.get("type", ""), n["id"]),
    )
    sorted_nodes = p_nodes + r_nodes + s_nodes + other_nodes

    sorted_edges = sorted(
        graph.get("edges", []),
        key=lambda e: (e["from"], e["to"]),
    )

    # Ensure capability_closure lists are sorted on each P node copy.
    for n in sorted_nodes:
        if n["type"] == "P" and "capability_closure" in n:
            n["capability_closure"] = sorted(n["capability_closure"])

    # Sort registry keys alphabetically.
    svc_reg   = {k: graph["service_registry"][k]
                 for k in sorted(graph.get("service_registry") or {})}
    act_hier  = {k: graph["actor_hierarchy"][k]
                 for k in sorted(graph.get("actor_hierarchy") or {})}
    state_reg = {k: graph["state_registry"][k]
                 for k in sorted(graph.get("state_registry") or {})}

    # ── Step 7-2: Compiler metadata ──────────────────────────────────────
    compile_timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    source_hash       = hashlib.sha256(project_name.encode("utf-8")).hexdigest()

    compiler_metadata: dict = {
        "compiler_version":      _COMPILER_VERSION,
        "spec_version":          _SPEC_VERSION,
        "policy_graph_version":  _POLICY_GRAPH_VERSION,
        "min_runtime_version":   _MIN_RUNTIME_VERSION,
        "intent_schema_version": _INTENT_SCHEMA_VERSION,
        "compile_timestamp":     compile_timestamp,
        "source_hash":           source_hash,
    }

    # ── Step 7-3: Hash source (compile_timestamp excluded) ───────────────
    meta_for_hash = {k: v for k, v in compiler_metadata.items()
                     if k != "compile_timestamp"}

    hash_source: dict = {
        "namespace":        graph.get("namespace"),
        "nodes":            sorted_nodes,
        "edges":            sorted_edges,
        "actor_hierarchy":  act_hier,
        "state_registry":   state_reg,
        "service_registry": svc_reg,
        "compiler_metadata": meta_for_hash,
    }
    # Include residual_schema if present (derive-only, no inference).
    if "residual_schema" in graph:
        hash_source["residual_schema"] = graph["residual_schema"]

    # ── Step 7-4: Canonical JSON (RFC 8785 stdlib approximation) ─────────
    canonical_json = json.dumps(
        hash_source,
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=True,
    )

    # ── Step 7-5: graph_hash ──────────────────────────────────────────────
    graph_hash = "sha256:" + hashlib.sha256(
        canonical_json.encode("utf-8")
    ).hexdigest()

    # ── Step 7-6: Assemble serialised graph ───────────────────────────────
    return {
        "schema_version":     "2.0",
        "is_valid_ctm_graph": True,
        "compiler_metadata":  compiler_metadata,
        "graph_hash":         graph_hash,
        "namespace":          graph.get("namespace"),
        "nodes":              sorted_nodes,
        "edges":              sorted_edges,
        "actor_hierarchy":    act_hier,
        "state_registry":     state_reg,
        "service_registry":   svc_reg,
        "residual_schema":    graph.get("residual_schema"),
    }
