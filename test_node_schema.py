#!/usr/bin/env python3
"""
test_node_schema.py — Canonical node schema compliance test.
Authoritative contract between graph_builder.py (producer)
and all graph consumers (dispatcher, serialiser, validators).

Run before any integration test session:
  venv/bin/python test_node_schema.py

If this fails: stop. Fix graph_builder.py. Do not patch consumers.
The producer owns the schema.
"""

import sys
sys.path.insert(0, ".")
from policy_compiler.compiler import PolicyCompiler

MINIMAL_SOURCE = """
namespace schema.test
actors { Root { Child } }
state RootState { tick: int }
service SvcA {
    endpoint: "file://a"
    simulation_safe: true
    schema { run() -> state_delta }
}
service SvcB {
    endpoint: "file://b"
    simulation_safe: false
    schema { run() -> state_delta }
}
policy RootPolicy {
    actor_scope: [Root]
    state RootState
    rule RuleA -> SvcA.run()
}
policy ChildPolicy extends RootPolicy {
    actor_scope: [Child]
    rule RuleB -> SvcB.run()
}
"""

def run():
    c = PolicyCompiler()
    result = c.compile(MINIMAL_SOURCE, "schema_test")
    assert result["is_valid_ctm_graph"], \
        f"Compile failed: {result['errors']}"

    graph  = result["graph"]
    nodes  = {n["id"]: n for n in graph["nodes"]}
    edges  = graph["edges"]
    p_nodes = [n for n in graph["nodes"] if n["type"] == "P"]
    r_nodes = [n for n in graph["nodes"] if n["type"] == "R"]
    s_nodes = [n for n in graph["nodes"] if n["type"] == "S"]

    # ── P NODE SCHEMA ─────────────────────────────────────────
    for p in p_nodes:
        label = p["label"]
        assert "id"    in p, f"{label}: missing top-level 'id'"
        assert "type"  in p, f"{label}: missing top-level 'type'"
        assert "label" in p, f"{label}: missing top-level 'label'"

        # actor_scope is TOP LEVEL — never inside meta
        assert "actor_scope" in p, (
            f"{label}: 'actor_scope' must be a TOP-LEVEL field "
            f"on P nodes — not inside meta. "
            f"Top-level keys found: {list(p.keys())}"
        )
        assert isinstance(p["actor_scope"], list), \
            f"{label}: actor_scope must be a list, got {type(p['actor_scope'])}"

        # capability_closure is TOP LEVEL
        assert "capability_closure" in p, (
            f"{label}: 'capability_closure' must be TOP LEVEL. "
            f"Top-level keys found: {list(p.keys())}"
        )
        assert isinstance(p["capability_closure"], list), \
            f"{label}: capability_closure must be a list"

        # meta exists and has required keys
        assert "meta" in p, \
            f"{label}: P node must have a 'meta' dict"
        assert "is_root" in p["meta"], \
            f"{label}: meta must contain 'is_root'"
        # actor_scope must NOT be inside meta
        assert "actor_scope" not in p["meta"], (
            f"{label}: actor_scope found INSIDE meta — "
            f"this is WRONG. actor_scope belongs at the "
            f"top level of the P node. "
            f"Any consumer reading meta.get('actor_scope') "
            f"will silently get None."
        )

    print(f"P NODE SCHEMA  PASS — {len(p_nodes)} nodes")

    # ── R NODE SCHEMA ─────────────────────────────────────────
    for r in r_nodes:
        label = r["label"]
        assert "id"    in r, f"{label}: missing 'id'"
        assert "type"  in r, f"{label}: missing 'type'"
        assert "label" in r, f"{label}: missing 'label'"

        # R nodes have NO meta dict — fields are top-level
        assert "meta" not in r, (
            f"{label}: R nodes must NOT have a 'meta' dict. "
            f"All R node fields are top-level."
        )

        # when condition is top-level field named 'when'
        assert "when" in r, (
            f"{label}: R node must have top-level 'when' field. "
            f"NOT 'when_condition' and NOT inside meta."
        )

        # rule_order_index is top-level
        assert "rule_order_index" in r, (
            f"{label}: R node must have top-level "
            f"'rule_order_index' field — not inside meta."
        )

    print(f"R NODE SCHEMA  PASS — {len(r_nodes)} nodes")

    # ── S NODE SCHEMA ─────────────────────────────────────────
    for s in s_nodes:
        label = s["label"]
        assert "id"    in s, f"{label}: missing 'id'"
        assert "type"  in s, f"{label}: missing 'type'"
        assert "label" in s, f"{label}: missing 'label'"

        # S nodes have NO meta dict — fields are top-level
        assert "meta" not in s, (
            f"{label}: S nodes must NOT have a 'meta' dict. "
            f"All S node fields are top-level."
        )

    print(f"S NODE SCHEMA  PASS — {len(s_nodes)} nodes")

    # ── EDGE SCHEMA ───────────────────────────────────────────
    valid_edge_types = {"P_CONTAINS_P", "P_CONTAINS_R",
                        "R_TARGETS_S"}
    for e in edges:
        assert "from" in e, f"Edge missing 'from': {e}"
        assert "to"   in e, f"Edge missing 'to': {e}"
        assert "type" in e, f"Edge missing 'type': {e}"
        assert e["type"] in valid_edge_types, (
            f"Illegal edge type '{e['type']}'. "
            f"Allowed: {valid_edge_types}"
        )
        assert e["from"] in nodes, \
            f"Edge 'from' unknown node id: {e['from']}"
        assert e["to"] in nodes, \
            f"Edge 'to' unknown node id: {e['to']}"

    print(f"EDGE SCHEMA    PASS — {len(edges)} edges")

    # ── ACTOR SCOPE POPULATED ─────────────────────────────────
    root_nodes = [p for p in p_nodes if p["meta"]["is_root"]]
    assert len(root_nodes) == 1, \
        f"Expected exactly 1 root policy, found {len(root_nodes)}"
    assert root_nodes[0]["actor_scope"], \
        "Root policy actor_scope must not be empty"

    child_nodes = [p for p in p_nodes
                   if not p["meta"]["is_root"]]
    for p in child_nodes:
        assert p["actor_scope"], (
            f"{p['label']}: child policy actor_scope is empty. "
            f"Child policies must declare actor_scope."
        )

    print(f"ACTOR SCOPE    PASS — "
          f"1 root + {len(child_nodes)} child policies")

    # ── SINGLE ROOT AXIOM ─────────────────────────────────────
    p_contains_p_targets = {
        e["to"] for e in edges if e["type"] == "P_CONTAINS_P"
    }
    roots = [p for p in p_nodes
             if p["id"] not in p_contains_p_targets]
    assert len(roots) == 1, (
        f"E008/E038: Expected exactly 1 root axiom, "
        f"found {len(roots)}: {[r['label'] for r in roots]}"
    )

    print(f"SINGLE ROOT    PASS — {roots[0]['label']}")

    print()
    print("═══════════════════════════════════════════════")
    print("ALL NODE SCHEMA COMPLIANCE TESTS PASS")
    print("graph_builder.py output contract verified.")
    print("Safe to proceed with integration tests.")
    print("═══════════════════════════════════════════════")


if __name__ == "__main__":
    try:
        run()
    except AssertionError as e:
        print(f"\n{'═'*50}")
        print("NODE SCHEMA COMPLIANCE FAILURE")
        print(f"{'═'*50}")
        print(f"\n{e}\n")
        print("DO NOT proceed with integration tests.")
        print("Fix graph_builder.py to match the canonical")
        print("node schema in CTM_PFSD_MVP_Implementation_Spec.")
        print("Do NOT patch the consumer to work around this.")
        print("The producer owns the schema.")
        sys.exit(1)
