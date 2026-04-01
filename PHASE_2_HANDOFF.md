# CTM-PFSD — Phase 2 Handoff Notes

## Phase 2 status: COMPLETE
Schema version: 1.1 (frozen)
is_valid_ctm_graph: false — unchanged, correct

## What Phase 2 delivered
- Typed gap registry (gaps[] per node with category, severity, layer)
- Formal edge type tokens (P_CONTAINS_R, R_TARGETS_S, P_CONTAINS_P)
- BDH structural pathway analysis (co-occurrence observations)
- Diagnostic classification — heuristic_v1 (gap_classifier.py)
- LLM pattern reports — llm_v1 (llm_provider_adapter.py + gap_classifier.py)
- GitHub URL input (/scan-github endpoint)
- Scaffold export (/export endpoint → .policy file)

## What Phase 2 did NOT do (constitutional)
- Did not suggest governance decisions
- Did not propose rules, actors, or conditions
- Did not move toward is_valid_ctm_graph: true
- Did not produce any artifact interpretable as executable structure

## Phase 3 entry points

### 3.1 Actor Graph (schema v2.0 — breaking change)
Add top-level actor_graph field to DistilledGraph:
  { "actor_graph": { "nodes": [{ "id", "label", "parent" }] } }
P nodes gain actor_scope references to actor_graph node IDs.
schema_version increments to "2.0".

### 3.2 Compiler Validation
When actor_scope, rule conditions, and service schemas are filled in,
the Compiler validates the scaffold and may set is_valid_ctm_graph: true.
Entry point: PolicyScript Compiler Architecture (see project knowledge).

### 3.3 Write Path (CRUD for governance gap filling)
Phase 3 introduces the first write operations:
- Accept/reject diagnostic interpretations
- Author actor_scope declarations
- Author rule conditions (when_condition)
Requires user authentication (Phase 3).

### 3.4 Full Distillation Pipeline
distillation_runner.py is present and importable.
Entry point: DistillationRunner.run(residual_store, session_id)
Phase 3 wires this through AgentForum for manager approval.

### 3.5 Forum Execution (Path B)
Edges P_CONTAINS_F and B_GOVERNS_F are defined in schema v1.1 (future use).
Phase 3 introduces Forum nodes (type "F") and Path B execution.

## Files added in Phase 2
- gap_classifier.py
- llm_provider_adapter.py
- github_fetcher.py
- policy_exporter.py

## Files modified in Phase 2
- scaffold_generator.py (BDH + diagnostics wiring)
- server.py (/scan-github, /export endpoints)
- config.py (LLM constants)
- App.jsx (GitHub mode, export)
- GraphView.jsx (BDH panel, export button)
- DistilledTreeView.jsx (DiagnosticsPanel)

## Schema version roadmap
1.0 — Phase 1 MVP (frozen)
1.1 — Phase 2 (frozen)
2.0 — Phase 3 (actor_graph, breaking change)
