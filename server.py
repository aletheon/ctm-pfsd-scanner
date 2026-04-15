"""
CTM-PFSD Code Governance Scanner — Flask API
GET  /health       → health check
POST /scan         → accepts project files, returns DistilledGraph JSON
GET  /scan-github  → fetches a public GitHub repo and returns DistilledGraph JSON
POST /export       → converts DistilledGraph to .policy scaffold text file
"""
from __future__ import annotations
import os
import time
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import config
from scaffold_generator import ScaffoldGenerator
from gap_classifier import GapClassifier
from github_fetcher import fetch_repo, GitHubFetchError
from policy_exporter import PolicyExporter
from policy_compiler.compiler import PolicyCompiler
from member_permission_registry import MemberPermissionRegistry
from message_validation_middleware import (
    MessageValidationMiddleware, parse_message
)
from beta_tester_bootstrap import BetaTesterBootstrap, PolicyRegistry
from proposal_store import ProposalStore
from graph_store    import GraphStore
from residual_store import make_entry, append as rs_append
from batch_scanner  import BatchScanner
from zone5_runner       import Zone5Runner
from runtime_clock      import RuntimeClock
from intent_queue       import CodeChangeIntent, IntentQueue
from policy_dispatcher  import PolicyDispatcher
from stub_services      import StubServiceRegistry
from policy_compiler.pic_chain import append_entry as pic_append
from llm_classifier         import LLMClassifier
from memory_graph_builder       import MemoryGraphBuilder
from state_store                import StateStore
from historical_context_service import HistoricalContextService
from governance_tools import (
    diff_graphs,
    trace_authority,
    detect_ungoverned,
)
from render_services import (
    GovernanceDiffRenderService,
    CapabilityClosureRenderService,
    PicChainRenderService,
)

_compiler            = PolicyCompiler()
_batch_scanner       = BatchScanner()
_zone5_runner        = Zone5Runner()
_runtime_clock       = RuntimeClock(config.RUNTIME_CLOCK_PATH)
_intent_queue        = IntentQueue()
_dispatcher          = PolicyDispatcher()
_stub_services       = StubServiceRegistry()
_classifier          = LLMClassifier()
_memory_graph_builder = MemoryGraphBuilder()
_state_store          = StateStore()
_hcs                  = HistoricalContextService()
_render_diff          = GovernanceDiffRenderService()
_render_closure       = CapabilityClosureRenderService()
_render_pic           = PicChainRenderService()
_permission_registry = MemberPermissionRegistry()
_policy_registry     = PolicyRegistry()
_bootstrap           = BetaTesterBootstrap(_policy_registry, _permission_registry)
_middleware          = MessageValidationMiddleware(_permission_registry)
_proposal_store      = ProposalStore()
_graph_store         = GraphStore()

def _orchestrate(project_name: str, files: list) -> dict:
    """
    Zone 2 kernel: orchestration sequence.
      Step 1: ScaffoldGenerator.scan()           → nodes + edges
      Step 5: ResidualStore.append()             → episodic write (Zone 2 only)
      Step 3: GapClassifier.classify()           → heuristic diagnostics (in-place)
      Step 4: GapClassifier.classify_with_llm()  → LLM diagnostics (in-place)

    BDH (Zone 5) is driven by distillation_runner.py between sessions.
    Zone 2 reads the persisted pathway snapshot — it never updates BDH.
    Services must not call each other. The kernel orchestrates.
    """
    # Step 1
    result = ScaffoldGenerator().scan(project_name, files)
    nodes  = result["nodes"]

    # Step 5 — write scan residuals to ResidualStore (Zone 2 only)
    scan_tick = int(time.monotonic() * 1000) % 1_000_000
    for node in nodes:
        if node["type"] == "S":
            rs_append(
                make_entry(
                    intent_id       = result.get("root_policy_id", "scan"),
                    tick            = scan_tick,
                    source_type     = "SCAN",
                    project_name    = project_name,
                    service         = node["label"],
                    outcome         = config.OUTCOME_SUCCESS,
                    policy_id       = None,
                    delta_magnitude = 0.0,
                    graph_hash      = None,
                ),
                config.RESIDUAL_STORE_PATH,
            )

    # Step 3 + 4
    classifier    = GapClassifier()
    file_contents = {f.get("path", ""): f.get("content", "")
                     for f in files if f.get("path")}
    classifier.classify(nodes)
    classifier.classify_with_llm(nodes, file_contents)

    # Zone 5 read-only snapshot — pathway registry state
    # BDH is driven by distillation_runner.py between sessions,
    # not by Zone 2 during execution. This is a read-only
    # snapshot of current pathway state for display only.
    try:
        from bdh_store import load as bdh_load
        stored = bdh_load(config.BDH_STORE_PATH)
        result["bdh_pathways"] = [
            {
                "pathway_id":      entry["pathway_id"],
                "service_i":       entry["service_i"],
                "service_j":       entry["service_j"],
                "context_label":   entry.get("context_label", ""),
                "coupling_weight": round(entry["coupling_weight"], 4),
                "observations":    entry["observations"],
                "status":          entry["status"],
                "interpretation":  "co-occurrence observed in same module",
            }
            for entry in stored.values()
            if entry.get("status") != config.BDH_STATUS_PRUNED
        ]
    except Exception:
        result["bdh_pathways"] = []

    return result


app = Flask(__name__)
CORS(app, origins=[
    "https://eleutherios.app",
    "https://www.eleutherios.app",
    "http://localhost:5000",
    "http://localhost:5002",
    "http://localhost:3000",
])


@app.route("/health", methods=["GET"])
def health():
    return jsonify({"status": "ok", "service": "ctm-pfsd-scanner"}), 200


@app.route("/scan", methods=["POST"])
def scan():
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    project_name = data.get("project_name", "Project")
    files        = data.get("files", [])

    if not files:
        return jsonify({"error": "No files provided"}), 400
    if len(files) > 500:
        return jsonify({"error": "Too many files (max 500)"}), 400

    total_size = sum(len(f.get("content", "")) for f in files)
    if total_size > 5 * 1024 * 1024:
        return jsonify({"error": "Project too large (max 5MB total)"}), 400

    t_start = time.monotonic()
    result  = _orchestrate(project_name, files)
    result["scan_duration_ms"] = int((time.monotonic() - t_start) * 1000)

    return jsonify(result), 200


@app.route("/scan-github", methods=["GET"])
def scan_github():
    """
    GET /scan-github?url=https://github.com/owner/repo
    Fetches .py files from a public GitHub repo and returns a DistilledGraph.
    """
    url = request.args.get("url", "").strip()
    if not url:
        return jsonify({"error": "Missing 'url' query parameter"}), 400

    if not url.startswith("https://github.com/"):
        return jsonify({"error": "Only public GitHub URLs are supported"}), 400

    try:
        project_name, files = fetch_repo(url)
    except GitHubFetchError as e:
        return jsonify({"error": str(e)}), 400

    if len(files) > 500:
        return jsonify({"error": "Too many files (max 500)"}), 400

    total_size = sum(len(f.get("content", "")) for f in files)
    if total_size > 5 * 1024 * 1024:
        return jsonify({"error": "Project too large (max 5MB total)"}), 400

    t_start = time.monotonic()
    result  = _orchestrate(project_name, files)
    result["scan_duration_ms"] = int((time.monotonic() - t_start) * 1000)
    result["source"]           = "github"
    result["source_url"]       = url

    return jsonify(result), 200


@app.route("/compile", methods=["POST"])
def compile_policy():
    """
    POST /compile
    Body: { "source": string, "project_name": string }
    Returns: CompileResult JSON

    Runs PolicyScript Stages 1–3 (Stage A).
    Stages 4–7 deferred to Stage B and C.
    /compile and /scan are independent pipelines.
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400
    source       = data.get("source", "").strip()
    project_name = data.get("project_name", "Project").strip()
    if not source:
        return jsonify({"error": "source required"}), 400

    result = _compiler.compile(source, project_name)
    return jsonify(result), 200


@app.route("/export", methods=["POST"])
def export_scaffold():
    """
    POST /export
    Body: DistilledGraph JSON
    Returns: .policy scaffold text file
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    if data.get("schema_version") not in ("1.0", "1.1", "2.0"):
        return jsonify({"error": "Unsupported schema_version"}), 400

    exporter    = PolicyExporter()
    policy_text = exporter.export(data)

    project_name = data.get("project_name", "scaffold")
    filename     = project_name.lower().replace(" ", "_") + ".policy"

    return Response(
        policy_text,
        mimetype = "text/plain",
        headers  = {
            "Content-Disposition": f'attachment; filename="{filename}"',
            "Content-Type":        "text/plain; charset=utf-8",
        }
    )


@app.route("/bootstrap", methods=["POST"])
def bootstrap_visitor():
    """
    POST /bootstrap  { "session_id": string }
    Creates betaTesterPolicy<uuid> for a new visitor.
    Idempotent — same session_id returns the same policy.
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400
    session_id = data.get("session_id", "").strip()
    if not session_id:
        return jsonify({"error": "session_id required"}), 400
    result = _bootstrap.bootstrap(session_id)
    return jsonify({
        "policy_id":          result.policy_id,
        "manager_service_id": result.manager_service_id,
        "default_forum_id":   result.default_forum_id,
        "session_id":         result.session_id,
        "created_at":         result.created_at,
        "is_new_session":     result.is_new_session,
        "genesis_steps":      result.genesis_steps,
        "parent_policy":      "betaTestersPolicy",
    }), 200


@app.route("/validate-message", methods=["POST"])
def validate_message_endpoint():
    """
    POST /validate-message
    { "member_id": string, "forum_id": string, "content": string }

    Path B middleware. Validates permission and returns routing target.
    The forum does not validate — this endpoint does.
    Attribution is always to member_id.
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400
    member_id = data.get("member_id", "").strip()
    forum_id  = data.get("forum_id",  "").strip()
    content   = data.get("content",   "").strip()
    if not (member_id and forum_id and content):
        return jsonify({"error": "member_id, forum_id, content required"}), 400
    message = parse_message(content, member_id, forum_id)
    result  = _middleware.validate(message)
    return jsonify({
        "permitted":          result.permitted,
        "member_id":          result.member_id,
        "forum_id":           result.forum_id,
        "permission_checked": result.permission_checked,
        "flow":               result.flow.value,
        "routed_to":          result.routed_to,
        "error_code":         result.error_code,
        "audit_entry_id":     result.audit_entry_id,
    }), 200


@app.route("/propose", methods=["POST"])
def propose():
    """
    POST /propose
    Body: { "member_id": string, "forum_id": string,
            "policy_id": string, "content": string }

    Path B PROPOSAL flow: B → G → middleware → F → M → P
    Step 1: Parse message via parse_message()
    Step 2: Validate via middleware — RT-E005 if not permitted
    Step 3: Record proposal in proposal_store
    Step 4: Return proposal record

    The forum does not validate — middleware does.
    Attribution is always to member_id.
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    member_id = data.get("member_id", "").strip()
    forum_id  = data.get("forum_id",  "").strip()
    policy_id = data.get("policy_id", "").strip()
    content   = data.get("content",   "").strip()

    if not (member_id and forum_id and policy_id and content):
        return jsonify({"error": "member_id, forum_id, policy_id, content required"}), 400

    # Verify this is a PROPOSAL flow message.
    content_lower = content.lower()
    if not (content_lower.startswith("propose:") or
            content_lower.startswith("proposal:")):
        return jsonify({
            "error": "Content must start with 'propose:' or 'proposal:' "
                     "to submit a governance proposal"
        }), 400

    # Path B: parse → middleware validation.
    message = parse_message(content, member_id, forum_id)
    result  = _middleware.validate(message)

    if not result.permitted:
        return jsonify({
            "error":              "RT-E005 InsufficientPermission",
            "member_id":          result.member_id,
            "forum_id":           result.forum_id,
            "permission_checked": result.permission_checked,
            "error_code":         "RT-E005",
            "audit_entry_id":     result.audit_entry_id,
        }), 403

    # Record proposal.
    record = _proposal_store.submit(
        member_id      = result.member_id,
        forum_id       = result.forum_id,
        policy_id      = policy_id,
        content        = content,
        audit_entry_id = result.audit_entry_id,
    )

    return jsonify({
        "proposal_id":    record.proposal_id,
        "member_id":      record.member_id,
        "forum_id":       record.forum_id,
        "policy_id":      record.policy_id,
        "content":        record.content,
        "state":          record.state.value,
        "submitted_at":   record.submitted_at,
        "audit_entry_id": record.audit_entry_id,
    }), 200


@app.route("/approve", methods=["POST"])
def approve_proposal():
    """
    POST /approve
    Body: { "proposal_id": string, "manager_id": string, "source": string }

    Governance mutation path: F → M → P → Compiler → PIC Chain → Graph Update
    Manager approves → compiler validates → PIC Chain committed → GraphStore
    hot-reloaded.  A compile failure does NOT undo the approval.
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    proposal_id = data.get("proposal_id", "").strip()
    manager_id  = data.get("manager_id",  "").strip()
    source      = data.get("source",      "").strip()

    if not (proposal_id and manager_id and source):
        return jsonify({"error": "proposal_id, manager_id, source required"}), 400

    # Approve proposal in store.
    try:
        record = _proposal_store.approve(proposal_id, manager_id)
    except KeyError:
        return jsonify({"error": "Proposal not found",
                        "proposal_id": proposal_id}), 404
    except ValueError as e:
        current = _proposal_store.get(proposal_id)
        return jsonify({
            "error":         str(e),
            "proposal_id":   proposal_id,
            "current_state": current.state.value if current else None,
        }), 409

    # Run compiler — always against record.policy_id as project name.
    compile_result = _compiler.compile(source, record.policy_id)

    # Persist compile outcome on proposal record regardless of success.
    record.compile_result = compile_result

    # Hot-reload graph store only on valid compile.
    graph_loaded = False
    graph_hash   = None
    pic_entry    = None

    if compile_result.get("is_valid_ctm_graph"):
        _graph_store.load(compile_result)
        graph_loaded = True
        graph_hash   = compile_result["graph"]["graph_hash"]
        pic_entry    = compile_result["graph"].get("pic_chain_entry")
        rs_append(
            make_entry(
                intent_id       = record.proposal_id,
                tick            = int(time.time()),
                source_type     = "APPROVE",
                project_name    = record.policy_id,
                service         = "PolicyCompiler",
                outcome         = config.OUTCOME_SUCCESS,
                policy_id       = record.policy_id,
                delta_magnitude = 0.0,
                graph_hash      = graph_hash,
            ),
            config.RESIDUAL_STORE_PATH,
        )

    cr = compile_result
    return jsonify({
        "proposal_id":  record.proposal_id,
        "manager_id":   manager_id,
        "state":        record.state.value,
        "compile_result": {
            "is_valid_ctm_graph": cr.get("is_valid_ctm_graph"),
            "stages_completed":   cr.get("stages_completed", []),
            "error_count":        len(cr.get("errors",   [])),
            "warning_count":      len(cr.get("warnings", [])),
            "errors":             cr.get("errors",   []),
            "warnings":           cr.get("warnings", []),
        },
        "graph_loaded":    graph_loaded,
        "graph_hash":      graph_hash,
        "pic_chain_entry": pic_entry,
    }), 200


@app.route("/reject", methods=["POST"])
def reject_proposal():
    """
    POST /reject
    Body: { "proposal_id": string, "manager_id": string, "reason": string }

    Manager rejects proposal. No compile triggered.
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    proposal_id = data.get("proposal_id", "").strip()
    manager_id  = data.get("manager_id",  "").strip()
    reason      = data.get("reason",      "").strip()

    if not (proposal_id and manager_id and reason):
        return jsonify({"error": "proposal_id, manager_id, reason required"}), 400

    try:
        record = _proposal_store.reject(proposal_id, manager_id, reason)
    except KeyError:
        return jsonify({"error": "Proposal not found",
                        "proposal_id": proposal_id}), 404
    except ValueError as e:
        return jsonify({"error":       str(e),
                        "proposal_id": proposal_id}), 409

    return jsonify({
        "proposal_id":      record.proposal_id,
        "manager_id":       manager_id,
        "state":            record.state.value,
        "rejection_reason": record.rejection_reason,
        "resolved_at":      record.resolved_at,
    }), 200


@app.route("/graph-status", methods=["GET"])
def graph_status():
    """
    GET /graph-status
    Returns the current state of the live graph store.
    """
    return jsonify(_graph_store.status()), 200


@app.route("/batch-scan", methods=["POST"])
def batch_scan():
    """
    POST /batch-scan
    Body: { "urls": ["https://github.com/..."], "dry_run": false }

    Fetches up to 20 GitHub repos and optionally runs _orchestrate() on each.
    dry_run=true returns fetch report only (no scanning).
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    urls    = data.get("urls", [])
    dry_run = bool(data.get("dry_run", False))

    if not urls or not isinstance(urls, list):
        return jsonify({"error": "urls must be a non-empty list"}), 400
    if len(urls) > 20:
        return jsonify({"error": "Maximum 20 URLs per batch"}), 400
    for url in urls:
        if not isinstance(url, str) or not url.startswith("https://github.com/"):
            return jsonify({"error": f"Only public GitHub URLs supported: {url}"}), 400

    fetched = _batch_scanner.fetch_all(urls)

    if dry_run:
        return jsonify({
            "dry_run":   True,
            "total":     len(fetched),
            "results": [
                {
                    "url":        r["url"],
                    "status":     r["status"],
                    "file_count": r.get("file_count", 0),
                    "reason":     r.get("reason"),
                }
                for r in fetched
            ],
        }), 200

    for r in fetched:
        if r["status"] == "FETCHED":
            t_start = time.monotonic()
            try:
                scan_result = _orchestrate(r["project_name"], r["files"])
                scan_result["scan_duration_ms"] = int(
                    (time.monotonic() - t_start) * 1000
                )
                r["scan_result"] = {
                    "status":               "OK",
                    "services_found":       scan_result["summary"]["services_count"],
                    "bdh_pathways":         len(scan_result.get("bdh_pathways", [])),
                    "scan_duration_ms":     scan_result["scan_duration_ms"],
                    "residual_count_added": scan_result["summary"]["services_count"],
                }
            except Exception as e:
                r["scan_result"] = {"status": "SCAN_ERROR", "reason": str(e)}
            del r["files"]

    return jsonify({
        "dry_run":          False,
        "total":            len(fetched),
        "scanned":          sum(
            1 for r in fetched
            if r.get("scan_result", {}).get("status") == "OK"
        ),
        "skipped":          sum(1 for r in fetched if r["status"] == "SKIPPED"),
        "errors":           sum(1 for r in fetched if r["status"] == "ERROR"),
        "residuals_written": sum(
            r.get("scan_result", {}).get("residual_count_added", 0)
            for r in fetched
        ),
        "results":          fetched,
    }), 200


@app.route("/run-distillation", methods=["POST"])
def run_distillation():
    """
    POST /run-distillation
    Body: { "session_id": string }  -- optional, defaults to timestamp

    Triggers Zone 5 distillation run between sessions.
    Reads ResidualStore, updates BDH, saves pathway registry.
    Called by operators between sessions — never automatically
    during scan or approve pipelines.
    """
    data       = request.get_json(force=True, silent=True) or {}
    session_id = data.get("session_id") or f"session_{int(time.time())}"
    result     = _zone5_runner.run(session_id)
    return jsonify(result), 200


@app.route("/build-memory-graph", methods=["POST"])
def build_memory_graph():
    """
    POST /build-memory-graph
    Body: { "session_id": string }  -- optional

    Triggers offline MemoryGraph construction between sessions.
    Reads ResidualStore + BDH pathway registry.
    Produces weighted ConceptNode graph.
    Persists to MEMORY_GRAPH_PATH.
    Returns MemoryGraph summary.

    Called by operators between sessions — never automatically
    during scan, execute, or approve pipelines.
    """
    data       = request.get_json(force=True, silent=True) or {}
    session_id = data.get("session_id") or f"mem_{int(time.time())}"
    result     = _memory_graph_builder.build(session_id)
    return jsonify(result), 200


def _run_execution(intent) -> dict:
    """
    Shared execution body for POST /execute and POST /commit.
    Receives a typed CodeChangeIntent. Pushes to queue, dispatches,
    invokes services, writes residuals, appends PIC entry, expires
    stale intents. Returns the execution trace dict (not a Flask response).
    """
    # Push to queue (sets tick_created)
    intent = _intent_queue.push(intent, _runtime_clock)

    # Get compiled graph
    graph          = _graph_store.get()
    compiled_graph = graph["graph"]

    # Dispatch — get rule firings
    firings = _dispatcher.dispatch(compiled_graph, intent)

    # Execute firing rules (Path A: P → R → S)
    rules_fired        = []
    services_invoked   = []
    residuals_written  = []
    execution_paths    = {}   # CTM-PATHVAL-001: service → "A" | "B"

    for firing in firings:
        if not firing.condition_met:
            continue

        # Advance clock — 1 tick per service dispatch
        tick = _runtime_clock.advance()

        # Invoke service
        svc_output = _stub_services.invoke(firing.service_label, intent, tick)
        delta = svc_output.get("state_delta", {})
        if delta:
            _state_store.commit_delta(delta, tick, firing.service_label)

        # Build and write residual with rule_id attribution
        entry = make_entry(
            intent_id       = intent.intent_id,
            tick            = tick,
            source_type     = "EXECUTE",
            project_name    = intent.owner_policy,
            service         = firing.service_label,
            outcome         = config.OUTCOME_SUCCESS,
            policy_id       = firing.policy_id,
            delta_magnitude = 0.0,
            graph_hash      = _graph_store.get_graph_hash(),
            rule_id         = firing.rule_id,
            diff_hash       = intent.diff_hash,
        )
        rs_append(entry, config.RESIDUAL_STORE_PATH)

        rules_fired.append({
            "rule_id":       firing.rule_id,
            "rule_label":    firing.rule_label,
            "policy_label":  firing.policy_label,
            "service_label": firing.service_label,
            "tick":          tick,
        })
        services_invoked.append(firing.service_label)
        residuals_written.append(entry["residual_id"])
        execution_paths[firing.service_label] = "A"   # Path A only

    # Expire TTL-exceeded intents
    expired = _intent_queue.expire(_runtime_clock.current())

    # PIC Chain entry per execution (CTM-PATH-002)
    execution_pic_entry = None
    if rules_fired:
        execution_pic_payload = {
            "intent_id":      intent.intent_id,
            "model_id":       intent.model_id,
            "prompt_hash":    intent.prompt_hash,
            "diff_hash":      intent.diff_hash,
            "files_changed":  list(intent.files_changed),
            "rules_fired":    [f["rule_label"] for f in rules_fired],
            "human_reviewed": intent.human_reviewed,
            "actor":          intent.actor,
            "change_type":    intent.change_type,
            "tick_final":     _runtime_clock.current(),
        }
        try:
            graph_data = _graph_store.get()["graph"]
            pic_append(
                {
                    "graph_hash":        _graph_store.get_graph_hash(),
                    "nodes":             graph_data.get("nodes", []),
                    "namespace":         graph_data.get("namespace"),
                    "compiler_metadata": graph_data.get("compiler_metadata", {}),
                },
                intent.owner_policy,
                config.PIC_CHAIN_PATH,
            )
            execution_pic_entry = execution_pic_payload
        except Exception as e:
            execution_pic_entry = {"error": str(e)}

    return {
        "intent_id":           intent.intent_id,
        "tick_created":        intent.tick_created,
        "tick_final":          _runtime_clock.current(),
        "rules_fired":         rules_fired,
        "services_invoked":    services_invoked,
        "residuals_written":   residuals_written,
        "execution_paths":     execution_paths,
        "execution_pic_entry": execution_pic_entry,
        "rules_skipped":       [f.rule_label for f in firings
                                if not f.condition_met],
        "expired_intents":     len(expired),
        "graph_hash":          _graph_store.get_graph_hash(),
        "is_valid_ctm_graph":  True,
    }


@app.route("/execute", methods=["POST"])
def execute():
    """
    POST /execute
    Body: { change_type, scope, actor, owner_policy, intent_origin,
            diff_summary, change_description, diff_hash,
            files_changed?, model_id?, session_id?,
            human_reviewed?, priority? }

    Path A execution: P → R → S
    Requires a compiled graph loaded in graph_store.
    Advances RuntimeClock once per service dispatch.
    Writes EXECUTE residuals with rule_id attribution.
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    # Step 1 — Validate required fields
    required = (
        "change_type", "scope", "actor", "owner_policy",
        "intent_origin", "diff_summary", "change_description", "diff_hash",
    )
    missing = [f for f in required if not data.get(f)]
    if missing:
        return jsonify({"error": f"Missing required fields: {missing}"}), 400

    # Step 2 — Require a loaded graph
    if not _graph_store.is_loaded():
        return jsonify({
            "error": (
                "No compiled graph loaded. "
                "POST /compile and POST /approve a valid .policy file first."
            )
        }), 400

    # Step 3 — Build intent
    intent = CodeChangeIntent.make(
        change_type        = data["change_type"],
        scope              = data["scope"],
        actor              = data["actor"],
        owner_policy       = data["owner_policy"],
        intent_origin      = data["intent_origin"],
        files_changed      = tuple(data.get("files_changed", [])),
        diff_summary       = data["diff_summary"],
        change_description = data["change_description"],
        diff_hash          = data["diff_hash"],
        model_id           = data.get("model_id", "unknown"),
        session_id         = data.get("session_id"),
        human_reviewed     = data.get("human_reviewed", False),
        priority           = data.get("priority", 1),
    )

    return jsonify(_run_execution(intent)), 200


@app.route("/commit", methods=["POST"])
def commit():
    """
    POST /commit
    Body: {
      "description":   string,   -- developer commit description (required)
      "diff_text":     string,   -- optional raw diff content
      "files_changed": [string], -- optional file list
      "session_id":    string    -- optional
    }

    The interception endpoint. Replaces direct git commit.
    Classifies the intent via LLMClassifier, constructs a typed
    CodeChangeIntent, then runs identical execution logic to /execute.

    Flow:
      developer description
        → LLMClassifier.classify()   Zone 1 boundary
        → CodeChangeIntent.make()    typed, immutable
        → _run_execution()           same path as /execute
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    # Step 1 — Require description
    description = (data.get("description") or "").strip()
    if not description:
        return jsonify({"error": "description is required"}), 400

    # Step 2 — Require a loaded graph
    if not _graph_store.is_loaded():
        return jsonify({
            "error": "No compiled graph loaded.",
            "hint":  "POST /compile and POST /approve first.",
        }), 400

    # Step 3 — Classify
    classification = _classifier.classify(
        description   = description,
        diff_text     = data.get("diff_text", ""),
        files_changed = data.get("files_changed", []),
        codebase_path = config.CODEBASE_PATH,
    )

    # Step 4 — Override session_id if provided
    if data.get("session_id"):
        classification["session_id"] = data["session_id"]

    # Step 5 — Build intent (strip classification_confidence)
    intent_kwargs = {
        k: v for k, v in classification.items()
        if k != "classification_confidence"
    }
    intent = CodeChangeIntent.make(**intent_kwargs)

    # Step 6 — Execute (identical path to /execute)
    trace = _run_execution(intent)

    # Step 7 — Augment with classification metadata
    trace["classification"] = {
        "change_type":   classification["change_type"],
        "scope":         classification["scope"],
        "actor":         classification["actor"],
        "diff_hash":     classification["diff_hash"],
        "confidence":    classification["classification_confidence"],
        "intent_origin": "LLMClassifier",
    }

    return jsonify(trace), 200


@app.route("/clock-status", methods=["GET"])
def clock_status():
    """GET /clock-status — Returns current RuntimeClock state."""
    return jsonify(_runtime_clock.status()), 200


@app.route("/state-status", methods=["GET"])
def state_status():
    """GET /state-status — Returns current StateStore accumulation status."""
    return jsonify(_state_store.status()), 200


@app.route("/governance-diff", methods=["POST"])
def governance_diff():
    """
    POST /governance-diff
    Body: {
      "source_a": string,   -- first .policy source
      "source_b": string,   -- second .policy source
      "project_name": string  -- optional, default "diff"
    }

    Compiles both sources and diffs the resulting graphs.
    Returns a structured diff showing what changed.
    Read-only forensic tool.
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    source_a     = data.get("source_a",     "").strip()
    source_b     = data.get("source_b",     "").strip()
    project_name = data.get("project_name", "diff").strip() or "diff"

    if not (source_a and source_b):
        return jsonify({"error": "source_a and source_b required"}), 400

    result_a = _compiler.compile(source_a, project_name + "_a")
    result_b = _compiler.compile(source_b, project_name + "_b")

    if not result_a.get("is_valid_ctm_graph") or \
       not result_b.get("is_valid_ctm_graph"):
        return jsonify({
            "error":    "Compilation failed",
            "errors_a": result_a.get("errors", []),
            "errors_b": result_b.get("errors", []),
        }), 400

    graph_a = result_a["graph"]
    graph_b = result_b["graph"]
    diff    = diff_graphs(graph_a, graph_b)
    return jsonify(diff), 200


@app.route("/authority", methods=["GET"])
def authority():
    """
    GET /authority?service=ServiceLabel

    Traces the authority chain for the named service in the
    currently loaded compiled graph.
    Returns which rules fire it, which policies own those rules,
    what actor scope governs them.
    Read-only forensic tool. Requires compiled graph loaded.
    """
    service_label = request.args.get("service", "").strip()
    if not service_label:
        return jsonify({"error": "service parameter required"}), 400

    if not _graph_store.is_loaded():
        return jsonify({"error": "No compiled graph loaded"}), 400

    graph  = _graph_store.get()["graph"]
    result = trace_authority(graph, service_label)
    return jsonify(result), 200


@app.route("/ungoverned", methods=["POST"])
def ungoverned():
    """
    POST /ungoverned
    Body: {
      "project_name": string,
      "files": [{"path": str, "content": str}, ...]
    }

    Scans the provided project files and compares detected
    services against the currently loaded compiled graph.
    Returns services that exist in code but are not governed
    by any P→R→S path.
    Read-only forensic tool. Requires compiled graph loaded.
    """
    data = request.get_json(force=True, silent=True)
    if not data:
        return jsonify({"error": "Invalid JSON body"}), 400

    project_name = data.get("project_name", "").strip()
    files        = data.get("files", [])

    if not (project_name and files):
        return jsonify({"error": "project_name and files required"}), 400

    if not _graph_store.is_loaded():
        return jsonify({"error": "No compiled graph loaded"}), 400

    scan_result      = _orchestrate(project_name, files)
    scanned_services = [
        n["label"] for n in scan_result["nodes"]
        if n["type"] == "S"
    ]

    graph  = _graph_store.get()["graph"]
    result = detect_ungoverned(graph, scanned_services)
    return jsonify(result), 200


@app.route("/render/governance-diff", methods=["POST"])
def render_governance_diff():
    """
    POST /render/governance-diff
    Body: {
      "source_a":     string,
      "source_b":     string,
      "project_name": string,   -- optional, default "render_diff"
      "member_id":    string    -- optional
    }

    Compiles both sources, diffs the resulting graphs,
    returns a render JSON payload for the governance_diff panel.
    Read-only. No state mutations.
    """
    data = request.get_json(force=True, silent=True) or {}

    source_a     = data.get("source_a",     "").strip()
    source_b     = data.get("source_b",     "").strip()
    project_name = data.get("project_name", "render_diff").strip() or "render_diff"
    member_id    = data.get("member_id")

    if not (source_a and source_b):
        return jsonify({"error": "source_a and source_b required"}), 400

    result_a = _compiler.compile(source_a, project_name + "_a")
    result_b = _compiler.compile(source_b, project_name + "_b")

    if not result_a.get("is_valid_ctm_graph") or \
       not result_b.get("is_valid_ctm_graph"):
        return jsonify({
            "panel":          "governance_diff",
            "render_type":    "diff",
            "render_title":   "Governance Diff",
            "data": {
                "error":    "Compilation failed",
                "errors_a": result_a.get("errors", []),
                "errors_b": result_b.get("errors", []),
                "identical": True,
                "summary":  "Compilation failed — diff unavailable",
            },
            "interactions":   [],
            "lifecycle_step": 4,
            "member_id":      member_id,
        }), 400

    graph_a = result_a["graph"]
    graph_b = result_b["graph"]
    result  = _render_diff.render(graph_a, graph_b, member_id)
    return jsonify(result), 200


@app.route("/render/capability-closure", methods=["POST"])
def render_capability_closure():
    """
    POST /render/capability-closure
    Body: {
      "service_label": string,
      "member_id":     string    -- optional
    }

    Traces the authority chain for the named service in the
    currently loaded compiled graph.
    Returns a render JSON payload for the capability_closure panel.
    Requires compiled graph loaded.
    """
    data = request.get_json(force=True, silent=True) or {}

    service_label = data.get("service_label", "").strip()
    member_id     = data.get("member_id")

    if not service_label:
        return jsonify({"error": "service_label required"}), 400

    if not _graph_store.is_loaded():
        return jsonify({"error": "No compiled graph loaded"}), 400

    graph  = _graph_store.get()["graph"]
    result = _render_closure.render(graph, service_label, member_id)
    return jsonify(result), 200


@app.route("/render/pic-chain", methods=["POST"])
def render_pic_chain():
    """
    POST /render/pic-chain
    Body: {
      "last_n":    int,     -- optional, default 20
      "member_id": string   -- optional
    }

    Returns the last N PIC Chain entries as a timeline payload.
    Shows both COMPILE_GRAPH and CODE_CHANGE_COMMITTED entries.
    Read-only. No compiled graph required.
    """
    data      = request.get_json(force=True, silent=True) or {}
    last_n    = int(data.get("last_n", 20))
    member_id = data.get("member_id")
    result    = _render_pic.render(last_n, member_id)
    return jsonify(result), 200


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
