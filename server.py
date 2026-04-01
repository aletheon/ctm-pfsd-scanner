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
from dataclasses import dataclass
from flask import Flask, request, jsonify, Response
from flask_cors import CORS
import config
from scaffold_generator import ScaffoldGenerator
from bdh_kernel import BDHKernel
from gap_classifier import GapClassifier
from github_fetcher import fetch_repo, GitHubFetchError
from policy_exporter import PolicyExporter
from member_permission_registry import MemberPermissionRegistry
from message_validation_middleware import (
    MessageValidationMiddleware, parse_message
)
from beta_tester_bootstrap import BetaTesterBootstrap, PolicyRegistry

_permission_registry = MemberPermissionRegistry()
_policy_registry     = PolicyRegistry()
_bootstrap           = BetaTesterBootstrap(_policy_registry, _permission_registry)
_middleware          = MessageValidationMiddleware(_permission_registry)

# ── Zone 2 kernel helpers ──────────────────────────────────────────────────

@dataclass
class _SyntheticResidual:
    intent_id:    str
    service:      str
    owner_policy: str
    outcome:      str
    tick:         int


def _find_module_for_service_server(s_id: str, nodes: list, edges: list,
                                    reverse: dict, node_by_id: dict) -> str | None:
    """Walk S ← R ← P to find the parent module name. Zone 2 only."""
    for r_id in reverse.get(s_id, []):
        r_node = node_by_id.get(r_id)
        if r_node and r_node["type"] == "R":
            for p_id in reverse.get(r_id, []):
                p_node = node_by_id.get(p_id)
                if p_node and p_node["type"] == "P" and not p_node["meta"].get("is_root"):
                    return p_node["meta"].get("module_name", p_node["label"])
    return None


def _build_residuals(nodes: list, edges: list) -> list:
    """Construct synthetic residuals from S nodes for BDH update. Zone 2 only."""
    reverse: dict = {}
    for e in edges:
        reverse.setdefault(e["to"], []).append(e["from"])
    node_by_id = {n["id"]: n for n in nodes}

    module_services: dict = {}
    for node in nodes:
        if node["type"] == "S":
            module_label = _find_module_for_service_server(
                node["id"], nodes, edges, reverse, node_by_id)
            if module_label:
                module_services.setdefault(module_label, []).append(node["label"])

    import re as _re
    residuals = []
    for module_label, svc_labels in module_services.items():
        lifecycle_id = _re.sub(r"[^a-z0-9_]", "_", module_label.lower()).strip("_") + "_lifecycle"
        for svc_label in svc_labels:
            residuals.append(_SyntheticResidual(
                intent_id    = lifecycle_id,
                service      = svc_label,
                owner_policy = module_label,
                outcome      = config.OUTCOME_SUCCESS,
                tick         = 0,
            ))
    return residuals


def _serialize_pathways(raw_pathways: list) -> list:
    """Serialize BDH PathwayEntry objects to dicts. Zone 2 only."""
    return [
        {
            "pathway_id":      p.pathway_id,
            "service_i":       p.service_i,
            "service_j":       p.service_j,
            "context_label":   p.intent_context,
            "coupling_weight": round(p.coupling_weight, 4),
            "observations":    p.observations,
            "status":          p.status,
            "interpretation":  "co-occurrence observed in same module",
        }
        for p in raw_pathways
    ]


def _orchestrate(project_name: str, files: list) -> dict:
    """
    Zone 2 kernel: 4-step orchestration sequence.
      Step 1: ScaffoldGenerator.scan()      → nodes + edges
      Step 2: BDHKernel.update()            → bdh_pathways
      Step 3: GapClassifier.classify()      → heuristic diagnostics (in-place)
      Step 4: GapClassifier.classify_with_llm() → LLM diagnostics (in-place)
    Services must not call each other. The kernel orchestrates.
    """
    # Step 1
    result = ScaffoldGenerator().scan(project_name, files)
    nodes  = result["nodes"]
    edges  = result["edges"]

    # Step 2
    bdh       = BDHKernel()
    residuals = _build_residuals(nodes, edges)
    bdh.update(residuals)
    raw = [p for p in bdh._pathways.values()
           if p.status != config.BDH_STATUS_PRUNED]
    result["bdh_pathways"] = _serialize_pathways(raw)

    # Step 3 + 4
    classifier    = GapClassifier()
    file_contents = {f.get("path", ""): f.get("content", "")
                     for f in files if f.get("path")}
    classifier.classify(nodes)
    classifier.classify_with_llm(nodes, file_contents)

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

    if data.get("schema_version") not in ("1.0", "1.1"):
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


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 8080))
    app.run(host="0.0.0.0", port=port, debug=False)
