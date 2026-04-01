"""
GapClassifier — produces diagnostic entries for each governance gap node.

Diagnostics explain what kind of gap exists and why it cannot be inferred
from source code. They do not propose how to fill it.

Output contract:
  - Each diagnostic has: diagnostic_id, category, explanation,
    confidence, source, layer
  - explanation text is factual and descriptive only
  - No suggestion, recommendation, or prescriptive language
  - No pseudo-rule syntax, no actor names as proposals,
    no condition values as proposals

§48 boundary: imports only config and stdlib.
"""
from __future__ import annotations
import hashlib
import json
from typing import Optional
import config
from llm_provider_adapter import get_provider

# ── Explanation templates ─────────────────────────────────────────────────
# Each template explains why the gap exists.
# Templates must not contain: "should", "must", "recommend", "suggest",
# "propose", "consider", "create", "add", "assign", or any imperative verb.

_EXPLANATIONS: dict[str, tuple[str, float]] = {
    "ACTOR_SCOPE": (
        "Actor scope cannot be inferred from source code. "
        "Actor hierarchy is a governance declaration belonging to the WHY layer, "
        "which requires human authorship.",
        0.92,
    ),
    "POLICY_OWNERSHIP": (
        "Policy authority cannot be derived from file structure or module names. "
        "At genesis, the system creates the P → M binding and the policy creator "
        "is bound as the initial manager. After genesis, additional managers are "
        "bound only by an existing manager holding add_manager permission. "
        "This WHY-layer governance structure requires human authorship.",
        0.90,
    ),
    "FORUM_ASSIGNMENT": (
        "The forum context for this module cannot be inferred from static code analysis. "
        "Forums are coordination spaces defined in the governance structure, "
        "not in the code structure.",
        0.85,
    ),
    "MANAGER_ASSIGNMENT": (
        "The manager role for this module cannot be derived from code. "
        "Managers are governance roles declared in the WHY layer.",
        0.85,
    ),
    "RULE_CONDITION": (
        "Rule condition cannot be inferred from a service signature alone. "
        "A function signature describes inputs and outputs; it does not "
        "describe the governance condition under which the service is invoked. "
        "This is a WHAT-layer decision requiring human authorship.",
        0.88,
    ),
    "RULE_ORDERING": (
        "Execution ordering cannot be determined from static code analysis. "
        "The depends_on relationship between rules is a WHAT-layer "
        "governance declaration, not a code-level property.",
        0.80,
    ),
    "SERVICE_SCHEMA": (
        "A Python type hint is not a CTM-PFSD service schema. "
        "The formal schema specifying what a service accepts and returns "
        "in governance terms requires human authorship.",
        0.93,
    ),
    "SERVICE_ENDPOINT": (
        "Service endpoint cannot be inferred from a function definition. "
        "Whether this service is invoked via HTTP, message queue, "
        "or internal call is a HOW-layer declaration requiring human authorship.",
        0.88,
    ),
    "SIMULATION_SAFE": (
        "Whether a service is safe to invoke in a simulation context "
        "cannot be determined from static analysis. "
        "This flag requires explicit human declaration.",
        0.82,
    ),
}

_LAYER_MAP: dict[str, str] = {
    "ACTOR_SCOPE":        "WHY",
    "POLICY_OWNERSHIP":   "WHY",
    "FORUM_ASSIGNMENT":   "WHY",
    "MANAGER_ASSIGNMENT": "WHY",
    "RULE_CONDITION":     "WHAT",
    "RULE_ORDERING":      "WHAT",
    "SERVICE_SCHEMA":     "HOW",
    "SERVICE_ENDPOINT":   "HOW",
    "SIMULATION_SAFE":    "HOW",
}


class GapClassifier:
    """
    Produces diagnostic entries for each gap in a node's gaps array.
    One diagnostic per gap entry. Source is always "heuristic_v1" in Stage 3.
    """

    def classify(self, nodes: list) -> list:
        """
        For each node, attach a diagnostics array derived from its gaps.
        Modifies nodes in place. Returns the modified list.
        """
        for node in nodes:
            diagnostics = []
            for gap in node.get("gaps", []):
                category = gap.get("category", "")
                diag = self._make_diagnostic(node["id"], category)
                if diag:
                    diagnostics.append(diag)
            node["diagnostics"] = diagnostics
        return nodes

    def classify_with_llm(self, nodes: list, file_contents: dict) -> list:
        """
        Extends the diagnostics on up to LLM_MAX_NODES_PER_SCAN nodes
        with LLM-generated structural observations.

        file_contents: dict of {path: content} for context.
        Modifies nodes in place. Returns modified list.
        """
        provider = get_provider(config.LLM_PROVIDER)
        if not provider.is_available():
            return nodes

        # Select the nodes with the most gaps (highest structural complexity)
        candidates = sorted(
            [n for n in nodes if n.get("gaps")],
            key=lambda n: len(n["gaps"]),
            reverse=True
        )[:config.LLM_MAX_NODES_PER_SCAN]

        for node in candidates:
            file_path = node.get("meta", {}).get("file", "")
            content_snippet = ""
            if file_path and file_path in file_contents:
                # Send only the first 800 chars — enough for structural context
                content_snippet = file_contents[file_path][:800]

            prompt = self._build_prompt(node, content_snippet)
            response_str = provider.complete(prompt, max_tokens=config.LLM_MAX_TOKENS)

            try:
                response = json.loads(response_str)
            except (json.JSONDecodeError, ValueError):
                continue

            for pattern in response.get("patterns", []):
                diag = self._validate_llm_pattern(node["id"], pattern)
                if diag:
                    # Merge: if same category already exists, keep higher confidence
                    existing = next(
                        (d for d in node["diagnostics"]
                         if d["category"] == diag["category"]), None
                    )
                    if existing:
                        if diag["confidence"] > existing["confidence"]:
                            existing["confidence"] = diag["confidence"]
                            existing["explanation"] = (
                                existing["explanation"] + " " + diag["explanation"]
                            ).strip()
                    else:
                        node["diagnostics"].append(diag)

        return nodes

    def _build_prompt(self, node: dict, content_snippet: str) -> str:
        gaps = [g["category"] for g in node.get("gaps", [])]
        return (
            f"Analyse this code module for structural governance gaps.\n\n"
            f"Module: {node.get('label', 'unknown')}\n"
            f"Gap categories identified: {', '.join(gaps)}\n"
            f"Code snippet:\n{content_snippet}\n\n"
            f"Describe the structural patterns you observe. "
            f"Do not propose solutions or governance decisions."
        )

    def _validate_llm_pattern(self, node_id: str, pattern: dict) -> Optional[dict]:
        """
        Validates and sanitises one LLM pattern entry.
        Returns a diagnostic dict or None if invalid or prescriptive.
        """
        required = {"type", "category", "description", "confidence", "layer"}
        if not required.issubset(pattern.keys()):
            return None

        # Reject prescriptive language
        FORBIDDEN = ["should", "must", "recommend", "suggest", "propose",
                     "consider", "create a", "add a", "assign", "use this",
                     "you need", "you should"]
        desc_lower = pattern["description"].lower()
        for word in FORBIDDEN:
            if word in desc_lower:
                return None

        # Reject executable-looking content
        EXEC_MARKERS = ["rule ", "-> ", "when:", "actor_scope:", "policy {"]
        for marker in EXEC_MARKERS:
            if marker in pattern["description"]:
                return None

        conf = float(pattern.get("confidence", 0))
        if not (config.LLM_MIN_CONFIDENCE <= conf <= config.LLM_MAX_CONFIDENCE):
            return None

        layer = pattern.get("layer", "")
        if layer not in {"WHY", "WHAT", "HOW"}:
            return None

        raw = f"{node_id}:{pattern['category']}:llm"
        diag_id = "d_llm_" + hashlib.sha256(raw.encode()).hexdigest()[:12]

        return {
            "diagnostic_id": diag_id,
            "category":      pattern["category"],
            "explanation":   pattern["description"],
            "confidence":    round(conf, 3),
            "source":        "llm_v1",
            "layer":         layer,
        }

    def _make_diagnostic(self, node_id: str, category: str) -> Optional[dict]:
        if category not in _EXPLANATIONS:
            return None
        explanation, confidence = _EXPLANATIONS[category]
        # Deterministic ID: hash of node_id + category
        raw = f"{node_id}:{category}"
        diag_id = "d_" + hashlib.sha256(raw.encode()).hexdigest()[:12]
        return {
            "diagnostic_id": diag_id,
            "category":      category,
            "explanation":   explanation,
            "confidence":    confidence,
            "source":        "heuristic_v1",
            "layer":         _LAYER_MAP.get(category, "UNKNOWN"),
        }
