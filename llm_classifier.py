"""
llm_classifier.py — Zone 1 → Zone 2 boundary interceptor.

CTM-PFSD Spec v1.6 §3d, §16. SovereignClaw §1.7, §1.8.

Classifies a developer commit description into a fully-typed
CodeChangeIntent dict. POST /commit feeds the result directly
to _run_execution(). No LLM at this stage — deterministic
keyword + path heuristics. LLM integration in Step 5b.

INTERCEPTION LAW: there is no fallback path to direct commit.
The LLM cannot commit directly. The classifier validates and
constructs — the kernel executes without re-validating.

§48 boundary: stdlib only (hashlib, json, os, pathlib,
              subprocess, datetime).
"""
from __future__ import annotations

import hashlib
import subprocess
from typing import Optional


# ── change_type keyword table (checked in order) ──────────────────────────

_CHANGE_TYPE_KEYWORDS: list[tuple[tuple[str, ...], str]] = [
    (("fix", "bug", "patch", "repair"),          "BUGFIX"),
    (("refactor", "restructure", "clean"),        "REFACTOR"),
    (("config", "setting", "env"),                "CONFIG"),
    (("doc", "readme", "changelog"),              "DOCS"),
]

# ── scope path table (checked in order) ───────────────────────────────────

_SCOPE_PATH_RULES: list[tuple[tuple[str, ...], str]] = [
    (("/api/", "/routes"),        "api"),
    (("test/", "/test", ".test.", "spec"), "test"),
    (("/ui/", "/components"),     "ui"),
    (("/infra/", ".yml"),         "infra"),
    (("/docs/", ".md"),           "docs"),
]

# ── actor mapping ──────────────────────────────────────────────────────────

_SCOPE_TO_ACTOR: dict[str, str] = {
    "api":   "APIModule",
    "test":  "TestModule",
    "ui":    "UIModule",
    "infra": "InfraModule",
    "docs":  "DocModule",
}

# ── owner_policy mapping ───────────────────────────────────────────────────

_ACTOR_TO_POLICY: dict[str, str] = {
    "APIModule":   "APIPolicy",
    "TestModule":  "TestPolicy",
    "DocModule":   "DocPolicy",
    "UIModule":    "RootPolicy",    # no child policy yet
    "InfraModule": "RootPolicy",    # no child policy yet
    "Codebase":    "RootPolicy",
}


class LLMClassifier:
    """
    Deterministic commit classifier (Step 5a).
    classify() returns a dict ready for CodeChangeIntent.make().
    Does NOT call /execute — POST /commit does that.
    """

    def classify(
        self,
        description:   str,
        diff_text:     str           = "",
        files_changed: Optional[list] = None,
        codebase_path: Optional[str]  = None,
    ) -> dict:
        """
        Classify a commit description into a typed intent dict.

        Returns a fully-populated dict including
        classification_confidence. Caller strips
        classification_confidence before passing to
        CodeChangeIntent.make().
        """
        if files_changed is None:
            files_changed = []

        # ── git file detection ────────────────────────────────────────────
        if codebase_path:
            try:
                result = subprocess.run(
                    ["git", "diff", "--name-only", "HEAD"],
                    cwd            = codebase_path,
                    capture_output = True,
                    text           = True,
                    timeout        = 5,
                )
                if result.returncode == 0 and result.stdout.strip():
                    files_changed = result.stdout.strip().split("\n")
            except Exception:
                pass   # fall back to parameter

        # ── change_type ───────────────────────────────────────────────────
        desc_lower  = description.lower()
        change_type = "FEATURE"
        for keywords, ctype in _CHANGE_TYPE_KEYWORDS:
            if any(kw in desc_lower for kw in keywords):
                change_type = ctype
                break

        # ── scope ─────────────────────────────────────────────────────────
        scope = "root"
        for patterns, s in _SCOPE_PATH_RULES:
            if any(pat in path for path in files_changed for pat in patterns):
                scope = s
                break

        # ── actor + owner_policy ──────────────────────────────────────────
        actor        = _SCOPE_TO_ACTOR.get(scope, "Codebase")
        owner_policy = _ACTOR_TO_POLICY.get(actor, "RootPolicy")

        # ── hashes ───────────────────────────────────────────────────────
        hash_source = diff_text if diff_text else description
        diff_hash   = "sha256:" + hashlib.sha256(
            hash_source.encode("utf-8")
        ).hexdigest()
        prompt_hash = "sha256:" + hashlib.sha256(
            description.encode("utf-8")
        ).hexdigest()

        # ── confidence ───────────────────────────────────────────────────
        if change_type != "FEATURE":
            confidence = 0.9
        elif scope != "root":
            confidence = 0.7
        else:
            confidence = 0.6

        return {
            "change_type":               change_type,
            "scope":                     scope,
            "actor":                     actor,
            "owner_policy":              owner_policy,
            "intent_origin":             "LLMClassifier",
            "files_changed":             files_changed,
            "diff_summary":              description[:200],
            "change_description":        description,
            "diff_hash":                 diff_hash,
            "model_id":                  "ctm-classifier-v1",
            "prompt_hash":               prompt_hash,
            "session_id":                None,
            "human_reviewed":            False,
            "priority":                  1,
            "classification_confidence": confidence,
        }
