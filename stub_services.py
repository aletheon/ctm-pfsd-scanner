"""
stub_services.py — Governed filesystem services for Path A execution.

CTM-PFSD Code Governance Spec v1.6 §7.1–7.3.
Three pure-function services that perform real filesystem writes
and a registry that maps service labels to invocations.

Zone 3 service purity rules:
  - Services are pure functions
  - No governance logic
  - No persistent internal state
  - No S→S calls
  - No imports from server.py, compiler, or any Zone 4 file

§48 boundary: stdlib only (os, pathlib, json, hashlib, datetime)
              + config (for CODEBASE_PATH default).
"""
from __future__ import annotations

import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import config


# ── DiffAnalyserService ────────────────────────────────────────────────────

class DiffAnalyserService:
    """
    §7.4 — Analyses file paths to produce change-impact flags.
    endpoint: process://git-diff
    simulation_safe: true
    """

    def analyse(self, files_changed: list) -> dict:
        paths = [str(p) for p in files_changed]

        api_signature_changed = any(
            "/api/" in p or "/schema" in p for p in paths
        )
        tests_affected = any(
            "test" in p or "spec" in p for p in paths
        )
        docs_affected = any(
            ".md" in p or "/docs/" in p for p in paths
        )
        schema_changed = any(
            "schema" in p or ".json" in p for p in paths
        )
        breaking_change = (
            any("/api/" in p for p in paths) and
            any("schema" in p for p in paths)
        )

        return {
            "status":                "ok",
            "service":               "DiffAnalyserService",
            "api_signature_changed": api_signature_changed,
            "tests_affected":        tests_affected,
            "docs_affected":         docs_affected,
            "schema_changed":        schema_changed,
            "breaking_change":       breaking_change,
            "files_analysed":        len(paths),
            "state_delta":           {},
        }


# ── SchemaValidatorService ─────────────────────────────────────────────────

class SchemaValidatorService:
    """
    §7.6 — Schema validation stub. Always returns valid (safe default).
    endpoint: process://schema-validator
    simulation_safe: true
    """

    def validate(self, scope: str) -> dict:
        schema_hash = hashlib.sha256(scope.encode()).hexdigest()[:16]
        return {
            "status":          "ok",
            "service":         "SchemaValidatorService",
            "scope":           scope,
            "valid":           True,
            "violation_count": 0,
            "schema_hash":     schema_hash,
            "state_delta":     {"schema_validated": True},
        }


# ── TestRunnerService ──────────────────────────────────────────────────────

class TestRunnerService:
    """
    §7.5 — Test runner stub. Returns safe passing defaults.
    endpoint: process://test-runner
    simulation_safe: false  — real runner would execute test suite
    failure_threshold: 3
    """

    def run(self, scope: str) -> dict:
        return {
            "status":        "ok",
            "service":       "TestRunnerService",
            "scope":         scope,
            "passing":       True,
            "coverage_pct":  0.85,
            "failure_count": 0,
            "state_delta":   {"tests_ran": True, "tests_passing": True},
        }


def _sha256_file(path: Path) -> str:
    """Return sha256 hex digest of the full contents of path."""
    h = hashlib.sha256()
    with open(path, "rb") as f:
        for chunk in iter(lambda: f.read(65536), b""):
            h.update(chunk)
    return h.hexdigest()


def _utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


class ReadmeSyncService:
    """
    §7.1 — Appends a governed update block to README.md.
    endpoint: file://codebase/README.md
    simulation_safe: true
    """

    def sync(
        self,
        diff_summary:  str,
        files_changed: tuple,
        codebase_path: Optional[str] = None,
    ) -> dict:
        root   = Path(codebase_path or config.CODEBASE_PATH)
        target = root / "README.md"
        try:
            root.mkdir(parents=True, exist_ok=True)

            file_list = "\n".join(
                f"- {f}" for f in files_changed
            ) or "- (none)"

            block = (
                f"\n## Governance Update\n"
                f"_Recorded by ReadmeSyncService at {_utc_now_iso()}_\n\n"
                f"**Change summary:** {diff_summary}\n\n"
                f"**Files changed:**\n{file_list}\n\n"
                f"---\n"
            )

            with open(target, "a", encoding="utf-8") as f:
                f.write(block)

            appended_bytes = len(block.encode("utf-8"))
            readme_hash    = _sha256_file(target)

            return {
                "status":        "ok",
                "service":       "ReadmeSyncService",
                "diff_summary":  diff_summary,
                "files_updated": list(files_changed),
                "readme_path":   str(target),
                "appended_bytes": appended_bytes,
                "state_delta": {
                    "readme_last_updated": True,
                    "readme_hash":         readme_hash,
                },
            }
        except OSError as e:
            return {
                "status":  "error",
                "service": "ReadmeSyncService",
                "error":   str(e),
                "state_delta": {"readme_last_updated": False},
            }


class ChangelogWriterService:
    """
    §7.2 — Appends one changelog entry to CHANGELOG.md.
    endpoint: file://codebase/CHANGELOG.md
    simulation_safe: true
    """

    def append(
        self,
        description:   str,
        actor:         str,
        codebase_path: Optional[str] = None,
    ) -> dict:
        root   = Path(codebase_path or config.CODEBASE_PATH)
        target = root / "CHANGELOG.md"
        try:
            root.mkdir(parents=True, exist_ok=True)

            entry = f"\n### [{_utc_date()}] {actor}\n{description}\n"

            with open(target, "a", encoding="utf-8") as f:
                f.write(entry)

            appended_bytes    = len(entry.encode("utf-8"))
            changelog_hash    = _sha256_file(target)

            return {
                "status":           "ok",
                "service":          "ChangelogWriterService",
                "description":      description,
                "actor":            actor,
                "changelog_path":   str(target),
                "appended_bytes":   appended_bytes,
                "state_delta": {
                    "changelog_entry_appended": True,
                    "changelog_last_hash":      changelog_hash,
                },
            }
        except OSError as e:
            return {
                "status":  "error",
                "service": "ChangelogWriterService",
                "error":   str(e),
                "state_delta": {"changelog_entry_appended": False},
            }


class ProvenanceLoggerService:
    """
    §7.3 — Appends one JSON provenance record to .governance/provenance.log.
    endpoint: file://codebase/.governance/provenance.log
    simulation_safe: true
    """

    def record(
        self,
        model_id:      str,
        prompt_hash:   str,
        diff_hash:     str,
        tick:          int,
        codebase_path: Optional[str] = None,
    ) -> dict:
        root   = Path(codebase_path or config.CODEBASE_PATH)
        target = root / ".governance" / "provenance.log"
        try:
            target.parent.mkdir(parents=True, exist_ok=True)

            record_dict = {
                "timestamp":   _utc_now_iso(),
                "model_id":    model_id,
                "prompt_hash": prompt_hash,
                "diff_hash":   diff_hash,
                "tick":        tick,
            }
            line = json.dumps(record_dict, separators=(",", ":")) + "\n"

            with open(target, "a", encoding="utf-8") as f:
                f.write(line)

            entry_hash = hashlib.sha256(line.encode("utf-8")).hexdigest()

            return {
                "status":          "ok",
                "service":         "ProvenanceLoggerService",
                "model_id":        model_id,
                "diff_hash":       diff_hash,
                "tick":            tick,
                "provenance_path": str(target),
                "state_delta": {
                    "provenance_recorded":     True,
                    "provenance_entry_hash":   entry_hash,
                },
            }
        except OSError as e:
            return {
                "status":  "error",
                "service": "ProvenanceLoggerService",
                "error":   str(e),
                "state_delta": {"provenance_recorded": False},
            }


class StubServiceRegistry:
    """
    Maps service labels to governed service invocations.
    Zone 2 calls invoke(service_label, intent, tick).
    Returns service output dict, or a default stub if the label
    does not match any registered service.
    """

    def __init__(self) -> None:
        self._readme          = ReadmeSyncService()
        self._changelog       = ChangelogWriterService()
        self._provenance      = ProvenanceLoggerService()
        self._diff_analyser   = DiffAnalyserService()
        self._schema_validator = SchemaValidatorService()
        self._test_runner     = TestRunnerService()
        from historical_context_service import HistoricalContextService
        self._hcs             = HistoricalContextService()

    def invoke(self, service_label: str, intent, tick: int) -> dict:
        if "Readme" in service_label or "Sync" in service_label:
            return self._readme.sync(
                intent.diff_summary,
                intent.files_changed,
                codebase_path=config.CODEBASE_PATH,
            )
        if "Changelog" in service_label or "Writer" in service_label:
            return self._changelog.append(
                intent.change_description,
                intent.actor,
                codebase_path=config.CODEBASE_PATH,
            )
        if "Provenance" in service_label or "Logger" in service_label:
            return self._provenance.record(
                intent.model_id,
                intent.prompt_hash,
                intent.diff_hash,
                tick,
                codebase_path=config.CODEBASE_PATH,
            )
        if "Diff" in service_label or "Analyser" in service_label:
            return self._diff_analyser.analyse(
                list(intent.files_changed))
        if "Schema" in service_label or "Validator" in service_label:
            scope = getattr(intent, "scope", "root")
            return self._schema_validator.validate(scope)
        if "Test" in service_label or "Runner" in service_label:
            scope = getattr(intent, "scope", "root")
            return self._test_runner.run(scope)
        if ("Historical" in service_label or
                "Context" in service_label or
                "HCS" in service_label):
            query_type = getattr(intent, "scope", "pattern")
            return self._hcs.query(
                query_type         = query_type,
                horizon_ticks      = 200,
                residual_window_id = intent.intent_id,
                memory_graph_hash  = "",
            )
        # Unknown service — return a stub default
        return {
            "status":  "ok",
            "service": service_label,
            "stub":    True,
            "tick":    tick,
        }
