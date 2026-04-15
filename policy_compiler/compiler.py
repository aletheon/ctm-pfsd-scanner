"""
policy_compiler.compiler — PolicyScript Compiler entry point (Stage C).

Stages 1–7:
  Stage 1: Lexer          (tokenise)
  Stage 2: Parser         (parse → AST)
  Stage 3: Graph build    (build → PolicyGraph)
           Validation     (validate_1_6, validate_7_12, validate_13_17)
  Stage 4: Authority Chain  (validate_stage4)
  Stage 5: Capability Closure  (validate_stage5 — mutates graph in-place)
  Stage 6: Residual Schema  (generate_residual_schema)
  Stage 7: Serialisation + PIC Chain  (serialise + append_entry)

§48 boundary: stdlib only + policy_compiler/* + config.
No imports from server.py or any other project file.
PolicyCompiler is stateless — compile() is a pure function.
"""
from __future__ import annotations

import sys
import os
# Allow importing config from the scanner root (one level up from policy_compiler/).
_SCANNER_ROOT = os.path.dirname(os.path.dirname(__file__))
if _SCANNER_ROOT not in sys.path:
    sys.path.insert(0, _SCANNER_ROOT)
import config

from .lexer         import tokenise, LexError
from .parser        import parse,    ParseError
from .graph_builder import build,    BuildError
from .validator     import (
    validate as validate_1_6,
    validate_7_12,
    validate_13_17,
    validate_stage4,
    validate_stage5,
)
from .serialiser    import generate_residual_schema, serialise
from .pic_chain     import append_entry

_COMPILER_VERSION = "0.8.0-stage-c"
_SPEC_VERSION     = "CTM-PFSD-v0.8"


class PolicyCompiler:
    """Stateless PolicyScript compiler.  Instantiate once; call compile() freely."""

    def compile(self, source: str, project_name: str) -> dict:
        """
        Compile a PolicyScript source string through all seven stages.

        Returns a CompileResult dict:
          {
            "compiler_version":    str,
            "spec_version":        str,
            "is_valid_ctm_graph":  bool,
            "stages_completed":    [1..7] if valid, [1..5] if Stage 3-5 errors,
                                   [] if Stage 1-2 halting failure,
            "stages_deferred":     [],
            "errors":              list[dict],
            "warnings":            list[dict],
            "graph":               serialised_dict | null,
          }
        """
        try:
            # Stage 1 — Lexer
            tokens = tokenise(source)

            # Stage 2 — Parser
            ast = parse(tokens)

            # Stage 3 — Graph construction + validation
            graph = build(ast, project_name)

            r1 = validate_1_6(graph)
            r2 = validate_7_12(graph)
            r3 = validate_13_17(graph)

            # Stage 4 — Authority chain
            r4 = validate_stage4(graph)

            # Stage 5 — Capability closure (mutates graph in-place)
            r5 = validate_stage5(graph)

            errors   = (r1["errors"]   + r2["errors"]   + r3["errors"]
                        + r4["errors"]   + r5["errors"])
            warnings = (r1["warnings"] + r2["warnings"] + r3["warnings"]
                        + r4["warnings"] + r5["warnings"])

        except LexError as e:
            return _error_result(
                "E001", "SyntaxError",
                f"Lex error at {e.line}:{e.col}: {e.message}",
                stage=1,
            )
        except ParseError as e:
            code = e.code if hasattr(e, "code") else "E001"
            return _error_result(code, "SyntaxError", str(e), stage=1)
        except BuildError as e:
            return _error_result(e.code, e.code, e.message, stage=2)

        is_valid = len(errors) == 0

        # Stage 6 — Residual schema registration
        if is_valid:
            residual_result = generate_residual_schema(graph)
            if residual_result.get("error"):
                errors.append(residual_result["error"])
                is_valid = False
            else:
                graph["residual_schema"] = residual_result["schema"]

        # Stage 7 — Serialisation + PIC Chain commit
        serialised = None
        if is_valid:
            serialised = serialise(graph, project_name)
            pic_entry  = append_entry(
                serialised, project_name, config.PIC_CHAIN_PATH)
            serialised["pic_chain_entry"] = pic_entry

        return {
            "compiler_version":    _COMPILER_VERSION,
            "spec_version":        _SPEC_VERSION,
            "is_valid_ctm_graph":  is_valid,
            "stages_completed":    [1, 2, 3, 4, 5, 6, 7] if is_valid else [1, 2, 3, 4, 5],
            "stages_deferred":     [],
            "errors":              errors,
            "warnings":            warnings,
            "graph":               serialised if is_valid else None,
        }


# ── Helper ─────────────────────────────────────────────────────────────────

def _error_result(code: str, name: str, message: str, stage: int) -> dict:
    """Return a halting-failure CompileResult with a single error entry."""
    return {
        "compiler_version":    _COMPILER_VERSION,
        "spec_version":        _SPEC_VERSION,
        "is_valid_ctm_graph":  False,
        "stages_completed":    [],
        "stages_deferred":     [],
        "errors": [{
            "code":    code,
            "name":    name,
            "message": message,
            "node_id": None,
            "stage":   stage,
        }],
        "warnings": [],
        "graph":    None,
    }
