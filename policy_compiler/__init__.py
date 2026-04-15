"""
policy_compiler — PolicyScript Compiler for CTM-PFSD.

Stage A (Stages 1–3): Lexer, Parser/AST, Policy Graph, Semantic Validation.
Stages 4–7 deferred to Stage B and C.

§48 boundary: all modules import only stdlib and config.
Zone 3 purity: no module calls any other project service.
"""
