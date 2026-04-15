"""Minimal config.py — provides constants for bdh_kernel.py (§48 boundary)."""
BDH_LEARNING_RATE              = 0.05
BDH_DECAY_RATE                 = 0.001
BDH_STABLE_WEIGHT_THRESHOLD    = 0.6
BDH_STABLE_MIN_OBSERVATIONS    = 5
BDH_PRUNE_WEIGHT_THRESHOLD     = 0.05
BDH_PRUNE_TICK_WINDOW          = 1000
BDH_PATHWAY_NAMESPACE          = "12345678-1234-5678-1234-567812345678"
BDH_STATUS_FORMING             = "FORMING"
BDH_STATUS_STABLE              = "STABLE"
BDH_STATUS_DECAYING            = "DECAYING"
BDH_STATUS_ELEVATED            = "ELEVATED"
BDH_STATUS_FORMALISED          = "FORMALISED"
BDH_STATUS_PRUNED              = "PRUNED"
OUTCOME_SUCCESS                = "SUCCESS"
OUTCOME_FAILED                 = "FAILED"
DISTILLATION_TARGET_SERVICE    = "HistoricalContextService"
DISTILLATION_BASELINE_SERVICE  = "ShellCommandService"
DISTILLATION_MIN_OBSERVATIONS  = 10
DISTILLATION_CROSSING_RATIO_THRESHOLD = 0.4
DISTILLATION_CONFIDENCE_FLOOR  = 0.6
DISTILLATION_CONFIDENCE_MAX    = 0.95
DISTILLATION_CONFIDENCE_SLOPE  = 1.5
DISTILLATION_COOLING_LOG_PATH  = "/tmp/cooling.json"
DISTILLATION_COOLING_KEY       = "service_call_ratio_threshold"
DISTILLATION_THRESHOLD_CURRENT = 0.4
DISTILLATION_THRESHOLD_PROPOSED= 0.25
DISTILLATION_SAFE_MIN          = 0.1
DISTILLATION_SAFE_MAX          = 0.9
DISTILLATION_CONFIG_PATCH_PATH = "service_call_ratio_threshold"
DISTILLATION_GENERATOR_ID      = "heuristic_detector_v1"
DISTILLATION_GENERATOR_VERSION = "1.0.0"
DISTILLATION_MIN_RATIONALE_LEN = 20
LLM_PROVIDER                   = "mock"
LLM_MAX_TOKENS                 = 1000
LLM_MIN_RESPONSE_LENGTH        = 10
LLM_ILP_MIN_CONFIDENCE         = 0.75
LNN_ILP_TARGET_SUCCESS_RATE    = 0.8
PROPOSAL_SOURCE_DW             = "distillation_worker"
PROPOSAL_SOURCE_LNN_ILP        = "lnn_ilp"
POLICY_AGENT_ROOT              = "CodebasePolicy"
DETECTOR_ID_HEURISTIC          = "heuristic"
DETECTOR_ID_LNN_ILP            = "lnn_ilp"
DETECTOR_ID_LLM                = "llm"
PATTERN_LOW_RATE               = "LOW_SERVICE_RATE"
PATTERN_THRESHOLD_DRIFT        = "THRESHOLD_DRIFT"
PIC_TYPE_PROPOSAL              = "PROPOSAL_SUBMITTED"
PIC_TYPE_REJECTION             = "PROPOSAL_REJECTED"
COMPILER_KNOWN_PATCH_PATHS     = ["service_call_ratio_threshold"]

# ── PolicyScript Compiler ─────────────────────────────────────────────────
PIC_CHAIN_PATH      = "/tmp/ctm_pic_chain.jsonl"
RESIDUAL_STORE_PATH = "/tmp/ctm_residual_store.jsonl"
BDH_STORE_PATH      = "/tmp/ctm_bdh_pathways.json"
CODEBASE_PATH       = __import__("os").environ.get(
    "CODEBASE_PATH", "/tmp/ctm_governed_codebase"
)
RUNTIME_CLOCK_PATH  = "/tmp/ctm_runtime_clock.json"
MEMORY_GRAPH_PATH   = "/tmp/ctm_memory_graph.json"

# ── LLM Pattern Reports (Phase 2 Stage 4) ─────────────────────────────────
LLM_PROVIDER            = "mock"              # "mock" | "anthropic"
LLM_ANTHROPIC_MODEL     = "claude-sonnet-4-6"
LLM_MAX_NODES_PER_SCAN  = 3                  # cost control: max nodes analysed per scan
LLM_MAX_TOKENS          = 400                # keep responses concise
LLM_MIN_CONFIDENCE      = 0.60
LLM_MAX_CONFIDENCE      = 0.95

# LLM system prompt — strictly observational, never prescriptive
LLM_SYSTEM_PROMPT = (
    "You are a structural analysis assistant for a code governance scanner. "
    "Your role is to describe what you observe in code structure — not to propose "
    "governance decisions. Governance decisions (who owns what, what rules apply, "
    "which actor governs which service) require human authorship and are outside "
    "your scope. Describe gaps, complexity, and ambiguity. Never propose solutions. "
    "Do not generate any text that resembles code, rules, or executable structures."
)
