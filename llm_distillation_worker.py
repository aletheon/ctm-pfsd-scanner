"""
LLM Distillation Worker (§54.3) — implements PatternDetectionInterface using an LLM.
Reads ResidualStore only. Produces ProposalBundles. Never runs during a tick.

§48 boundary: imports only config, stdlib, and learning modules — no runtime engine classes.
"""
from __future__ import annotations
import hashlib
import json
import uuid
from typing import Optional
import config
from learning.pattern_detection import PatternDetectionInterface, PatternReport
from learning.llm_provider import LLMProviderInterface, get_provider
from learning.llm_residual_exporter import LLMResidualExporter

_LLM_NAMESPACE = uuid.UUID("6ba7b820-9dad-11d1-80b4-00c04fd430c8")

SYSTEM_PROMPT = """You are a policy governance assistant for a \
sovereign execution system. You analyse runtime residuals and \
propose governance rule changes to improve system performance.

You MUST respond with valid JSON only. No markdown. No explanation \
outside the JSON structure.

Response format:
{
  "patterns": [
    {
      "pattern_type": "THRESHOLD_DRIFT|HIGH_ERROR_RATE|RULE_GAP|LOW_SERVICE_RATE",
      "config_path": "the config key to change",
      "current_value": <number>,
      "proposed_value": <number>,
      "confidence": <0.75-0.99>,
      "observations": <integer>,
      "rationale": "explanation of at least 20 characters",
      "safe_min": <number>,
      "safe_max": <number>
    }
  ]
}

Rules you must follow:
- Only propose changes to numeric config thresholds
- Never propose new actors, services, or state schema changes
- confidence must be between 0.75 and 0.99
- rationale must be at least 20 characters
- proposed_value must be between safe_min and safe_max
- If no patterns warrant changes, return {"patterns": []}
"""


class LLMDistillationWorker(PatternDetectionInterface):
    """
    LLM-backed pattern detector (§54.3).
    §48 boundary: imports only config, stdlib, and learning modules.
    """

    def __init__(
        self,
        provider:  Optional[LLMProviderInterface] = None,
        window_id: str = "default",
    ):
        self.provider  = provider or get_provider(config.LLM_PROVIDER)
        self.exporter  = LLMResidualExporter()
        self.window_id = window_id
        self._last_prompt_hash: Optional[str] = None

    def analyse(
        self,
        residual_window: list,
        bdh_pathways:    list,
        policy_graph:    dict,
    ) -> list:
        context     = self.exporter.export(
            residual_window, bdh_pathways, policy_graph, self.window_id
        )
        prompt_hash = self.exporter.export_hash(context)

        if not self.provider.is_available():
            return []

        user_prompt  = f"Analyse the following residual window:\n\n{context}"
        response_str = self.provider.complete(
            prompt     = user_prompt,
            max_tokens = config.LLM_MAX_TOKENS,
        )

        if len(response_str.strip()) < config.LLM_MIN_RESPONSE_LENGTH:
            return []

        try:
            response = json.loads(response_str)
        except (json.JSONDecodeError, ValueError):
            return []

        reports = []
        for p in response.get("patterns", []):
            if not self._validate_pattern(p):
                continue
            reports.append(PatternReport(
                pattern_id       = self._make_pattern_id(p, prompt_hash),
                detector_id      = config.DETECTOR_ID_LLM,
                service_pair     = (p.get("config_path", ""),),
                pattern_type     = p["pattern_type"],
                confidence       = float(p["confidence"]),
                observations     = int(p["observations"]),
                improvement_rate = 0.0,
                bdh_weight       = 0.0,
                evidence         = p,
                recommended_rule = p["rationale"],
            ))

        self._last_prompt_hash = prompt_hash
        return [r for r in reports if r.confidence >= config.LLM_ILP_MIN_CONFIDENCE]

    def _validate_pattern(self, p: dict) -> bool:
        required = [
            "pattern_type", "config_path", "current_value",
            "proposed_value", "confidence", "observations",
            "rationale", "safe_min", "safe_max",
        ]
        if not all(k in p for k in required):
            return False
        try:
            conf     = float(p["confidence"])
            proposed = float(p["proposed_value"])
            safe_min = float(p["safe_min"])
            safe_max = float(p["safe_max"])
            rat_len  = len(str(p["rationale"]))
        except (TypeError, ValueError):
            return False
        if not (0.0 <= conf <= 1.0):
            return False
        if rat_len < config.DISTILLATION_MIN_RATIONALE_LEN:
            return False
        if not (safe_min <= proposed <= safe_max):
            return False
        return True

    def _make_pattern_id(self, p: dict, prompt_hash: str) -> str:
        key = (
            str(p.get("pattern_type", ""))
            + str(p.get("config_path", ""))
            + str(p.get("proposed_value", ""))
            + prompt_hash[:16]
        )
        return str(uuid.uuid5(_LLM_NAMESPACE, key))
