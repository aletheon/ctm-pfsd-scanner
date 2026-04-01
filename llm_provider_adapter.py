"""
llm_provider_adapter.py — minimal LLM provider for Phase 2 pattern reports.

Provides MockLLMProvider (always used in testing) and AnthropicProvider
(used when LLM_PROVIDER == "anthropic" and ANTHROPIC_API_KEY is set).

§48 boundary: imports only config, stdlib, and optional anthropic SDK.
"""
from __future__ import annotations
import json
import os
import config


class MockLLMProvider:
    """
    Returns canned structural observations for testing.
    No external API calls. Deterministic output.
    """

    def is_available(self) -> bool:
        return True

    def complete(self, prompt: str, max_tokens: int = 400) -> str:
        # Canned response: one structural observation, no prescriptive language
        return json.dumps({
            "patterns": [
                {
                    "type":        "structural_observation",
                    "category":    "ACTOR_SCOPE",
                    "description": (
                        "Multiple services detected across distinct concern areas "
                        "without a unified governance boundary. The absence of actor "
                        "scope declarations means the authority structure for this "
                        "module is undefined in governance terms."
                    ),
                    "confidence":  0.72,
                    "layer":       "WHY"
                }
            ]
        })


class AnthropicProvider:
    """
    Calls the Anthropic API when ANTHROPIC_API_KEY is set.
    Falls back to unavailable if the SDK is not installed or key is absent.
    """

    def __init__(self):
        self._client = None
        try:
            import anthropic
            api_key = os.environ.get("ANTHROPIC_API_KEY", "")
            if api_key:
                self._client = anthropic.Anthropic(api_key=api_key)
        except ImportError:
            pass

    def is_available(self) -> bool:
        return self._client is not None

    def complete(self, prompt: str, max_tokens: int = 400) -> str:
        if not self._client:
            return '{"patterns":[]}'
        try:
            msg = self._client.messages.create(
                model      = config.LLM_ANTHROPIC_MODEL,
                max_tokens = max_tokens,
                system     = config.LLM_SYSTEM_PROMPT,
                messages   = [{"role": "user", "content": prompt}],
            )
            return msg.content[0].text if msg.content else '{"patterns":[]}'
        except Exception:
            return '{"patterns":[]}'


def get_provider(provider_name: str):
    if provider_name == "anthropic":
        return AnthropicProvider()
    return MockLLMProvider()
