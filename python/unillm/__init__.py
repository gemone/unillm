"""unillm — unified LLM SDK.

Talk to OpenAI, Anthropic, OpenRouter, and DeepSeek through one normalized API.

This pure-Python facade sits atop the Rust core (exposed as :mod:`unillm._native`); all
provider normalization lives in exactly one place.
"""

from __future__ import annotations

from ._native import __version__  # type: ignore[attr-defined]

__all__ = ["__version__"]
