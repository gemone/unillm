"""unillm — unified LLM SDK.

Talk to OpenAI, Anthropic, OpenRouter, and DeepSeek through one normalized API. This pure-Python
facade sits atop the Rust core (exposed as :mod:`unillm._native`); all provider normalization lives
in exactly one place.
"""

from __future__ import annotations

import json
import os
from dataclasses import dataclass
from typing import Any, Mapping, Sequence

from ._native import Client as _NativeClient
from ._native import UnillmError
from ._native import __version__

__all__ = ["Client", "Response", "Usage", "UnillmError", "from_env", "__version__"]


def _build_input(
    input_: str | Sequence[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    if input_ is None:
        return []
    if isinstance(input_, str):
        return [{"type": "message", "role": "user", "content": input_}]
    return [dict(item) for item in input_]


@dataclass
class Usage:
    input_tokens: int
    output_tokens: int
    cache_read: int
    cache_creation: int
    cost_usd: float | None = None

    @property
    def total_input(self) -> int:
        return self.input_tokens + self.cache_read + self.cache_creation


@dataclass
class Response:
    id: str
    model: str
    provider: str
    output: list[Mapping[str, Any]]
    stop_reason: str
    usage: Usage

    @classmethod
    def from_json(cls, raw: str) -> "Response":
        d = json.loads(raw)
        usage = d.get("usage") or {}
        return cls(
            id=d.get("id", ""),
            model=d.get("model", ""),
            provider=d.get("provider", ""),
            output=list(d.get("output", [])),
            stop_reason=d.get("stop_reason", "other"),
            usage=Usage(
                input_tokens=usage.get("input_tokens", 0),
                output_tokens=usage.get("output_tokens", 0),
                cache_read=usage.get("cache_read", 0),
                cache_creation=usage.get("cache_creation", 0),
                cost_usd=usage.get("cost_usd"),
            ),
        )

    @property
    def text(self) -> str:
        """Concatenated assistant text (lossy convenience)."""
        parts: list[str] = []
        for item in self.output:
            if item.get("type") == "message" and item.get("role") == "assistant":
                content = item.get("content")
                if isinstance(content, str):
                    parts.append(content)
                elif isinstance(content, list):
                    for block in content:
                        if isinstance(block, Mapping) and block.get("type") == "text":
                            parts.append(block.get("text", ""))
        return "".join(parts)

    @property
    def tool_calls(self) -> list[Mapping[str, Any]]:
        return [i for i in self.output if i.get("type") == "function_call"]


class Client:
    """Async client for talking to a provider through the canonical API."""

    def __init__(
        self,
        provider: str,
        api_key: str,
        *,
        base_url: str | None = None,
        timeout: float | None = None,
    ) -> None:
        self._native = _NativeClient(provider, api_key, base_url=base_url, timeout=timeout)

    @classmethod
    def from_env(cls) -> "Client":
        provider = os.environ["UNILLM_PROVIDER"]
        api_key = os.environ["UNILLM_API_KEY"]
        base_url = os.environ.get("UNILLM_BASE_URL")
        timeout = (
            float(os.environ["UNILLM_TIMEOUT"]) if "UNILLM_TIMEOUT" in os.environ else None
        )
        return cls(provider, api_key, base_url=base_url, timeout=timeout)

    async def create(
        self,
        model: str,
        *,
        instructions: str | None = None,
        input: str | Sequence[Mapping[str, Any]] | None = None,
        max_tokens: int | None = None,
        temperature: float | None = None,
        top_p: float | None = None,
        stop: Sequence[str] | None = None,
        tools: Sequence[Mapping[str, Any]] | None = None,
        tool_choice: Mapping[str, Any] | None = None,
        cache: Mapping[str, Any] | None = None,
        metadata: Mapping[str, Any] | None = None,
    ) -> Response:
        request: dict[str, Any] = {"model": model, "input": _build_input(input)}
        if instructions is not None:
            request["instructions"] = instructions
        if max_tokens is not None:
            request["max_tokens"] = max_tokens
        if temperature is not None:
            request["temperature"] = temperature
        if top_p is not None:
            request["top_p"] = top_p
        if stop is not None:
            request["stop"] = list(stop)
        if tools is not None:
            request["tools"] = [dict(t) for t in tools]
        if tool_choice is not None:
            request["tool_choice"] = dict(tool_choice)
        if cache is not None:
            request["cache"] = dict(cache)
        if metadata is not None:
            request["metadata"] = dict(metadata)

        raw = await self._native.create(json.dumps(request))
        return Response.from_json(raw)
