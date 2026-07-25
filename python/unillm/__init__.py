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
from ._native import EventStream as _NativeEventStream
from ._native import UnillmError as _NativeUnillmError
from ._native import __version__
from .exceptions import (
    AuthenticationError,
    InvalidRequestError,
    NotFoundError,
    ProviderError,
    RateLimitError,
    SerializationError,
    StreamError,
    UnillmError,
    from_native,
)

__all__ = [
    # client + types
    "Client",
    "EventStream",
    "Response",
    "Usage",
    # exceptions
    "UnillmError",
    "InvalidRequestError",
    "AuthenticationError",
    "NotFoundError",
    "RateLimitError",
    "ProviderError",
    "StreamError",
    "SerializationError",
    # helpers
    "user",
    "assistant",
    "system",
    "tool_result",
    "image_url",
    "image_base64",
    "Cache",
    "from_env",
    "__version__",
]


def _build_input(
    input_: str | Sequence[Mapping[str, Any]] | None,
) -> list[dict[str, Any]]:
    if input_ is None:
        return []
    if isinstance(input_, str):
        return [{"type": "message", "role": "user", "content": input_}]
    return [dict(item) for item in input_]


def _build_request(
    model: str,
    *,
    instructions: str | None,
    input: str | Sequence[Mapping[str, Any]] | None,
    max_tokens: int | None,
    temperature: float | None,
    top_p: float | None,
    stop: Sequence[str] | None,
    tools: Sequence[Mapping[str, Any]] | None,
    tool_choice: Mapping[str, Any] | None,
    cache: Mapping[str, Any] | None,
    metadata: Mapping[str, Any] | None,
) -> dict[str, Any]:
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
    return request


# --- typed wrappers -----------------------------------------------------------


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
    def from_dict(cls, d: Mapping[str, Any]) -> "Response":
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

    @classmethod
    def from_json(cls, raw: str) -> "Response":
        return cls.from_dict(json.loads(raw))

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


class EventStream:
    """Async iterator of canonical stream-event dicts, with a ``.collect()`` helper."""

    def __init__(self, native: _NativeEventStream) -> None:
        self._native = native

    def __aiter__(self) -> "EventStream":
        return self

    async def __anext__(self) -> dict[str, Any]:
        try:
            raw = await self._native.__anext__()  # StopAsyncIteration propagates at end of stream
        except _NativeUnillmError as e:
            raise from_native(e) from None
        event: dict[str, Any] = json.loads(raw)
        return event

    async def collect(self) -> Response:
        """Drain the stream and return the completed `Response`."""
        completed: Mapping[str, Any] | None = None
        async for event in self:
            if event.get("type") == "completed":
                completed = event.get("response")
        if completed is None:
            raise UnillmError("stream ended without a completed event")
        return Response.from_dict(completed)


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

    async def aclose(self) -> None:
        """Release the underlying client. The transport is also released on GC; this is a no-op
        kept for API parity with the spec."""

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
        request = _build_request(
            model,
            instructions=instructions,
            input=input,
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p,
            stop=stop,
            tools=tools,
            tool_choice=tool_choice,
            cache=cache,
            metadata=metadata,
        )
        try:
            raw = await self._native.create(json.dumps(request))
        except _NativeUnillmError as e:
            raise from_native(e) from None
        return Response.from_json(raw)

    def stream(
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
    ) -> EventStream:
        request = _build_request(
            model,
            instructions=instructions,
            input=input,
            max_tokens=max_tokens,
            temperature=temperature,
            top_p=top_p,
            stop=stop,
            tools=tools,
            tool_choice=tool_choice,
            cache=cache,
            metadata=metadata,
        )
        return EventStream(self._native.stream(json.dumps(request)))


# --- item / content helpers (DESIGN.md §9.1) ---------------------------------


def user(content: str | Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """A user message item."""
    return {"type": "message", "role": "user", "content": content}


def assistant(content: str | Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """An assistant message item."""
    return {"type": "message", "role": "assistant", "content": content}


def system(text: str) -> dict[str, Any]:
    """A system message item."""
    return {"type": "message", "role": "system", "content": text}


def tool_result(call_id: str, output: str) -> dict[str, Any]:
    """A function-call output item (`call_id` correlates with the `function_call.id`)."""
    return {"type": "function_call_output", "call_id": call_id, "output": output}


def image_url(url: str) -> dict[str, Any]:
    """An image content block by URL."""
    return {"type": "image", "source": {"type": "url", "url": url}}


def image_base64(media_type: str, data: str) -> dict[str, Any]:
    """An image content block from inline base64 data."""
    return {"type": "image", "source": {"type": "base64", "media_type": media_type, "data": data}}


class Cache:
    """Cache-strategy builders (`DESIGN.md` §7)."""

    @staticmethod
    def auto() -> dict[str, Any]:
        return {"kind": "auto"}

    @staticmethod
    def none() -> dict[str, Any]:
        return {"kind": "none"}

    @staticmethod
    def explicit(breakpoints: Sequence[Mapping[str, Any]], ttl: str = "5m") -> dict[str, Any]:
        return {
            "kind": "explicit",
            "breakpoints": [dict(b) for b in breakpoints],
            "ttl": ttl,
        }


def from_env() -> Client:
    """Convenience: ``Client.from_env()``."""
    return Client.from_env()
