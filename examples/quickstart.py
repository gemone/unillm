"""unillm quickstart — talk to any provider through one normalized API.

Direct to a provider:
    pip install -e .
    export UNILLM_PROVIDER=openai UNILLM_API_KEY=sk-...
    python examples/quickstart.py

…or through the unillm-proxy (any backend, virtual keys, caching, usage/cost logging):
    export UNILLM_BASE_URL=http://localhost:8080
    export UNILLM_PROVIDER=openai UNILLM_API_KEY=sk-unillm-...   # a proxy virtual key
    python examples/quickstart.py
"""
from __future__ import annotations

import asyncio

import unillm


async def create_one(c: unillm.Client) -> None:
    r = await c.create("gpt-4o", input="Say hi in one word.")
    print(f"[create]     {r.text!r}  ({r.usage.output_tokens} out tokens)")


async def stream_one(c: unillm.Client) -> None:
    print("[stream]    ", end="", flush=True)
    # `stream` returns an async iterator directly (no `await`); canonical events (DESIGN.md §4.9).
    async for ev in c.stream("gpt-4o", input="Count from 1 to 3."):
        if ev.get("type") == "text_delta":
            print(ev["text"], end="", flush=True)
    print()


async def tool_use(c: unillm.Client) -> None:
    tools = [
        {
            "name": "get_weather",
            "description": "Get the current weather for a city.",
            "input_schema": {
                "type": "object",
                "properties": {"city": {"type": "string"}},
                "required": ["city"],
            },
        }
    ]
    r = await c.create("gpt-4o", input="What's the weather in Paris?", tools=tools)
    if r.tool_calls:
        call = r.tool_calls[0]
        print(f"[tool_use]   model called {call['name']}({call['arguments']})")
    else:
        print(f"[tool_use]   no tool call — {r.text!r}")


async def main() -> None:
    # from_env() reads UNILLM_PROVIDER / UNILLM_API_KEY / UNILLM_BASE_URL / UNILLM_TIMEOUT.
    client = unillm.Client.from_env()
    try:
        await create_one(client)
        await stream_one(client)
        await tool_use(client)
    except unillm.UnillmError as e:
        # Typed hierarchy: InvalidRequestError / AuthenticationError / RateLimitError / …
        print(f"[error] {type(e).__name__}: {e.message}")
    finally:
        await client.aclose()


if __name__ == "__main__":
    asyncio.run(main())
