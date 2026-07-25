"""SDK streaming + cancellation against a mock SSE upstream."""

from __future__ import annotations

import gc

import anyio

import unillm

from .conftest import MockServer

# A complete Chat Completions SSE document that spells "Hello" and ends with [DONE].
CC_HELLO_SSE = [
    'data: {"id":"c1","model":"gpt-4o","choices":[{"index":0,"delta":{"content":"Hel"}}]}\n\n',
    'data: {"id":"c1","model":"gpt-4o","choices":[{"index":0,"delta":{"content":"lo"}}]}\n\n',
    'data: {"id":"c1","model":"gpt-4o","choices":[{"index":0,"delta":{},"finish_reason":"stop"}]}\n\n',
    'data: {"id":"c1","model":"gpt-4o","choices":[],"usage":{"prompt_tokens":5,"completion_tokens":1}}\n\n',
    "data: [DONE]\n\n",
]


async def test_stream_iteration(mock_server: MockServer) -> None:
    mock_server.sse = CC_HELLO_SSE
    client = unillm.Client("openai", "sk-test", base_url=mock_server.url)

    text: list[str] = []
    completed = None
    async for event in client.stream("gpt-4o", input="hi"):
        if event.get("type") == "text_delta":
            text.append(event["text"])
        elif event.get("type") == "completed":
            completed = event.get("response")

    assert "".join(text) == "Hello"
    assert completed is not None
    assert completed["stop_reason"] == "end_turn"
    assert completed["usage"]["input_tokens"] == 5
    # the wire request asked to stream
    assert mock_server.received[0]["stream"] is True


async def test_stream_collect(mock_server: MockServer) -> None:
    mock_server.sse = CC_HELLO_SSE
    client = unillm.Client("openai", "sk-test", base_url=mock_server.url)

    resp = await client.stream("gpt-4o", input="hi").collect()

    assert resp.text == "Hello"
    assert resp.stop_reason == "end_turn"
    assert resp.usage.input_tokens == 5


async def test_stream_cancellation_closes_upstream(mock_server: MockServer) -> None:
    # An endless slow stream: the mock keeps sending until the client disconnects.
    mock_server.sse = [
        'data: {"id":"c1","model":"gpt-4o","choices":[{"index":0,"delta":{"content":"x"}}]}\n\n'
    ]
    mock_server.slow = True
    client = unillm.Client("openai", "sk-test", base_url=mock_server.url)

    stream = client.stream("gpt-4o", input="hi")
    seen = 0
    async for _event in stream:
        seen += 1
        if seen >= 3:
            break

    # Drop the stream and force collection so the Rust Drop handler aborts the producer.
    del stream
    gc.collect()
    await anyio.sleep(0.3)
    snapshot = mock_server.frames_sent
    await anyio.sleep(0.3)

    # If cancellation worked, the upstream stopped receiving reads shortly after the drop —
    # frames_sent must not keep climbing unbounded.
    assert mock_server.frames_sent - snapshot < 50
    assert seen == 3
