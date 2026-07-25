"""SDK `Client.create` end-to-end against a mock upstream."""

from __future__ import annotations

import json

import pytest

import unillm

from .conftest import MockServer


async def test_create_openai_text(mock_server: MockServer) -> None:
    mock_server.body = {
        "id": "chatcmpl-1",
        "model": "gpt-4o",
        "choices": [
            {
                "index": 0,
                "message": {"role": "assistant", "content": "hello"},
                "finish_reason": "stop",
            }
        ],
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 2,
            "prompt_tokens_details": {"cached_tokens": 4},
        },
    }
    client = unillm.Client("openai", "sk-test", base_url=mock_server.url)

    resp = await client.create("gpt-4o", input="hi")

    assert resp.text == "hello"
    assert resp.stop_reason == "end_turn"
    assert resp.usage.cache_read == 4
    assert resp.usage.input_tokens == 6
    assert resp.usage.total_input == 10

    assert len(mock_server.received) == 1
    sent = mock_server.received[0]
    assert sent["path"] == "/chat/completions"
    assert sent["headers"]["authorization"] == "Bearer sk-test"
    body = json.loads(sent["body"])
    assert body["model"] == "gpt-4o"
    assert body["messages"] == [{"role": "user", "content": "hi"}]


async def test_create_anthropic(mock_server: MockServer) -> None:
    mock_server.body = {
        "id": "msg_1",
        "model": "claude-sonnet-4-6",
        "content": [{"type": "text", "text": "hello"}],
        "stop_reason": "end_turn",
        "usage": {"input_tokens": 10, "output_tokens": 3},
    }
    client = unillm.Client("anthropic", "sk-ant", base_url=mock_server.url)

    resp = await client.create("claude-sonnet-4-6", input="hi")

    assert resp.provider == "anthropic"
    assert resp.text == "hello"
    sent = mock_server.received[0]
    assert sent["path"] == "/messages"
    assert sent["headers"]["x-api-key"] == "sk-ant"
    assert sent["headers"]["anthropic-version"] == "2023-06-01"
    body = json.loads(sent["body"])
    assert body["max_tokens"] == 4096  # default injected


async def test_create_error_maps_to_unillm_error(mock_server: MockServer) -> None:
    mock_server.status = 429
    mock_server.body = {"error": {"message": "slow down"}}
    client = unillm.Client("openai", "sk-test", base_url=mock_server.url)

    with pytest.raises(unillm.RateLimitError) as exc_info:
        await client.create("gpt-4o", input="hi")
    assert exc_info.value.kind == "rate_limited"
    assert "slow down" in str(exc_info.value)
    # RateLimitError is still an UnillmError
    assert isinstance(exc_info.value, unillm.UnillmError)


async def test_string_input_becomes_user_message(mock_server: MockServer) -> None:
    mock_server.body = {
        "id": "c1",
        "model": "gpt-4o",
        "choices": [
            {"index": 0, "message": {"role": "assistant", "content": "ok"}, "finish_reason": "stop"}
        ],
        "usage": {"prompt_tokens": 1, "completion_tokens": 1},
    }
    client = unillm.Client("openai", "sk-test", base_url=mock_server.url)
    await client.create("gpt-4o", input="ping")
    body = json.loads(mock_server.received[0]["body"])
    assert body["messages"] == [{"role": "user", "content": "ping"}]
