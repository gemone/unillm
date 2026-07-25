"""Facade helpers and from_env (no network)."""

from __future__ import annotations

import pytest

import unillm


def test_item_helpers() -> None:
    assert unillm.user("hi") == {"type": "message", "role": "user", "content": "hi"}
    assert unillm.system("be brief")["role"] == "system"
    assert unillm.tool_result("c1", "{}")["call_id"] == "c1"
    assert unillm.image_url("https://x/a.png")["source"] == {"type": "url", "url": "https://x/a.png"}
    assert unillm.image_base64("image/png", "QUJD")["source"] == {
        "type": "base64",
        "media_type": "image/png",
        "data": "QUJD",
    }


def test_cache_builders() -> None:
    assert unillm.Cache.auto() == {"kind": "auto"}
    assert unillm.Cache.none() == {"kind": "none"}
    explicit = unillm.Cache.explicit([{"at": "last"}], ttl="1h")
    assert explicit == {"kind": "explicit", "breakpoints": [{"at": "last"}], "ttl": "1h"}


def test_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("UNILLM_PROVIDER", "anthropic")
    monkeypatch.setenv("UNILLM_API_KEY", "sk-test")
    monkeypatch.setenv("UNILLM_BASE_URL", "https://example.invalid")
    monkeypatch.setenv("UNILLM_TIMEOUT", "12.5")

    client = unillm.from_env()

    assert isinstance(client, unillm.Client)
