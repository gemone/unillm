"""Exception hierarchy (`DESIGN.md` §15.3).

The native core raises its own ``UnillmError`` carrying ``"<kind>: <message>"``; the facade converts
that into this typed Python hierarchy so callers can ``except unillm.RateLimitError``.
"""

from __future__ import annotations

from typing import Any


class UnillmError(Exception):
    """Base class for all unillm errors."""

    def __init__(self, message: str = "", *, kind: str = "other") -> None:
        super().__init__(message)
        self.message = message
        self.kind = kind


class InvalidRequestError(UnillmError):
    """400 — schema/validation failure."""


class AuthenticationError(UnillmError):
    """401/403 — bad or missing credentials."""


class NotFoundError(UnillmError):
    """404 — unknown model/route/key."""


class RateLimitError(UnillmError):
    """429 — rate-limit or budget exceeded."""

    def __init__(
        self, message: str = "", *, kind: str = "rate_limited", retry_after: float | None = None
    ) -> None:
        super().__init__(message, kind=kind)
        self.retry_after = retry_after


class ProviderError(UnillmError):
    """Upstream non-2xx."""

    def __init__(
        self,
        message: str = "",
        *,
        kind: str = "provider_error",
        status: int | None = None,
        raw: Any = None,
    ) -> None:
        super().__init__(message, kind=kind)
        self.status = status
        self.raw = raw


class StreamError(UnillmError):
    """Malformed or interrupted stream."""


class SerializationError(UnillmError):
    """Decode/encode failure."""


_KIND_TO_CLASS: dict[str, type[UnillmError]] = {
    "invalid_request": InvalidRequestError,
    "unauthorized": AuthenticationError,
    "not_found": NotFoundError,
    "rate_limited": RateLimitError,
    "provider_error": ProviderError,
    "stream": StreamError,
    "serde": SerializationError,
}


def from_native(native_exc: BaseException) -> UnillmError:
    """Convert a native ``UnillmError`` (``"<kind>: <message>"``) into the typed hierarchy."""
    text = str(native_exc)
    kind, sep, message = text.partition(": ")
    if not sep:
        kind, message = "other", text
    cls = _KIND_TO_CLASS.get(kind, UnillmError)
    return cls(message, kind=kind)
