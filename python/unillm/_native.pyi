from typing import Awaitable

__version__: str


class UnillmError(Exception): ...


class Client:
    def __init__(
        self,
        provider: str,
        api_key: str,
        *,
        base_url: str | None = ...,
        timeout: float | None = ...,
    ) -> None: ...

    def create(self, request_json: str) -> Awaitable[str]: ...

    def stream(self, request_json: str) -> EventStream: ...


class EventStream:
    def __aiter__(self) -> EventStream: ...

    def __anext__(self) -> Awaitable[str]: ...
