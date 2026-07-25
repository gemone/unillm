"""Shared fixtures for the Python SDK tests.

A tiny threaded HTTP server stands in for an upstream provider so tests need no network. For
streaming requests it writes SSE frames (optionally slowly and forever, to exercise cancellation).
"""

from __future__ import annotations

import json
import threading
import time
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


class MockServer:
    """Mutable upstream mock.

    Non-stream: set ``.status``/``.body``. Stream: set ``.sse`` (list of SSE frames); when
    ``.slow`` is set, frames repeat forever with a delay until the client disconnects — use
    ``.frames_sent`` to assert the connection was torn down.
    """

    def __init__(self, url: str, received: list[dict]) -> None:
        self.url = url
        self.received = received
        self.status = 200
        self.body: object = {}
        self.sse: list[str] = []
        self.slow = False
        self.frames_sent = 0


@pytest.fixture
def mock_server() -> Iterator[MockServer]:
    state = MockServer("", [])

    class Handler(BaseHTTPRequestHandler):
        # HTTP/1.0 → responses are connection-close delimited, which is fine for SSE.
        protocol_version = "HTTP/1.0"

        def do_POST(self) -> None:  # noqa: N802 - http.server convention
            length = int(self.headers.get("Content-Length", "0"))
            raw = self.rfile.read(length)
            try:
                parsed = json.loads(raw)
            except json.JSONDecodeError:
                parsed = {}
            state.received.append(
                {
                    "path": self.path,
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                    "body": raw,
                    "stream": bool(parsed.get("stream")),
                }
            )

            if parsed.get("stream"):
                self.send_response(200)
                self.send_header("Content-Type", "text/event-stream")
                self.end_headers()
                try:
                    while True:
                        for frame in state.sse:
                            self.wfile.write(frame.encode())
                            self.wfile.flush()
                            state.frames_sent += 1
                            if state.slow:
                                time.sleep(0.01)
                        if not state.slow:
                            break
                except (BrokenPipeError, ConnectionResetError, OSError):
                    pass
                return

            payload = json.dumps(state.body).encode()
            self.send_response(state.status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            if 200 <= state.status < 300:
                self.wfile.write(payload)

        def log_message(self, *args: object) -> None:  # silence test noise
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    port = server.server_address[1]
    state.url = f"http://127.0.0.1:{port}"
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield state
    finally:
        server.shutdown()
        thread.join(timeout=2)
