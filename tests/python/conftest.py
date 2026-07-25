"""Shared fixtures for the Python SDK tests.

A tiny threaded HTTP server stands in for an upstream provider so tests need no network.
"""

from __future__ import annotations

import json
import threading
from collections.abc import Iterator
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

import pytest


class MockServer:
    """Mutable upstream mock: set `.status`/`.body` before the call; read `.received` after."""

    def __init__(self, url: str, received: list[dict]) -> None:
        self.url = url
        self.received = received
        self.status = 200
        self.body: object = {}


@pytest.fixture
def mock_server() -> Iterator[MockServer]:
    state = MockServer("", [])

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self) -> None:
            length = int(self.headers.get("Content-Length", "0"))
            state.received.append(
                {
                    "path": self.path,
                    "headers": {k.lower(): v for k, v in self.headers.items()},
                    "body": self.rfile.read(length),
                }
            )
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
