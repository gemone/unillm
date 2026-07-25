# unillm

A unified LLM framework: talk to **OpenAI, Anthropic, OpenRouter, and DeepSeek** through one
normalized, Responses-style API — plus a standalone **proxy** that accepts any client format, routes
to any backend, and returns any format, with key management, request logging/usage, rate-limiting,
and response caching.

> **Status:** early development. The canonical IR and error model (`unillm-core`) are being built
> first. See [`DESIGN.md`](./DESIGN.md) for the complete specification and the M0→M5 roadmap.

## Crates

| Crate | Role |
|---|---|
| `unillm-core` | Canonical IR, provider adapters, SSE codec, cache logic (shared brain) |
| `unillm-python` | PyO3 + anyio Python SDK (`pip install unillm`) |
| `unillm-proxy` | `axum` universal translator gateway + storage + admin API |
| `unillm-storage` | Storage trait + `sqlx` (sqlite/postgres) + redis |

## License

Dual-licensed under MIT or Apache-2.0, at your option.
