# unillm

A unified LLM framework. Talk to **OpenAI, Anthropic, OpenRouter, and DeepSeek** through one
normalized, Responses-style API — plus a standalone **proxy** that accepts any client format,
routes to any backend, and returns any format, with virtual keys, rate-limiting, usage/cost
logging, and an exact-hash response cache.

One shared Rust core does all the normalization, so nothing is translated twice.

- **Python SDK** — `pip install unillm` (PyO3 + anyio).
- **Proxy** — `docker run ghcr.io/gemone/unillm-proxy` (axum gateway + admin API).
- **Contract** — [`DESIGN.md`](./DESIGN.md) is the verbatim spec; [`CHANGELOG.md`](./CHANGELOG.md)
  records what shipped.

## Crates

| Crate | Role |
|---|---|
| `unillm-core` | Canonical IR, provider adapters, SSE codec, cache logic — the shared brain |
| `unillm-python` | PyO3 + anyio Python SDK (`pip install unillm`) |
| `unillm-proxy` | `axum` universal translator gateway: storage, keys, rate-limit, logging, cache, admin |
| `unillm-storage` | Storage traits + `sqlx` (SQLite + PostgreSQL) + in-memory rate-limiter/cache |

## Quickstart — proxy

The proxy accepts OpenAI Chat Completions (`/v1/chat/completions`), Anthropic Messages
(`/v1/messages`), or canonical (`/unillm/v1/responses`) on the data plane, normalizes to its IR,
routes to any configured backend, and returns the client's requested outbound format (override
with `X-Unillm-Response-Format`).

```bash
# 1. Configure (§14.1). At minimum: a pepper, an admin token, and one upstream key.
export UNILLM_KEY_PEPPER=$(openssl rand -hex 32)
export UNILLM_ADMIN_TOKEN=$(openssl rand -hex 16)
export UNILLM_SEED_KEY=dev-key-secret        # a dev key seeded on first boot
export UNILLM_PROV_OPENAI_KEY=sk-...

# 2. Run.
cargo run -p unillm-proxy
#   or: docker run --rm -p 8080:8080 -e UNILLM_KEY_PEPPER=... -e UNILLM_ADMIN_TOKEN=... \
#         -e UNILLM_PROV_OPENAI_KEY=... ghcr.io/gemone/unillm-proxy

# 3. Call the data plane with the seeded dev key.
curl -s localhost:8080/v1/chat/completions \
  -H "authorization: Bearer dev-key-secret" \
  -H "content-type: application/json" \
  -d '{"model":"gpt-4o","messages":[{"role":"user","content":"hi"}]}'

# 4. Manage via the admin API / CLI.
export UNILLM_PROXY_URL=http://127.0.0.1:8080
unillm-proxy admin keys create --tenant 00000000-0000-0000-0000-000000000000 --scope data
unillm-proxy admin routes create --json '{"alias":"gpt-4o","provider":"openai","native_model":"gpt-4o"}'
unillm-proxy admin usage
```

Operational endpoints (no auth unless noted):

| Endpoint | Purpose |
|---|---|
| `GET /health` | Liveness (always 200). |
| `GET /ready` | Readiness (200 once ≥1 upstream is configured). |
| `GET /metrics` | Prometheus text exposition. |
| `GET /openapi.json` · `GET /docs` | OpenAPI 3.0 doc + Scalar UI (admin surface). |
| `/admin/*` | Keys/models/routes/usage/logs/cache CRUD — **admin bearer token required**. |

### Caching & limits

Set `UNILLM_CACHE_ENABLED=true` (default TTL `UNILLM_CACHE_TTL=300`s) to serve repeat
non-streaming requests from the exact-hash cache — keyed by the canonical request (minus
`metadata`) scoped to the virtual key, so identical logical requests hit and there is never
cross-key leakage (`X-Unillm-Cache: HIT|MISS`). Per-key `rpm`/`tpm`/`budget_daily_tokens`/
`max_concurrency` enforce `429` + `Retry-After` (`DESIGN.md` §12).

## Quickstart — Python SDK

```python
import asyncio, unillm

async def main():
    # Direct to a provider…
    c = unillm.Client.from_env()                      # UNILLM_PROVIDER / UNILLM_API_KEY
    r = await c.create("gpt-4o", input="hello")
    print(r.text)

    # …or through the proxy (any backend, virtual keys, caching, rate limits, usage logging).
    p = unillm.Client("openai", "sk-unillm-...", base_url="http://localhost:8080")
    async for ev in await p.stream("claude", input="stream me"):
        print(ev)

asyncio.run(main())
```

Streaming runs over a bounded async iterator; dropping it cancels the upstream connection within
~1s (`DESIGN.md` §6.6).

## Configuration

| Variable | Scope | Default | Meaning |
|---|---|---|---|
| `UNILLM_PROXY_BIND` | proxy | `0.0.0.0:8080` | listen address |
| `UNILLM_DATABASE_URL` | proxy | `sqlite:unillm.db` | sqlx URL (sqlite/postgres) |
| `UNILLM_KEY_PEPPER` | proxy | *(insecure dev)* | pepper for virtual-key hashing |
| `UNILLM_ADMIN_TOKEN` | proxy/CLI | — | gates `/admin/*` |
| `UNILLM_SEED_KEY` | proxy | — | dev key seeded on first boot |
| `UNILLM_PROV_{OPENAI,ANTHROPIC,OPENROUTER,DEEPSEEK}_KEY` | proxy | — | upstream keys |
| `UNILLM_CACHE_ENABLED` / `UNILLM_CACHE_TTL` | proxy | `false` / `300` | response cache |
| `UNILLM_PROVIDER` / `UNILLM_API_KEY` / `UNILLM_BASE_URL` | SDK | — | client defaults |

Full list in [`DESIGN.md` §14.1](./DESIGN.md#141-environment-variables).

## Development

```bash
cargo fmt --all --check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy -p unillm-storage --features postgres --all-targets -- -D warnings
cargo test --workspace

# Python SDK
maturin develop && pytest tests/python -q && mypy --strict python/unillm

# Postgres cross-DB gate (requires docker compose up -d postgres)
UNILLM_PG_TEST_URL=postgres://unillm:unillm@localhost:5432/unillm \
    cargo test -p unillm-storage --features postgres --test pg
```

`docker compose up -d` provides Postgres 16 + Redis for local integration work.

## Status & roadmap

v1 ships the full M0→M5 roadmap: canonical IR, four provider adapters, PyO3 SDK, the proxy
translator, storage/keys/rate-limit/logging, the response cache, metrics, OpenAPI, and packaging.
Deferred to post-v1 (tracked in [`CHANGELOG.md`](./CHANGELOG.md)): Responses dialect, semantic
cache, OpenRouter fan-out, `n>1`, embeddings.

## License

Dual-licensed under MIT or Apache-2.0, at your option.
