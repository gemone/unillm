# Changelog

All notable changes to this project are documented here. The format is based on
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and this project adheres to
[Semantic Versioning](https://semver.org/spec/v2.0.0.html).

The full design contract lives in [`DESIGN.md`](./DESIGN.md); this file records what each
release actually shipped.

## [0.1.0] — 2026-07-27

First stable release. A unified LLM framework: a shared Rust core, a Python SDK, and a
standalone translator proxy covering **OpenAI, Anthropic, OpenRouter, and DeepSeek**.

### Added

- **Canonical IR (`unillm-core`)** — one Responses-style typed data model (Request/Response,
  Item, Content/ContentBlock, tools, Usage, CacheStrategy) every provider maps to and from.
  Internally tagged serde for all enums; `function_call.arguments` kept as a JSON string,
  `tool_use.input` as a parsed object (`DESIGN.md` §4).
- **Provider adapters** — Chat Completions (OpenAI/DeepSeek/OpenRouter) and Anthropic Messages,
  with usage normalization (`input_tokens + cache_read + cache_creation == provider prompt
  tokens`) and an SSE codec + per-dialect stream decoders (`DESIGN.md` §5, §6).
- **Reasoning pass-through** — a backend's `reasoning_content` (DeepSeek reasoner / v4-flash) maps
  to the canonical `Reasoning` item and is surfaced on egress as CC `reasoning_content`, Anthropic
  `thinking`, or the canonical item — both non-streaming and streaming (`StreamEvent::ReasoningDelta`,
  with the CC decoder opening the reasoning item lazily ahead of the answer). Reverses the §5.5
  "drop reasoning" degradation (`DESIGN.md` §5.4, §5.5).
- **Python SDK (`unillm`)** — PyO3 + anyio `Client`/`Response`/`EventStream` with a bounded
  tokio-channel async iterator bridge; cancellation drops the upstream connection
  (`DESIGN.md` §9, §6.6).
- **Proxy gateway (`unillm-proxy`)** — `axum` universal translator: any of 3 inbound formats →
  any of 4 backends → any outbound format, with SSE re-translated event-by-event and
  route-chain fallback on 5xx/429/timeout (`DESIGN.md` §10).
- **Storage (`unillm-storage`)** — SQLite + PostgreSQL backends behind per-concern traits
  (`KeyStore`, `ModelStore`, `RouteStore`, `LogStore`, `CacheStore`) + an in-memory
  `RateLimiter`. sqlx runtime queries (one codebase, both backends), migrations applied at
  startup (`DESIGN.md` §11).
- **Virtual-key auth** — SHA-256 + pepper hashed secrets shown once; scopes (`data` / `admin` /
  `read-usage`), per-key model allowlists, budgets, RPM/TPM/concurrency limits; query-param
  keys rejected (`DESIGN.md` §13.1, §16).
- **Rate limiting & concurrency** — RPM sliding window, TPM, daily token budget, max
  concurrency with slot release on stream completion/drop; `429` + `Retry-After` +
  `X-Unillm-RateLimit-*`; fail-open (`DESIGN.md` §12).
- **Logging & usage** — fire-and-forget request logs + token/cost records (metadata + sizes
  only, no bodies — §16 PII hygiene); aggregated usage analytics (`DESIGN.md` §10.3 step 9,
  §13.5).
- **Admin REST + CLI** — `/admin/{keys,models,routes,usage,logs}` CRUD behind a distinct admin
  token, plus `unillm-proxy admin …` CLI mirroring the API (`DESIGN.md` §10.6, §13.4).
- **Exact-hash response cache** — `sha256(canonical_request_minus_metadata + virtual_key_scope)`,
  key-scoped (no cross-key leakage), metadata excluded, canonical `Response` value (one entry
  serves all outbound formats), non-stream 2xx only, `X-Unillm-Cache: HIT|MISS`, admin
  invalidation (`DESIGN.md` §7.4).
- **Observability** — `/metrics` Prometheus exposition (request counters, token/cost totals,
  latency histogram); `/health` (liveness), `/ready` (readiness); `/openapi.json` + Scalar
  `/docs` for the admin surface (`DESIGN.md` §17).
- **Packaging** — multi-stage `Dockerfile` (`rust:1.95` → `debian:stable-slim`); GitHub Actions
  `ci.yml` (fmt, clippy `-D warnings`, workspace + postgres tests, pytest, mypy `--strict`,
  `cargo audit`) and `release.yml` (PyPI wheels via maturin, ghcr image, GitHub release on
  `v*` tags).

### Fixed

- **SQLite `create_if_missing`** — sqlx 0.9 defaults `create_if_missing(false)`, so the proxy
  failed to boot with `SQLITE_CANTOPEN` (code 14) on the default `sqlite://./unillm.db` url
  (in-memory tests were unaffected). `SqliteStore::connect` now forces `create_if_missing(true)`;
  default url aligned to `sqlite://./unillm.db` (`DESIGN.md` §14.1).

### Deferred (tracked, post-v1)

Responses dialect (`DESIGN.md` §5.3/§6.5), semantic cache (§7.4), OpenRouter `provider.order`
fan-out (§2.6), `n>1` (§4.6), embeddings/reranking, WebSocket/gRPC. Redis-backed rate-limit
and cache primaries (in-memory now, trait-pluggable).

[0.1.0]: https://github.com/gemone/unillm/releases/tag/v0.1.0
