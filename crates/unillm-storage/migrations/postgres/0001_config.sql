-- M4.1 config tables (DESIGN.md §11.3) in native PostgreSQL shapes: UUID, JSONB, TIMESTAMPTZ,
-- BOOLEAN, DOUBLE PRECISION. Mirrors the SQLite schema in ../sqlite/0001_config.sql column-for-column.

CREATE TABLE virtual_keys (
  id                    UUID    PRIMARY KEY,
  key_hash              TEXT    NOT NULL UNIQUE,        -- hash of the secret; secret never stored
  key_prefix            TEXT    NOT NULL,               -- first ~8 chars, for display/lookup
  tenant_id             UUID    NOT NULL,
  scopes                JSONB   NOT NULL DEFAULT '[]',  -- ["data","admin","read-usage"]
  model_allowlist       JSONB,                           -- NULL = inherit tenant/default
  budget_daily_tokens   BIGINT,
  rpm                   INT,
  tpm                   BIGINT,
  max_concurrency       INT,
  created_at            TIMESTAMPTZ NOT NULL DEFAULT now(),
  expires_at            TIMESTAMPTZ,
  revoked_at            TIMESTAMPTZ
);
CREATE INDEX ix_keys_tenant ON virtual_keys(tenant_id);
CREATE INDEX ix_keys_prefix ON virtual_keys(key_prefix);

CREATE TABLE models (
  id                UUID    PRIMARY KEY,
  provider          TEXT    NOT NULL,
  native_model      TEXT    NOT NULL,
  display_name      TEXT    NOT NULL,
  context_window    INT,
  max_output        INT,
  price_in          DOUBLE PRECISION,                   -- per 1M tokens (f64)
  price_out         DOUBLE PRECISION,
  price_cache_read  DOUBLE PRECISION,
  enabled           BOOLEAN NOT NULL DEFAULT true,
  created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
  UNIQUE (provider, native_model)
);

CREATE TABLE routes (
  alias        TEXT    NOT NULL,
  tenant_id    UUID,                                   -- NULL = global default
  provider     TEXT    NOT NULL,
  native_model TEXT    NOT NULL,
  fallback     JSONB   NOT NULL DEFAULT '[]',          -- [{provider,native_model}, ...]
  priority     INT     NOT NULL DEFAULT 0,
  enabled      BOOLEAN NOT NULL DEFAULT true,
  PRIMARY KEY (alias, tenant_id)
);
