-- M4.1 config tables (DESIGN.md §11.3) in SQLite shapes: TEXT for UUIDs/JSON, INTEGER for
-- booleans, REAL for prices. No partitioning. request_logs / usage / response_cache arrive in M4.5.

CREATE TABLE virtual_keys (
  id                    TEXT    PRIMARY KEY,
  key_hash              TEXT    NOT NULL UNIQUE,        -- SHA-256+pepper of the secret; secret never stored
  key_prefix            TEXT    NOT NULL,               -- first ~8 chars, for display/lookup
  tenant_id             TEXT    NOT NULL,
  scopes                TEXT    NOT NULL DEFAULT '[]',  -- JSON array of scopes (data/admin/read-usage)
  model_allowlist       TEXT,                            -- JSON array; NULL = inherit tenant/default
  budget_daily_tokens   INTEGER,
  rpm                   INTEGER,
  tpm                   INTEGER,
  max_concurrency       INTEGER,
  created_at            TEXT    NOT NULL,               -- RFC3339
  expires_at            TEXT,
  revoked_at            TEXT
);
CREATE INDEX ix_keys_tenant ON virtual_keys(tenant_id);
CREATE INDEX ix_keys_prefix ON virtual_keys(key_prefix);

CREATE TABLE models (
  id                TEXT    PRIMARY KEY,
  provider          TEXT    NOT NULL,
  native_model      TEXT    NOT NULL,
  display_name      TEXT    NOT NULL,
  context_window    INTEGER,
  max_output        INTEGER,
  price_in          REAL,                               -- per 1M tokens
  price_out         REAL,
  price_cache_read  REAL,
  enabled           INTEGER NOT NULL DEFAULT 1,
  created_at        TEXT    NOT NULL,
  UNIQUE (provider, native_model)
);

CREATE TABLE routes (
  alias        TEXT    NOT NULL,
  tenant_id    TEXT,                                    -- NULL = global default
  provider     TEXT    NOT NULL,
  native_model TEXT    NOT NULL,
  fallback     TEXT    NOT NULL DEFAULT '[]',           -- JSON [{provider,native_model}, ...]
  priority     INTEGER NOT NULL DEFAULT 0,
  enabled      INTEGER NOT NULL DEFAULT 1,
  PRIMARY KEY (alias, tenant_id)
);
