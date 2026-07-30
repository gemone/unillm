-- M4.5 request logs + usage (DESIGN.md §11.3) in native PostgreSQL shapes. §16 PII hygiene: these
-- tables store metadata + token sizes only — never request or response bodies.
--
-- Production adds `PARTITION BY RANGE (created_at)` with monthly partitions (§11.3); this dev
-- migration ships an unpartitioned table so the cross-DB test suite can INSERT/SELECT directly.

CREATE TABLE request_logs (
  id              UUID    PRIMARY KEY,
  request_id      TEXT    NOT NULL,
  virtual_key_id  UUID,
  tenant_id       UUID    NOT NULL,
  provider        TEXT    NOT NULL,
  model           TEXT    NOT NULL,
  inbound_format  TEXT    NOT NULL,
  outbound_format TEXT    NOT NULL,
  status          SMALLINT NOT NULL,                   -- HTTP status returned to the client
  cached          BOOLEAN NOT NULL DEFAULT false,      -- exact-hash cache (M5)
  latency_ms      INT,
  created_at      TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX ix_logs_key_time ON request_logs(virtual_key_id, created_at DESC);
CREATE INDEX ix_logs_tenant_time ON request_logs(tenant_id, created_at DESC);

CREATE TABLE usage (
  request_log_id  UUID    PRIMARY KEY REFERENCES request_logs(id) ON DELETE CASCADE,
  input_tokens    BIGINT  NOT NULL DEFAULT 0,
  output_tokens   BIGINT  NOT NULL DEFAULT 0,
  cache_read      BIGINT  NOT NULL DEFAULT 0,
  cache_creation  BIGINT  NOT NULL DEFAULT 0,
  cost_usd        DOUBLE PRECISION
);
CREATE INDEX ix_usage_key ON usage(request_log_id);
