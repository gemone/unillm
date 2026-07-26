-- M4.5 request logs + usage (DESIGN.md §11.3) in SQLite shapes: TEXT for UUIDs, INTEGER for
-- booleans/bigints, REAL for cost. No partitioning. §16 PII hygiene: these tables store metadata +
-- token sizes only — never request or response bodies.

CREATE TABLE request_logs (
  id              TEXT    PRIMARY KEY,
  request_id      TEXT    NOT NULL,
  virtual_key_id  TEXT    NOT NULL,
  tenant_id       TEXT    NOT NULL,
  provider        TEXT    NOT NULL,
  model           TEXT    NOT NULL,
  inbound_format  TEXT    NOT NULL,
  outbound_format TEXT    NOT NULL,
  status          INTEGER NOT NULL,               -- HTTP status returned to the client
  cached          INTEGER NOT NULL DEFAULT 0,     -- exact-hash cache (M5); 0 until then
  latency_ms      INTEGER,
  created_at      TEXT    NOT NULL                -- RFC3339
);
CREATE INDEX ix_logs_key_time ON request_logs(virtual_key_id, created_at DESC);
CREATE INDEX ix_logs_tenant_time ON request_logs(tenant_id, created_at DESC);

CREATE TABLE usage (
  request_log_id  TEXT    PRIMARY KEY REFERENCES request_logs(id) ON DELETE CASCADE,
  input_tokens    INTEGER NOT NULL DEFAULT 0,
  output_tokens   INTEGER NOT NULL DEFAULT 0,
  cache_read      INTEGER NOT NULL DEFAULT 0,
  cache_creation  INTEGER NOT NULL DEFAULT 0,
  cost_usd        REAL
);
CREATE INDEX ix_usage_key ON usage(request_log_id);
