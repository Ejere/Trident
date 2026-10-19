-- Trident PostgreSQL Schema
-- Convenience full-schema snapshot for local/dev bootstrap and documentation.
-- The migration chain in ./migrations/ (0001-0026) is the source of truth and is
-- what CI and production apply; this file must mirror the end state of that chain.
-- Keep in sync whenever a migration is added.

CREATE EXTENSION IF NOT EXISTS "pgcrypto";

-- ---------------------------------------------------------------------------
-- soroban_events  (migration 0017: RANGE-partitioned by ledger_sequence)
-- Primary store for every indexed Soroban contract event.
--
-- Partition key: ledger_sequence — deterministic, aligns with the ingest
-- cursor, makes retention a cheap partition DROP rather than a bulk DELETE.
-- PK is (ledger_sequence, id) because PostgreSQL requires all partition key
-- columns to appear in every unique constraint.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS soroban_events (
    id                  UUID        NOT NULL DEFAULT gen_random_uuid(),
    contract_id         TEXT        NOT NULL,
    ledger_sequence     BIGINT      NOT NULL,
    ledger_timestamp    TIMESTAMPTZ NOT NULL,
    transaction_hash    TEXT        NOT NULL,
    event_index         INTEGER     NOT NULL,
    event_type          TEXT        NOT NULL CHECK (event_type IN ('contract', 'system', 'diagnostic')),
    network             TEXT        NOT NULL DEFAULT 'testnet',
    topics              JSONB       NOT NULL DEFAULT '[]',
    topic_0             TEXT        GENERATED ALWAYS AS (topics ->> 0) STORED,
    topic_1             TEXT        GENERATED ALWAYS AS (topics ->> 1) STORED,
    data                JSONB       NOT NULL DEFAULT '{}',
    created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (ledger_sequence, id)
) PARTITION BY RANGE (ledger_sequence);

-- Indexes — canonical set produced by migrations 0004 and 0009.
-- network isolation (0004)
CREATE INDEX IF NOT EXISTS idx_soroban_events_network          ON soroban_events (network);
CREATE INDEX IF NOT EXISTS idx_soroban_events_network_contract ON soroban_events (network, contract_id);
-- high-cardinality query patterns (0009)
CREATE INDEX IF NOT EXISTS idx_soroban_events_contract_ledger  ON soroban_events (contract_id, ledger_sequence DESC);
CREATE INDEX IF NOT EXISTS idx_soroban_events_contract_topic0  ON soroban_events (contract_id, topic_0)
    WHERE topic_0 IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_soroban_events_id_desc          ON soroban_events (id DESC);
CREATE INDEX IF NOT EXISTS idx_soroban_events_ledger_timestamp ON soroban_events (ledger_timestamp DESC);

-- ---------------------------------------------------------------------------
-- system_state
-- Persistent cursor tracking so the indexer can resume after restart without
-- re-scanning from genesis.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS system_state (
    key                   TEXT PRIMARY KEY,
    value                 TEXT        NOT NULL,
    updated_at            TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- indexer health columns (migration 0002)
    last_poll_at          TIMESTAMPTZ,
    last_ledger_indexed   BIGINT,
    events_indexed_total  BIGINT      NOT NULL DEFAULT 0,
    events_in_last_poll   INT         NOT NULL DEFAULT 0,
    poll_duration_ms      INT         NOT NULL DEFAULT 0,
    -- alerting state columns (migration 0003)
    last_alert_at         TIMESTAMPTZ,
    alert_fired           BOOLEAN     NOT NULL DEFAULT FALSE
);

-- Seed the cursor row so the indexer can always do an UPDATE rather than
-- an upsert on the hot path.
INSERT INTO system_state (key, value)
VALUES ('latest_ledger_cursor', '0')
ON CONFLICT (key) DO NOTHING;

-- ---------------------------------------------------------------------------
-- indexed_contracts
-- Registry of contracts whose events Trident is actively indexing.
-- A NULL network means "all networks".
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS indexed_contracts (
    id              UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    contract_id     TEXT        NOT NULL,
    network         TEXT,
    label           TEXT,
    index_from      BIGINT      NOT NULL DEFAULT 0,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_indexed_contracts_id_network UNIQUE (contract_id, network)
);

CREATE INDEX IF NOT EXISTS idx_indexed_contracts_contract_id ON indexed_contracts (contract_id);

-- ---------------------------------------------------------------------------
-- ledger_metadata
-- Lightweight record of every processed ledger for gap detection and
-- provenance tracking.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS ledger_metadata (
    ledger_sequence     BIGINT      PRIMARY KEY,
    ledger_hash         TEXT        NOT NULL,
    ledger_timestamp    TIMESTAMPTZ NOT NULL,
    event_count         INTEGER     NOT NULL DEFAULT 0,
    processed_at        TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ledger_metadata_timestamp ON ledger_metadata (ledger_timestamp);

-- ---------------------------------------------------------------------------
-- api_keys
-- Canonical definition; mirrors migration 0005_api_keys.sql.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS api_keys (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    key_hash         TEXT        NOT NULL UNIQUE,   -- SHA-256 hex of full key
    key_prefix       TEXT        NOT NULL,           -- first 16 chars of plaintext key (for display)
    label            TEXT        NOT NULL DEFAULT '',
    network          TEXT        NOT NULL DEFAULT 'mainnet',
    rate_limit_tier  TEXT        NOT NULL DEFAULT 'standard',
    created_by       TEXT,                           -- optional creator identifier
    last_used_at     TIMESTAMPTZ,
    request_count    BIGINT      NOT NULL DEFAULT 0,
    revoked_at       TIMESTAMPTZ,                    -- NULL means active; set to revoke
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_api_keys_key_hash ON api_keys (key_hash);
CREATE INDEX IF NOT EXISTS idx_api_keys_active ON api_keys (key_hash)
    WHERE revoked_at IS NULL;

-- ---------------------------------------------------------------------------
-- audit_log
-- Per-request audit trail for API key usage (migration 0006).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS audit_log (
    id           BIGSERIAL   PRIMARY KEY,
    api_key_id   UUID        REFERENCES api_keys(id) ON DELETE SET NULL,
    endpoint     TEXT        NOT NULL,
    method       TEXT        NOT NULL,
    ip           INET,
    user_agent   TEXT,
    status_code  INT         NOT NULL,
    duration_ms  INT         NOT NULL,
    result_count INT,
    request_id   TEXT        NOT NULL,
    network      TEXT,
    ts           TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_audit_log_key_ts ON audit_log (api_key_id, ts DESC);
CREATE INDEX IF NOT EXISTS idx_audit_log_ts     ON audit_log (ts DESC);

-- ---------------------------------------------------------------------------
-- parse_errors
-- Audit trail for events that failed XDR decoding (migration 0008).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS parse_errors (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    ledger_sequence  BIGINT      NOT NULL,
    event_index      INT         NOT NULL,
    raw_payload      TEXT        NOT NULL,
    error_message    TEXT        NOT NULL,
    occurred_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_parse_errors_occurred_at ON parse_errors (occurred_at DESC);

-- ---------------------------------------------------------------------------
-- event_outbox
-- Transactional outbox guaranteeing every committed event reaches the Redis
-- stream at least once (migration 0011).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS event_outbox (
    seq          BIGSERIAL   PRIMARY KEY,
    event_id     UUID        NOT NULL UNIQUE,
    payload      JSONB       NOT NULL,
    published    BOOLEAN     NOT NULL DEFAULT FALSE,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    published_at TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_event_outbox_unpublished
    ON event_outbox (seq)
    WHERE published = FALSE;

-- ---------------------------------------------------------------------------
-- updated_at trigger  (migration 0016)
-- ---------------------------------------------------------------------------
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER LANGUAGE plpgsql AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$;

CREATE TRIGGER trg_system_state_updated_at
    BEFORE UPDATE ON system_state
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_indexed_contracts_updated_at
    BEFORE UPDATE ON indexed_contracts
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_api_keys_updated_at
    BEFORE UPDATE ON api_keys
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();
CREATE TRIGGER trg_webhook_subscriptions_updated_at
    BEFORE UPDATE ON webhook_subscriptions
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- ---------------------------------------------------------------------------
-- webhook_subscriptions
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS webhook_subscriptions (
    id           UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    api_key_id   UUID        NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    contract_id  TEXT        NOT NULL,
    topic0       TEXT,
    target_url   TEXT        NOT NULL,
    secret       TEXT        NOT NULL,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    paused_at    TIMESTAMPTZ,
    network      TEXT        NOT NULL DEFAULT 'testnet'
);

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    id              BIGSERIAL   PRIMARY KEY,
    subscription_id UUID        NOT NULL REFERENCES webhook_subscriptions(id) ON DELETE CASCADE,
    -- event_id is a logical FK to soroban_events(id); the DB-level constraint
    -- was dropped in migration 0017 because soroban_events is now partitioned
    -- by ledger_sequence, and PostgreSQL does not allow a global UNIQUE (id)
    -- on a partitioned table. Referential integrity is upheld by the application.
    event_id        UUID        NOT NULL,
    attempt         INT         NOT NULL DEFAULT 1,
    status_code     INT,
    response_body   TEXT,
    delivered_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    success         BOOLEAN     NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_webhook_subscriptions_contract_id ON webhook_subscriptions (contract_id);
CREATE INDEX IF NOT EXISTS idx_webhook_subscriptions_paused_at ON webhook_subscriptions (paused_at);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_subscription_id ON webhook_deliveries (subscription_id);

-- ---------------------------------------------------------------------------
-- usage_rollup
-- Per-API-key daily usage rollup, aggregated from audit_log (migration 0024).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS usage_rollup (
    api_key_id      UUID             NOT NULL REFERENCES api_keys(id) ON DELETE CASCADE,
    period_start    TIMESTAMPTZ      NOT NULL,
    period_end      TIMESTAMPTZ      NOT NULL,
    request_count   BIGINT           NOT NULL DEFAULT 0,
    error_count     BIGINT           NOT NULL DEFAULT 0,
    avg_duration_ms DOUBLE PRECISION NOT NULL DEFAULT 0,
    updated_at      TIMESTAMPTZ      NOT NULL DEFAULT NOW(),
    PRIMARY KEY (api_key_id, period_start)
);

-- Durable per-subscription webhook delivery queue (migration 0026).
CREATE TABLE IF NOT EXISTS webhook_outbox (
    id              BIGSERIAL   PRIMARY KEY,
    subscription_id UUID        NOT NULL REFERENCES webhook_subscriptions(id) ON DELETE CASCADE,
    event_id        TEXT        NOT NULL,
    event           JSONB       NOT NULL,
    attempt         INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    locked_until    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT uq_webhook_outbox_subscription_event UNIQUE (subscription_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_webhook_outbox_due ON webhook_outbox (next_attempt_at);

CREATE INDEX IF NOT EXISTS idx_usage_rollup_key_period ON usage_rollup (api_key_id, period_start DESC);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_delivered_at ON webhook_deliveries (delivered_at);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_event_id ON webhook_deliveries (event_id);
