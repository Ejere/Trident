-- Trident PostgreSQL Schema
-- Convenience full-schema snapshot for local/dev bootstrap and documentation.
-- The migration chain in ./migrations/ (0001-0029) is the source of truth and is
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
    network             TEXT        NOT NULL,
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
-- Natural-key uniqueness (migration 0025). ledger_sequence is included only
-- because PostgreSQL requires the partition key in every unique constraint on
-- a partitioned table; the protocol-level key is
-- (transaction_hash, event_index, network).
ALTER TABLE soroban_events
    ADD CONSTRAINT uq_soroban_events_tx_index_network
    UNIQUE (ledger_sequence, transaction_hash, event_index, network);

CREATE INDEX IF NOT EXISTS idx_soroban_events_contract_ledger  ON soroban_events (contract_id, ledger_sequence DESC);
CREATE INDEX IF NOT EXISTS idx_soroban_events_contract_topic0  ON soroban_events (contract_id, topic_0)
    WHERE topic_0 IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_soroban_events_id_desc          ON soroban_events (id DESC);
CREATE INDEX IF NOT EXISTS idx_soroban_events_ledger_timestamp ON soroban_events (ledger_timestamp DESC);

-- ---------------------------------------------------------------------------
-- system_state
-- Persistent cursor tracking so the indexer can resume after restart without
-- re-scanning from genesis. One row per network, keyed
-- 'latest_ledger_cursor:<network>' (migration 0033, issue #600), so two
-- indexers on different networks against the same database don't fight over
-- a single cursor.
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

-- Seed a cursor row per supported network (migration 0031's vocabulary) so
-- the indexer can always do an UPDATE rather than an upsert on the hot path,
-- for whichever network it's configured for.
INSERT INTO system_state (key, value)
VALUES
    ('latest_ledger_cursor:mainnet', '0'),
    ('latest_ledger_cursor:testnet', '0'),
    ('latest_ledger_cursor:futurenet', '0'),
    ('latest_ledger_cursor:sandbox', '0')
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
    expires_at       TIMESTAMPTZ,                    -- NULL means never expires (migration 0037)
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
    id                    BIGSERIAL   PRIMARY KEY,
    api_key_id            UUID        REFERENCES api_keys(id) ON DELETE SET NULL,
    endpoint              TEXT        NOT NULL,
    method                TEXT        NOT NULL,
    ip                    INET,
    user_agent            TEXT,
    status_code           INT         NOT NULL,
    duration_ms           INT         NOT NULL,
    result_count          INT,
    request_id            TEXT        NOT NULL,
    network               TEXT,
    -- Set only on a failed authentication attempt (migration 0033, issue
    -- #609): api_key_id is NULL in that case since the key never resolved to
    -- a row, so these are the only trace of which key was tried and why it
    -- was rejected. attempted_key_prefix is a prefix only - never the full
    -- attempted key.
    attempted_key_prefix  TEXT,
    failure_reason        TEXT,
    ts                    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Covers the admin analytics queries (migration 0029). The INCLUDE columns
-- are read but never seeked on, so they live only in the leaves and make
-- those queries index-only rather than paying a heap visit per matching row.
-- Supersedes the original idx_audit_log_key_ts (api_key_id, ts DESC), which
-- was a strict prefix of this index.
CREATE INDEX IF NOT EXISTS idx_audit_log_key_ts_covering
    ON audit_log (api_key_id, ts DESC)
    INCLUDE (status_code, endpoint, duration_ms);

-- Covers the usage rollup job (migration 0029). Partial on the job's own
-- WHERE clause: rows whose key was deleted (api_key_id is ON DELETE SET
-- NULL) can never contribute to a per-key rollup.
CREATE INDEX IF NOT EXISTS idx_audit_log_rollup
    ON audit_log (ts)
    INCLUDE (api_key_id, status_code, duration_ms)
    WHERE api_key_id IS NOT NULL;

-- Time-range scans over all rows, including those with a NULL api_key_id
-- that idx_audit_log_rollup excludes. Also what the retention cleanup falls
-- back to once its predicate becomes selective enough to prefer an index —
-- see docs/db/explain-audit-log-247.txt.
CREATE INDEX IF NOT EXISTS idx_audit_log_ts ON audit_log (ts DESC);

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
    occurred_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    network          TEXT        NOT NULL DEFAULT 'testnet'
        CONSTRAINT chk_parse_errors_network CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'))
);

CREATE INDEX IF NOT EXISTS idx_parse_errors_occurred_at ON parse_errors (occurred_at DESC);
-- migration 0038
CREATE INDEX IF NOT EXISTS idx_parse_errors_network_ledger ON parse_errors (network, ledger_sequence);

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
    published_at TIMESTAMPTZ,
    dead_lettered_at   TIMESTAMPTZ,
    dead_letter_reason TEXT
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
    -- Previous secret, retained during a rotation overlap window (issue #452).
    secondary_secret TEXT,
    created_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    paused_at    TIMESTAMPTZ,
    network      TEXT        NOT NULL
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
    success         BOOLEAN     NOT NULL,
    -- Delivery state machine and retry counter (migration 0013).
    status          TEXT        NOT NULL DEFAULT 'pending'
                                CHECK (status IN ('pending', 'success', 'failed', 'dead_lettered')),
    attempts        INTEGER     NOT NULL DEFAULT 1
);

-- Defined here rather than beside the other updated_at triggers above: a
-- trigger cannot be created before its table, and schema.sql must apply
-- cleanly to an empty database (checked by scripts/check-schema-drift.sh).
CREATE TRIGGER trg_webhook_subscriptions_updated_at
    BEFORE UPDATE ON webhook_subscriptions
    FOR EACH ROW EXECUTE FUNCTION set_updated_at();

CREATE INDEX IF NOT EXISTS idx_webhook_subscriptions_contract_id ON webhook_subscriptions (contract_id);
CREATE INDEX IF NOT EXISTS idx_webhook_subscriptions_paused_at ON webhook_subscriptions (paused_at);
-- Partial index over rows in a rotation overlap window, used by the cleanup
-- job that expires secondary secrets (issue #452).
CREATE INDEX IF NOT EXISTS idx_webhook_subscriptions_secondary_secret
    ON webhook_subscriptions (updated_at)
    WHERE secondary_secret IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_subscription_id ON webhook_deliveries (subscription_id);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_dead_lettered ON webhook_deliveries (subscription_id, delivered_at DESC) WHERE (status = 'dead_lettered');

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

CREATE INDEX IF NOT EXISTS idx_usage_rollup_key_period ON usage_rollup (api_key_id, period_start DESC);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_delivered_at ON webhook_deliveries (delivered_at);
CREATE INDEX IF NOT EXISTS idx_webhook_deliveries_event_id ON webhook_deliveries (event_id);

-- ---------------------------------------------------------------------------
-- Tables added by migrations 0010-0023
--
-- These were absent from this file entirely until the drift check added in
-- #436 (scripts/check-schema-drift.sh) reported them. That is the same class
-- of silent divergence as #437, in the other direction: there, schema.sql was
-- correct and the migrations had quietly stopped matching it.
--
-- Constraints are spelled as ALTER TABLE rather than inline so the names match
-- what the migration chain produces; the drift check compares constraint names.
-- ---------------------------------------------------------------------------

-- ---------------------------------------------------------------------------
-- token_events  (migration 0010)
-- Normalised SEP-41 token transfer/mint/burn projection of soroban_events.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS token_events (
    event_id uuid NOT NULL,
    contract_id text NOT NULL,
    network text NOT NULL,
    event_type text NOT NULL,
    from_address text,
    to_address text,
    spender_address text,
    admin_address text,
    amount text,
    expiration_ledger bigint,
    ledger_sequence bigint NOT NULL,
    ledger_timestamp timestamp with time zone NOT NULL,
    transaction_hash text NOT NULL,
    event_index integer NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    asset_code text,
    asset_issuer text,
    CONSTRAINT token_events_event_type_check CHECK ((event_type = ANY (ARRAY['transfer'::text, 'mint'::text, 'burn'::text, 'clawback'::text, 'approve'::text])))
);

ALTER TABLE token_events ADD CONSTRAINT token_events_pkey PRIMARY KEY (event_id);

CREATE INDEX IF NOT EXISTS idx_token_events_asset_code ON token_events USING btree (asset_code, ledger_sequence DESC) WHERE (asset_code IS NOT NULL);
CREATE INDEX IF NOT EXISTS idx_token_events_contract_ledger ON token_events USING btree (contract_id, ledger_sequence DESC);
CREATE INDEX IF NOT EXISTS idx_token_events_from ON token_events USING btree (from_address, ledger_sequence DESC) WHERE (from_address IS NOT NULL);
CREATE INDEX IF NOT EXISTS idx_token_events_to ON token_events USING btree (to_address, ledger_sequence DESC) WHERE (to_address IS NOT NULL);
CREATE INDEX IF NOT EXISTS idx_token_events_tx_hash ON token_events USING btree (transaction_hash);
CREATE INDEX IF NOT EXISTS idx_token_events_type ON token_events USING btree (event_type, ledger_sequence DESC);

-- ---------------------------------------------------------------------------
-- contract_invocation_metrics  (migration 0012)
-- Per-invocation fee and declared-resource metering for tracked contracts.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contract_invocation_metrics (
    id uuid DEFAULT gen_random_uuid() NOT NULL,
    contract_id text NOT NULL,
    network text NOT NULL,
    transaction_hash text NOT NULL,
    ledger_sequence bigint NOT NULL,
    ledger_timestamp timestamp with time zone NOT NULL,
    fee_charged bigint NOT NULL,
    resource_fee bigint,
    cpu_instructions bigint,
    read_bytes bigint,
    write_bytes bigint,
    provenance text DEFAULT 'declared_resources'::text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE contract_invocation_metrics ADD CONSTRAINT contract_invocation_metrics_pkey PRIMARY KEY (id);
ALTER TABLE contract_invocation_metrics ADD CONSTRAINT uq_contract_invocation_metrics UNIQUE (contract_id, transaction_hash);

CREATE INDEX IF NOT EXISTS idx_contract_invocation_metrics_contract_ledger ON contract_invocation_metrics USING btree (contract_id, ledger_sequence DESC);
CREATE INDEX IF NOT EXISTS idx_contract_invocation_metrics_tx_hash ON contract_invocation_metrics USING btree (transaction_hash);
CREATE UNIQUE INDEX IF NOT EXISTS uq_contract_invocation_metrics ON contract_invocation_metrics USING btree (contract_id, transaction_hash);

-- ---------------------------------------------------------------------------
-- contract_liveness  (migration 0014)
-- Last-seen activity per contract, for liveness reporting.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contract_liveness (
    id bigint NOT NULL,
    contract_id text NOT NULL,
    network text NOT NULL,
    status text NOT NULL,
    live_until_ledger bigint,
    ledgers_until_archive bigint,
    last_checked_ledger bigint NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT contract_liveness_status_check CHECK ((status = ANY (ARRAY['live'::text, 'archived'::text])))
);

ALTER TABLE contract_liveness ADD CONSTRAINT contract_liveness_contract_id_network_key UNIQUE (contract_id, network);
ALTER TABLE contract_liveness ADD CONSTRAINT contract_liveness_pkey PRIMARY KEY (id);

CREATE UNIQUE INDEX IF NOT EXISTS contract_liveness_contract_id_network_key ON contract_liveness USING btree (contract_id, network);
CREATE INDEX IF NOT EXISTS idx_contract_liveness_near_archival ON contract_liveness USING btree (ledgers_until_archive) WHERE (status = 'live'::text);

-- ---------------------------------------------------------------------------
-- contract_verification  (migration 0015)
-- Source-verification status for contracts.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contract_verification (
    id bigint NOT NULL,
    contract_id text NOT NULL,
    network text NOT NULL,
    status text NOT NULL,
    on_chain_hash text NOT NULL,
    source_hash text,
    repository_url text,
    commit_sha text,
    toolchain_version text,
    build_command text,
    wasm_path text,
    verified_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    CONSTRAINT contract_verification_status_check CHECK ((status = ANY (ARRAY['unverified'::text, 'pending'::text, 'verified'::text, 'mismatch'::text, 'failed'::text])))
);

ALTER TABLE contract_verification ADD CONSTRAINT contract_verification_contract_id_network_key UNIQUE (contract_id, network);
ALTER TABLE contract_verification ADD CONSTRAINT contract_verification_pkey PRIMARY KEY (id);

CREATE UNIQUE INDEX IF NOT EXISTS contract_verification_contract_id_network_key ON contract_verification USING btree (contract_id, network);
CREATE INDEX IF NOT EXISTS idx_contract_verification_contract_id ON contract_verification USING btree (contract_id);
CREATE INDEX IF NOT EXISTS idx_contract_verification_status ON contract_verification USING btree (status);

-- ---------------------------------------------------------------------------
-- contract_specs  (migration 0018)
-- Decoded contract interface specs (SEP-48).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contract_specs (
    id bigint NOT NULL,
    contract_id text NOT NULL,
    network text NOT NULL,
    code_hash text NOT NULL,
    has_spec boolean DEFAULT false NOT NULL,
    functions jsonb DEFAULT '[]'::jsonb NOT NULL,
    contract_type text DEFAULT 'unknown'::text NOT NULL,
    interfaces jsonb DEFAULT '[]'::jsonb NOT NULL,
    fetched_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE contract_specs ADD CONSTRAINT contract_specs_contract_id_network_key UNIQUE (contract_id, network);
ALTER TABLE contract_specs ADD CONSTRAINT contract_specs_pkey PRIMARY KEY (id);

CREATE UNIQUE INDEX IF NOT EXISTS contract_specs_contract_id_network_key ON contract_specs USING btree (contract_id, network);
CREATE INDEX IF NOT EXISTS idx_contract_specs_code_hash ON contract_specs USING btree (code_hash);
CREATE INDEX IF NOT EXISTS idx_contract_specs_contract_type ON contract_specs USING btree (contract_type);

-- ---------------------------------------------------------------------------
-- contract_stats_rollup  (migration 0019)
-- Pre-aggregated per-contract event counts.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contract_stats_rollup (
    contract_id text NOT NULL,
    network text NOT NULL,
    event_count bigint DEFAULT 0 NOT NULL,
    contract_event_count bigint DEFAULT 0 NOT NULL,
    system_event_count bigint DEFAULT 0 NOT NULL,
    diagnostic_event_count bigint DEFAULT 0 NOT NULL,
    first_seen_ledger bigint NOT NULL,
    last_seen_ledger bigint NOT NULL,
    last_seen_at timestamp with time zone NOT NULL,
    refreshed_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE contract_stats_rollup ADD CONSTRAINT contract_stats_rollup_pkey PRIMARY KEY (contract_id, network);

CREATE INDEX IF NOT EXISTS idx_contract_stats_rollup_network_count ON contract_stats_rollup USING btree (network, event_count DESC);

-- ---------------------------------------------------------------------------
-- contract_event_schemas  (migration 0021)
-- Inferred per-event topic/value schemas.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contract_event_schemas (
    id bigint NOT NULL,
    contract_id text NOT NULL,
    network text NOT NULL,
    event_name text NOT NULL,
    code_hash text NOT NULL,
    field_schema jsonb NOT NULL,
    observed_source text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE contract_event_schemas ADD CONSTRAINT contract_event_schemas_contract_id_network_event_name_code__key UNIQUE (contract_id, network, event_name, code_hash);
ALTER TABLE contract_event_schemas ADD CONSTRAINT contract_event_schemas_pkey PRIMARY KEY (id);

CREATE UNIQUE INDEX IF NOT EXISTS contract_event_schemas_contract_id_network_event_name_code__key ON contract_event_schemas USING btree (contract_id, network, event_name, code_hash);
CREATE INDEX IF NOT EXISTS idx_contract_event_schemas_contract ON contract_event_schemas USING btree (contract_id, network, code_hash);
CREATE INDEX IF NOT EXISTS idx_contract_event_schemas_event_name ON contract_event_schemas USING btree (event_name);

-- ---------------------------------------------------------------------------
-- token_metadata  (migration 0022)
-- Cached SEP-41 name/symbol/decimals per contract and network.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS token_metadata (
    id bigint NOT NULL,
    contract_id text NOT NULL,
    network text NOT NULL,
    name text,
    symbol text,
    decimals integer,
    is_token boolean DEFAULT true NOT NULL,
    resolved_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE token_metadata ADD CONSTRAINT token_metadata_contract_id_network_key UNIQUE (contract_id, network);
ALTER TABLE token_metadata ADD CONSTRAINT token_metadata_pkey PRIMARY KEY (id);

CREATE INDEX IF NOT EXISTS idx_token_metadata_contract ON token_metadata USING btree (contract_id, network);
CREATE UNIQUE INDEX IF NOT EXISTS token_metadata_contract_id_network_key ON token_metadata USING btree (contract_id, network);

-- ---------------------------------------------------------------------------
-- contract_storage_snapshots  (migration 0023)
-- Observed contract storage entry changes.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS contract_storage_snapshots (
    id bigint NOT NULL,
    contract_id text NOT NULL,
    network text NOT NULL,
    storage_key text NOT NULL,
    key_json jsonb,
    value_json jsonb,
    ledger_sequence bigint NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);

ALTER TABLE contract_storage_snapshots ADD CONSTRAINT contract_storage_snapshots_contract_id_network_storage_key__key UNIQUE (contract_id, network, storage_key, ledger_sequence);
ALTER TABLE contract_storage_snapshots ADD CONSTRAINT contract_storage_snapshots_pkey PRIMARY KEY (id);

CREATE UNIQUE INDEX IF NOT EXISTS contract_storage_snapshots_contract_id_network_storage_key__key ON contract_storage_snapshots USING btree (contract_id, network, storage_key, ledger_sequence);
CREATE INDEX IF NOT EXISTS idx_contract_storage_snapshots_latest ON contract_storage_snapshots USING btree (contract_id, network, storage_key, ledger_sequence DESC);

-- ---------------------------------------------------------------------------
-- failed_events  (migration 0027)
-- Dead-letter queue for well-formed events that repeatedly failed to persist.
-- Distinct from parse_errors above: this is for events that decoded fine but
-- whose INSERT kept failing after bounded retries.
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS failed_events (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    ledger_sequence  BIGINT      NOT NULL,
    contract_id      TEXT        NOT NULL,
    transaction_hash TEXT        NOT NULL,
    event_index      INT         NOT NULL,
    event_payload    JSONB       NOT NULL,
    error_message    TEXT        NOT NULL,
    attempts         INT         NOT NULL DEFAULT 1,
    occurred_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    replayed_at      TIMESTAMPTZ,
    network          TEXT        NOT NULL DEFAULT 'testnet'
        CONSTRAINT chk_failed_events_network CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'))
);

CREATE INDEX IF NOT EXISTS idx_failed_events_occurred_at ON failed_events (occurred_at DESC);
CREATE INDEX IF NOT EXISTS idx_failed_events_pending ON failed_events (occurred_at) WHERE replayed_at IS NULL;
-- Pending rows are unique per event (migration 0030): redelivered failures
-- update in place, so COUNT(*) of pending rows = distinct poisoned events.
CREATE UNIQUE INDEX IF NOT EXISTS uq_failed_events_pending
    ON failed_events (network, contract_id, ledger_sequence, event_index)
    WHERE replayed_at IS NULL;
-- migration 0038
CREATE INDEX IF NOT EXISTS idx_failed_events_network_pending
    ON failed_events (network, occurred_at) WHERE replayed_at IS NULL;

-- ---------------------------------------------------------------------------
-- backfill_jobs  (migration 0032)
-- Queue of ledger-range backfills enqueued by the indexer's gap scan and
-- executed by `crates/backfill --from-queue` (issue #216).
-- ---------------------------------------------------------------------------
CREATE TABLE IF NOT EXISTS backfill_jobs (
    id               UUID        PRIMARY KEY DEFAULT gen_random_uuid(),
    from_ledger      BIGINT      NOT NULL,
    to_ledger        BIGINT      NOT NULL,
    network          TEXT        NOT NULL,
    status           TEXT        NOT NULL DEFAULT 'pending'
                                  CHECK (status IN ('pending', 'running', 'done', 'failed')),
    claimed_at       TIMESTAMPTZ,
    completed_at     TIMESTAMPTZ,
    error            TEXT,
    created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    CONSTRAINT chk_backfill_jobs_range CHECK (to_ledger >= from_ledger)
);

CREATE UNIQUE INDEX IF NOT EXISTS uq_backfill_jobs_pending_range
    ON backfill_jobs (network, from_ledger, to_ledger)
    WHERE status IN ('pending', 'running');
CREATE INDEX IF NOT EXISTS idx_backfill_jobs_pending
    ON backfill_jobs (created_at)
    WHERE status = 'pending';
CREATE INDEX IF NOT EXISTS idx_backfill_jobs_stale
    ON backfill_jobs (claimed_at)
    WHERE status = 'running';



-- ---------------------------------------------------------------------------
-- Network value constraints  (migration 0031, issue #252)
-- A typo such as 'testnett' silently splits the dataset in two: every query
-- filtered on network misses those rows, and nothing errors. These CHECKs turn
-- that into a write-time failure. Values match what the code writes -- see
-- crates/common/src/types.rs Network.
-- ---------------------------------------------------------------------------
ALTER TABLE soroban_events
    ADD CONSTRAINT chk_soroban_events_network
    CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));

ALTER TABLE indexed_contracts
    ADD CONSTRAINT chk_indexed_contracts_network
    CHECK (network IS NULL OR network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));

ALTER TABLE api_keys
    ADD CONSTRAINT chk_api_keys_network
    CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));

ALTER TABLE audit_log
    ADD CONSTRAINT chk_audit_log_network
    CHECK (network IS NULL OR network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));

ALTER TABLE webhook_subscriptions
    ADD CONSTRAINT chk_webhook_subscriptions_network
    CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));

ALTER TABLE token_events
    ADD CONSTRAINT chk_token_events_network
    CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));

ALTER TABLE contract_invocation_metrics
    ADD CONSTRAINT chk_contract_invocation_metrics_network
    CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));

ALTER TABLE contract_liveness
    ADD CONSTRAINT chk_contract_liveness_network
    CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));

ALTER TABLE contract_verification
    ADD CONSTRAINT chk_contract_verification_network
    CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));

ALTER TABLE contract_specs
    ADD CONSTRAINT chk_contract_specs_network
    CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));

ALTER TABLE contract_stats_rollup
    ADD CONSTRAINT chk_contract_stats_rollup_network
    CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));

ALTER TABLE contract_event_schemas
    ADD CONSTRAINT chk_contract_event_schemas_network
    CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));

ALTER TABLE token_metadata
    ADD CONSTRAINT chk_token_metadata_network
    CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));

ALTER TABLE contract_storage_snapshots
    ADD CONSTRAINT chk_contract_storage_snapshots_network
    CHECK (network IN ('mainnet', 'testnet', 'futurenet', 'sandbox'));
