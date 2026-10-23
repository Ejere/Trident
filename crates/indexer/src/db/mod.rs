use std::collections::{HashMap, HashSet};
use std::str::FromStr;
use std::time::Duration;

use chrono::{DateTime, Utc};
use sqlx::postgres::{PgConnectOptions, PgPoolOptions};
use sqlx::PgPool;
use trident_common::{EventType, SorobanEvent, TridentError};
use uuid::Uuid;

use crate::parser::token_events::TokenEvent;

/// Build a bounded Postgres connection pool sized for this service.
///
/// `statement_cache_capacity(0)` disables sqlx's named-prepared-statement cache.
/// This is mandatory when connecting through PgBouncer in transaction pooling
/// mode: a cached prepared statement is bound to one server connection, but in
/// transaction mode the next transaction may be routed to a different server
/// connection where that statement does not exist, which makes the query fail.
/// See docs/deployment.md (issue #87).
#[allow(dead_code)]
pub async fn connect_pool(database_url: &str, pool_size: u32) -> Result<PgPool, TridentError> {
    let connect_options = PgConnectOptions::from_str(database_url)
        .map_err(|e| TridentError::config(anyhow::Error::new(e).context("invalid DATABASE_URL")))?
        .statement_cache_capacity(0);

    PgPoolOptions::new()
        .max_connections(pool_size)
        .connect_with(connect_options)
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("connect_pool")))
}

/// Classify a storage failure as permanent (the data itself is unpersistable —
/// dead-letter it) or transient (the database is the problem — retry/propagate
/// instead) (issue #573).
///
/// `commit_page` and every `insert_*_batch` helper wrap every `sqlx::Error` in
/// `TridentError::storage`, which `TridentError::severity` classifies as
/// uniformly `Retryable` — correct for the poll loop's top-level retry, but not
/// precise enough for `commit_page_with_fallback`'s per-event isolation stage
/// (issue #208): "an event that fails even alone is unpersistable" only holds
/// when the failure is specific to that event. During a failover, lock storm,
/// or pool exhaustion every event in the page fails identically in isolation,
/// and without this distinction the whole page was dead-lettered wholesale
/// instead of retried.
///
/// `anyhow::Error::new(e).context(...)` (how every call site here builds the
/// `TridentError::StorageError` source) still carries the original
/// `sqlx::Error` in its chain, so `chain().find_map` recovers it without
/// touching any insert function's signature.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StorageFailure {
    /// The database itself is unavailable or overloaded right now — a
    /// connection/IO/pool/protocol failure, or a Postgres error whose SQLSTATE
    /// class is connection (`08`), resource exhaustion (`53`), operator
    /// intervention (`57`, covering `57014` query_canceled i.e. a statement
    /// timeout), or serialization/deadlock (`40001`/`40P01`). Retrying — or,
    /// in the per-event fallback, propagating so the whole page is retried —
    /// is the right response.
    Transient,
    /// The data itself is the problem: a constraint violation (SQLSTATE class
    /// `23`), an invalid text representation / data exception (`22`), or a
    /// row/column/type-level `sqlx::Error` that can never succeed by retrying.
    /// Dead-lettering — what the queue is for — is the right response.
    Permanent,
}

/// Classify a `TridentError` produced by this module's storage functions.
/// Errors with no recoverable `sqlx::Error` in their chain (e.g. the
/// timestamp-parse failure in `commit_page`) are treated as `Permanent`:
/// retrying an identical page cannot change a parse outcome.
pub fn classify_storage_failure(err: &TridentError) -> StorageFailure {
    let TridentError::StorageError { source } = err else {
        // Non-storage errors reaching this classifier is a caller bug, but
        // failing safe here means treating it as permanent rather than
        // looping forever on something retrying can never fix.
        return StorageFailure::Permanent;
    };

    let Some(db_err) = source.chain().find_map(|c| c.downcast_ref::<sqlx::Error>()) else {
        return StorageFailure::Permanent;
    };

    match db_err {
        // Connection/IO/pool/protocol failures: the database is unreachable
        // or the pool is exhausted, not that this row is bad.
        sqlx::Error::Io(_)
        | sqlx::Error::Tls(_)
        | sqlx::Error::Protocol(_)
        | sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed => StorageFailure::Transient,

        sqlx::Error::Database(db) => match db.code() {
            Some(code) => match code.as_ref() {
                // Class 08: connection exception.
                c if c.starts_with("08") => StorageFailure::Transient,
                // Class 53: insufficient resources (disk/memory/connections).
                c if c.starts_with("53") => StorageFailure::Transient,
                // Class 57: operator intervention — includes 57014
                // query_canceled, e.g. statement_timeout firing under load.
                c if c.starts_with("57") => StorageFailure::Transient,
                // 40001 serialization_failure, 40P01 deadlock_detected.
                "40001" | "40P01" => StorageFailure::Transient,
                // Class 23 (integrity_constraint_violation) and class 22
                // (data_exception) are the row's own fault — retrying an
                // identical INSERT reproduces the same violation.
                _ => StorageFailure::Permanent,
            },
            // A DatabaseError with no SQLSTATE code is not a shape Postgres
            // produces; fail safe rather than assume it will clear on retry.
            None => StorageFailure::Permanent,
        },

        // Row/column/type/decoding errors are about the data or the query
        // shape, not the database's availability.
        sqlx::Error::RowNotFound
        | sqlx::Error::TypeNotFound { .. }
        | sqlx::Error::ColumnIndexOutOfBounds { .. }
        | sqlx::Error::ColumnNotFound(_)
        | sqlx::Error::ColumnDecode { .. }
        | sqlx::Error::Decode(_)
        | sqlx::Error::Configuration(_)
        | sqlx::Error::Migrate(_) => StorageFailure::Permanent,

        // sqlx::Error is #[non_exhaustive]; an unrecognised future variant is
        // treated the same as "no sqlx::Error found" — permanent, since
        // retrying blind is worse than a false-positive dead-letter here.
        _ => StorageFailure::Permanent,
    }
}

// Stable namespace for deterministic event UUIDs (UUIDv5).
// Using the DNS namespace is arbitrary; what matters is that it is fixed.
const EVENT_NS: Uuid = Uuid::NAMESPACE_DNS;

/// Derive a deterministic UUID for an event from inputs that depend only on
/// the event itself, not on how it was batched.
///
/// Deliberately keyed on `(transaction_hash, raw_event_index)`, NOT
/// `(ledger_sequence, event_index)`: `event_index` is mutated by
/// `assign_unique_event_indexes` to break ties within a batch, so its final
/// value depends on batch composition and iteration order, not the event
/// itself. The same protocol event re-indexed under a different page
/// boundary previously got a different UUID and inserted as a duplicate row,
/// since `ON CONFLICT DO NOTHING` only catches an exact id match (issue
/// #599). `transaction_hash` is already globally unique on its own, and
/// `raw_event_index` (the RPC-reported/derived position, fixed at parse
/// time and never mutated) is stable across runs regardless of batching, so
/// the pair is both unique and reproducible.
fn event_uuid(transaction_hash: &str, raw_event_index: u32) -> Uuid {
    let key = format!("{transaction_hash}:{raw_event_index}");
    Uuid::new_v5(&EVENT_NS, key.as_bytes())
}

pub mod outbox;

/// Ledger provenance recorded alongside a committed page.
pub struct LedgerMeta<'a> {
    pub sequence: u64,
    pub hash: &'a str,
    pub timestamp: &'a str,
    pub event_count: i32,
}

/// Everything one RPC page contributes to the database, committed atomically.
///
/// Bundling the events with the cursor advance is what stops a crash between
/// the two from leaving the cursor ahead of the data it claims to cover.
/// A decoded token event paired with the indexed event it projects (issue #211).
pub struct TokenProjection<'a> {
    pub event: &'a SorobanEvent,
    pub token: &'a TokenEvent,
}

/// One (contract, transaction) invocation metrics row, ready to persist into
/// `contract_invocation_metrics` (issue #266).
pub struct InvocationMetricRow<'a> {
    pub contract_id: &'a str,
    pub transaction_hash: &'a str,
    pub ledger_sequence: u64,
    /// ISO 8601 UTC timestamp of the ledger close.
    pub ledger_timestamp: &'a str,
    pub metrics: &'a crate::parser::invocation_metrics::InvocationMetrics,
}

pub struct PageCommit<'a> {
    pub events: &'a [SorobanEvent],
    /// Normalised token-event rows for this page. Every referenced event must
    /// also appear in `events` — the projection is foreign-keyed to it.
    pub token_events: &'a [TokenProjection<'a>],
    /// Per-invocation fee + declared-resource metering for tracked contracts
    /// in this page (issue #266). Empty in index-all mode — metering is
    /// bounded to the allowlist.
    pub invocation_metrics: &'a [InvocationMetricRow<'a>],
    /// Contract storage snapshot changes observed in this page (issue #270).
    /// Empty unless a tracked contract was detected as a SEP-41 token and one
    /// of its holders moved funds in this page.
    pub storage_snapshots: &'a [StorageSnapshotRow<'a>],
    /// Network this indexer instance is running against (e.g. `"mainnet"` or
    /// `"testnet"`). Stamped onto every `soroban_events`,
    /// `contract_invocation_metrics`, and `contract_storage_snapshots` row in
    /// this page (issue #595) — without it, rows silently fall back to the
    /// columns' `DEFAULT 'testnet'` regardless of the indexer's actual network.
    pub network: &'a str,
    /// New cursor value, when the page advanced it.
    pub cursor: Option<u64>,
    /// Ledger metadata row, written only when the cursor advanced.
    pub ledger: Option<LedgerMeta<'a>>,
    /// Inclusive start of the ledger range this page covers
    /// (issue #723): commit_page records one `ledger_metadata` row per
    /// ledger in `[covered_from_ledger ..= ledger.meta.sequence]` instead
    /// of only the page's last ledger. A `getEvents` page traverses every
    /// ledger in that span — ledgers without matching events included —
    /// so recording only the last one made every inter-page span look
    /// like a hole to `scan_ledger_gaps` / `detect_ledger_gaps`. `None`
    /// (per-event fallback commits, replays, tests) records only the
    /// single `ledger` row, i.e. the pre-#723 behaviour.
    pub covered_from_ledger: Option<u64>,
    /// Maximum rows per INSERT statement, bounding statement size and memory.
    pub batch_size: usize,
}

/// Columns of a batch of events, transposed into the parallel arrays that the
/// `UNNEST` insert binds. Building this once avoids re-deriving per row.
struct EventColumns {
    ids: Vec<Uuid>,
    contract_ids: Vec<String>,
    ledger_sequences: Vec<i64>,
    ledger_timestamps: Vec<DateTime<Utc>>,
    transaction_hashes: Vec<String>,
    event_indexes: Vec<i32>,
    event_types: Vec<String>,
    topics: Vec<serde_json::Value>,
    data: Vec<serde_json::Value>,
}

fn event_type_str(event_type: &EventType) -> &'static str {
    match event_type {
        EventType::Contract => "contract",
        EventType::System => "system",
        EventType::Diagnostic => "diagnostic",
    }
}

impl EventColumns {
    fn build(events: &[SorobanEvent]) -> Result<Self, TridentError> {
        let mut cols = EventColumns {
            ids: Vec::with_capacity(events.len()),
            contract_ids: Vec::with_capacity(events.len()),
            ledger_sequences: Vec::with_capacity(events.len()),
            ledger_timestamps: Vec::with_capacity(events.len()),
            transaction_hashes: Vec::with_capacity(events.len()),
            event_indexes: Vec::with_capacity(events.len()),
            event_types: Vec::with_capacity(events.len()),
            topics: Vec::with_capacity(events.len()),
            data: Vec::with_capacity(events.len()),
        };

        for event in events {
            let ledger_ts: DateTime<Utc> = event.ledger_timestamp.parse().map_err(|e| {
                TridentError::storage(anyhow::Error::new(e).context("ledger timestamp parse"))
            })?;
            let topics = serde_json::to_value(&event.topics).map_err(|e| {
                TridentError::storage(anyhow::Error::new(e).context("topics serialise"))
            })?;

            cols.ids
                .push(event_uuid(&event.transaction_hash, event.raw_event_index));
            cols.contract_ids.push(event.contract_id.clone());
            cols.ledger_sequences.push(event.ledger_sequence as i64);
            cols.ledger_timestamps.push(ledger_ts);
            cols.transaction_hashes.push(event.transaction_hash.clone());
            cols.event_indexes.push(event.event_index as i32);
            cols.event_types
                .push(event_type_str(&event.event_type).to_string());
            cols.topics.push(topics);
            cols.data.push(event.data.clone());
        }

        Ok(cols)
    }
}

/// Insert a batch of events in a single statement (issue #199).
///
/// One round-trip per batch instead of one per row. A replayed page inserts
/// nothing new: the deterministic UUIDv5 id reproduces the same
/// (ledger_sequence, id) pair, and `ON CONFLICT DO NOTHING` absorbs it.
///
/// The conflict clause is deliberately **untargeted** (issue #418).
/// `soroban_events` carries two unique constraints:
///
///   1. `PRIMARY KEY (ledger_sequence, id)` — from migration 0017, which made
///      the table RANGE-partitioned by ledger_sequence. PostgreSQL requires
///      every unique constraint on a partitioned table to include the
///      partition key, so a single-column PK on id alone no longer exists.
///   2. `uq_soroban_events_tx_index_network
///      (ledger_sequence, transaction_hash, event_index, network)` — from
///      migration 0025, mirroring the protocol guarantee that a
///      (transaction_hash, event_index) pair identifies exactly one event.
///
/// A targeted `ON CONFLICT (ledger_sequence, id)` only guards the first. That
/// is enough for a single writer replaying its own page, but not for two
/// indexer replicas committing the same page concurrently: the second writer's
/// rows collide on the natural key, which the targeted clause does not absorb,
/// and the whole batch fails with a unique-violation error rather than being
/// silently ignored. In production that aborts a poll cycle during any rollout
/// overlap or double deploy. Caught by
/// `concurrent_indexers_persist_each_event_exactly_once`.
///
/// Untargeted `DO NOTHING` covers both constraints. It does not weaken the
/// safety net migration 0025 describes: a genuine id-derivation bug still
/// cannot write a duplicate row, it is simply dropped rather than raised. The
/// natural-key constraint remains as the enforcement point, and 0025's
/// pre-flight duplicate check still runs against existing data.
///
/// `network` must match the value used for `indexed_contracts`/cursor state in
/// this deployment (e.g. `"mainnet"` or `"testnet"`) — every row in `events`
/// is stamped with it. Previously this was left unbound and every row fell
/// back to the column's `DEFAULT 'testnet'` regardless of the indexer's
/// actual network, silently mislabeling mainnet data (issue #595).
pub async fn insert_events_batch<'e, E>(
    executor: E,
    network: &str,
    events: &[SorobanEvent],
) -> Result<(), TridentError>
where
    E: sqlx::PgExecutor<'e>,
{
    if events.is_empty() {
        return Ok(());
    }

    let cols = EventColumns::build(events)?;
    let networks: Vec<String> = std::iter::repeat(network.to_string())
        .take(events.len())
        .collect();

    sqlx::query(
        r#"
        INSERT INTO soroban_events
            (id, contract_id, ledger_sequence, ledger_timestamp, transaction_hash,
             event_index, event_type, topics, data, network)
        SELECT * FROM UNNEST(
            $1::uuid[], $2::text[], $3::bigint[], $4::timestamptz[], $5::text[],
            $6::int[], $7::text[], $8::jsonb[], $9::jsonb[], $10::text[]
        )
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(&cols.ids)
    .bind(&cols.contract_ids)
    .bind(&cols.ledger_sequences)
    .bind(&cols.ledger_timestamps)
    .bind(&cols.transaction_hashes)
    .bind(&cols.event_indexes)
    .bind(&cols.event_types)
    .bind(&cols.topics)
    .bind(&cols.data)
    .bind(&networks)
    .execute(executor)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("insert_events_batch")))?;

    Ok(())
}

/// Insert a batch of decoded token events into the `token_events` projection
/// (issue #211).
///
/// Keyed by the originating event's UUID, so replaying a page re-projects the
/// same rows and `ON CONFLICT DO NOTHING` absorbs them.
///
/// `network` must match the value used for the events in the same page —
/// previously left unbound, so every row fell back to the column's
/// `DEFAULT 'testnet'` regardless of the indexer's actual network (issue #595
/// fixed this for `soroban_events`/`contract_invocation_metrics` but missed
/// this table).
pub async fn insert_token_events_batch<'e, E>(
    executor: E,
    network: &str,
    projections: &[TokenProjection<'_>],
) -> Result<(), TridentError>
where
    E: sqlx::PgExecutor<'e>,
{
    if projections.is_empty() {
        return Ok(());
    }

    let mut event_ids = Vec::with_capacity(projections.len());
    let mut contract_ids = Vec::with_capacity(projections.len());
    let mut networks = Vec::with_capacity(projections.len());
    let mut event_types = Vec::with_capacity(projections.len());
    let mut from_addresses = Vec::with_capacity(projections.len());
    let mut to_addresses = Vec::with_capacity(projections.len());
    let mut spender_addresses = Vec::with_capacity(projections.len());
    let mut admin_addresses = Vec::with_capacity(projections.len());
    let mut amounts = Vec::with_capacity(projections.len());
    let mut expiration_ledgers = Vec::with_capacity(projections.len());
    let mut asset_codes = Vec::with_capacity(projections.len());
    let mut asset_issuers = Vec::with_capacity(projections.len());
    let mut ledger_sequences = Vec::with_capacity(projections.len());
    let mut ledger_timestamps = Vec::with_capacity(projections.len());
    let mut transaction_hashes = Vec::with_capacity(projections.len());
    let mut event_indexes = Vec::with_capacity(projections.len());

    for projection in projections {
        let event = projection.event;
        let token = projection.token;
        let ledger_ts: DateTime<Utc> = event.ledger_timestamp.parse().map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("ledger timestamp parse"))
        })?;

        event_ids.push(event_uuid(&event.transaction_hash, event.raw_event_index));
        contract_ids.push(event.contract_id.clone());
        networks.push(network.to_string());
        event_types.push(token.event_type.as_str().to_string());
        from_addresses.push(token.from.clone());
        to_addresses.push(token.to.clone());
        spender_addresses.push(token.spender.clone());
        admin_addresses.push(token.admin.clone());
        amounts.push(token.amount.clone());
        expiration_ledgers.push(token.expiration_ledger);
        asset_codes.push(token.asset_code.clone());
        asset_issuers.push(token.asset_issuer.clone());
        ledger_sequences.push(event.ledger_sequence as i64);
        ledger_timestamps.push(ledger_ts);
        transaction_hashes.push(event.transaction_hash.clone());
        event_indexes.push(event.event_index as i32);
    }

    sqlx::query(
        r#"
        INSERT INTO token_events
            (event_id, contract_id, network, event_type, from_address, to_address,
             spender_address, admin_address, amount, expiration_ledger,
             asset_code, asset_issuer,
             ledger_sequence, ledger_timestamp, transaction_hash, event_index)
        SELECT * FROM UNNEST(
            $1::uuid[], $2::text[], $3::text[], $4::text[], $5::text[], $6::text[],
            $7::text[], $8::text[], $9::text[], $10::bigint[],
            $11::text[], $12::text[],
            $13::bigint[], $14::timestamptz[], $15::text[], $16::int[]
        )
        ON CONFLICT (event_id) DO NOTHING
        "#,
    )
    .bind(&event_ids)
    .bind(&contract_ids)
    .bind(&networks)
    .bind(&event_types)
    .bind(&from_addresses)
    .bind(&to_addresses)
    .bind(&spender_addresses)
    .bind(&admin_addresses)
    .bind(&amounts)
    .bind(&expiration_ledgers)
    .bind(&asset_codes)
    .bind(&asset_issuers)
    .bind(&ledger_sequences)
    .bind(&ledger_timestamps)
    .bind(&transaction_hashes)
    .bind(&event_indexes)
    .execute(executor)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("insert_token_events_batch"))
    })?;

    Ok(())
}

/// Insert a batch of per-invocation fee/resource metering rows into
/// `contract_invocation_metrics` (issue #266).
///
/// Keyed by `(contract_id, transaction_hash)`; a replayed page inserts
/// nothing new via `ON CONFLICT DO NOTHING`, matching the idempotency of the
/// other page-scoped inserts.
///
/// `network` must match the value used for the events in the same page —
/// previously left unbound, so every row fell back to the column's
/// `DEFAULT 'testnet'` regardless of the indexer's actual network (issue #595).
pub async fn insert_invocation_metrics_batch<'e, E>(
    executor: E,
    network: &str,
    rows: &[InvocationMetricRow<'_>],
) -> Result<(), TridentError>
where
    E: sqlx::PgExecutor<'e>,
{
    if rows.is_empty() {
        return Ok(());
    }

    let mut contract_ids = Vec::with_capacity(rows.len());
    let mut networks = Vec::with_capacity(rows.len());
    let mut transaction_hashes = Vec::with_capacity(rows.len());
    let mut ledger_sequences = Vec::with_capacity(rows.len());
    let mut ledger_timestamps = Vec::with_capacity(rows.len());
    let mut fee_charged = Vec::with_capacity(rows.len());
    let mut resource_fee = Vec::with_capacity(rows.len());
    let mut cpu_instructions = Vec::with_capacity(rows.len());
    let mut read_bytes = Vec::with_capacity(rows.len());
    let mut write_bytes = Vec::with_capacity(rows.len());
    let mut provenance = Vec::with_capacity(rows.len());

    for row in rows {
        let ledger_ts: DateTime<Utc> = row.ledger_timestamp.parse().map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("ledger timestamp parse"))
        })?;

        contract_ids.push(row.contract_id.to_string());
        networks.push(network.to_string());
        transaction_hashes.push(row.transaction_hash.to_string());
        ledger_sequences.push(row.ledger_sequence as i64);
        ledger_timestamps.push(ledger_ts);
        fee_charged.push(row.metrics.fee_charged);
        resource_fee.push(row.metrics.resource_fee);
        cpu_instructions.push(row.metrics.cpu_instructions);
        read_bytes.push(row.metrics.read_bytes);
        write_bytes.push(row.metrics.write_bytes);
        provenance.push(row.metrics.provenance.to_string());
    }

    sqlx::query(
        r#"
        INSERT INTO contract_invocation_metrics
            (contract_id, network, transaction_hash, ledger_sequence, ledger_timestamp,
             fee_charged, resource_fee, cpu_instructions, read_bytes, write_bytes, provenance)
        SELECT * FROM UNNEST(
            $1::text[], $2::text[], $3::text[], $4::bigint[], $5::timestamptz[],
            $6::bigint[], $7::bigint[], $8::bigint[], $9::bigint[], $10::bigint[], $11::text[]
        )
        ON CONFLICT (contract_id, transaction_hash) DO NOTHING
        "#,
    )
    .bind(&contract_ids)
    .bind(&networks)
    .bind(&transaction_hashes)
    .bind(&ledger_sequences)
    .bind(&ledger_timestamps)
    .bind(&fee_charged)
    .bind(&resource_fee)
    .bind(&cpu_instructions)
    .bind(&read_bytes)
    .bind(&write_bytes)
    .bind(&provenance)
    .execute(executor)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("insert_invocation_metrics_batch"))
    })?;

    Ok(())
}

/// Insert the outbox rows for a batch of events in a single statement (#200).
///
/// Mirrors [`insert_events_batch`]: same deterministic ids, same idempotency.
/// `ON CONFLICT (event_id) DO NOTHING` means a replayed page does not re-queue
/// a delivery for an event that already has one.
pub async fn insert_outbox_batch<'e, E>(
    executor: E,
    events: &[SorobanEvent],
) -> Result<(), TridentError>
where
    E: sqlx::PgExecutor<'e>,
{
    if events.is_empty() {
        return Ok(());
    }

    let mut ids: Vec<Uuid> = Vec::with_capacity(events.len());
    let mut payloads: Vec<serde_json::Value> = Vec::with_capacity(events.len());
    for event in events {
        ids.push(event_uuid(&event.transaction_hash, event.raw_event_index));
        payloads.push(serde_json::to_value(event).map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("outbox payload serialise"))
        })?);
    }

    sqlx::query(
        r#"
        INSERT INTO event_outbox (event_id, payload)
        SELECT * FROM UNNEST($1::uuid[], $2::jsonb[])
        ON CONFLICT (event_id) DO NOTHING
        "#,
    )
    .bind(&ids)
    .bind(&payloads)
    .execute(executor)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("insert_outbox_batch")))?;

    Ok(())
}

/// Persist (or refresh) a tracked contract's parsed spec + detected
/// interfaces, keyed by `(contract_id, network)` (issues #260, #269).
/// Called only when the observed code hash differs from the last one synced,
/// so a redeploy refreshes the row instead of leaving it stale.
pub async fn upsert_contract_spec(
    pool: &PgPool,
    contract_id: &str,
    network: &str,
    spec: &crate::spec::ContractSpec,
) -> Result<(), TridentError> {
    let functions = serde_json::to_value(&spec.functions).map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("serialise spec functions"))
    })?;
    let interfaces = serde_json::to_value(&spec.interfaces).map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("serialise spec interfaces"))
    })?;

    sqlx::query(
        r#"
        INSERT INTO contract_specs
            (contract_id, network, code_hash, has_spec, functions, contract_type, interfaces)
        VALUES ($1, $2, $3, $4, $5::jsonb, $6, $7::jsonb)
        ON CONFLICT (contract_id, network) DO UPDATE SET
            code_hash     = EXCLUDED.code_hash,
            has_spec      = EXCLUDED.has_spec,
            functions     = EXCLUDED.functions,
            contract_type = EXCLUDED.contract_type,
            interfaces    = EXCLUDED.interfaces,
            updated_at    = NOW()
        "#,
    )
    .bind(contract_id)
    .bind(network)
    .bind(&spec.code_hash)
    .bind(spec.has_spec)
    .bind(functions)
    .bind(&spec.contract_type)
    .bind(interfaces)
    .execute(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("upsert_contract_spec")))?;

    Ok(())
}

/// One persisted contract spec, as the spec-sync seed reads it (issue #726).
pub struct PersistedContractSpec {
    pub contract_id: String,
    pub code_hash: String,
    /// Parsed functions, or `None` when the stored JSON cannot be parsed
    /// (corrupt row). The code hash is still handed back in that case so a
    /// redeploy stays detectable, while the missing functions make the next
    /// pass re-fetch the WASM and rewrite the row.
    pub functions: Option<Vec<crate::spec::SpecFunction>>,
}

/// Load every persisted contract spec for `network`, used to seed the
/// spec-sync cache at startup (issue #726).
///
/// `known_code_hashes` used to live only in memory, so after a restart — or a
/// downtime window in which a contract was redeployed — the first post-restart
/// observation looked like the first ever for that contract, and stale token
/// metadata was never invalidated. Seeding from `contract_specs` makes the
/// first pass after a restart behave like a continuous run: any code hash
/// mismatch against an existing row is detected and refreshes it.
///
/// The functions are cached by code hash, which is content-addressed, so an
/// old row's functions remain valid for that hash; comparing hashes is what
/// detects a redeploy.
pub async fn load_contract_specs(
    pool: &PgPool,
    network: &str,
) -> Result<Vec<PersistedContractSpec>, TridentError> {
    let rows: Vec<(String, String, serde_json::Value)> = sqlx::query_as(
        "SELECT contract_id, code_hash, functions FROM contract_specs WHERE network = $1",
    )
    .bind(network)
    .fetch_all(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("load_contract_specs")))?;

    Ok(rows
        .into_iter()
        .map(|(contract_id, code_hash, functions)| {
            let functions =
                match serde_json::from_value::<Vec<crate::spec::SpecFunction>>(functions) {
                    Ok(functions) => Some(functions),
                    Err(e) => {
                        tracing::warn!(
                            contract_id = %contract_id,
                            code_hash,
                            error = %e,
                            "Stored contract spec functions unreadable; the next spec sync will \
                             re-fetch this contract's WASM and rewrite the row"
                        );
                        None
                    }
                };
            PersistedContractSpec {
                contract_id,
                code_hash,
                functions,
            }
        })
        .collect())
}

/// Latest persisted value for a single contract-storage key, if any (issue
/// #270). Used to detect whether a freshly observed value has changed before
/// writing a new snapshot row.
pub async fn get_latest_storage_value(
    pool: &PgPool,
    contract_id: &str,
    network: &str,
    storage_key: &str,
) -> Result<Option<serde_json::Value>, TridentError> {
    let row: Option<(Option<serde_json::Value>,)> = sqlx::query_as(
        r#"
        SELECT value_json FROM contract_storage_snapshots
        WHERE contract_id = $1 AND network = $2 AND storage_key = $3
        ORDER BY ledger_sequence DESC
        LIMIT 1
        "#,
    )
    .bind(contract_id)
    .bind(network)
    .bind(storage_key)
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("get_latest_storage_value"))
    })?;

    Ok(row.and_then(|(v,)| v))
}

/// One contract-storage snapshot row, ready to persist (issue #270).
pub struct StorageSnapshotRow<'a> {
    pub contract_id: &'a str,
    pub storage_key: &'a str,
    pub key_json: &'a serde_json::Value,
    pub value_json: Option<&'a serde_json::Value>,
    pub ledger_sequence: u64,
}

/// Insert a batch of contract-storage snapshot changes (issue #270).
/// Callers are expected to have already diffed against
/// [`get_latest_storage_value`] — this only appends, it never overwrites a
/// prior snapshot, so historical values stay queryable.
pub async fn insert_storage_snapshots_batch<'e, E>(
    executor: E,
    network: &str,
    rows: &[StorageSnapshotRow<'_>],
) -> Result<(), TridentError>
where
    E: sqlx::PgExecutor<'e>,
{
    if rows.is_empty() {
        return Ok(());
    }

    let mut contract_ids = Vec::with_capacity(rows.len());
    let mut networks = Vec::with_capacity(rows.len());
    let mut storage_keys = Vec::with_capacity(rows.len());
    let mut key_jsons = Vec::with_capacity(rows.len());
    let mut value_jsons: Vec<Option<serde_json::Value>> = Vec::with_capacity(rows.len());
    let mut ledger_sequences = Vec::with_capacity(rows.len());

    for row in rows {
        contract_ids.push(row.contract_id.to_string());
        networks.push(network.to_string());
        storage_keys.push(row.storage_key.to_string());
        key_jsons.push(row.key_json.clone());
        value_jsons.push(row.value_json.cloned());
        ledger_sequences.push(row.ledger_sequence as i64);
    }

    sqlx::query(
        r#"
        INSERT INTO contract_storage_snapshots
            (contract_id, network, storage_key, key_json, value_json, ledger_sequence)
        SELECT * FROM UNNEST(
            $1::text[], $2::text[], $3::text[], $4::jsonb[], $5::jsonb[], $6::bigint[]
        )
        ON CONFLICT (contract_id, network, storage_key, ledger_sequence) DO NOTHING
        "#,
    )
    .bind(&contract_ids)
    .bind(&networks)
    .bind(&storage_keys)
    .bind(&key_jsons)
    .bind(&value_jsons)
    .bind(&ledger_sequences)
    .execute(executor)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("insert_storage_snapshots_batch"))
    })?;

    Ok(())
}

/// Persist one RPC page — events, outbox rows, cursor, and ledger metadata — in
/// a single transaction (issues #199, #200).
///
/// Events are chunked to `batch_size` so a very large page cannot produce an
/// unbounded statement, but every chunk shares the one transaction: either the
/// whole page and its cursor advance land, or none of it does.
pub async fn commit_page(pool: &PgPool, commit: PageCommit<'_>) -> Result<(), TridentError> {
    let batch_size = commit.batch_size.max(1);

    let mut tx = pool
        .begin()
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("commit_page begin")))?;

    for chunk in commit.events.chunks(batch_size) {
        insert_events_batch(&mut *tx, commit.network, chunk).await?;
        // Outbox rows ride the same transaction as the events they deliver
        // (issue #200): either both land or neither does, so a committed event
        // can never exist without a delivery record for the relay to pick up.
        insert_outbox_batch(&mut *tx, chunk).await?;
    }

    // token_events.event_id logically references soroban_events(id) (the DB-level
    // FK was dropped in migration 0017 — soroban_events is partitioned, so a
    // single-column UNIQUE (id) can't be enforced globally). Referential
    // integrity is instead upheld here: projection rows must follow the event
    // insert inside the same transaction, so a token_events row can never exist
    // without its corresponding soroban_events row already committed.
    for chunk in commit.token_events.chunks(batch_size) {
        insert_token_events_batch(&mut *tx, commit.network, chunk).await?;
    }

    // Invocation metrics ride the same transaction as the page they were
    // derived from (issue #266), same idempotency contract as the rest.
    for chunk in commit.invocation_metrics.chunks(batch_size) {
        insert_invocation_metrics_batch(&mut *tx, commit.network, chunk).await?;
    }

    // Storage snapshot changes ride the same transaction as the page that
    // observed them (issue #270).
    for chunk in commit.storage_snapshots.chunks(batch_size) {
        insert_storage_snapshots_batch(&mut *tx, commit.network, chunk).await?;
    }

    if let Some(cursor) = commit.cursor {
        // Monotonic cursor advance (issue #418). Two indexer replicas — by
        // deliberate scale-out or an accidental double deploy mid-rollout —
        // can commit overlapping pages concurrently. An unconditional
        // `SET value = $1` lets the slower writer's older cursor land last and
        // rewind the cursor behind data that is already committed, so the
        // replicas re-poll a range they have both already indexed.
        //
        // The `WHERE ... < $1` guard makes the advance a no-op when the stored
        // cursor is already at or ahead of this page: the row is only written
        // when it genuinely moves forward. The comparison is numeric, not the
        // lexicographic ordering the TEXT column would otherwise give ("9" >
        // "10"), so it stays correct across digit-count boundaries.
        //
        // `UPDATE` takes a row lock, so concurrent advances on this single row
        // serialise: the second transaction blocks until the first commits,
        // then re-evaluates the guard against the committed value rather than
        // the snapshot it started with.
        sqlx::query(
            r#"
            INSERT INTO system_state (key, value)
            VALUES ($1, $2)
            ON CONFLICT (key) DO UPDATE
            SET value = $2, updated_at = NOW()
            WHERE system_state.value ~ '^[0-9]+$' IS NOT TRUE OR system_state.value::numeric < $3
            "#,
        )
        .bind(cursor_key(commit.network))
        .bind(cursor.to_string())
        .bind(cursor as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("commit_page set_cursor"))
        })?;
    }

    if let Some(ledger) = commit.ledger {
        let ts: DateTime<Utc> = ledger.timestamp.parse().map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("ledger timestamp parse"))
        })?;

        // Record every ledger this page traversed, not just its last one
        // (issue #723). A getEvents page advances ledger-by-ledger within
        // the span [first_ledger ..= last_ledger] — ledgers without
        // matching events included — so a row only for `ledger.sequence`
        // (the page's last ledger) left every ledger between two
        // consecutive pages' last ledgers absent from `ledger_metadata`.
        // The gap scanners read that table, so every inter-page span was
        // falsely reported as a hole and enqueued for backfill.
        //
        // The span rows are placeholders (empty hash, timestamp of the
        // page's closing ledger, event_count 0); the stretch between two
        // pages with no events for this indexer's filter is genuinely
        // traversed-and-empty, not unprocessed. A real hole — a ledger the
        // poll loop never saw — still has no row at all and is still
        // reported exactly by `scan_ledger_gaps` / `detect_ledger_gaps`.
        //
        // generate_series is server-side, so the cost is one statement
        // per page instead of one per ledger, and the whole span lands
        // in the same transaction as the cursor advance (#199/#200).
        // `ON CONFLICT DO NOTHING` keeps replays, per-event fallback
        // commits, and a backfill worker re-committing an overlapping
        // range from clobbering a real row with a placeholder (or vice
        // versa): first writer wins, matching the pre-#723 semantics.
        // The page's last ledger always ends the span, and the real hash /
        // event_count always attach to it; `covered_from_ledger` above it
        // (never possible with a monotonic cursor, but public API) clamps
        // to a single-row span rather than an inverted `generate_series`.
        let span_start: i64 = commit
            .covered_from_ledger
            .unwrap_or(ledger.sequence)
            .min(ledger.sequence) as i64;
        let span_end: i64 = ledger.sequence as i64;

        sqlx::query(
            r#"
            INSERT INTO ledger_metadata (ledger_sequence, ledger_hash, ledger_timestamp, event_count)
            SELECT seq,
                CASE WHEN seq = $4::bigint THEN $2::text ELSE '' END,
                $3::timestamptz,
                CASE WHEN seq = $4::bigint THEN $5::int ELSE 0 END
            FROM generate_series($1::bigint, $4::bigint) AS seq
            ON CONFLICT (ledger_sequence) DO NOTHING
            "#,
        )
        .bind(span_start)
        .bind(ledger.hash)
        .bind(ts)
        .bind(span_end)
        .bind(ledger.event_count)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("commit_page ledger_metadata"))
        })?;
    }

    tx.commit()
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("commit_page commit")))?;

    Ok(())
}

/// A contiguous run of missing ledger sequences in `ledger_metadata`
/// (issue #216): every ledger in `[from_ledger, to_ledger]` (inclusive) is
/// absent, while the sequences immediately outside that range are present.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct LedgerGap {
    pub from_ledger: u64,
    pub to_ledger: u64,
}

/// Scan `ledger_metadata` for holes in the processed range (issue #216).
///
/// A transient skip — an aborted poll before the transactional commit_page
/// fix, or a bounded reorg rewind edge (#196) racing a crash — can leave a
/// sequence permanently missing with no code path that would ever notice.
/// This finds every contiguous run of missing sequences strictly between the
/// lowest and highest processed ledger (a hole before the first or after the
/// last processed ledger is not a gap — it is unprocessed history, which is
/// what the normal poll loop / an initial backfill handles, not this scan).
///
/// `max_gaps` bounds how many gap rows a single call returns, so a
/// pathologically gappy table cannot make one scan unboundedly expensive —
/// the caller (a periodic, rate-limited task) picks up whatever it missed on
/// the next run.
pub async fn scan_ledger_gaps(
    pool: &PgPool,
    max_gaps: i64,
) -> Result<Vec<LedgerGap>, TridentError> {
    let rows: Vec<(i64, i64)> = sqlx::query_as(
        r#"
        WITH ordered AS (
            SELECT
                ledger_sequence,
                LAG(ledger_sequence) OVER (ORDER BY ledger_sequence) AS prev_sequence
            FROM ledger_metadata
        )
        SELECT prev_sequence + 1 AS gap_start, ledger_sequence - 1 AS gap_end
        FROM ordered
        WHERE prev_sequence IS NOT NULL
          AND ledger_sequence - prev_sequence > 1
        ORDER BY gap_start
        LIMIT $1
        "#,
    )
    .bind(max_gaps)
    .fetch_all(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("scan_ledger_gaps")))?;

    Ok(rows
        .into_iter()
        .map(|(from_ledger, to_ledger)| LedgerGap {
            from_ledger: from_ledger as u64,
            to_ledger: to_ledger as u64,
        })
        .collect())
}

/// Enqueue a `backfill_jobs` row for one gap, so `crates/backfill
/// --from-queue` picks it up and re-fetches the missing range (issue #216).
///
/// Idempotent: `uq_backfill_jobs_pending_range` (migration 0032) makes this a
/// no-op when an identical pending/running job already exists, so a scan
/// that re-finds the same still-unrepaired gap on its next run does not pile
/// up duplicate jobs.
pub async fn enqueue_backfill_job(
    pool: &PgPool,
    gap: LedgerGap,
    network: &str,
) -> Result<(), TridentError> {
    sqlx::query(
        r#"
        INSERT INTO backfill_jobs (from_ledger, to_ledger, network)
        VALUES ($1, $2, $3)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(gap.from_ledger as i64)
    .bind(gap.to_ledger as i64)
    .bind(network)
    .execute(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("enqueue_backfill_job")))?;

    Ok(())
}

/// Mark `pending`/`running` `backfill_jobs` rows as `done` when their exact
/// range no longer appears among `current_gaps` (issue #216) — i.e. a scan
/// confirms the range has since been filled, whether by the backfill worker
/// or by the live poll loop catching back up over it. Returns the number of
/// rows closed, for `metrics::record_ledger_gaps_closed`.
///
/// Compares on the exact `(from_ledger, to_ledger)` pair rather than range
/// overlap: `scan_ledger_gaps` always reports the maximal contiguous run of
/// missing sequences, so a job whose range is only partially filled would
/// show up in `current_gaps` as a *different*, smaller range — not an exact
/// match — and is correctly left open rather than closed prematurely.
pub async fn close_filled_backfill_jobs(
    pool: &PgPool,
    network: &str,
    current_gaps: &[LedgerGap],
) -> Result<u64, TridentError> {
    if current_gaps.is_empty() {
        // Every open job's range is, by definition, no longer a gap.
        let result = sqlx::query(
            r#"
            UPDATE backfill_jobs
            SET status = 'done', completed_at = NOW()
            WHERE network = $1 AND status IN ('pending', 'running')
            "#,
        )
        .bind(network)
        .execute(pool)
        .await
        .map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("close_filled_backfill_jobs"))
        })?;
        return Ok(result.rows_affected());
    }

    let from_ledgers: Vec<i64> = current_gaps.iter().map(|g| g.from_ledger as i64).collect();
    let to_ledgers: Vec<i64> = current_gaps.iter().map(|g| g.to_ledger as i64).collect();

    let result = sqlx::query(
        r#"
        UPDATE backfill_jobs
        SET status = 'done', completed_at = NOW()
        WHERE network = $1
          AND status IN ('pending', 'running')
          AND NOT EXISTS (
              SELECT 1 FROM unnest($2::bigint[], $3::bigint[]) AS gap(from_ledger, to_ledger)
              WHERE gap.from_ledger = backfill_jobs.from_ledger
                AND gap.to_ledger = backfill_jobs.to_ledger
          )
        "#,
    )
    .bind(network)
    .bind(&from_ledgers)
    .bind(&to_ledgers)
    .execute(pool)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("close_filled_backfill_jobs"))
    })?;

    Ok(result.rows_affected())
}

/// The single global `'latest_ledger_cursor'` key made two indexers on
/// different networks against the same database fight over one cursor row
/// (issue #600): the monotonic advance guard in `commit_page` prevents the
/// lower-sequence network's cursor from ever moving once the other network's
/// cursor is ahead, silently stopping that network's indexing entirely.
/// Namespacing the key per network gives each network its own row under the
/// same `system_state` table and schema — migration 0033 copies the
/// pre-existing single row's value into the namespaced key for whichever
/// network the deployment was already running, so no position is lost.
fn cursor_key(network: &str) -> String {
    format!("latest_ledger_cursor:{network}")
}

/// Read the latest processed ledger cursor from system_state for `network`.
pub async fn get_cursor(pool: &PgPool, network: &str) -> Result<u64, TridentError> {
    let row: (String,) = sqlx::query_as("SELECT value FROM system_state WHERE key = $1")
        .bind(cursor_key(network))
        .fetch_one(pool)
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("get_cursor")))?;

    row.0
        .parse::<u64>()
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("cursor parse")))
}

/// Fetch recent ledger sequences and hashes from `ledger_metadata` ordered by sequence descending.
/// Used for reorg detection (issue #196).
///
/// Only rows that actually carry a hash are returned. Since issue #723,
/// `commit_page` records a placeholder row (empty hash) for every ledger a
/// page traversed, so the newest rows are usually placeholders; returning
/// them would make the reorg check compare the RPC's real hash against `""`
/// and report a reorg after every ordinary ingest. Backfill and page-ending
/// ledgers do carry a hash, so the check still compares the newest known
/// hashes — which is what a shallow reorg is detected against.
pub async fn get_recent_ledger_metadata(
    pool: &PgPool,
    limit: i64,
) -> Result<Vec<(u64, String)>, TridentError> {
    let rows: Vec<(i64, String)> = sqlx::query_as(
        r#"
        SELECT ledger_sequence, ledger_hash
        FROM ledger_metadata
        WHERE ledger_hash <> ''
        ORDER BY ledger_sequence DESC
        LIMIT $1
        "#,
    )
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("get_recent_ledger_metadata"))
    })?;

    Ok(rows
        .into_iter()
        .map(|(seq, hash)| (seq as u64, hash))
        .collect())
}

/// Atomically remove all indexed data from `from_sequence` onwards and rewind the cursor to `new_cursor` (issue #196).
pub async fn handle_reorg_rollback(
    pool: &PgPool,
    network: &str,
    from_sequence: u64,
    new_cursor: u64,
) -> Result<(), TridentError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("reorg begin tx")))?;

    let seq_i64 = from_sequence as i64;

    // Delete token projections first (foreign key / dependent rows)
    sqlx::query("DELETE FROM token_events WHERE ledger_sequence >= $1")
        .bind(seq_i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("reorg delete token_events"))
        })?;

    // event_outbox keys on event_id with no FK/cascade, so rows referencing
    // soon-to-be-deleted soroban_events rows must be cleaned up explicitly
    // or they orphan (issue #412 follow-up).
    sqlx::query(
        "DELETE FROM event_outbox WHERE event_id IN \
         (SELECT id FROM soroban_events WHERE ledger_sequence >= $1)",
    )
    .bind(seq_i64)
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("reorg delete event_outbox"))
    })?;

    // Delete invocation metrics
    sqlx::query("DELETE FROM contract_invocation_metrics WHERE ledger_sequence >= $1")
        .bind(seq_i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            TridentError::storage(
                anyhow::Error::new(e).context("reorg delete contract_invocation_metrics"),
            )
        })?;

    // Delete storage snapshots
    sqlx::query("DELETE FROM contract_storage_snapshots WHERE ledger_sequence >= $1")
        .bind(seq_i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            TridentError::storage(
                anyhow::Error::new(e).context("reorg delete contract_storage_snapshots"),
            )
        })?;

    // Delete primary soroban events
    sqlx::query("DELETE FROM soroban_events WHERE ledger_sequence >= $1")
        .bind(seq_i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("reorg delete soroban_events"))
        })?;

    // Delete ledger metadata
    sqlx::query("DELETE FROM ledger_metadata WHERE ledger_sequence >= $1")
        .bind(seq_i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("reorg delete ledger_metadata"))
        })?;

    // Rewind cursor in system_state
    sqlx::query("UPDATE system_state SET value = $1, updated_at = NOW() WHERE key = $2")
        .bind(new_cursor.to_string())
        .bind(cursor_key(network))
        .execute(&mut *tx)
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("reorg rewind cursor")))?;

    tx.commit()
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("reorg commit tx")))?;

    Ok(())
}

/// Check whether the given ledger hash matches the stored hash for `sequence`.
///
/// Returns `Ok(true)` if the hashes match (or no stored hash exists),
/// `Ok(false)` if a reorg is detected (hash mismatch), or an error on DB failure.
pub async fn check_ledger_reorg(
    pool: &PgPool,
    sequence: u64,
    new_hash: &str,
) -> Result<bool, TridentError> {
    if new_hash.is_empty() {
        return Ok(true); // no hash to compare
    }
    let row: Option<(String,)> =
        sqlx::query_as("SELECT ledger_hash FROM ledger_metadata WHERE ledger_sequence = $1")
            .bind(sequence as i64)
            .fetch_optional(pool)
            .await
            .map_err(|e| {
                TridentError::storage(anyhow::Error::new(e).context("check_ledger_reorg"))
            })?;

    match row {
        Some((stored_hash,)) if !stored_hash.is_empty() && stored_hash != new_hash => Ok(false),
        _ => Ok(true),
    }
}

/// Rewind the cursor to `target` and delete all ledger metadata and events
/// at sequences > `target`. Used to undo a reorg.
pub async fn rewind_cursor(pool: &PgPool, target: u64) -> Result<(), TridentError> {
    let mut tx = pool
        .begin()
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("rewind_cursor begin")))?;

    sqlx::query("DELETE FROM ledger_metadata WHERE ledger_sequence > $1")
        .bind(target as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("rewind_cursor delete_metadata"))
        })?;

    // soroban_events, not `events` — there is no table by that name, so the
    // original statement here aborted the transaction and left the reorg
    // unrepaired while the logs reported it as handled.
    //
    // token_events references soroban_events(id) ON DELETE CASCADE, so it is
    // cleaned up by this delete. event_outbox keys on event_id with no FK, so
    // orphaned rows are removed explicitly below. The remaining ledger-keyed
    // tables have neither an FK nor a cascade and are deleted by sequence.
    sqlx::query(
        "DELETE FROM event_outbox WHERE event_id IN \
         (SELECT id FROM soroban_events WHERE ledger_sequence > $1)",
    )
    .bind(target as i64)
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("rewind_cursor delete_outbox"))
    })?;

    sqlx::query("DELETE FROM soroban_events WHERE ledger_sequence > $1")
        .bind(target as i64)
        .execute(&mut *tx)
        .await
        .map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("rewind_cursor delete_events"))
        })?;

    for table in [
        "contract_invocation_metrics",
        "contract_storage_snapshots",
        "parse_errors",
    ] {
        sqlx::query(&format!("DELETE FROM {table} WHERE ledger_sequence > $1"))
            .bind(target as i64)
            .execute(&mut *tx)
            .await
            .map_err(|e| {
                TridentError::storage(
                    anyhow::Error::new(e).context(format!("rewind_cursor delete_{table}")),
                )
            })?;
    }

    sqlx::query(
        "UPDATE system_state SET value = $1, updated_at = NOW() WHERE key = 'latest_ledger_cursor'",
    )
    .bind(target.to_string())
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("rewind_cursor set_cursor"))
    })?;

    tx.commit().await.map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("rewind_cursor commit"))
    })?;

    Ok(())
}

/// Detect gaps (missing sequences) in the processed ledger range.
///
/// Returns a list of missing sequence numbers. The scan is bounded to
/// sequences between `from` (inclusive) and `to` (inclusive).
pub async fn detect_ledger_gaps(
    pool: &PgPool,
    from: u64,
    to: u64,
) -> Result<Vec<u64>, TridentError> {
    if from >= to {
        return Ok(vec![]);
    }
    let rows: Vec<(i64,)> = sqlx::query_as(
        r#"
        SELECT generate_series($1::bigint, $2::bigint) AS seq
        EXCEPT
        SELECT ledger_sequence FROM ledger_metadata
        WHERE ledger_sequence >= $1 AND ledger_sequence <= $2
        ORDER BY seq
        "#,
    )
    .bind(from as i64)
    .bind(to as i64)
    .fetch_all(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("detect_ledger_gaps")))?;

    Ok(rows.into_iter().map(|(s,)| s as u64).collect())
}

/// Write indexer health metrics into the `system_state` health columns after
/// every successful poll cycle (issue #62).
///
/// Uses a single `UPDATE` on the known cursor row so there is never a
/// duplicate-key issue and the write is O(1) regardless of table size.
pub async fn update_health_stats(
    pool: &PgPool,
    network: &str,
    last_ledger: i64,
    events_in_poll: i32,
    poll_duration: Duration,
) -> Result<(), TridentError> {
    let poll_ms = poll_duration.as_millis().min(i32::MAX as u128) as i32;

    sqlx::query(
        r#"
        UPDATE system_state
        SET
            last_poll_at          = NOW(),
            last_ledger_indexed   = $1,
            events_in_last_poll   = $2,
            poll_duration_ms      = $3,
            events_indexed_total  = COALESCE(events_indexed_total, 0) + $2,
            updated_at            = NOW()
        WHERE key = $4
        "#,
    )
    .bind(last_ledger)
    .bind(events_in_poll)
    .bind(poll_ms)
    .bind(cursor_key(network))
    .execute(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("update_health_stats")))?;

    Ok(())
}

/// Load all contract IDs and their `index_from` values from
/// `indexed_contracts` for the given network (or network-agnostic rows where
/// `network IS NULL`).
///
/// Returns an empty map if the table has no rows — the caller treats an empty
/// map as "index all contracts" (issue #47).
///
/// `index_from` (BIGINT, default 0, added in 0001_init.sql) is the first
/// ledger from which events for that contract should be indexed. Events below
/// it are skipped client-side, because `getEvents` carries a single
/// `startLedger` for the whole request and so cannot express a per-contract
/// boundary (issue #202).
pub async fn load_indexed_contracts(
    pool: &PgPool,
    network: &str,
) -> Result<HashMap<String, i64>, TridentError> {
    let rows: Vec<(String, i64)> = sqlx::query_as(
        "SELECT contract_id, index_from FROM indexed_contracts WHERE network = $1 OR network IS NULL",
    )
    .bind(network)
    .fetch_all(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("load_indexed_contracts")))?;

    Ok(rows.into_iter().collect())
}

/// Return the upper bound (exclusive) of the highest **named** range partition
/// on `soroban_events`, or `None` if the table has no named range partitions
/// (only the DEFAULT catch-all exists).
///
/// This is the ledger sequence at which the ingest frontier will overflow into
/// `soroban_events_default` — the silent data-loss path issue #525 guards
/// against. The value is read from `pg_class` / `pg_constraint` so it is
/// always authoritative even after `create_soroban_partition()` adds new ones.
///
/// The query selects the maximum `confreljoin` exclusion boundary, which
/// Postgres stores as the `FROM … TO (upper_bound)` value of every range
/// partition constraint on the parent table.
/// Returns the `[lower, upper)` bounds of every named `soroban_events`
/// partition, ascending.
///
/// A single MAX(upper_bound) is not sufficient: migration 0017 seeds
/// partitions for 0–6M and then 50M–60M, leaving a 44-million-ledger hole. A
/// max-only guard reports 60M and happily accepts a ledger at 20M, which then
/// falls through to `soroban_events_default` — precisely the silent overflow
/// this check exists to prevent (issue #525).
pub async fn named_partition_ranges(pool: &PgPool) -> Result<Vec<(i64, i64)>, TridentError> {
    // The DEFAULT partition has no FROM/TO clause, so both captures are NULL
    // and it is filtered out below; we only want explicitly-bounded partitions.
    // pg_get_expr(relpartbound) renders the bound as
    //   FOR VALUES FROM ('0') TO ('2000000')
    // Parsing that with a regex is brittle (quoting and spacing vary), so read
    // the bounds structurally from pg_class.relpartbound instead: the parse
    // tree exposes the datums directly and the DEFAULT partition has none.
    let rows: Vec<(i64, i64)> = sqlx::query_as(
        r#"
        SELECT (regexp_match(bound, 'FROM \(''?([0-9]+)''?\)'))[1]::bigint AS lower_bound,
               (regexp_match(bound, 'TO \(''?([0-9]+)''?\)'))[1]::bigint   AS upper_bound
        FROM (
            SELECT pg_catalog.pg_get_expr(child.relpartbound, child.oid) AS bound
            FROM   pg_catalog.pg_inherits inh
            JOIN   pg_catalog.pg_class    parent ON parent.oid = inh.inhparent
            JOIN   pg_catalog.pg_class    child  ON child.oid  = inh.inhrelid
            WHERE  parent.relname = 'soroban_events'
        ) b
        WHERE bound IS NOT NULL
          AND bound NOT LIKE '%DEFAULT%'
          AND (regexp_match(bound, 'FROM \(''?([0-9]+)''?\)'))[1] IS NOT NULL
          AND (regexp_match(bound, 'TO \(''?([0-9]+)''?\)'))[1] IS NOT NULL
        ORDER BY 1
        "#,
    )
    .fetch_all(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("named_partition_ranges")))?;

    Ok(rows)
}

/// Verify that none of the `ledger_sequences` in `batch` are destined for the
/// DEFAULT catch-all partition of `soroban_events` (issue #525).
///
/// A row lands in the DEFAULT partition when its `ledger_sequence` falls
/// outside every named range partition. At that point named partitions are
/// exhausted and data is silently accumulating in an unindexed, unretained
/// catch-all that the operator has no tooling to manage.
///
/// Returns `Ok(())` when every sequence in `batch` is covered by a named
/// partition. Returns `Err(TridentError::ConfigError)` — which the streamer
/// treats as `Severity::Fatal` — when any sequence would land in DEFAULT.
///
/// `last_upper` is the value returned by [`last_named_partition_upper_bound`];
/// the caller caches it per poll cycle so this is a pure, zero-round-trip
/// check.
/// Fails if any ledger in `batch` is not covered by a named partition.
///
/// Checks containment against every range rather than only the highest upper
/// bound: a ledger can fall into a *gap* between named partitions and still be
/// below the maximum, in which case it silently lands in
/// `soroban_events_default` (issue #525).
pub fn assert_no_default_partition_overflow(
    batch: &[i64],
    ranges: &[(i64, i64)],
) -> Result<(), TridentError> {
    let covered = |seq: i64| ranges.iter().any(|&(lo, hi)| seq >= lo && seq < hi);

    if let Some(&uncovered) = batch.iter().find(|&&seq| !covered(seq)) {
        let highest = ranges.iter().map(|&(_, hi)| hi).max().unwrap_or(0);
        let suggested_lo = uncovered - (uncovered % 2_000_000);
        return Err(TridentError::config(anyhow::anyhow!(
            "partition exhaustion: ledger_sequence {} is not covered by any named              soroban_events partition (highest known upper bound {}).              Events for this ledger would land in soroban_events_default.              Run `SELECT create_soroban_partition({}, {});` to create the              covering partition before resuming the indexer (issue #525).",
            uncovered,
            highest,
            suggested_lo,
            suggested_lo + 2_000_000,
        )));
    }
    Ok(())
}

/// Read the current row count of `soroban_events_default` (issue #605), via
/// the `soroban_events_default_partition_status` view (migration 0034).
///
/// Non-zero means `create_soroban_partition` will refuse to create any
/// partition whose range overlaps these rows — see
/// docs/db/default-partition-recovery.md for what to do about it. Returns
/// `Ok(None)` rather than an error on a database that hasn't run migration
/// 0034 yet (view does not exist): this is a monitoring signal, not a
/// prerequisite for indexing, so its own absence should not fail the poll
/// cycle it is checked from.
pub async fn default_partition_row_count(pool: &PgPool) -> Result<Option<i64>, TridentError> {
    let result: Result<(i64,), sqlx::Error> =
        sqlx::query_as("SELECT row_count FROM soroban_events_default_partition_status")
            .fetch_one(pool)
            .await;

    match result {
        Ok((count,)) => Ok(Some(count)),
        Err(sqlx::Error::Database(e)) if e.code().as_deref() == Some("42P01") => {
            // undefined_table: migration 0034 has not run on this database yet.
            tracing::debug!(
                "soroban_events_default_partition_status view not found (migration 0034 not applied); \
                 skipping DEFAULT-partition check this cycle"
            );
            Ok(None)
        }
        Err(e) => Err(TridentError::storage(
            anyhow::Error::new(e).context("default_partition_row_count"),
        )),
    }
}

/// Read alert state (last_alert_at, alert_fired) from system_state for
/// `network` (issue #75). Shares the per-network cursor row (issue #600) so
/// each network's lag-alert state is independent too.
pub async fn get_alert_state(
    pool: &PgPool,
    network: &str,
) -> Result<crate::alerting::AlertState, TridentError> {
    let row: (Option<chrono::DateTime<chrono::Utc>>, bool) =
        sqlx::query_as("SELECT last_alert_at, alert_fired FROM system_state WHERE key = $1")
            .bind(cursor_key(network))
            .fetch_one(pool)
            .await
            .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("get_alert_state")))?;

    Ok(crate::alerting::AlertState {
        last_alert_at: row.0,
        alert_fired: row.1,
        rpc_degraded_fired: false,
        rpc_degraded_last_alert_at: None,
        // Not persisted, same as rpc_degraded_* above (issue #605): re-derived
        // fresh from soroban_events_default_partition_status every poll cycle
        // by the caller, via default_partition_row_count, rather than round
        // tripping through system_state.
        default_partition_fired: false,
        default_partition_last_alert_at: None,
    })
}

/// Persist alert state back to system_state after an alerting evaluation for
/// `network` (issue #75, issue #600).
pub async fn set_alert_state(
    pool: &PgPool,
    network: &str,
    state: &crate::alerting::AlertState,
) -> Result<(), TridentError> {
    sqlx::query(
        r#"
        UPDATE system_state
        SET last_alert_at = $1,
            alert_fired   = $2,
            updated_at    = NOW()
        WHERE key = $3
        "#,
    )
    .bind(state.last_alert_at)
    .bind(state.alert_fired)
    .bind(cursor_key(network))
    .execute(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("set_alert_state")))?;

    Ok(())
}

/// Record a parse error to parse_errors table for auditing and potential replay.
pub async fn insert_parse_error(
    pool: &PgPool,
    network: &str,
    ledger_sequence: u64,
    event_index: u32,
    raw_payload: &str,
    error_message: &str,
) -> Result<(), TridentError> {
    sqlx::query(
        r#"
        INSERT INTO parse_errors
            (ledger_sequence, event_index, raw_payload, error_message, network)
        VALUES ($1, $2, $3, $4, $5)
        "#,
    )
    .bind(ledger_sequence as i64)
    .bind(event_index as i32)
    .bind(raw_payload)
    .bind(error_message)
    .bind(network)
    .execute(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("insert_parse_error")))?;

    Ok(())
}

/// Dead-letter a well-formed event that repeatedly failed to persist into
/// `soroban_events` (issue #208).
///
/// Unlike `insert_parse_error`, `event` here decoded successfully — the
/// failure is in the storage layer, not the XDR. The full event is stored as
/// JSONB so it can be inspected and replayed once the underlying cause (a
/// constraint violation, an outage that outlasted the retry budget, etc.) is
/// understood, without needing to re-fetch it from Stellar RPC.
///
/// Rows are tagged with `network` so indexers sharing one database keep
/// separate dead-letter queues. Keyed over PENDING rows by the event's natural
/// key within that network — the same (contract_id, ledger_sequence,
/// event_index) triple `event_uuid` and
/// migration 0025 make canonical (issue #508): a poison
/// event re-encountered across polls — an RPC redelivery, a backfill overlap
/// — updates its existing row (attempt count folded in, latest error kept)
/// instead of accumulating duplicates, so the pending row count equals the
/// number of distinct poisoned events, which is exactly what the backlog
/// gauge and its alert report. A row an operator already replayed is history
/// and does not block recording a fresh failure of the same event.
pub async fn insert_failed_event(
    pool: &PgPool,
    network: &str,
    event: &SorobanEvent,
    error_message: &str,
    attempts: u32,
) -> Result<(), TridentError> {
    let payload = serde_json::to_value(event).map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("failed_events serialise"))
    })?;

    sqlx::query(
        r#"
        INSERT INTO failed_events
            (ledger_sequence, contract_id, transaction_hash, event_index,
             event_payload, error_message, attempts, network)
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
        ON CONFLICT (network, contract_id, ledger_sequence, event_index) WHERE replayed_at IS NULL
        DO UPDATE SET
            attempts      = failed_events.attempts + EXCLUDED.attempts,
            error_message = EXCLUDED.error_message
        "#,
    )
    .bind(event.ledger_sequence as i64)
    .bind(&event.contract_id)
    .bind(&event.transaction_hash)
    .bind(event.event_index as i32)
    .bind(payload)
    .bind(error_message)
    .bind(attempts as i32)
    .bind(network)
    .execute(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("insert_failed_event")))?;

    Ok(())
}

/// One row from `failed_events`, as returned by [`list_pending_failed_events`].
pub struct FailedEventRow {
    pub id: Uuid,
    pub ledger_sequence: i64,
    pub contract_id: String,
    pub transaction_hash: String,
    pub error_message: String,
    pub attempts: i32,
    pub occurred_at: DateTime<Utc>,
}

/// List dead-lettered events still awaiting replay (`replayed_at IS NULL`),
/// for `network`, oldest first — the replay tool's `--list` output and the query the
/// dead-letter-queue runbook used to ask an operator to run by hand (issue
/// #574). Uses `idx_failed_events_pending`, the partial index migration 0028
/// built for exactly this query.
pub async fn list_pending_failed_events(
    pool: &PgPool,
    network: &str,
    limit: i64,
) -> Result<Vec<FailedEventRow>, TridentError> {
    let rows = sqlx::query_as::<_, (Uuid, i64, String, String, String, i32, DateTime<Utc>)>(
        r#"
        SELECT id, ledger_sequence, contract_id, transaction_hash, error_message, attempts, occurred_at
        FROM failed_events
        WHERE replayed_at IS NULL AND network = $1
        ORDER BY occurred_at
        LIMIT $2
        "#,
    )
    .bind(network)
    .bind(limit)
    .fetch_all(pool)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("list_pending_failed_events"))
    })?;

    Ok(rows
        .into_iter()
        .map(
            |(
                id,
                ledger_sequence,
                contract_id,
                transaction_hash,
                error_message,
                attempts,
                occurred_at,
            )| {
                FailedEventRow {
                    id,
                    ledger_sequence,
                    contract_id,
                    transaction_hash,
                    error_message,
                    attempts,
                    occurred_at,
                }
            },
        )
        .collect())
}

/// How replaying one `failed_events` row turned out.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplayOutcome {
    /// The event was inserted into `soroban_events` (or already existed there
    /// under its deterministic UUIDv5 id) and the row is now marked replayed.
    Replayed,
    /// No row with this id has `replayed_at IS NULL` — either the id does not
    /// exist, or it was already replayed. Idempotent callers (a retried
    /// `--all` run, a double-click on a replay button) see this instead of
    /// an error.
    AlreadyReplayedOrMissing,
}

/// Replay one dead-lettered event: re-run the same insert `commit_page`
/// would have done (`soroban_events` row plus its outbox row, so a replayed
/// event still reaches Redis subscribers via `redis_stream::relay`, not just
/// Postgres), then stamp `replayed_at` — all in one transaction, so a crash
/// mid-replay can never leave the event inserted but the row still showing
/// as pending, or vice versa (issue #574).
///
/// Idempotent: `insert_events_batch`'s `ON CONFLICT DO NOTHING` on the
/// deterministic UUIDv5 id (same derivation `event_uuid` uses at ingest time)
/// makes a second replay of the same row a no-op on `soroban_events`, and the
/// `WHERE replayed_at IS NULL` guard on the UPDATE below makes the second
/// replay a no-op on `failed_events` too — replaying twice cannot
/// double-insert or double-count.
///
/// `network` is both the replayed row's network tag and a scope on the lookup:
/// a row dead-lettered by another network's indexer is reported as missing
/// rather than being replayed into the wrong network's `soroban_events`
/// (issue #595).
pub async fn replay_failed_event(
    pool: &PgPool,
    id: Uuid,
    network: &str,
) -> Result<ReplayOutcome, TridentError> {
    let row: Option<(serde_json::Value,)> = sqlx::query_as(
        "SELECT event_payload FROM failed_events
         WHERE id = $1 AND network = $2 AND replayed_at IS NULL",
    )
    .bind(id)
    .bind(network)
    .fetch_optional(pool)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("replay_failed_event select"))
    })?;

    let Some((payload,)) = row else {
        return Ok(ReplayOutcome::AlreadyReplayedOrMissing);
    };

    let event: SorobanEvent = serde_json::from_value(payload).map_err(|e| {
        TridentError::storage(
            anyhow::Error::new(e).context("replay_failed_event deserialise event_payload"),
        )
    })?;

    let mut tx = pool.begin().await.map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("replay_failed_event begin"))
    })?;

    let events = std::slice::from_ref(&event);
    insert_events_batch(&mut *tx, network, events).await?;
    insert_outbox_batch(&mut *tx, events).await?;

    let updated = sqlx::query(
        "UPDATE failed_events SET replayed_at = NOW() WHERE id = $1 AND replayed_at IS NULL",
    )
    .bind(id)
    .execute(&mut *tx)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("replay_failed_event mark replayed"))
    })?;

    tx.commit().await.map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("replay_failed_event commit"))
    })?;

    if updated.rows_affected() == 0 {
        // Lost a race with a concurrent replay of the same id between the
        // SELECT above and this UPDATE. The insert it just performed is a
        // harmless no-op (ON CONFLICT DO NOTHING on the same deterministic
        // id the other replay used), so this is not an error — just report
        // it the same way as "already replayed".
        return Ok(ReplayOutcome::AlreadyReplayedOrMissing);
    }

    Ok(ReplayOutcome::Replayed)
}

/// Number of dead-lettered events still awaiting replay. Published as the
/// `trident_indexer_persist_dead_letter_backlog` gauge each active poll
/// cycle (and on every dead-letter write), so the non-empty-DLQ alert has a
/// live series to fire on — a silent queue is indistinguishable from data
/// loss (issue #508). Scoped to `network` so a shared database does not mix
/// one indexer's backlog into another's gauge. Counts only pending rows: a replayed row is resolved
/// history, not backlog.
pub async fn count_pending_failed_events(
    pool: &PgPool,
    network: &str,
) -> Result<i64, TridentError> {
    let row: (i64,) = sqlx::query_as(
        "SELECT COUNT(*) FROM failed_events WHERE replayed_at IS NULL AND network = $1",
    )
    .bind(network)
    .fetch_one(pool)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("count_pending_failed_events"))
    })?;
    Ok(row.0)
}

/// Contracts among `contract_ids` whose `token_metadata` row is still fresh
/// (resolved or refreshed since `cutoff`), for either a positive or a cached
/// negative ("not a token") result (issue #263). Contracts absent from this
/// set need a fresh resolution attempt.
pub async fn fresh_token_metadata_contract_ids(
    pool: &PgPool,
    contract_ids: &[String],
    network: &str,
    cutoff: DateTime<Utc>,
) -> Result<HashSet<String>, TridentError> {
    if contract_ids.is_empty() {
        return Ok(HashSet::new());
    }

    let rows: Vec<(String,)> = sqlx::query_as(
        "SELECT contract_id FROM token_metadata
         WHERE contract_id = ANY($1) AND network = $2 AND updated_at > $3",
    )
    .bind(contract_ids)
    .bind(network)
    .bind(cutoff)
    .fetch_all(pool)
    .await
    .map_err(|e| {
        TridentError::storage(anyhow::Error::new(e).context("fresh_token_metadata_contract_ids"))
    })?;

    Ok(rows.into_iter().map(|(id,)| id).collect())
}

/// Cache a resolved (or negative) token metadata result for one contract
/// (issue #263). Re-resolving an already-cached contract refreshes the row in
/// place rather than duplicating it.
pub async fn upsert_token_metadata(
    pool: &PgPool,
    contract_id: &str,
    network: &str,
    resolution: &crate::token_metadata::TokenMetadataResolution,
) -> Result<(), TridentError> {
    let (name, symbol, decimals, is_token) = match resolution {
        crate::token_metadata::TokenMetadataResolution::Token(meta) => (
            Some(meta.name.as_str()),
            Some(meta.symbol.as_str()),
            Some(meta.decimals as i32),
            true,
        ),
        crate::token_metadata::TokenMetadataResolution::NotAToken => (None, None, None, false),
    };

    sqlx::query(
        r#"
        INSERT INTO token_metadata (contract_id, network, name, symbol, decimals, is_token)
        VALUES ($1, $2, $3, $4, $5, $6)
        ON CONFLICT (contract_id, network) DO UPDATE SET
            name       = EXCLUDED.name,
            symbol     = EXCLUDED.symbol,
            decimals   = EXCLUDED.decimals,
            is_token   = EXCLUDED.is_token,
            updated_at = NOW()
        "#,
    )
    .bind(contract_id)
    .bind(network)
    .bind(name)
    .bind(symbol)
    .bind(decimals)
    .bind(is_token)
    .execute(pool)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("upsert_token_metadata")))?;

    Ok(())
}

/// Evict a contract's cached token metadata when its WASM code hash changes
/// (issue #685): `token_metadata` has no code-hash column of its own and no
/// other signal that the contract behind an address has been redeployed, so
/// a cached name/symbol/decimals for the old code could otherwise serve
/// indefinitely for the new one. Deleting the row rather than updating it
/// lets `resolve_stale_token_metadata`'s own freshness check
/// (`fresh_token_metadata_contract_ids`) treat it as absent and re-resolve
/// from scratch on its next sighting, the same path a never-before-seen
/// contract already takes.
pub async fn delete_token_metadata(
    pool: &PgPool,
    contract_id: &str,
    network: &str,
) -> Result<(), TridentError> {
    sqlx::query("DELETE FROM token_metadata WHERE contract_id = $1 AND network = $2")
        .bind(contract_id)
        .bind(network)
        .execute(pool)
        .await
        .map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("delete_token_metadata"))
        })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use trident_common::{EventType, SorobanEvent};

    fn make_event(contract_id: &str, ledger_sequence: u64, event_index: u32) -> SorobanEvent {
        SorobanEvent {
            contract_id: contract_id.to_string(),
            ledger_sequence,
            ledger_timestamp: "2024-01-01T00:00:00Z".to_string(),
            transaction_hash: "txhash_abc123".to_string(),
            event_index,
            raw_event_index: event_index,
            event_type: EventType::Contract,
            topics: vec![],
            data: json!({}),
        }
    }

    /// Exercises `classify_storage_failure` against `sqlx::Error` variants
    /// that don't require a database connection to construct. The
    /// SQLSTATE-string branches (connection/resource/timeout/serialization
    /// classes → transient, constraint/data-exception classes → permanent)
    /// are instead exercised end-to-end against a real Postgres error in
    /// `streamer::tests::transient_db_outage_retries_the_page_instead_of_dead_lettering_it`
    /// and the existing `poison_event_is_dead_lettered_and_page_still_advances_cursor`.
    #[test]
    fn connection_level_io_error_is_transient() {
        let io_err = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset");
        let err =
            TridentError::storage(anyhow::Error::new(sqlx::Error::Io(io_err)).context("test"));
        assert_eq!(classify_storage_failure(&err), StorageFailure::Transient);
    }

    #[test]
    fn pool_timeout_is_transient() {
        let err =
            TridentError::storage(anyhow::Error::new(sqlx::Error::PoolTimedOut).context("test"));
        assert_eq!(classify_storage_failure(&err), StorageFailure::Transient);
    }

    #[test]
    fn column_decode_error_is_permanent() {
        let decode_err = Box::<dyn std::error::Error + Send + Sync>::from("bad column");
        let err = TridentError::storage(
            anyhow::Error::new(sqlx::Error::ColumnDecode {
                index: "0".to_string(),
                source: decode_err,
            })
            .context("test"),
        );
        assert_eq!(classify_storage_failure(&err), StorageFailure::Permanent);
    }

    #[test]
    fn error_with_no_sqlx_source_is_permanent() {
        // e.g. the ledger-timestamp DateTime parse failure in commit_page:
        // a TridentError::StorageError whose source chain never touched
        // sqlx at all. Retrying an identical page cannot change a parse
        // outcome, so this must not be treated as retryable.
        let err = TridentError::storage(anyhow::anyhow!("not a valid timestamp"));
        assert_eq!(classify_storage_failure(&err), StorageFailure::Permanent);
    }

    #[test]
    fn non_storage_error_is_permanent() {
        let err = TridentError::config(anyhow::anyhow!("missing DATABASE_URL"));
        assert_eq!(classify_storage_failure(&err), StorageFailure::Permanent);
    }

    /// Deterministic UUID: same inputs must produce the same id.
    #[test]
    fn event_uuid_is_deterministic() {
        let a = event_uuid("txhash-aa", 0);
        let b = event_uuid("txhash-aa", 0);
        assert_eq!(a, b);
    }

    /// Different natural keys must produce different UUIDs.
    #[test]
    fn event_uuid_varies_with_inputs() {
        let a = event_uuid("txhash-aa", 0);
        let b = event_uuid("txhash-aa", 1);
        assert_ne!(a, b);
    }

    /// #599 regression: event_uuid must be keyed on raw_event_index, not on
    /// event_index. The same event re-indexed under a different batch (so
    /// assign_unique_event_indexes ties-breaks it to a different event_index)
    /// must still produce the same UUID, so ON CONFLICT DO NOTHING actually
    /// absorbs the replay instead of inserting a duplicate row.
    #[test]
    fn event_uuid_is_stable_across_different_tie_broken_event_indexes() {
        // Same transaction_hash and raw_event_index; only the batch-local,
        // tie-broken event_index differs (as it would across two runs whose
        // page boundaries land differently).
        let first_run = event_uuid("txhash-aa", 0);
        let second_run = event_uuid("txhash-aa", 0);
        assert_eq!(
            first_run, second_run,
            "event_uuid must depend only on (transaction_hash, raw_event_index)"
        );
    }

    /// Dead-lettering the same event twice must collapse to ONE pending row
    /// with the attempt counts folded together (issue #508) — the pending
    /// count is what the backlog gauge reports, and it must mean "distinct
    /// poisoned events", not "retry bursts".
    ///
    /// Uses the shared test database (TEST_DATABASE_URL) like the other
    /// integration tests; skips when it is not configured.
    #[tokio::test]
    async fn failed_event_redelivery_updates_one_pending_row() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        // A unique contract per run, like the other failed_events test: the
        // shared fixture reuses one transaction hash everywhere, so scoping
        // by contract keeps parallel tests out of each other's rows.
        let contract_id = format!("CDLQ_{}", Uuid::new_v4());
        let event = make_event(&contract_id, 43, 0);
        sqlx::query("DELETE FROM failed_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .expect("cleanup failed");

        insert_failed_event(&pool, "testnet", &event, "first failure", 4)
            .await
            .expect("first dead-letter failed");
        insert_failed_event(&pool, "testnet", &event, "second failure", 4)
            .await
            .expect("redelivered dead-letter must not error");

        let (rows, attempts, error): (i64, i32, String) = sqlx::query_as(
            "SELECT COUNT(*) OVER (), attempts, error_message FROM failed_events              WHERE contract_id = $1 AND replayed_at IS NULL",
        )
        .bind(&contract_id)
        .fetch_one(&pool)
        .await
        .expect("row query failed");
        assert_eq!(rows, 1, "redelivery must update, not duplicate");
        assert_eq!(attempts, 8, "attempt counts fold together");
        assert_eq!(error, "second failure", "latest error wins");

        // A REPLAYED row is history: the same event failing again gets a
        // fresh pending row rather than resurrecting the resolved one.
        sqlx::query("UPDATE failed_events SET replayed_at = NOW() WHERE transaction_hash = $1")
            .bind(&event.transaction_hash)
            .execute(&pool)
            .await
            .expect("mark replayed");
        insert_failed_event(&pool, "testnet", &event, "regression after replay", 1)
            .await
            .expect("post-replay dead-letter failed");
        let pending: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM failed_events              WHERE contract_id = $1 AND replayed_at IS NULL",
        )
        .bind(&contract_id)
        .fetch_one(&pool)
        .await
        .expect("pending count failed");
        assert_eq!(
            pending.0, 1,
            "a replayed row must not block a fresh failure"
        );
    }

    /// Committing the same event twice must not error and the row count in
    /// `soroban_events` must remain 1.
    ///
    /// Uses the shared test database (TEST_DATABASE_URL) like the other
    /// integration tests; skips when it is not configured.
    #[tokio::test]
    async fn batch_insert_is_idempotent() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            // Hard-fail under the rust-integration CI job (REQUIRE_TEST_SERVICES)
            // so a misconfigured DB URL cannot silently skip and go green.
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let event = make_event("CABC_CONTRACT_001", 42, 0);

        // Isolate from other tests sharing the database.
        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&event.contract_id)
            .execute(&pool)
            .await
            .expect("cleanup failed");

        let events = [event.clone()];
        insert_events_batch(&pool, "testnet", &events)
            .await
            .expect("first insert failed");
        insert_events_batch(&pool, "testnet", &events)
            .await
            .expect("second insert must not error");

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1")
                .bind(&event.contract_id)
                .fetch_one(&pool)
                .await
                .expect("count query failed");

        assert_eq!(count.0, 1, "duplicate insert should be silently ignored");
    }

    /// #599 regression: the same protocol event indexed twice under
    /// different batch compositions (so `event_index` ties-break to
    /// different values each time — simulated here directly, since
    /// `assign_unique_event_indexes` lives in `crates::parser`) must still
    /// produce the same `event_uuid` and collapse to one row via
    /// `ON CONFLICT DO NOTHING`. Before the fix, `event_uuid` was derived
    /// from the batch-local `event_index`, so this scenario inserted a
    /// duplicate row instead.
    #[tokio::test]
    async fn reindexing_the_same_page_under_different_batch_boundaries_inserts_one_row() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        // A transaction_hash unique to this test run: make_event's fixture
        // hash ("txhash_abc123") is shared across many other tests in this
        // module, and event_uuid is deliberately keyed on
        // (transaction_hash, raw_event_index) alone (issue #599) - reusing
        // the shared fixture hash here would collide with an unrelated
        // test's row using the same raw_event_index.
        let tx_hash = format!("txhash_599_{}", Uuid::new_v4());
        let contract_id = "CABC_CONTRACT_599";

        // Same underlying event (same transaction_hash, same raw_event_index
        // of 0), but as it would appear tie-broken to event_index = 0 in one
        // batch's page boundary...
        let mut first_run_event = make_event(contract_id, 42, 0);
        first_run_event.transaction_hash = tx_hash.clone();
        first_run_event.raw_event_index = 0;

        // ...and tie-broken to event_index = 1 in a different run whose page
        // boundary grouped it alongside a different, earlier event on the
        // same (ledger, tx_hash) pair. raw_event_index is unaffected by
        // tie-breaking, so it stays 0 in both runs.
        let mut second_run_event = make_event(contract_id, 42, 1);
        second_run_event.transaction_hash = tx_hash.clone();
        second_run_event.raw_event_index = 0;

        insert_events_batch(&pool, "testnet", &[first_run_event])
            .await
            .expect("first run insert failed");
        insert_events_batch(&pool, "testnet", &[second_run_event])
            .await
            .expect("second run insert must not error");

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE transaction_hash = $1")
                .bind(&tx_hash)
                .fetch_one(&pool)
                .await
                .expect("count query failed");

        assert_eq!(
            count.0, 1,
            "the same event re-indexed under a different batch boundary must not duplicate"
        );

        sqlx::query("DELETE FROM soroban_events WHERE transaction_hash = $1")
            .bind(&tx_hash)
            .execute(&pool)
            .await
            .expect("cleanup failed");
    }

    /// #599: the `raw_event_index = 0` fallback (stellar-rpc#382 — neither
    /// `operationIndex` nor a parseable `id` suffix) must still be stable
    /// across batches, exactly like any other index value.
    #[tokio::test]
    async fn raw_event_index_zero_fallback_is_stable_across_batches() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = "CABC_CONTRACT_599_FALLBACK";
        // Unique transaction hashes for this test run — see the comment in
        // reindexing_the_same_page_under_different_batch_boundaries_inserts_one_row
        // on why the shared make_event fixture hash can't be reused here.
        let tx_hash_a = format!("txhash_599fb_a_{}", Uuid::new_v4());
        let tx_hash_b = format!("txhash_599fb_b_{}", Uuid::new_v4());

        // Every event in the batch fell back to raw_event_index = 0 (the
        // stellar-rpc#382 condition), forcing assign_unique_event_indexes to
        // tie-break the whole batch — event_index ends up 0, 1, 2, ... but
        // raw_event_index stays 0 for every one of them.
        let mut event_a = make_event(contract_id, 100, 0);
        event_a.transaction_hash = tx_hash_a.clone();
        event_a.raw_event_index = 0;
        let mut event_b = make_event(contract_id, 100, 1);
        event_b.transaction_hash = tx_hash_b.clone();
        event_b.raw_event_index = 0;

        insert_events_batch(&pool, "testnet", &[event_a.clone(), event_b.clone()])
            .await
            .expect("insert failed");

        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM soroban_events WHERE transaction_hash IN ($1, $2)",
        )
        .bind(&tx_hash_a)
        .bind(&tx_hash_b)
        .fetch_one(&pool)
        .await
        .expect("count query failed");

        // Different transaction_hash values, so despite sharing
        // raw_event_index = 0, these are two distinct events and both must
        // be kept — this fallback path must not silently collapse unrelated
        // events into each other either.
        assert_eq!(
            count.0, 2,
            "two distinct events sharing the raw_event_index=0 fallback must both be inserted"
        );

        // Re-run the exact same insert (simulating a re-index of the same
        // page): must still collapse to the same two rows, not four.
        insert_events_batch(&pool, "testnet", &[event_a, event_b])
            .await
            .expect("re-insert must not error");

        let count_after_replay: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM soroban_events WHERE transaction_hash IN ($1, $2)",
        )
        .bind(&tx_hash_a)
        .bind(&tx_hash_b)
        .fetch_one(&pool)
        .await
        .expect("count query failed");
        assert_eq!(
            count_after_replay.0, 2,
            "replay must not duplicate either row"
        );

        sqlx::query("DELETE FROM soroban_events WHERE transaction_hash IN ($1, $2)")
            .bind(&tx_hash_a)
            .bind(&tx_hash_b)
            .execute(&pool)
            .await
            .expect("cleanup failed");
    }

    /// The `chk_soroban_events_network` CHECK constraint (migration 0031,
    /// issue #252) must reject a row whose network is neither 'mainnet' nor
    /// 'testnet' — the failure mode this exists to close is a typo like
    /// 'tesnet' silently creating an invisible data partition.
    #[tokio::test]
    async fn soroban_events_rejects_unknown_network() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CNETCHK_{}", Uuid::new_v4());
        let result = sqlx::query(
            "INSERT INTO soroban_events \
             (contract_id, ledger_sequence, ledger_timestamp, transaction_hash, \
              event_index, event_type, network) \
             VALUES ($1, 1, NOW(), 'txhash_netchk', 0, 'contract', 'tesnet')",
        )
        .bind(&contract_id)
        .execute(&pool)
        .await;

        assert!(
            result.is_err(),
            "insert with network='tesnet' must violate chk_soroban_events_network"
        );

        // Belt and braces: confirm nothing landed despite the driver error.
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&pool)
                .await
                .expect("count query failed");
        assert_eq!(count.0, 0);
    }

    /// A known network value must still be accepted (the constraint is not
    /// over-broad).
    #[tokio::test]
    async fn soroban_events_accepts_known_network() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CNETOK_{}", Uuid::new_v4());
        sqlx::query(
            "INSERT INTO soroban_events \
             (contract_id, ledger_sequence, ledger_timestamp, transaction_hash, \
              event_index, event_type, network) \
             VALUES ($1, 1, NOW(), 'txhash_netok', 0, 'contract', 'testnet')",
        )
        .bind(&contract_id)
        .execute(&pool)
        .await
        .expect("insert with a known network must succeed");

        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .expect("cleanup failed");
    }

    /// An empty batch must be a no-op rather than an invalid statement.
    #[tokio::test]
    async fn empty_batch_is_a_noop() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();
        insert_events_batch(&pool, "testnet", &[])
            .await
            .expect("empty batch must succeed");
    }

    /// `insert_events_batch` must stamp every row with the caller's actual
    /// network, not rely on the `soroban_events.network` column's
    /// `DEFAULT 'testnet'` (issue #595) — a mainnet deployment silently wrote
    /// every row as `'testnet'` before this was bound.
    #[tokio::test]
    async fn insert_events_batch_tags_rows_with_the_given_network() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let event = make_event("CNETTAG_MAINNET", 77, 0);
        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&event.contract_id)
            .execute(&pool)
            .await
            .expect("cleanup failed");

        insert_events_batch(&pool, "mainnet", &[event.clone()])
            .await
            .expect("insert failed");

        let row: (String,) =
            sqlx::query_as("SELECT network FROM soroban_events WHERE contract_id = $1")
                .bind(&event.contract_id)
                .fetch_one(&pool)
                .await
                .expect("select failed");
        assert_eq!(row.0, "mainnet");

        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&event.contract_id)
            .execute(&pool)
            .await
            .expect("cleanup failed");
    }

    /// The natural-key constraint (migration 0025) is network-scoped: the same
    /// `(transaction_hash, event_index)` must be able to persist once per
    /// network without one insert silently absorbing the other via the
    /// untargeted `ON CONFLICT DO NOTHING` (issue #595).
    #[tokio::test]
    async fn insert_events_batch_persists_same_tx_index_on_different_networks() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        // Different contract_id so the two rows land under different
        // deterministic ids (`event_uuid` hashes contract_id in) and never
        // collide on the partitioned primary key; same transaction_hash and
        // event_index is what actually exercises the network-scoped natural
        // key below.
        let mainnet_event = make_event("CNETDUAL_MAINNET", 88, 1);
        let testnet_event = make_event("CNETDUAL_TESTNET", 88, 1);
        assert_eq!(
            mainnet_event.transaction_hash,
            testnet_event.transaction_hash
        );
        assert_eq!(mainnet_event.event_index, testnet_event.event_index);

        sqlx::query("DELETE FROM soroban_events WHERE contract_id = ANY($1)")
            .bind(
                &[
                    mainnet_event.contract_id.clone(),
                    testnet_event.contract_id.clone(),
                ][..],
            )
            .execute(&pool)
            .await
            .expect("cleanup failed");

        insert_events_batch(&pool, "mainnet", &[mainnet_event.clone()])
            .await
            .expect("mainnet insert failed");
        insert_events_batch(&pool, "testnet", &[testnet_event.clone()])
            .await
            .expect("testnet insert failed");

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE contract_id = ANY($1)")
                .bind(
                    &[
                        mainnet_event.contract_id.clone(),
                        testnet_event.contract_id.clone(),
                    ][..],
                )
                .fetch_one(&pool)
                .await
                .expect("count query failed");
        assert_eq!(
            count.0, 2,
            "the same (transaction_hash, event_index) must persist once per network"
        );

        sqlx::query("DELETE FROM soroban_events WHERE contract_id = ANY($1)")
            .bind(&[mainnet_event.contract_id, testnet_event.contract_id][..])
            .execute(&pool)
            .await
            .expect("cleanup failed");
    }

    /// `insert_invocation_metrics_batch` is the sibling omission called out in
    /// issue #595: it must stamp its rows with the caller's actual network
    /// too, not fall back to `contract_invocation_metrics.network`'s
    /// `DEFAULT 'testnet'`.
    #[tokio::test]
    async fn insert_invocation_metrics_batch_tags_rows_with_the_given_network() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CINVMETRICNET_{}", Uuid::new_v4());
        let metrics = crate::parser::invocation_metrics::InvocationMetrics {
            fee_charged: 100,
            resource_fee: Some(50),
            cpu_instructions: Some(1000),
            read_bytes: Some(10),
            write_bytes: Some(5),
            provenance: "declared_resources",
        };
        let row = InvocationMetricRow {
            contract_id: &contract_id,
            transaction_hash: "txhash_invmetric",
            ledger_sequence: 1,
            ledger_timestamp: "2024-01-01T00:00:00Z",
            metrics: &metrics,
        };

        insert_invocation_metrics_batch(&pool, "mainnet", &[row])
            .await
            .expect("insert failed");

        let result: (String,) = sqlx::query_as(
            "SELECT network FROM contract_invocation_metrics WHERE contract_id = $1",
        )
        .bind(&contract_id)
        .fetch_one(&pool)
        .await
        .expect("select failed");
        assert_eq!(result.0, "mainnet");

        sqlx::query("DELETE FROM contract_invocation_metrics WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .expect("cleanup failed");
    }

    /// `insert_token_events_batch` is the omission issue #595 missed: it must
    /// stamp its rows with the caller's actual network too, not fall back to
    /// `token_events.network`'s `DEFAULT 'testnet'` — previously every mainnet
    /// token event was silently tagged `testnet`.
    #[tokio::test]
    async fn insert_token_events_batch_tags_rows_with_the_given_network() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let event = make_event("CTOKENNETTAG_MAINNET", 99, 2);
        let token = crate::parser::token_events::TokenEvent {
            event_type: crate::parser::token_events::TokenEventType::Transfer,
            from: Some("GFROM".to_string()),
            to: Some("GTO".to_string()),
            spender: None,
            admin: None,
            amount: Some("100".to_string()),
            expiration_ledger: None,
            asset_code: None,
            asset_issuer: None,
        };
        let projection = TokenProjection {
            event: &event,
            token: &token,
        };

        sqlx::query("DELETE FROM token_events WHERE contract_id = $1")
            .bind(&event.contract_id)
            .execute(&pool)
            .await
            .expect("cleanup failed");

        insert_token_events_batch(&pool, "mainnet", &[projection])
            .await
            .expect("insert failed");

        let row: (String,) =
            sqlx::query_as("SELECT network FROM token_events WHERE contract_id = $1")
                .bind(&event.contract_id)
                .fetch_one(&pool)
                .await
                .expect("select failed");
        assert_eq!(row.0, "mainnet");

        sqlx::query("DELETE FROM token_events WHERE contract_id = $1")
            .bind(&event.contract_id)
            .execute(&pool)
            .await
            .expect("cleanup failed");
    }

    /// A page larger than `batch_size` must still land in full — chunking splits
    /// the statement, not the transaction — and a replay must insert nothing new.
    #[tokio::test]
    async fn commit_page_chunks_large_pages_and_advances_cursor_once() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CBATCH_{}", Uuid::new_v4());
        let events: Vec<SorobanEvent> = (0..25).map(|i| make_event(&contract_id, 900, i)).collect();

        fn commit(events: &[SorobanEvent]) -> PageCommit<'_> {
            PageCommit {
                events,
                token_events: &[],
                invocation_metrics: &[],
                storage_snapshots: &[],
                network: "testnet",
                cursor: Some(900),
                covered_from_ledger: None,
                ledger: Some(LedgerMeta {
                    sequence: 900,
                    hash: "hash900",
                    timestamp: "2024-01-01T00:00:00Z",
                    event_count: events.len() as i32,
                }),
                // Deliberately smaller than the page so chunking is exercised.
                batch_size: 10,
            }
        }

        commit_page(&pool, commit(&events))
            .await
            .expect("commit_page failed");

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count.0, 25, "every chunk of the page must land");
        assert_eq!(get_cursor(&pool, "testnet").await.unwrap(), 900);

        commit_page(&pool, commit(&events))
            .await
            .expect("replay failed");

        let recount: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(recount.0, 25, "replaying a page must insert nothing new");

        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// scan_ledger_gaps must find every contiguous hole strictly between the
    /// lowest and highest processed ledger, and must not report anything
    /// before the first or after the last processed sequence (issue #216).
    #[tokio::test]
    async fn scan_ledger_gaps_finds_holes_but_not_the_edges() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        // Distinct range for this test: 7_500_000..7_500_020 with two holes,
        // one 1-ledger and one 3-ledger.
        let base = 7_500_000u64;
        // Present: base, base+1 | gap: base+2 | present: base+3..=base+5 |
        // gap: base+6..=base+8 | present: base+9.
        let present: &[u64] = &[base, base + 1, base + 3, base + 4, base + 5, base + 9];
        for &seq in present {
            sqlx::query(
                "INSERT INTO ledger_metadata (ledger_sequence, ledger_hash, ledger_timestamp) \
                 VALUES ($1, $2, NOW()) ON CONFLICT (ledger_sequence) DO NOTHING",
            )
            .bind(seq as i64)
            .bind(format!("hash-{seq}"))
            .execute(&pool)
            .await
            .unwrap();
        }

        let gaps = scan_ledger_gaps(&pool, 100).await.unwrap();
        let found: Vec<LedgerGap> = gaps
            .into_iter()
            .filter(|g| g.from_ledger >= base && g.to_ledger <= base + 9)
            .collect();

        assert_eq!(
            found,
            vec![
                LedgerGap {
                    from_ledger: base + 2,
                    to_ledger: base + 2
                },
                LedgerGap {
                    from_ledger: base + 6,
                    to_ledger: base + 8
                },
            ],
            "must find exactly the two holes, ordered by start, and nothing at the edges"
        );

        sqlx::query("DELETE FROM ledger_metadata WHERE ledger_sequence BETWEEN $1 AND $2")
            .bind(base as i64)
            .bind((base + 9) as i64)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// Issue #723: `commit_page` must record every ledger a page traversed,
    /// not just the page's last one, so a normal multi-page ingest leaves the
    /// gap scanners with nothing to report.
    #[tokio::test]
    async fn commit_page_span_leaves_no_gaps_for_a_multi_page_ingest() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        // Two pages of a normal ingest: [base..=base+4] then
        // [base+5..=base+9]. Before #723 only base+4 and base+9 got a row,
        // so base..=base+3 and base+5..=base+8 read as holes.
        let base = 8_300_000u64;
        // A job the old per-page writer falsely enqueued for an interior span
        // of this ingest. It must be closed once the scan confirms the range is
        // covered — "close the false jobs already enqueued" from #723.
        let job_network = format!("span-test-{}", Uuid::new_v4());
        enqueue_backfill_job(
            &pool,
            LedgerGap {
                from_ledger: base + 5,
                to_ledger: base + 8,
            },
            &job_network,
        )
        .await
        .unwrap();

        for (from, to, hash, event_count) in [
            (base, base + 4, "span-hash-1", 3i32),
            (base + 5, base + 9, "span-hash-2", 1i32),
        ] {
            commit_page(
                &pool,
                PageCommit {
                    events: &[],
                    token_events: &[],
                    invocation_metrics: &[],
                    storage_snapshots: &[],
                    network: "testnet",
                    cursor: Some(to),
                    covered_from_ledger: Some(from),
                    ledger: Some(LedgerMeta {
                        sequence: to,
                        hash,
                        timestamp: "2024-01-01T00:00:00Z",
                        event_count,
                    }),
                    batch_size: 50,
                },
            )
            .await
            .expect("commit_page failed");
        }

        let count: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM ledger_metadata WHERE ledger_sequence BETWEEN $1 AND $2",
        )
        .bind(base as i64)
        .bind((base + 9) as i64)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(count.0, 10, "every traversed ledger must be recorded");

        // Only the page's last ledger carries the real hash and event count;
        // the ledgers in between are placeholders.
        let hashed: Vec<i64> = sqlx::query_scalar(
            "SELECT ledger_sequence FROM ledger_metadata \
             WHERE ledger_sequence BETWEEN $1 AND $2 AND ledger_hash <> '' ORDER BY ledger_sequence",
        )
        .bind(base as i64)
        .bind((base + 9) as i64)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(hashed, vec![(base + 4) as i64, (base + 9) as i64]);

        let counts: Vec<(i64, i32)> = sqlx::query_as(
            "SELECT ledger_sequence, event_count FROM ledger_metadata \
             WHERE ledger_sequence BETWEEN $1 AND $2 ORDER BY ledger_sequence",
        )
        .bind(base as i64)
        .bind((base + 9) as i64)
        .fetch_all(&pool)
        .await
        .unwrap();
        assert_eq!(counts[4], ((base + 4) as i64, 3));
        assert_eq!(counts[9], ((base + 9) as i64, 1));
        assert!(
            counts[..4].iter().all(|(_, event_count)| *event_count == 0),
            "interior ledgers must be traversed-and-empty, not fake event counts"
        );

        // Reorg detection compares the newest stored hashes against the RPC.
        // Placeholder rows must never be returned, or every ordinary ingest
        // would look like a hash mismatch (issue #723).
        let recent = get_recent_ledger_metadata(&pool, 50).await.unwrap();
        assert!(
            recent
                .iter()
                .filter(|(sequence, _)| *sequence >= base && *sequence <= base + 9)
                .all(|(_, hash)| !hash.is_empty()),
            "reorg detection must only see real hashes: {recent:?}"
        );
        assert_eq!(
            recent
                .iter()
                .find(|(sequence, _)| *sequence <= base + 9)
                .map(|(sequence, _)| *sequence),
            Some(base + 9),
            "the newest known hash is the page's last ledger"
        );

        // The gauge and the enqueue path both read these two: neither may
        // report anything for a fully covered range.
        assert!(
            detect_ledger_gaps(&pool, base, base + 9)
                .await
                .unwrap()
                .is_empty(),
            "a normally ingested range must have no gaps"
        );
        let gaps = scan_ledger_gaps(&pool, 10_000).await.unwrap();
        assert!(
            !gaps
                .iter()
                .any(|gap| gap.from_ledger <= base + 9 && gap.to_ledger >= base),
            "scan_ledger_gaps must not report a covered range: {gaps:?}"
        );

        let closed = close_filled_backfill_jobs(&pool, &job_network, &[])
            .await
            .unwrap();
        assert_eq!(closed, 1, "the falsely enqueued job must be closed");
        sqlx::query("DELETE FROM backfill_jobs WHERE network = $1")
            .bind(&job_network)
            .execute(&pool)
            .await
            .unwrap();

        sqlx::query("DELETE FROM ledger_metadata WHERE ledger_sequence BETWEEN $1 AND $2")
            .bind(base as i64)
            .bind((base + 9) as i64)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// Issue #723: a range the indexer deliberately skipped (e.g. adopting the
    /// RPC's retained floor) must still be reported exactly — the span fix
    /// records what was traversed, it does not paper over a real skip.
    #[tokio::test]
    async fn deliberately_skipped_range_is_still_reported_exactly() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let base = 8_400_000u64;
        // First page covers base..=base+4, then the poll loop skips
        // base+5..=base+9 (e.g. a retained-floor jump) and the next page
        // covers base+10..=base+12.
        for (from, to, hash) in [
            (base, base + 4, "skip-hash-1"),
            (base + 10, base + 12, "skip-hash-2"),
        ] {
            commit_page(
                &pool,
                PageCommit {
                    events: &[],
                    token_events: &[],
                    invocation_metrics: &[],
                    storage_snapshots: &[],
                    network: "testnet",
                    cursor: Some(to),
                    covered_from_ledger: Some(from),
                    ledger: Some(LedgerMeta {
                        sequence: to,
                        hash,
                        timestamp: "2024-01-01T00:00:00Z",
                        event_count: 0,
                    }),
                    batch_size: 50,
                },
            )
            .await
            .expect("commit_page failed");
        }

        let gaps: Vec<LedgerGap> = scan_ledger_gaps(&pool, 10_000)
            .await
            .unwrap()
            .into_iter()
            .filter(|gap| gap.from_ledger >= base && gap.to_ledger <= base + 12)
            .collect();
        assert_eq!(
            gaps,
            vec![LedgerGap {
                from_ledger: base + 5,
                to_ledger: base + 9,
            }],
            "the deliberate skip must be reported exactly, and nothing else"
        );

        assert_eq!(
            detect_ledger_gaps(&pool, base, base + 12).await.unwrap(),
            (base + 5..=base + 9).collect::<Vec<u64>>(),
            "the gauge sees exactly the skipped ledgers"
        );

        sqlx::query("DELETE FROM ledger_metadata WHERE ledger_sequence BETWEEN $1 AND $2")
            .bind(base as i64)
            .bind((base + 12) as i64)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// enqueue_backfill_job must be idempotent for the same pending range
    /// (issue #216): a repeat scan finding the same unrepaired gap must not
    /// create a duplicate job.
    #[tokio::test]
    async fn enqueue_backfill_job_is_idempotent_for_pending_jobs() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let network = format!("gaptest-{}", Uuid::new_v4());
        let gap = LedgerGap {
            from_ledger: 8_100_000,
            to_ledger: 8_100_010,
        };

        enqueue_backfill_job(&pool, gap, &network).await.unwrap();
        enqueue_backfill_job(&pool, gap, &network).await.unwrap();

        let count: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM backfill_jobs WHERE network = $1")
            .bind(&network)
            .fetch_one(&pool)
            .await
            .unwrap();
        assert_eq!(
            count.0, 1,
            "the same pending range must not be enqueued twice"
        );

        sqlx::query("DELETE FROM backfill_jobs WHERE network = $1")
            .bind(&network)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// close_filled_backfill_jobs must mark a job done once its exact range
    /// is no longer among the current gaps, and must leave a job whose range
    /// is still (even partially) a gap alone (issue #216).
    #[tokio::test]
    async fn close_filled_backfill_jobs_closes_exact_matches_only() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let network = format!("gaptest-{}", Uuid::new_v4());
        let filled_gap = LedgerGap {
            from_ledger: 8_200_000,
            to_ledger: 8_200_010,
        };
        let still_open_gap = LedgerGap {
            from_ledger: 8_300_000,
            to_ledger: 8_300_010,
        };
        enqueue_backfill_job(&pool, filled_gap, &network)
            .await
            .unwrap();
        enqueue_backfill_job(&pool, still_open_gap, &network)
            .await
            .unwrap();

        // Only still_open_gap is reported as a current gap: filled_gap has
        // since been backfilled.
        let closed = close_filled_backfill_jobs(&pool, &network, &[still_open_gap])
            .await
            .unwrap();
        assert_eq!(closed, 1);

        let filled_status: String = sqlx::query_scalar(
            "SELECT status FROM backfill_jobs WHERE network = $1 AND from_ledger = $2",
        )
        .bind(&network)
        .bind(filled_gap.from_ledger as i64)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(filled_status, "done");

        let open_status: String = sqlx::query_scalar(
            "SELECT status FROM backfill_jobs WHERE network = $1 AND from_ledger = $2",
        )
        .bind(&network)
        .bind(still_open_gap.from_ledger as i64)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            open_status, "pending",
            "a job still reported as a gap must not be closed"
        );

        sqlx::query("DELETE FROM backfill_jobs WHERE network = $1")
            .bind(&network)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// Upserting token metadata twice for the same (contract_id, network)
    /// must update the row in place, not duplicate it, and a resolved
    /// contract must count as fresh until the refresh interval elapses
    /// (issue #263).
    #[tokio::test]
    async fn token_metadata_upsert_refreshes_in_place() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CTOKENMETA_{}", Uuid::new_v4());
        let network = "testnet";

        sqlx::query("DELETE FROM token_metadata WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();

        // Not yet resolved: absent from the fresh set.
        let fresh = fresh_token_metadata_contract_ids(
            &pool,
            std::slice::from_ref(&contract_id),
            network,
            Utc::now() - chrono::Duration::days(1),
        )
        .await
        .unwrap();
        assert!(!fresh.contains(&contract_id));

        let token = crate::token_metadata::TokenMetadataResolution::Token(
            crate::token_metadata::TokenMetadata {
                name: "Example Token".to_string(),
                symbol: "EXT".to_string(),
                decimals: 7,
            },
        );
        upsert_token_metadata(&pool, &contract_id, network, &token)
            .await
            .expect("upsert failed");

        let row: (String, String, i32, bool) = sqlx::query_as(
            "SELECT name, symbol, decimals, is_token FROM token_metadata
             WHERE contract_id = $1 AND network = $2",
        )
        .bind(&contract_id)
        .bind(network)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            row,
            ("Example Token".to_string(), "EXT".to_string(), 7, true)
        );

        // Fresh (updated within the last day) after resolution.
        let fresh = fresh_token_metadata_contract_ids(
            &pool,
            std::slice::from_ref(&contract_id),
            network,
            Utc::now() - chrono::Duration::days(1),
        )
        .await
        .unwrap();
        assert!(fresh.contains(&contract_id));

        // Re-resolving as "not a token" updates the same row rather than
        // inserting a second one.
        upsert_token_metadata(
            &pool,
            &contract_id,
            network,
            &crate::token_metadata::TokenMetadataResolution::NotAToken,
        )
        .await
        .expect("re-upsert failed");

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM token_metadata WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count.0, 1, "re-resolution must update, not duplicate");

        let is_token: (bool,) =
            sqlx::query_as("SELECT is_token FROM token_metadata WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(!is_token.0);

        sqlx::query("DELETE FROM token_metadata WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// delete_token_metadata must evict a cached row so a redeployed
    /// contract's next sighting re-resolves from scratch (issue #685).
    #[tokio::test]
    async fn delete_token_metadata_evicts_the_cached_row() {
        let Some(db_url) = test_db_url("delete_token_metadata_evicts_the_cached_row") else {
            return;
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CREDEPLOY_{}", Uuid::new_v4());
        let network = "testnet";

        sqlx::query("DELETE FROM token_metadata WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();

        let token = crate::token_metadata::TokenMetadataResolution::Token(
            crate::token_metadata::TokenMetadata {
                name: "Old Code's Token".to_string(),
                symbol: "OLD".to_string(),
                decimals: 7,
            },
        );
        upsert_token_metadata(&pool, &contract_id, network, &token)
            .await
            .expect("upsert failed");

        // Resolved and fresh before the simulated redeploy.
        let fresh = fresh_token_metadata_contract_ids(
            &pool,
            std::slice::from_ref(&contract_id),
            network,
            Utc::now() - chrono::Duration::days(1),
        )
        .await
        .unwrap();
        assert!(fresh.contains(&contract_id));

        delete_token_metadata(&pool, &contract_id, network)
            .await
            .expect("delete failed");

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM token_metadata WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            count.0, 0,
            "redeploy invalidation must remove the stale row"
        );

        // Absent from the fresh set after eviction, exactly like a
        // never-before-seen contract -- the new code's name/symbol/decimals
        // must be re-resolved rather than silently continuing to serve the
        // deleted row's cached values.
        let fresh_after = fresh_token_metadata_contract_ids(
            &pool,
            std::slice::from_ref(&contract_id),
            network,
            Utc::now() - chrono::Duration::days(1),
        )
        .await
        .unwrap();
        assert!(!fresh_after.contains(&contract_id));
    }

    /// Deleting a contract's metadata on one network must not touch the same
    /// contract id's row on a different network -- the two are unrelated
    /// deployments that happen to share an address.
    #[tokio::test]
    async fn delete_token_metadata_is_scoped_to_one_network() {
        let Some(db_url) = test_db_url("delete_token_metadata_is_scoped_to_one_network") else {
            return;
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CMULTINET_{}", Uuid::new_v4());

        for network in ["testnet", "mainnet"] {
            sqlx::query("DELETE FROM token_metadata WHERE contract_id = $1 AND network = $2")
                .bind(&contract_id)
                .bind(network)
                .execute(&pool)
                .await
                .unwrap();

            let token = crate::token_metadata::TokenMetadataResolution::Token(
                crate::token_metadata::TokenMetadata {
                    name: format!("{network} token"),
                    symbol: "MN".to_string(),
                    decimals: 7,
                },
            );
            upsert_token_metadata(&pool, &contract_id, network, &token)
                .await
                .expect("upsert failed");
        }

        delete_token_metadata(&pool, &contract_id, "testnet")
            .await
            .expect("delete failed");

        let remaining: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM token_metadata WHERE contract_id = $1 AND network = 'mainnet'",
        )
        .bind(&contract_id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            remaining.0, 1,
            "deleting testnet's row must leave mainnet's row untouched"
        );

        sqlx::query("DELETE FROM token_metadata WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// A reorg rewind must actually remove the rows above the fork point.
    ///
    /// This is a regression test for a `DELETE FROM events` that referenced a
    /// table which does not exist: the statement errored, the transaction
    /// rolled back, and the rewind silently did nothing while the logs
    /// reported a reorg as handled. Asserting on row counts after the call is
    /// what catches that — the query string alone looks plausible.
    #[tokio::test]
    async fn rewind_cursor_deletes_rows_above_the_fork_point() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract = "CREORG_TEST_CONTRACT";
        let keep = make_event(contract, 5_000, 0);
        let discard = make_event(contract, 5_001, 0);

        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(contract)
            .execute(&pool)
            .await
            .expect("cleanup failed");

        insert_events_batch(&pool, "testnet", &[keep.clone(), discard.clone()])
            .await
            .expect("seed insert failed");

        rewind_cursor(&pool, 5_000).await.expect("rewind failed");

        let remaining: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1 AND ledger_sequence > $2",
        )
        .bind(contract)
        .bind(5_000_i64)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(
            remaining.0, 0,
            "rows above the fork point must be deleted by the rewind"
        );

        let kept: (i64,) = sqlx::query_as(
            "SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1 AND ledger_sequence = $2",
        )
        .bind(contract)
        .bind(5_000_i64)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(kept.0, 1, "rows at or below the fork point must survive");

        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(contract)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// Resolve the shared test database URL, honouring the same
    /// skip-vs-hard-fail contract as the other integration tests here.
    fn test_db_url(test_name: &str) -> Option<String> {
        match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => Some(url),
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: {test_name} requires TEST_DATABASE_URL");
                None
            }
        }
    }

    /// Two indexer replicas committing the SAME ledger range concurrently against
    /// one database must produce exactly one row per event — no duplicates, no
    /// lost events (issue #418).
    ///
    /// This is the deliberate-scale-out / double-deploy scenario: both writers
    /// see the same RPC page and race to persist it. Exactly-once persistence
    /// rests on the natural-key UNIQUE constraint (migration 0025) combined with
    /// the ON CONFLICT DO NOTHING insert path.
    ///
    /// Note the integration job runs with `--test-threads=1`, so the concurrency
    /// here comes from tokio tasks over independent pools inside the test, not
    /// from the test harness running tests in parallel.
    #[tokio::test]
    async fn concurrent_indexers_persist_each_event_exactly_once() {
        let Some(db_url) = test_db_url("concurrent_indexers_persist_each_event_exactly_once")
        else {
            return;
        };

        // Independent pools stand in for independent indexer processes: separate
        // connections, separate transactions, no shared in-process state.
        let pool_a = PgPool::connect(&db_url).await.unwrap();
        let pool_b = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CCONC_{}", Uuid::new_v4());
        const EVENT_COUNT: u32 = 50;
        // Distinct transaction hashes, as real protocol events have: separate
        // events come from separate transactions. `make_event` hardcodes one
        // hash, which is fine for a single-ledger test but would make the
        // natural key repeat across the ledgers this test spans.
        let events: Vec<SorobanEvent> = (0..EVENT_COUNT)
            .map(|i| {
                let mut event = make_event(&contract_id, 1000 + u64::from(i) / 10, i);
                event.transaction_hash = format!("txhash_conc_{i:04}");
                event
            })
            .collect();

        fn page(events: &[SorobanEvent], cursor: u64) -> PageCommit<'_> {
            PageCommit {
                events,
                token_events: &[],
                invocation_metrics: &[],
                storage_snapshots: &[],
                network: "testnet",
                cursor: Some(cursor),
                covered_from_ledger: None,
                ledger: None,
                batch_size: 10,
            }
        }

        // Both replicas commit the identical page at the same time.
        let events_a = events.clone();
        let events_b = events.clone();
        let (res_a, res_b) = tokio::join!(
            tokio::spawn(async move { commit_page(&pool_a, page(&events_a, 1004)).await }),
            tokio::spawn(async move { commit_page(&pool_b, page(&events_b, 1004)).await }),
        );

        res_a
            .expect("replica A task panicked")
            .expect("replica A commit failed");
        res_b
            .expect("replica B task panicked")
            .expect("replica B commit failed");

        let verify = PgPool::connect(&db_url).await.unwrap();

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&verify)
                .await
                .unwrap();
        assert_eq!(
            count.0,
            i64::from(EVENT_COUNT),
            "concurrent replicas must not lose or duplicate events"
        );

        // Explicitly assert the natural key is unique — a duplicate would show up
        // here as a group with count > 1 even if the total happened to match.
        let dupes: Vec<(i64, i32, i64)> = sqlx::query_as(
            r#"
            SELECT ledger_sequence, event_index, COUNT(*)
            FROM soroban_events
            WHERE contract_id = $1
            GROUP BY ledger_sequence, event_index
            HAVING COUNT(*) > 1
            "#,
        )
        .bind(&contract_id)
        .fetch_all(&verify)
        .await
        .unwrap();
        assert!(
            dupes.is_empty(),
            "natural key (ledger_sequence, event_index) duplicated under concurrency: {dupes:?}"
        );

        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&verify)
            .await
            .unwrap();
    }

    /// The cursor must never move backwards, even when a slower replica commits
    /// an older page after a faster one has already advanced (issue #418).
    ///
    /// Without the monotonic guard in `commit_page` this is a lost-update race:
    /// the stale writer's `SET value = $1` lands last and rewinds the cursor
    /// behind data already committed.
    #[tokio::test]
    async fn cursor_never_rewinds_under_concurrent_writers() {
        let Some(db_url) = test_db_url("cursor_never_rewinds_under_concurrent_writers") else {
            return;
        };

        let pool = PgPool::connect(&db_url).await.unwrap();

        // Establish a known starting point.
        sqlx::query(
            "INSERT INTO system_state (key, value) VALUES ('latest_ledger_cursor:testnet', '5000')
             ON CONFLICT (key) DO UPDATE SET value = '5000'",
        )
        .execute(&pool)
        .await
        .unwrap();

        let contract_id = format!("CCURS_{}", Uuid::new_v4());
        let ahead = [make_event(&contract_id, 5100, 0)];
        let behind = [make_event(&contract_id, 5050, 1)];

        fn page(events: &[SorobanEvent], cursor: u64) -> PageCommit<'_> {
            PageCommit {
                events,
                token_events: &[],
                invocation_metrics: &[],
                storage_snapshots: &[],
                network: "testnet",
                cursor: Some(cursor),
                covered_from_ledger: None,
                ledger: None,
                batch_size: 10,
            }
        }

        // The faster replica advances to 5100 first.
        commit_page(&pool, page(&ahead, 5100))
            .await
            .expect("advance commit failed");
        assert_eq!(get_cursor(&pool, "testnet").await.unwrap(), 5100);

        // The slower replica now commits an older page. Its events must still
        // land, but the cursor must hold at 5100.
        commit_page(&pool, page(&behind, 5050))
            .await
            .expect("stale commit failed");

        assert_eq!(
            get_cursor(&pool, "testnet").await.unwrap(),
            5100,
            "a stale replica must not rewind the cursor"
        );

        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(count.0, 2, "a stale page's events must still be persisted");

        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// #600 regression: two indexers on different networks against one
    /// database must advance completely independently. Before namespacing
    /// the cursor key per network, the single shared 'latest_ledger_cursor'
    /// row's monotonic-advance guard meant a mainnet indexer far ahead in
    /// absolute ledger numbers would permanently block a lower-sequence
    /// testnet indexer's cursor from ever moving, since the guard has no way
    /// to tell the two networks' ledger numbers apart.
    #[tokio::test]
    async fn two_networks_advance_their_cursors_independently() {
        let Some(db_url) = test_db_url("two_networks_advance_their_cursors_independently") else {
            return;
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        // Reset both networks to a known starting point.
        for network in ["mainnet", "testnet"] {
            sqlx::query(&format!(
                "INSERT INTO system_state (key, value) VALUES ('latest_ledger_cursor:{network}', '0')
                 ON CONFLICT (key) DO UPDATE SET value = '0'"
            ))
            .execute(&pool)
            .await
            .unwrap();
        }

        let mainnet_contract = format!("CMAIN_{}", Uuid::new_v4());
        let testnet_contract = format!("CTEST_{}", Uuid::new_v4());

        fn page<'a>(events: &'a [SorobanEvent], network: &'a str, cursor: u64) -> PageCommit<'a> {
            PageCommit {
                events,
                token_events: &[],
                invocation_metrics: &[],
                storage_snapshots: &[],
                network,
                cursor: Some(cursor),
                covered_from_ledger: None,
                ledger: None,
                batch_size: 10,
            }
        }

        // Mainnet advances far ahead in absolute ledger numbers.
        let mainnet_events = [make_event(&mainnet_contract, 9_000_000, 0)];
        commit_page(&pool, page(&mainnet_events, "mainnet", 9_000_000))
            .await
            .expect("mainnet commit failed");

        // Testnet, at a much lower absolute ledger number, must still be
        // able to advance — this is exactly the scenario the single global
        // key made impossible (a lower-sequence network could never move a
        // cursor a higher-sequence network had already advanced).
        let testnet_events = [make_event(&testnet_contract, 100, 0)];
        commit_page(&pool, page(&testnet_events, "testnet", 100))
            .await
            .expect("testnet commit failed");

        assert_eq!(
            get_cursor(&pool, "mainnet").await.unwrap(),
            9_000_000,
            "mainnet's cursor must be unaffected by testnet advancing"
        );
        assert_eq!(
            get_cursor(&pool, "testnet").await.unwrap(),
            100,
            "testnet must be able to advance despite mainnet being far ahead in absolute ledger numbers"
        );

        sqlx::query("DELETE FROM soroban_events WHERE contract_id IN ($1, $2)")
            .bind(&mainnet_contract)
            .bind(&testnet_contract)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// Many writers racing to advance the cursor must converge on the highest
    /// value, not on whichever transaction happened to commit last (issue #418).
    #[tokio::test]
    async fn concurrent_cursor_advances_converge_on_maximum() {
        let Some(db_url) = test_db_url("concurrent_cursor_advances_converge_on_maximum") else {
            return;
        };

        let setup = PgPool::connect(&db_url).await.unwrap();
        sqlx::query(
            "INSERT INTO system_state (key, value) VALUES ('latest_ledger_cursor:testnet', '0')
             ON CONFLICT (key) DO UPDATE SET value = '0'",
        )
        .execute(&setup)
        .await
        .unwrap();

        let contract_id = format!("CRACE_{}", Uuid::new_v4());

        // Interleave ascending and descending orders so the "last writer" is not
        // the highest cursor: a lost-update bug lands on a low value here.
        let cursors: Vec<u64> = (1..=12).map(|i| i * 100).rev().collect();

        let mut handles = Vec::new();
        for cursor in cursors {
            let url = db_url.clone();
            let contract = contract_id.clone();
            handles.push(tokio::spawn(async move {
                let pool = PgPool::connect(&url).await.unwrap();
                let events = [make_event(&contract, cursor, 0)];
                commit_page(
                    &pool,
                    PageCommit {
                        events: &events,
                        token_events: &[],
                        invocation_metrics: &[],
                        storage_snapshots: &[],
                        network: "testnet",
                        cursor: Some(cursor),
                        covered_from_ledger: None,
                        ledger: None,
                        batch_size: 10,
                    },
                )
                .await
            }));
        }

        for handle in handles {
            handle
                .await
                .expect("writer task panicked")
                .expect("writer commit failed");
        }

        assert_eq!(
            get_cursor(&setup, "testnet").await.unwrap(),
            1200,
            "cursor must settle on the highest committed ledger, not the last writer"
        );

        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&setup)
            .await
            .unwrap();
    }

    // -------------------------------------------------------------------------
    // Partition exhaustion tests (issue #525)
    // -------------------------------------------------------------------------

    /// `assert_no_default_partition_overflow` must be a no-op when every
    /// ledger_sequence in the batch is strictly below the upper bound.
    #[test]
    fn overflow_check_passes_when_all_sequences_within_bound() {
        let sequences = vec![0i64, 1_000_000, 1_999_999];
        assert!(
            assert_no_default_partition_overflow(&sequences, &[(0, 2_000_000)]).is_ok(),
            "sequences strictly below upper bound must not trigger overflow"
        );
    }

    /// A batch containing a sequence exactly at the upper bound must fail —
    /// the upper bound is exclusive (Postgres `FOR VALUES FROM (start) TO (end)`
    /// means `start <= seq < end`).
    #[test]
    fn overflow_check_fails_when_sequence_equals_upper_bound() {
        let sequences = vec![1_999_998i64, 2_000_000];
        let err = assert_no_default_partition_overflow(&sequences, &[(0, 2_000_000)])
            .expect_err("sequence == upper_bound should trigger overflow");
        // Must be a ConfigError so the poll loop treats it as Fatal.
        assert!(
            matches!(err, TridentError::ConfigError { .. }),
            "overflow must produce a ConfigError (Fatal severity), got: {err}"
        );
        assert!(
            err.to_string().contains("2000000"),
            "error message must name the offending sequence"
        );
    }

    /// A batch containing a sequence beyond the upper bound must fail.
    #[test]
    fn overflow_check_fails_when_sequence_exceeds_upper_bound() {
        let sequences = vec![58_000_001i64, 60_000_001];
        let err = assert_no_default_partition_overflow(&sequences, &[(0, 60_000_000)])
            .expect_err("sequence > upper_bound should trigger overflow");
        assert!(matches!(err, TridentError::ConfigError { .. }));
        // Error message must carry the create_soroban_partition hint so on-call
        // knows exactly what SQL to run.
        let msg = err.to_string();
        assert!(
            msg.contains("create_soroban_partition"),
            "error message must include the create_soroban_partition hint"
        );
    }

    /// Regression: a ledger that falls in a *gap* between named partitions is
    /// below the highest upper bound but covered by nothing, so it lands in
    /// soroban_events_default. Migration 0017 seeds exactly this shape —
    /// partitions for 0-6M and 50M-60M, with a 44M-ledger hole between them —
    /// so a guard that only compares against MAX(upper_bound) accepts ledger
    /// 20_000_000 and silently corrupts the dataset (issue #525).
    #[test]
    fn overflow_check_fails_for_sequence_in_gap_between_partitions() {
        let migration_0017_shape = &[
            (0i64, 2_000_000i64),
            (2_000_000, 4_000_000),
            (4_000_000, 6_000_000),
            (50_000_000, 52_000_000),
            (52_000_000, 54_000_000),
            (54_000_000, 56_000_000),
            (56_000_000, 58_000_000),
            (58_000_000, 60_000_000),
        ];
        // Below MAX(upper_bound) = 60_000_000, but inside the 6M-50M hole.
        let err = assert_no_default_partition_overflow(&[20_000_000i64], migration_0017_shape)
            .expect_err("a ledger inside a partition gap must be rejected");
        let msg = err.to_string();
        assert!(
            msg.contains("20000000"),
            "error must name the uncovered sequence, got: {msg}"
        );
        assert!(
            msg.contains("create_soroban_partition"),
            "error must include the remediation hint, got: {msg}"
        );
        // Sequences that ARE covered by the same shape must still pass.
        assert!(
            assert_no_default_partition_overflow(&[5_999_999i64, 50_000_000], migration_0017_shape)
                .is_ok(),
            "covered sequences must not be flagged"
        );
    }

    /// An empty batch must never trigger the overflow check regardless of the
    /// partition boundary value.
    #[test]
    fn overflow_check_passes_for_empty_batch() {
        assert!(
            assert_no_default_partition_overflow(&[], &[]).is_ok(),
            "empty batch must never trigger overflow"
        );
    }

    /// The overflow is detected as Fatal by the error taxonomy, so the poll
    /// loop halts rather than retrying.
    #[test]
    fn overflow_error_is_fatal_not_retryable() {
        use trident_common::errors::Severity;
        let err = assert_no_default_partition_overflow(&[60_000_001i64], &[(0, 60_000_000)])
            .expect_err("should overflow");
        assert_eq!(
            err.severity(),
            Severity::Fatal,
            "partition overflow must be Fatal so the indexer halts"
        );
        assert!(err.fatal());
        assert!(!err.retryable());
    }

    /// Integration test: `named_partition_ranges` returns the seeded partition
    /// ranges against a real database using the standard migration chain, and
    /// those ranges expose the gap that migration 0017 leaves behind.
    #[tokio::test]
    async fn named_partition_ranges_returns_migration_seed_values() {
        let Some(db_url) = test_db_url("named_partition_ranges_returns_migration_seed_values")
        else {
            return;
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let ranges = named_partition_ranges(&pool)
            .await
            .expect("query must not fail");

        assert!(
            !ranges.is_empty(),
            "migrations seed named partitions; ranges must not be empty"
        );
        for (lo, hi) in &ranges {
            assert!(hi > lo, "each range must be non-empty, got ({lo}, {hi})");
        }
        let highest = ranges.iter().map(|&(_, hi)| hi).max().unwrap();
        assert!(
            highest >= 60_000_000,
            "highest upper bound must be at least 60,000,000, got {highest}"
        );
        // The seeded schema is deliberately non-contiguous (0-6M then 50M-60M).
        // The guard must therefore reject a ledger inside that hole, which a
        // MAX(upper_bound) check would have accepted.
        assert!(
            assert_no_default_partition_overflow(&[20_000_000i64], &ranges).is_err(),
            "a ledger inside the 6M-50M gap must be rejected"
        );
    }

    /// Integration test: inserting an event with a ledger_sequence that matches
    /// the partition boundary condition is caught BEFORE the database is touched.
    ///
    /// This is the "deliberate exhaustion" test from issue #525: we prove the
    /// guard fires on a sequence that would land in the DEFAULT partition, using
    /// the boundary value we know from the seeded migrations.
    #[tokio::test]
    async fn overflow_guard_fires_before_commit_at_exhaustion_boundary() {
        let Some(db_url) = test_db_url("overflow_guard_fires_before_commit_at_exhaustion_boundary")
        else {
            return;
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        // Get the actual ranges from the DB so this test stays correct even
        // after additional partitions are added.
        let ranges = named_partition_ranges(&pool)
            .await
            .expect("query must not fail");
        assert!(!ranges.is_empty(), "seeded partitions must exist");
        let last_upper = ranges.iter().map(|&(_, hi)| hi).max().unwrap();

        // A sequence exactly at the highest upper bound would land in DEFAULT.
        let overflow_seq = last_upper;
        let err = assert_no_default_partition_overflow(&[overflow_seq], &ranges)
            .expect_err("overflow at boundary must be caught");

        assert!(
            matches!(err, TridentError::ConfigError { .. }),
            "boundary overflow must be a ConfigError (Fatal)"
        );

        // Verify the event was NOT written to the database — the guard must
        // fire before any DB write, not after. We check by querying for any
        // row at that sequence belonging to a sentinel contract.
        let sentinel = format!("CEXHAUST_TEST_{last_upper}");
        let count: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1")
                .bind(&sentinel)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            count.0, 0,
            "no row must have been written: the guard must fire before commit_page"
        );
    }

    /// A dead-lettered event must land in `failed_events` with its full
    /// payload and error message intact, so it can be inspected and replayed
    /// later (issue #208).
    #[tokio::test]
    async fn insert_failed_event_persists_payload_and_error() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CFAILED_{}", Uuid::new_v4());
        let event = make_event(&contract_id, 999, 0);

        sqlx::query("DELETE FROM failed_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();

        insert_failed_event(&pool, "testnet", &event, "simulated persistent failure", 3)
            .await
            .expect("insert_failed_event must succeed");

        let row: (String, String, i32, serde_json::Value) = sqlx::query_as(
            "SELECT error_message, transaction_hash, attempts, event_payload
             FROM failed_events WHERE contract_id = $1",
        )
        .bind(&contract_id)
        .fetch_one(&pool)
        .await
        .expect("row must exist");

        assert_eq!(row.0, "simulated persistent failure");
        assert_eq!(row.1, event.transaction_hash);
        assert_eq!(row.2, 3);
        assert_eq!(
            row.3.get("contract_id").and_then(|v| v.as_str()),
            Some(contract_id.as_str()),
            "event_payload must round-trip the full event"
        );

        sqlx::query("DELETE FROM failed_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// A row inserted via `insert_failed_event` must appear in
    /// `list_pending_failed_events` until it is replayed, and disappear from
    /// that listing (while staying in the table) once it is (issue #574).
    #[tokio::test]
    async fn list_pending_failed_events_excludes_replayed_rows() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CPENDING_{}", Uuid::new_v4());
        let event = make_event(&contract_id, 998, 0);

        sqlx::query("DELETE FROM failed_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();

        insert_failed_event(&pool, "testnet", &event, "simulated failure", 3)
            .await
            .unwrap();

        let id: (Uuid,) = sqlx::query_as("SELECT id FROM failed_events WHERE contract_id = $1")
            .bind(&contract_id)
            .fetch_one(&pool)
            .await
            .unwrap();

        let pending = list_pending_failed_events(&pool, "testnet", 1000).await.unwrap();
        assert!(
            pending.iter().any(|row| row.id == id.0),
            "freshly dead-lettered row must be listed as pending"
        );

        let outcome = replay_failed_event(&pool, id.0, "testnet").await.unwrap();
        assert_eq!(outcome, ReplayOutcome::Replayed);

        let pending_after = list_pending_failed_events(&pool, "testnet", 1000).await.unwrap();
        assert!(
            !pending_after.iter().any(|row| row.id == id.0),
            "replayed row must no longer be listed as pending"
        );

        let still_in_table: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM failed_events WHERE id = $1")
                .bind(id.0)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            still_in_table.0, 1,
            "replay marks the row done, it does not delete it"
        );

        sqlx::query("DELETE FROM failed_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// The event ends up in `soroban_events` after replay, `replayed_at` is
    /// stamped, and — the idempotency guarantee the deterministic UUIDv5 key
    /// and `ON CONFLICT DO NOTHING` are supposed to provide — replaying the
    /// same id a second time neither double-inserts the event nor errors
    /// (issue #574).
    #[tokio::test]
    async fn replay_failed_event_persists_event_and_is_idempotent() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CREPLAY_{}", Uuid::new_v4());
        let event = make_event(&contract_id, 997, 0);

        sqlx::query("DELETE FROM failed_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();

        insert_failed_event(&pool, "testnet", &event, "simulated failure", 3)
            .await
            .unwrap();
        let id: (Uuid,) = sqlx::query_as("SELECT id FROM failed_events WHERE contract_id = $1")
            .bind(&contract_id)
            .fetch_one(&pool)
            .await
            .unwrap();

        assert_eq!(
            replay_failed_event(&pool, id.0, "testnet").await.unwrap(),
            ReplayOutcome::Replayed
        );

        let persisted: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(persisted.0, 1, "the event must now exist in soroban_events");

        let replayed_at: (Option<DateTime<Utc>>,) =
            sqlx::query_as("SELECT replayed_at FROM failed_events WHERE id = $1")
                .bind(id.0)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert!(replayed_at.0.is_some(), "replayed_at must be stamped");

        // Replaying again must be a no-op: AlreadyReplayedOrMissing, not a
        // second row in soroban_events.
        assert_eq!(
            replay_failed_event(&pool, id.0, "testnet").await.unwrap(),
            ReplayOutcome::AlreadyReplayedOrMissing
        );
        let persisted_again: (i64,) =
            sqlx::query_as("SELECT COUNT(*) FROM soroban_events WHERE contract_id = $1")
                .bind(&contract_id)
                .fetch_one(&pool)
                .await
                .unwrap();
        assert_eq!(
            persisted_again.0, 1,
            "a second replay must not double-insert"
        );

        sqlx::query("DELETE FROM failed_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();
        sqlx::query("DELETE FROM soroban_events WHERE contract_id = $1")
            .bind(&contract_id)
            .execute(&pool)
            .await
            .unwrap();
    }

    /// Replaying an id that was never dead-lettered must not error — the
    /// same "idempotent, not a hard failure" contract as replaying twice.
    #[tokio::test]
    async fn replay_failed_event_missing_id_is_not_an_error() {
        let db_url = match std::env::var("TEST_DATABASE_URL") {
            Ok(url) => url,
            Err(_) if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            Err(_) => {
                eprintln!("SKIP: TEST_DATABASE_URL not set");
                return;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();

        let outcome = replay_failed_event(&pool, Uuid::new_v4(), "testnet")
            .await
            .unwrap();
        assert_eq!(outcome, ReplayOutcome::AlreadyReplayedOrMissing);
    }
}
