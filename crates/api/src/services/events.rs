use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use chrono::{DateTime, Utc};
use opentelemetry::propagation::Extractor;
use redis::streams::{StreamReadOptions, StreamReadReply};
use redis::AsyncCommands;
use sqlx::PgPool;
use tonic::{Request, Response, Status};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;
use uuid::Uuid;

use crate::trident::{
    events_server::Events, Event, GetEventRequest, ListEventsRequest, ListEventsResponse,
    StreamEventsRequest,
};

// ---------------------------------------------------------------------------
// W3C TraceContext extraction from tonic metadata
// ---------------------------------------------------------------------------

struct MetadataCarrier<'a>(&'a tonic::metadata::MetadataMap);

impl<'a> Extractor for MetadataCarrier<'a> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0
            .keys()
            .filter_map(|k| {
                if let tonic::metadata::KeyRef::Ascii(k) = k {
                    Some(k.as_str())
                } else {
                    None
                }
            })
            .collect()
    }
}

fn extract_context(metadata: &tonic::metadata::MetadataMap) -> opentelemetry::Context {
    opentelemetry::global::get_text_map_propagator(|prop| prop.extract(&MetadataCarrier(metadata)))
}

const REDIS_STREAM_KEY: &str = "trident:events";

/// Default in-flight buffer per subscriber. Bounded so one slow consumer
/// cannot make the server accumulate events without limit; when it fills, the
/// consumer task blocks on send and stops reading Redis.
const DEFAULT_STREAM_CHANNEL_BUF: usize = 128;

/// How long a single blocking XREAD waits before looping. Short enough that a
/// dropped client is noticed promptly, long enough to avoid busy polling.
const XREAD_BLOCK_MS: usize = 5_000;

/// Redis stream ID meaning "only entries added after this call".
const STREAM_ID_LIVE_TAIL: &str = "$";

const DEFAULT_NETWORK: &str = "testnet";

/// Per-subscriber buffer size, overridable via `STREAM_CHANNEL_BUFFER`.
fn stream_channel_buffer() -> usize {
    std::env::var("STREAM_CHANNEL_BUFFER")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(DEFAULT_STREAM_CHANNEL_BUF)
}

/// Validate a client-supplied resume point.
///
/// Redis stream IDs are `<millis>-<seq>`. `0` (replay everything still in the
/// stream) and `$` (live tail) are also accepted. Anything else is rejected
/// rather than passed through, because Redis would answer a malformed ID with a
/// connection-level error that the subscriber would see as an opaque failure.
// `Status` is the error type the gRPC surface must return; boxing it here would
// only move the size to the caller.
#[allow(clippy::result_large_err)]
fn validate_start_id(start_id: &str) -> Result<String, Status> {
    if start_id.is_empty() {
        return Ok(STREAM_ID_LIVE_TAIL.to_string());
    }
    if start_id == STREAM_ID_LIVE_TAIL || start_id == "0" {
        return Ok(start_id.to_string());
    }

    let mut parts = start_id.split('-');
    let valid = matches!((parts.next(), parts.next(), parts.next()), (Some(ms), Some(seq), None)
        if !ms.is_empty()
            && !seq.is_empty()
            && ms.bytes().all(|b| b.is_ascii_digit())
            && seq.bytes().all(|b| b.is_ascii_digit()));

    if valid {
        Ok(start_id.to_string())
    } else {
        Err(Status::invalid_argument(
            "start_id must be a Redis stream ID (<millis>-<seq>), \"0\", or \"$\"",
        ))
    }
}

pub struct EventsServiceImpl {
    pub db: PgPool,
    pub redis: redis::aio::ConnectionManager,
    redis_client: redis::Client,
    hub: tokio::sync::OnceCell<Arc<StreamHub>>,
}

impl EventsServiceImpl {
    /// `redis_client` opens the hub's dedicated blocking connection, so its
    /// XREAD never stalls replay reads on the shared `redis` manager.
    pub fn new(
        db: PgPool,
        redis: redis::aio::ConnectionManager,
        redis_client: redis::Client,
    ) -> Self {
        Self {
            db,
            redis,
            redis_client,
            hub: tokio::sync::OnceCell::new(),
        }
    }
}

// ---------------------------------------------------------------------------
// DB row → proto conversion
// ---------------------------------------------------------------------------

#[derive(sqlx::FromRow)]
struct EventRow {
    id: Uuid,
    contract_id: String,
    ledger_sequence: i64,
    ledger_timestamp: DateTime<Utc>,
    transaction_hash: String,
    event_index: i32,
    event_type: String,
    topics: serde_json::Value,
    data: serde_json::Value,
    created_at: DateTime<Utc>,
}

fn row_to_event(row: EventRow) -> Event {
    let topics: Vec<String> = serde_json::from_value(row.topics).unwrap_or_default();
    Event {
        id: row.id.to_string(),
        contract_id: row.contract_id,
        ledger_sequence: row.ledger_sequence as u64,
        ledger_timestamp: row.ledger_timestamp.to_rfc3339(),
        transaction_hash: row.transaction_hash,
        event_index: row.event_index as u32,
        event_type: row.event_type,
        topics,
        data: row.data.to_string(),
        created_at: row.created_at.to_rfc3339(),
    }
}

fn db_err(e: sqlx::Error) -> Status {
    tracing::error!(error = %e, "database error");
    Status::unavailable("database temporarily unavailable")
}

// Same rationale as `validate_start_id` above: `Status` is the gRPC surface's
// error type, so boxing it here would only move the size to the caller.
#[allow(clippy::result_large_err)]
fn validate_list_events(req: &ListEventsRequest) -> Result<(), Status> {
    if req.contract_id.len() > 256 {
        return Err(Status::invalid_argument(
            "contract_id must be at most 256 characters",
        ));
    }

    if req.topic_0.len() > 128 {
        return Err(Status::invalid_argument(
            "topic_0 must be at most 128 characters",
        ));
    }

    if req.topic_1.len() > 128 {
        return Err(Status::invalid_argument(
            "topic_1 must be at most 128 characters",
        ));
    }

    if req.network.len() > 64 {
        return Err(Status::invalid_argument(
            "network must be at most 64 characters",
        ));
    }

    Ok(())
}

#[allow(clippy::result_large_err)]
fn validate_get_event(req: &GetEventRequest) -> Result<(), Status> {
    if req.id.is_empty() {
        return Err(Status::invalid_argument("id is required"));
    }

    if req.network.len() > 64 {
        return Err(Status::invalid_argument(
            "network must be at most 64 characters",
        ));
    }

    Ok(())
}

/// Normalise a network string — defaults to "testnet" when empty.
fn resolve_network(network: &str) -> &str {
    if network.is_empty() {
        DEFAULT_NETWORK
    } else {
        network
    }
}

// ---------------------------------------------------------------------------
// Redis stream fan-out (issues #236, #742)
// ---------------------------------------------------------------------------

/// What one subscriber asked for.
struct StreamSubscription {
    contract_id: String,
    topic_0: Option<String>,
    /// Validated Redis stream ID to read from.
    start_id: String,
}

/// Read one field of a stream entry as a string, defaulting to empty.
fn entry_field(entry: &redis::streams::StreamId, field: &str) -> String {
    entry
        .map
        .get(field)
        .and_then(|v| {
            if let redis::Value::Data(b) = v {
                String::from_utf8(b.clone()).ok()
            } else {
                None
            }
        })
        .unwrap_or_default()
}

fn entry_topics(entry: &redis::streams::StreamId) -> Vec<String> {
    serde_json::from_str(&entry_field(entry, "topics")).unwrap_or_default()
}

fn entry_to_event(entry: &redis::streams::StreamId, topics: Vec<String>) -> Event {
    Event {
        id: String::new(),
        contract_id: entry_field(entry, "contract_id"),
        ledger_sequence: entry_field(entry, "ledger_sequence").parse().unwrap_or(0),
        ledger_timestamp: entry_field(entry, "ledger_timestamp"),
        transaction_hash: entry_field(entry, "transaction_hash"),
        event_index: entry_field(entry, "event_index").parse().unwrap_or(0),
        event_type: entry_field(entry, "event_type"),
        topics,
        data: entry_field(entry, "data"),
        created_at: String::new(),
    }
}

fn topic_matches(topic_0: &Option<String>, topics: &[String]) -> bool {
    match topic_0 {
        Some(t0) => topics.first().map(String::as_str) == Some(t0.as_str()),
        None => true,
    }
}

/// Parse a `<millis>-<seq>` stream id for ordering comparisons.
fn parse_stream_id(id: &str) -> (u64, u64) {
    let (ms, seq) = id.split_once('-').unwrap_or((id, "0"));
    (ms.parse().unwrap_or(0), seq.parse().unwrap_or(0))
}

struct Subscriber {
    id: u64,
    topic_0: Option<String>,
    tx: tokio::sync::mpsc::Sender<Event>,
    /// Set when the hub dropped this subscriber because its buffer was full,
    /// so the forwarder can tell a slow client from a disconnected one.
    overflowed: Arc<AtomicBool>,
}

#[derive(Default)]
struct HubState {
    /// Id of the last stream entry the hub has dispatched.
    pos: String,
    next_id: u64,
    subs: HashMap<String, Vec<Subscriber>>,
}

impl HubState {
    /// Hand one batch of entries to the matching subscribers. Never awaits: a
    /// subscriber whose buffer is full is dropped rather than stalling the
    /// single reader (and with it every other subscriber).
    fn dispatch(&mut self, entries: &[redis::streams::StreamId]) {
        for entry in entries {
            self.pos = entry.id.clone();

            let contract_id = entry_field(entry, "contract_id");
            let Some(list) = self.subs.get_mut(&contract_id) else {
                continue;
            };

            let topics = entry_topics(entry);
            let event = entry_to_event(entry, topics.clone());
            list.retain(|sub| {
                if !topic_matches(&sub.topic_0, &topics) {
                    return true;
                }
                match sub.tx.try_send(event.clone()) {
                    Ok(()) => true,
                    Err(tokio::sync::mpsc::error::TrySendError::Full(_)) => {
                        sub.overflowed.store(true, Ordering::Release);
                        false
                    }
                    Err(tokio::sync::mpsc::error::TrySendError::Closed(_)) => false,
                }
            });
            if list.is_empty() {
                self.subs.remove(&contract_id);
            }
        }
    }

    fn remove(&mut self, contract_id: &str, id: u64) {
        if let Some(list) = self.subs.get_mut(contract_id) {
            list.retain(|s| s.id != id);
            if list.is_empty() {
                self.subs.remove(contract_id);
            }
        }
    }
}

/// One Redis reader per process, fanning stream entries out to subscribers by
/// contract. Redis read volume is independent of the subscriber count
/// (issue #742); each subscriber only costs a bounded in-memory channel.
pub struct StreamHub {
    state: Arc<Mutex<HubState>>,
    /// Non-blocking connection used for replaying history to resuming clients.
    replay: redis::aio::ConnectionManager,
}

impl StreamHub {
    /// Start the reader on `reader`, a connection dedicated to blocking XREAD.
    /// The reader stops once the hub and all of its subscribers are gone.
    pub async fn start(
        mut reader: redis::aio::ConnectionManager,
        replay: redis::aio::ConnectionManager,
    ) -> redis::RedisResult<Arc<Self>> {
        let tail: redis::streams::StreamRangeReply = redis::cmd("XREVRANGE")
            .arg(REDIS_STREAM_KEY)
            .arg("+")
            .arg("-")
            .arg("COUNT")
            .arg(1)
            .query_async(&mut reader)
            .await?;
        let pos = tail
            .ids
            .first()
            .map(|e| e.id.clone())
            .unwrap_or_else(|| "0-0".to_string());

        let state = Arc::new(Mutex::new(HubState {
            pos,
            ..HubState::default()
        }));
        tokio::spawn(run_hub_reader(reader, Arc::downgrade(&state)));
        Ok(Arc::new(Self { state, replay }))
    }

    /// Register a subscriber and spawn its forwarder. Events reach `out`; the
    /// forwarder exits (and unregisters) as soon as `out`'s receiver is dropped.
    fn subscribe(
        &self,
        sub: StreamSubscription,
        out: tokio::sync::mpsc::Sender<Result<Event, Status>>,
    ) {
        let StreamSubscription {
            contract_id,
            topic_0,
            start_id,
        } = sub;
        let (live_tx, live_rx) = tokio::sync::mpsc::channel(stream_channel_buffer());
        let overflowed = Arc::new(AtomicBool::new(false));

        // Registering and snapshotting `pos` under one lock means everything the
        // hub dispatches from now on has an id above `live_after`, and anything
        // at or below it is the replay's job.
        let (sub_id, live_after) = {
            let mut st = self.state.lock().expect("hub state poisoned");
            let id = st.next_id;
            st.next_id += 1;
            st.subs
                .entry(contract_id.clone())
                .or_default()
                .push(Subscriber {
                    id,
                    topic_0: topic_0.clone(),
                    tx: live_tx,
                    overflowed: overflowed.clone(),
                });
            (id, st.pos.clone())
        };

        let replay_from = (start_id != STREAM_ID_LIVE_TAIL).then_some(start_id);
        tokio::spawn(forward_subscriber(Forwarder {
            state: self.state.clone(),
            replay: self.replay.clone(),
            contract_id,
            topic_0,
            sub_id,
            live_after,
            replay_from,
            overflowed,
            live_rx,
            out,
        }));
    }
}

/// The single XREAD loop. Holds only a weak reference to the state so it ends
/// once the hub is dropped and no forwarder is left.
async fn run_hub_reader(mut redis: redis::aio::ConnectionManager, state: Weak<Mutex<HubState>>) {
    let opts = StreamReadOptions::default()
        .block(XREAD_BLOCK_MS)
        .count(100);

    loop {
        let Some(strong) = state.upgrade() else {
            return;
        };
        let pos = strong.lock().expect("hub state poisoned").pos.clone();
        drop(strong);

        let reply: redis::RedisResult<StreamReadReply> = redis
            .xread_options(&[REDIS_STREAM_KEY], &[pos], &opts)
            .await;

        match reply {
            Ok(StreamReadReply { keys }) => {
                let Some(strong) = state.upgrade() else {
                    return;
                };
                let mut st = strong.lock().expect("hub state poisoned");
                for stream_key in keys {
                    st.dispatch(&stream_key.ids);
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "Redis XREAD error in stream hub, retrying");
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        }
    }
}

struct Forwarder {
    state: Arc<Mutex<HubState>>,
    replay: redis::aio::ConnectionManager,
    contract_id: String,
    topic_0: Option<String>,
    sub_id: u64,
    live_after: String,
    /// `Some(id)` replays retained history after `id` before going live.
    replay_from: Option<String>,
    overflowed: Arc<AtomicBool>,
    live_rx: tokio::sync::mpsc::Receiver<Event>,
    out: tokio::sync::mpsc::Sender<Result<Event, Status>>,
}

/// Feed one subscriber: replay history up to the hub position it registered at,
/// then forward live events. Backpressure is the bounded `out` channel; when it
/// stays full the hub drops the subscriber instead of buffering without limit.
async fn forward_subscriber(mut f: Forwarder) {
    forward_inner(&mut f).await;
    f.state
        .lock()
        .expect("hub state poisoned")
        .remove(&f.contract_id, f.sub_id);
}

async fn forward_inner(f: &mut Forwarder) {
    if let Some(mut last_id) = f.replay_from.take() {
        let opts = StreamReadOptions::default().count(100);
        let live_after = parse_stream_id(&f.live_after);
        loop {
            let ids = [last_id.clone()];
            let reply: redis::RedisResult<StreamReadReply> = tokio::select! {
                biased;
                _ = f.out.closed() => return,
                reply = f.replay.xread_options(&[REDIS_STREAM_KEY], &ids, &opts) => reply,
            };
            let keys = match reply {
                Ok(reply) => reply.keys,
                Err(e) => {
                    tracing::warn!(error = %e, "Redis XREAD error replaying stream history");
                    let _ = f
                        .out
                        .send(Err(Status::unavailable(
                            "event stream temporarily unavailable",
                        )))
                        .await;
                    return;
                }
            };
            let entries: Vec<_> = keys.into_iter().flat_map(|k| k.ids).collect();
            if entries.is_empty() {
                break;
            }
            for entry in entries {
                if parse_stream_id(&entry.id) > live_after {
                    // The hub delivers everything past this point.
                    return forward_live(f).await;
                }
                last_id = entry.id.clone();
                if entry_field(&entry, "contract_id") != f.contract_id {
                    continue;
                }
                let topics = entry_topics(&entry);
                if !topic_matches(&f.topic_0, &topics) {
                    continue;
                }
                if f.out
                    .send(Ok(entry_to_event(&entry, topics)))
                    .await
                    .is_err()
                {
                    return;
                }
            }
        }
    }
    forward_live(f).await;
}

async fn forward_live(f: &mut Forwarder) {
    loop {
        tokio::select! {
            biased;
            _ = f.out.closed() => return,
            event = f.live_rx.recv() => match event {
                Some(event) => {
                    if f.out.send(Ok(event)).await.is_err() {
                        return;
                    }
                }
                None => {
                    if f.overflowed.load(Ordering::Acquire) {
                        let _ = f
                            .out
                            .send(Err(Status::resource_exhausted(
                                "subscriber too slow; stream dropped",
                            )))
                            .await;
                    }
                    return;
                }
            },
        }
    }
}

// ---------------------------------------------------------------------------
// gRPC service implementation
// ---------------------------------------------------------------------------

#[tonic::async_trait]
impl Events for EventsServiceImpl {
    async fn list_events(
        &self,
        request: Request<ListEventsRequest>,
    ) -> Result<Response<ListEventsResponse>, Status> {
        let parent_cx = extract_context(request.metadata());
        let span = tracing::info_span!("list_events", "rpc.system" = "grpc");
        span.set_parent(parent_cx);

        let db = self.db.clone();
        let req = request.into_inner();
        let contract_id_attr = req.contract_id.clone();
        span.record("contract_id", contract_id_attr.as_str());

        validate_list_events(&req).map_err(|e| {
            tracing::warn!(error = %e, "invalid list_events request");
            e
        })?;

        async move {
            let limit = req.limit.clamp(1, 200) as i64;
            let network = resolve_network(&req.network);

            let (cursor_seq, cursor_idx): (Option<i64>, Option<i32>) = if req.cursor.is_empty() {
                (None, None)
            } else {
                let cursor_id = Uuid::parse_str(&req.cursor)
                    .map_err(|_| Status::invalid_argument("cursor must be a valid UUID"))?;

                let row: Option<(i64, i32)> = sqlx::query_as(
                    "SELECT ledger_sequence, event_index FROM soroban_events WHERE id = $1",
                )
                .bind(cursor_id)
                .fetch_optional(&db)
                .await
                .map_err(db_err)?;

                match row {
                    Some((seq, idx)) => (Some(seq), Some(idx)),
                    None => {
                        return Err(Status::invalid_argument("cursor references unknown event"))
                    }
                }
            };

            let rows: Vec<EventRow> = sqlx::query_as(
                r#"
                SELECT id, contract_id, ledger_sequence, ledger_timestamp,
                       transaction_hash, event_index, event_type, topics, data, created_at
                FROM soroban_events
                WHERE
                    network = $1
                    AND ($2::text = '' OR contract_id = $2)
                    AND ($3::text = '' OR topic_0 = $3)
                    AND ($4::text = '' OR topic_1 = $4)
                    AND ($5::bigint = 0 OR ledger_sequence >= $5)
                    AND ($6::bigint = 0 OR ledger_sequence <= $6)
                    AND (
                        $7::bigint IS NULL
                        OR (ledger_sequence, event_index) > ($7, $8)
                    )
                ORDER BY ledger_sequence ASC, event_index ASC
                LIMIT $9
                "#,
            )
            .bind(network)
            .bind(&req.contract_id)
            .bind(&req.topic_0)
            .bind(&req.topic_1)
            .bind(req.ledger_from as i64)
            .bind(req.ledger_to as i64)
            .bind(cursor_seq)
            .bind(cursor_idx)
            .bind(limit)
            .fetch_all(&db)
            .await
            .map_err(db_err)?;

            let has_more = rows.len() as i64 == limit;
            let next_cursor = if has_more {
                rows.last().map(|r| r.id.to_string()).unwrap_or_default()
            } else {
                String::new()
            };

            let events: Vec<Event> = rows.into_iter().map(row_to_event).collect();

            Ok(Response::new(ListEventsResponse {
                events,
                next_cursor,
                has_more,
            }))
        }
        .instrument(span)
        .await
    }

    async fn get_event(
        &self,
        request: Request<GetEventRequest>,
    ) -> Result<Response<Event>, Status> {
        let parent_cx = extract_context(request.metadata());
        let span = tracing::info_span!("get_event", "rpc.system" = "grpc");
        span.set_parent(parent_cx);

        let db = self.db.clone();
        let req = request.into_inner();

        validate_get_event(&req).map_err(|e| {
            tracing::warn!(error = %e, "invalid get_event request");
            e
        })?;

        async move {
            let id = Uuid::parse_str(&req.id)
                .map_err(|_| Status::invalid_argument("id must be a valid UUID"))?;

            let network = resolve_network(&req.network);

            let row: Option<EventRow> = sqlx::query_as(
                r#"
            SELECT id, contract_id, ledger_sequence, ledger_timestamp,
                   transaction_hash, event_index, event_type, topics, data, created_at
            FROM soroban_events
            WHERE id = $1
              AND network = $2
            "#,
            )
            .bind(id)
            .bind(network)
            .fetch_optional(&db)
            .await
            .map_err(db_err)?;

            match row {
                Some(r) => Ok(Response::new(row_to_event(r))),
                None => Err(Status::not_found(format!("event {id} not found"))),
            }
        }
        .instrument(span)
        .await
    }

    type StreamEventsStream = tokio_stream::wrappers::ReceiverStream<Result<Event, Status>>;

    async fn stream_events(
        &self,
        request: Request<StreamEventsRequest>,
    ) -> Result<Response<Self::StreamEventsStream>, Status> {
        let parent_cx = extract_context(request.metadata());
        let span = tracing::info_span!("stream_events", "rpc.system" = "grpc");
        span.set_parent(parent_cx);
        let _entered = span.entered();

        let req = request.into_inner();

        if req.contract_id.is_empty() {
            return Err(Status::invalid_argument("contract_id is required"));
        }

        let contract_id = req.contract_id;
        let topic_0_filter = if req.topic_0.is_empty() {
            None
        } else {
            Some(req.topic_0)
        };

        // Resume from where the client left off when it supplies a start_id
        // (issue #236); otherwise tail the stream live.
        let last_id = validate_start_id(&req.start_id)?;

        let (tx, rx) = tokio::sync::mpsc::channel(stream_channel_buffer());

        // The entered span guard is not `Send`, so it cannot live across the
        // awaits below.
        drop(_entered);

        let hub = self
            .hub
            .get_or_try_init(|| async {
                let reader = self.redis_client.get_connection_manager().await?;
                StreamHub::start(reader, self.redis.clone()).await
            })
            .await
            .map_err(|e| {
                tracing::error!(error = %e, "failed to start stream hub");
                Status::unavailable("event stream temporarily unavailable")
            })?;

        hub.subscribe(
            StreamSubscription {
                contract_id,
                topic_0: topic_0_filter,
                start_id: last_id,
            },
            tx,
        );

        Ok(Response::new(tokio_stream::wrappers::ReceiverStream::new(
            rx,
        )))
    }
}

#[cfg(test)]
mod tests {
    use tokio_stream::StreamExt as _;

    use super::*;

    // When REQUIRE_TEST_SERVICES is set (the rust-integration CI job sets it),
    // a missing URL is a hard failure instead of a silent skip — otherwise a
    // misconfigured integration job would go green without running anything.
    macro_rules! require_services {
        () => {{
            let required = std::env::var("REQUIRE_TEST_SERVICES").is_ok();
            match (
                std::env::var("TEST_DATABASE_URL"),
                std::env::var("TEST_REDIS_URL"),
            ) {
                (Ok(db), Ok(rd)) => (db, rd),
                _ if required => panic!(
                    "TEST_DATABASE_URL and TEST_REDIS_URL must be set when REQUIRE_TEST_SERVICES is set"
                ),
                _ => {
                    eprintln!("SKIP: TEST_DATABASE_URL / TEST_REDIS_URL not set");
                    return;
                }
            }
        }};
    }

    async fn make_svc(db_url: &str, redis_url: &str) -> EventsServiceImpl {
        let db = PgPool::connect(db_url).await.unwrap();
        let client = redis::Client::open(redis_url).unwrap();
        let redis = client.get_connection_manager().await.unwrap();
        EventsServiceImpl::new(db, redis, client)
    }

    async fn seed_events(pool: &PgPool, contract_id: &str, network: &str, count: usize) {
        for i in 0..count {
            sqlx::query(
                r#"
                INSERT INTO soroban_events
                    (contract_id, network, ledger_sequence, ledger_timestamp, transaction_hash,
                     event_index, event_type, topics, data)
                VALUES ($1, $2, $3, NOW(), $4, $5, 'contract', '["transfer"]', '{}')
                ON CONFLICT DO NOTHING
                "#,
            )
            .bind(contract_id)
            .bind(network)
            .bind((100 + i) as i64)
            .bind(format!("txhash_{contract_id}_{network}_{i}"))
            .bind(i as i32)
            .execute(pool)
            .await
            .unwrap();
        }
    }

    async fn insert_one_event(pool: &PgPool, network: &str) -> Uuid {
        let id = Uuid::new_v4();
        let tx_hash = format!("txhashtest-{id}");
        sqlx::query(
            r#"
            INSERT INTO soroban_events
                (id, contract_id, network, ledger_sequence, ledger_timestamp, transaction_hash,
                 event_index, event_type, topics, data)
            VALUES ($1, 'CTEST', $2, 999, NOW(), $3, 0, 'contract', '["transfer"]', '{}')
            ON CONFLICT DO NOTHING
            "#,
        )
        .bind(id)
        .bind(network)
        .bind(tx_hash)
        .execute(pool)
        .await
        .unwrap();
        id
    }

    #[tokio::test]
    async fn list_events_filters_by_contract_id() {
        let (db_url, redis_url) = require_services!();
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_a = format!("CONTRACT_A_{}", uuid::Uuid::new_v4());
        let contract_b = format!("CONTRACT_B_{}", uuid::Uuid::new_v4());

        seed_events(&pool, &contract_a, "testnet", 3).await;
        seed_events(&pool, &contract_b, "testnet", 2).await;

        let svc = make_svc(&db_url, &redis_url).await;
        let req = Request::new(ListEventsRequest {
            contract_id: contract_a.clone(),
            network: "testnet".to_string(),
            limit: 200,
            ..Default::default()
        });
        let resp = svc.list_events(req).await.unwrap().into_inner();

        assert_eq!(resp.events.len(), 3);
        assert!(resp.events.iter().all(|e| e.contract_id == contract_a));
        assert!(!resp.has_more);
    }

    #[tokio::test]
    async fn list_events_cursor_pagination() {
        let (db_url, redis_url) = require_services!();
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CONTRACT_PAGE_{}", uuid::Uuid::new_v4());
        seed_events(&pool, &contract_id, "testnet", 5).await;

        let svc = make_svc(&db_url, &redis_url).await;

        let first_page = svc
            .list_events(Request::new(ListEventsRequest {
                contract_id: contract_id.clone(),
                network: "testnet".to_string(),
                limit: 2,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(first_page.events.len(), 2);
        assert!(first_page.has_more);
        assert!(!first_page.next_cursor.is_empty());

        let second_page = svc
            .list_events(Request::new(ListEventsRequest {
                contract_id: contract_id.clone(),
                network: "testnet".to_string(),
                limit: 200,
                cursor: first_page.next_cursor,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(second_page.events.len(), 3);
        assert!(!second_page.has_more);
    }

    #[tokio::test]
    async fn list_events_isolated_by_network() {
        let (db_url, redis_url) = require_services!();
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CONTRACT_NET_{}", uuid::Uuid::new_v4());
        seed_events(&pool, &contract_id, "testnet", 3).await;
        seed_events(&pool, &contract_id, "mainnet", 2).await;

        let svc = make_svc(&db_url, &redis_url).await;

        let testnet_resp = svc
            .list_events(Request::new(ListEventsRequest {
                contract_id: contract_id.clone(),
                network: "testnet".to_string(),
                limit: 200,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        let mainnet_resp = svc
            .list_events(Request::new(ListEventsRequest {
                contract_id: contract_id.clone(),
                network: "mainnet".to_string(),
                limit: 200,
                ..Default::default()
            }))
            .await
            .unwrap()
            .into_inner();

        assert_eq!(testnet_resp.events.len(), 3);
        assert_eq!(mainnet_resp.events.len(), 2);
    }

    #[tokio::test]
    async fn get_existing_event_returns_correct_fields() {
        let (db_url, redis_url) = require_services!();
        let pool = PgPool::connect(&db_url).await.unwrap();

        let event_id = insert_one_event(&pool, "testnet").await;

        let svc = make_svc(&db_url, &redis_url).await;
        let req = Request::new(GetEventRequest {
            id: event_id.to_string(),
            network: "testnet".to_string(),
        });
        let event = svc.get_event(req).await.unwrap().into_inner();

        assert_eq!(event.id, event_id.to_string());
        assert_eq!(event.contract_id, "CTEST");
        assert_eq!(event.event_type, "contract");
        assert_eq!(event.topics, vec!["transfer".to_string()]);
    }

    #[tokio::test]
    async fn get_event_wrong_network_returns_not_found() {
        let (db_url, redis_url) = require_services!();
        let pool = PgPool::connect(&db_url).await.unwrap();

        let event_id = insert_one_event(&pool, "testnet").await;

        let svc = make_svc(&db_url, &redis_url).await;
        let req = Request::new(GetEventRequest {
            id: event_id.to_string(),
            network: "mainnet".to_string(),
        });
        let err = svc.get_event(req).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_unknown_uuid_returns_not_found() {
        let (db_url, redis_url) = require_services!();
        let svc = make_svc(&db_url, &redis_url).await;

        let req = Request::new(GetEventRequest {
            id: Uuid::new_v4().to_string(),
            network: "testnet".to_string(),
        });
        let err = svc.get_event(req).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn list_events_empty_topic_0_is_valid() {
        let (db_url, redis_url) = require_services!();
        let pool = PgPool::connect(&db_url).await.unwrap();

        let contract_id = format!("CONTRACT_EMPTY_{}", uuid::Uuid::new_v4());
        seed_events(&pool, &contract_id, "testnet", 2).await;

        let svc = make_svc(&db_url, &redis_url).await;
        let req = Request::new(ListEventsRequest {
            contract_id: contract_id.clone(),
            topic_0: String::new(),
            network: "testnet".to_string(),
            limit: 200,
            ..Default::default()
        });
        let resp = svc.list_events(req).await.unwrap().into_inner();

        assert_eq!(resp.events.len(), 2);
    }

    #[tokio::test]
    async fn list_events_long_contract_id_returns_invalid_argument() {
        let (db_url, redis_url) = require_services!();
        let svc = make_svc(&db_url, &redis_url).await;

        let req = Request::new(ListEventsRequest {
            contract_id: "X".repeat(257),
            network: "testnet".to_string(),
            limit: 200,
            ..Default::default()
        });
        let err = svc.list_events(req).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn get_event_empty_id_returns_invalid_argument() {
        let (db_url, redis_url) = require_services!();
        let svc = make_svc(&db_url, &redis_url).await;

        let req = Request::new(GetEventRequest {
            id: String::new(),
            network: "testnet".to_string(),
        });
        let err = svc.get_event(req).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn list_events_with_invalid_cursor_returns_invalid_argument() {
        let (db_url, redis_url) = require_services!();
        let svc = make_svc(&db_url, &redis_url).await;

        let req = Request::new(ListEventsRequest {
            contract_id: "CTEST".to_string(),
            network: "testnet".to_string(),
            cursor: "not-a-uuid".to_string(),
            limit: 200,
            ..Default::default()
        });
        let err = svc.list_events(req).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn db_error_returns_unavailable() {
        let (db_url, redis_url) = require_services!();
        let svc = make_svc(&db_url, &redis_url).await;

        let req = Request::new(GetEventRequest {
            id: Uuid::new_v4().to_string(),
            network: "nonexistent".to_string(),
        });
        let err = svc.get_event(req).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::NotFound);
    }

    #[tokio::test]
    async fn get_malformed_uuid_returns_invalid_argument() {
        let (db_url, redis_url) = require_services!();
        let svc = make_svc(&db_url, &redis_url).await;

        let req = Request::new(GetEventRequest {
            id: "not-a-uuid".to_string(),
            network: "testnet".to_string(),
        });
        let err = svc.get_event(req).await.unwrap_err();

        assert_eq!(err.code(), tonic::Code::InvalidArgument);
    }

    #[tokio::test]
    async fn stream_events_delivers_published_event() {
        let (db_url, redis_url) = require_services!();
        let svc = make_svc(&db_url, &redis_url).await;

        let req = Request::new(StreamEventsRequest {
            contract_id: "CTEST_STREAM".to_string(),
            topic_0: String::new(),
            start_id: String::new(),
        });

        let mut stream = svc.stream_events(req).await.unwrap().into_inner();

        tokio::time::sleep(Duration::from_millis(100)).await;
        let mut pub_conn = redis::Client::open(redis_url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();
        let _: String = redis::cmd("XADD")
            .arg(REDIS_STREAM_KEY)
            .arg("*")
            .arg("contract_id")
            .arg("CTEST_STREAM")
            .arg("ledger_sequence")
            .arg("777")
            .arg("ledger_timestamp")
            .arg("2024-01-01T00:00:00Z")
            .arg("transaction_hash")
            .arg("txhashstream")
            .arg("event_index")
            .arg("0")
            .arg("event_type")
            .arg("contract")
            .arg("topics")
            .arg(r#"["transfer"]"#)
            .arg("data")
            .arg("null")
            .query_async(&mut pub_conn)
            .await
            .unwrap();

        let event: Event = tokio::time::timeout(Duration::from_secs(8), stream.next())
            .await
            .expect("timed out waiting for streamed event")
            .expect("stream ended unexpectedly")
            .expect("stream returned error");

        assert_eq!(event.contract_id, "CTEST_STREAM");
        assert_eq!(event.ledger_sequence, 777);
    }

    // -----------------------------------------------------------------------
    // Resume, cancellation, and buffering (issue #236)
    // -----------------------------------------------------------------------

    #[test]
    fn empty_start_id_means_live_tail() {
        assert_eq!(validate_start_id("").unwrap(), "$");
    }

    #[test]
    fn explicit_stream_ids_are_accepted() {
        assert_eq!(validate_start_id("$").unwrap(), "$");
        assert_eq!(validate_start_id("0").unwrap(), "0");
        assert_eq!(
            validate_start_id("1700000000000-0").unwrap(),
            "1700000000000-0"
        );
    }

    #[test]
    fn malformed_start_ids_are_rejected() {
        for bad in ["abc", "123", "1-2-3", "-1", "1-", "1700000000000-x"] {
            let err =
                validate_start_id(bad).expect_err(&format!("{bad:?} should have been rejected"));
            assert_eq!(err.code(), tonic::Code::InvalidArgument);
        }
    }

    #[tokio::test]
    async fn stream_resumes_from_a_supplied_start_id() {
        let (db_url, redis_url) = require_services!();
        let svc = make_svc(&db_url, &redis_url).await;

        let mut pub_conn = redis::Client::open(redis_url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let contract = format!("CRESUME_{}", Uuid::new_v4());

        // Published *before* subscribing: a live tail would never see this.
        let first_id: String = redis::cmd("XADD")
            .arg(REDIS_STREAM_KEY)
            .arg("*")
            .arg("contract_id")
            .arg(&contract)
            .arg("ledger_sequence")
            .arg("500")
            .arg("ledger_timestamp")
            .arg("2024-01-01T00:00:00Z")
            .arg("transaction_hash")
            .arg("txresume")
            .arg("event_index")
            .arg("0")
            .arg("event_type")
            .arg("contract")
            .arg("topics")
            .arg(r#"["transfer"]"#)
            .arg("data")
            .arg("null")
            .query_async(&mut pub_conn)
            .await
            .unwrap();

        // Resume from just before that entry by decrementing the sequence part.
        let (ms, _seq) = first_id.split_once('-').unwrap();
        let resume_from = format!("{}-0", ms.parse::<u64>().unwrap() - 1);

        let req = Request::new(StreamEventsRequest {
            contract_id: contract.clone(),
            topic_0: String::new(),
            start_id: resume_from,
        });
        let mut stream = svc.stream_events(req).await.unwrap().into_inner();

        let event: Event = tokio::time::timeout(Duration::from_secs(8), stream.next())
            .await
            .expect("timed out waiting for the replayed event")
            .expect("stream ended unexpectedly")
            .expect("stream returned error");

        assert_eq!(event.contract_id, contract);
        assert_eq!(
            event.ledger_sequence, 500,
            "resume must replay the entry published before subscribing"
        );
    }

    async fn xadd_event(conn: &mut redis::aio::MultiplexedConnection, contract: &str, seq: u32) {
        let _: String = redis::cmd("XADD")
            .arg(REDIS_STREAM_KEY)
            .arg("*")
            .arg("contract_id")
            .arg(contract)
            .arg("ledger_sequence")
            .arg(seq)
            .arg("ledger_timestamp")
            .arg("2024-01-01T00:00:00Z")
            .arg("transaction_hash")
            .arg("txhub")
            .arg("event_index")
            .arg("0")
            .arg("event_type")
            .arg("contract")
            .arg("topics")
            .arg(r#"["transfer"]"#)
            .arg("data")
            .arg("null")
            .query_async(conn)
            .await
            .unwrap();
    }

    #[tokio::test]
    async fn dropping_the_receiver_unregisters_the_subscriber() {
        let (db_url, redis_url) = require_services!();
        let svc = make_svc(&db_url, &redis_url).await;

        let req = Request::new(StreamEventsRequest {
            contract_id: "CTEST_CANCEL".to_string(),
            topic_0: String::new(),
            start_id: String::new(),
        });
        let stream = svc.stream_events(req).await.unwrap().into_inner();
        let hub = svc.hub.get().unwrap().clone();
        assert!(hub.state.lock().unwrap().subs.contains_key("CTEST_CANCEL"));

        drop(stream);

        tokio::time::timeout(Duration::from_secs(2), async {
            while hub.state.lock().unwrap().subs.contains_key("CTEST_CANCEL") {
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .expect("subscriber was not unregistered after the receiver was dropped");
    }

    /// Redis read volume must not grow with the number of subscribers: one
    /// reader serves them all (issue #742). Compares the XREAD calls and bytes
    /// Redis serves for the same publish workload at 10 vs 1,000 subscribers on
    /// distinct contracts.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn redis_read_volume_is_independent_of_subscriber_count() {
        let (db_url, redis_url) = require_services!();
        let svc = make_svc(&db_url, &redis_url).await;
        let mut admin = redis::Client::open(redis_url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        async fn xread_stats(conn: &mut redis::aio::MultiplexedConnection) -> (u64, u64) {
            let stats: String = redis::cmd("INFO")
                .arg("commandstats")
                .query_async(conn)
                .await
                .unwrap();
            let calls = stats
                .lines()
                .find(|l| l.starts_with("cmdstat_xread:"))
                .and_then(|l| l.split("calls=").nth(1))
                .and_then(|r| r.split(',').next())
                .and_then(|n| n.parse().ok())
                .unwrap_or(0);
            let server: String = redis::cmd("INFO")
                .arg("stats")
                .query_async(conn)
                .await
                .unwrap();
            let out = server
                .lines()
                .find(|l| l.starts_with("total_net_output_bytes:"))
                .and_then(|l| l.split(':').nth(1))
                .and_then(|n| n.trim().parse().ok())
                .unwrap_or(0);
            (calls, out)
        }

        let measure = |subscribers: usize| {
            let svc = &svc;
            async move {
                let mut streams = Vec::new();
                for i in 0..subscribers {
                    let req = Request::new(StreamEventsRequest {
                        contract_id: format!("CLOAD_{subscribers}_{i}"),
                        topic_0: String::new(),
                        start_id: String::new(),
                    });
                    streams.push(svc.stream_events(req).await.unwrap().into_inner());
                }
                streams
            }
        };

        let mut publisher = redis::Client::open(redis_url.as_str())
            .unwrap()
            .get_multiplexed_async_connection()
            .await
            .unwrap();

        let mut results = Vec::new();
        for subscribers in [10usize, 1_000] {
            let streams = measure(subscribers).await;
            tokio::time::sleep(Duration::from_millis(500)).await;

            let (calls_before, bytes_before) = xread_stats(&mut admin).await;
            for n in 0..200u32 {
                xadd_event(
                    &mut publisher,
                    &format!("CLOAD_{subscribers}_{}", n as usize % subscribers),
                    n,
                )
                .await;
            }
            tokio::time::sleep(Duration::from_secs(2)).await;
            let (calls_after, bytes_after) = xread_stats(&mut admin).await;

            results.push((calls_after - calls_before, bytes_after - bytes_before));
            drop(streams);
            tokio::time::sleep(Duration::from_millis(500)).await;
        }

        let (small_calls, small_bytes) = results[0];
        let (large_calls, large_bytes) = results[1];
        eprintln!("10 subs: {small_calls} XREAD calls, {small_bytes} bytes; 1000 subs: {large_calls} XREAD calls, {large_bytes} bytes");
        assert!(
            large_calls <= small_calls * 2 + 5,
            "XREAD calls grew with subscribers: {small_calls} -> {large_calls}"
        );
        assert!(
            large_bytes <= small_bytes * 2 + 50_000,
            "Redis output bytes grew with subscribers: {small_bytes} -> {large_bytes}"
        );
    }
}
