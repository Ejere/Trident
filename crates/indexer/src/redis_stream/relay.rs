//! Outbox relay: drains `event_outbox` onto the Redis Stream (issue #200).
//!
//! The poll loop commits events with an unpublished outbox row; this task
//! publishes them. It runs on its own interval and takes a bounded batch per
//! pass so it can never starve the poll loop or monopolise the database.
//!
//! Failure handling is deliberately conservative: a batch stops at the first
//! publish failure and only the rows published *before* it are marked, so
//! ordering is preserved and nothing is lost. The next pass resumes at the
//! failed row. That makes delivery at-least-once — consumers dedupe by event
//! id (see `db::outbox`).

use std::time::Duration;

use sqlx::PgPool;
use tokio_util::sync::CancellationToken;
use trident_common::TridentError;

use crate::db::outbox;
use crate::metrics;
use crate::redis_stream;

/// Tuning for [`OutboxRelay`].
#[derive(Debug, Clone)]
pub struct RelayConfig {
    /// How often the relay scans for unpublished rows.
    pub interval: Duration,
    /// Maximum rows published per pass.
    pub batch_size: i64,
    /// Backlog size above which the relay logs an alert-worthy warning.
    pub backlog_alert_threshold: i64,
    /// Redis stream trim length, mirrors the direct-publish path.
    pub stream_maxlen: u64,
}

pub struct OutboxRelay {
    db: PgPool,
    redis: redis::aio::MultiplexedConnection,
    config: RelayConfig,
}

impl OutboxRelay {
    pub fn new(db: PgPool, redis: redis::aio::MultiplexedConnection, config: RelayConfig) -> Self {
        Self { db, redis, config }
    }

    /// Run until `shutdown` is cancelled, publishing pending events every
    /// `interval`. A failing pass is logged and retried on the next tick rather
    /// than taking the process down: the rows stay unpublished, so nothing is
    /// dropped.
    pub async fn run(&mut self, shutdown: CancellationToken) {
        tracing::info!(
            interval_ms = self.config.interval.as_millis() as u64,
            batch_size = self.config.batch_size,
            "Outbox relay started"
        );

        loop {
            if shutdown.is_cancelled() {
                break;
            }

            match self.publish_pending().await {
                Ok(published) if published > 0 => {
                    tracing::debug!(published, "Outbox batch published");
                }
                Ok(_) => {}
                Err(e) => {
                    tracing::warn!(error = %e, "Outbox relay pass failed, retrying next interval");
                }
            }

            tokio::select! {
                _ = tokio::time::sleep(self.config.interval) => {}
                _ = shutdown.cancelled() => break,
            }
        }

        tracing::info!("Outbox relay stopped");
    }

    /// Publish one bounded batch. Returns how many events reached Redis.
    ///
    /// The batch is claimed inside a transaction with `FOR UPDATE SKIP LOCKED`,
    /// published in one pipelined round trip, and marked published before the
    /// transaction commits. A Redis failure rolls the claim back so the whole
    /// batch is retried; a crash after the pipeline succeeds but before commit
    /// re-delivers it, which consumers dedupe by event id.
    pub async fn publish_pending(&mut self) -> Result<usize, TridentError> {
        let mut tx = self.db.begin().await.map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("outbox relay begin"))
        })?;

        let records = outbox::fetch_unpublished(&mut tx, self.config.batch_size).await?;
        if records.is_empty() {
            drop(tx);
            self.report_backlog(0).await;
            return Ok(0);
        }

        let mut done_seqs: Vec<i64> = Vec::with_capacity(records.len());
        let mut decoded = Vec::with_capacity(records.len());
        for record in &records {
            match record.event() {
                Ok(event) => decoded.push((record.seq, record.event_id.to_string(), event)),
                Err(e) => {
                    // A payload we cannot decode will never publish. Mark it so
                    // it stops blocking the queue; the row is retained for audit.
                    tracing::error!(
                        seq = record.seq,
                        event_id = %record.event_id,
                        error = %e,
                        "Undecodable outbox payload, skipping"
                    );
                    done_seqs.push(record.seq);
                }
            }
        }

        let batch: Vec<(&trident_common::SorobanEvent, &str)> = decoded
            .iter()
            .map(|(_, id, event)| (event, id.as_str()))
            .collect();
        if let Err(e) =
            redis_stream::publish_events(&mut self.redis, &batch, self.config.stream_maxlen).await
        {
            metrics::record_outbox_publish_failure();
            // Dropping `tx` rolls back and releases the row locks.
            return Err(e);
        }
        for (seq, _, _) in &decoded {
            done_seqs.push(*seq);
            metrics::record_outbox_published();
        }

        outbox::mark_published(&mut tx, &done_seqs).await?;
        tx.commit().await.map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("outbox relay commit"))
        })?;
        self.report_backlog(done_seqs.len() as i64).await;

        Ok(done_seqs.len())
    }

    /// Publish the current backlog gauge, warning when it grows past the
    /// configured threshold (the alert signal for a stuck relay).
    async fn report_backlog(&self, _published: i64) {
        match outbox::backlog(&self.db).await {
            Ok(backlog) => {
                metrics::set_outbox_backlog(backlog);
                if backlog >= self.config.backlog_alert_threshold {
                    tracing::warn!(
                        backlog,
                        threshold = self.config.backlog_alert_threshold,
                        "Outbox backlog above alert threshold — live subscribers are falling behind"
                    );
                }
            }
            Err(e) => tracing::warn!(error = %e, "Failed to read outbox backlog"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::insert_outbox_batch;
    use std::collections::HashMap;
    use std::time::Instant;
    use trident_common::{EventType, SorobanEvent};

    const STREAM_KEY: &str = "trident:events";

    fn config(batch_size: i64) -> RelayConfig {
        RelayConfig {
            interval: Duration::from_millis(10),
            batch_size,
            backlog_alert_threshold: i64::MAX,
            stream_maxlen: 1_000_000,
        }
    }

    fn make_events(n: u32) -> Vec<SorobanEvent> {
        (0..n)
            .map(|i| SorobanEvent {
                contract_id: "CRELAYTEST".to_string(),
                ledger_sequence: 1_000_000 + (i / 10) as u64,
                ledger_timestamp: "2024-01-01T00:00:00Z".to_string(),
                transaction_hash: format!("tx{i}"),
                event_index: i,
                event_type: EventType::Contract,
                topics: vec!["transfer".to_string()],
                data: serde_json::json!({ "n": i }),
            })
            .collect()
    }

    /// Connects to the shared test services and seeds `n` unpublished outbox
    /// rows on an empty outbox and stream. Returns `None` when the services
    /// are not configured (mirrors the other integration tests).
    async fn setup(n: u32) -> Option<(PgPool, redis::aio::MultiplexedConnection, redis::Client)> {
        let (db_url, redis_url) = match (
            std::env::var("TEST_DATABASE_URL"),
            std::env::var("TEST_REDIS_URL"),
        ) {
            (Ok(d), Ok(r)) => (d, r),
            _ if std::env::var("REQUIRE_TEST_SERVICES").is_ok() => {
                panic!("TEST_DATABASE_URL and TEST_REDIS_URL must be set when REQUIRE_TEST_SERVICES is set");
            }
            _ => {
                eprintln!("SKIP: TEST_DATABASE_URL / TEST_REDIS_URL not set");
                return None;
            }
        };
        let pool = PgPool::connect(&db_url).await.unwrap();
        let client = redis::Client::open(redis_url).unwrap();
        let mut conn = client.get_multiplexed_tokio_connection().await.unwrap();

        sqlx::query("DELETE FROM event_outbox")
            .execute(&pool)
            .await
            .unwrap();
        let _: () = redis::cmd("DEL")
            .arg(STREAM_KEY)
            .query_async(&mut conn)
            .await
            .unwrap();

        for chunk in make_events(n).chunks(5_000) {
            insert_outbox_batch(&pool, chunk).await.unwrap();
        }
        Some((pool, conn, client))
    }

    async fn stream_event_ids(conn: &mut redis::aio::MultiplexedConnection) -> Vec<String> {
        let reply: redis::streams::StreamRangeReply = redis::cmd("XRANGE")
            .arg(STREAM_KEY)
            .arg("-")
            .arg("+")
            .query_async(conn)
            .await
            .unwrap();
        reply
            .ids
            .into_iter()
            .filter_map(|entry| entry.get::<String>("event_id"))
            .collect()
    }

    /// Two relays draining one backlog must publish every row exactly once.
    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn two_relays_publish_each_row_exactly_once() {
        const ROWS: u32 = 500;
        let Some((pool, conn, client)) = setup(ROWS).await else {
            return;
        };

        let conn_b = client.get_multiplexed_tokio_connection().await.unwrap();
        let mut handles = Vec::new();
        for redis in [conn.clone(), conn_b] {
            let mut relay = OutboxRelay::new(pool.clone(), redis, config(25));
            handles.push(tokio::spawn(async move {
                let mut total = 0;
                loop {
                    let n = relay.publish_pending().await.unwrap();
                    if n == 0 {
                        break;
                    }
                    total += n;
                }
                total
            }));
        }
        let mut published = 0;
        for h in handles {
            published += h.await.unwrap();
        }
        assert_eq!(published, ROWS as usize);

        let mut conn = conn;
        let ids = stream_event_ids(&mut conn).await;
        let mut counts: HashMap<&str, usize> = HashMap::new();
        for id in &ids {
            *counts.entry(id.as_str()).or_default() += 1;
        }
        assert_eq!(ids.len(), ROWS as usize, "stream length");
        assert!(
            counts.values().all(|&c| c == 1),
            "every event must be published exactly once"
        );
        assert_eq!(outbox::backlog(&pool).await.unwrap(), 0);
    }

    /// Drain throughput of a large backlog: one XADD per round trip (the
    /// previous behaviour) versus the pipelined relay. Opt-in:
    /// `BENCH_OUTBOX_ROWS=50000 cargo test -p trident-indexer bench_outbox -- --ignored --nocapture`
    #[tokio::test]
    #[ignore]
    async fn bench_outbox_drain_throughput() {
        let rows: u32 = std::env::var("BENCH_OUTBOX_ROWS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(50_000);

        let Some((pool, mut conn, _client)) = setup(rows).await else {
            return;
        };
        let records = {
            let mut tx = pool.begin().await.unwrap();
            outbox::fetch_unpublished(&mut tx, rows as i64)
                .await
                .unwrap()
        };
        let decoded: Vec<_> = records
            .iter()
            .map(|r| (r.event().unwrap(), r.event_id.to_string()))
            .collect();

        let start = Instant::now();
        for (event, id) in &decoded {
            redis_stream::publish_event(&mut conn, event, 1_000_000, Some(id))
                .await
                .unwrap();
        }
        let sequential = start.elapsed();

        let _: () = redis::cmd("DEL")
            .arg(STREAM_KEY)
            .query_async(&mut conn)
            .await
            .unwrap();

        let mut relay = OutboxRelay::new(pool.clone(), conn.clone(), config(1_000));
        let start = Instant::now();
        while relay.publish_pending().await.unwrap() > 0 {}
        let pipelined = start.elapsed();

        let rate = |d: Duration| rows as f64 / d.as_secs_f64();
        println!(
            "sequential: {:?} ({:.0} rows/s)  pipelined relay: {:?} ({:.0} rows/s)  speedup: {:.1}x",
            sequential,
            rate(sequential),
            pipelined,
            rate(pipelined),
            sequential.as_secs_f64() / pipelined.as_secs_f64()
        );
    }
}
