//! Transactional outbox storage for at-least-once Redis delivery (issue #200).
//!
//! Writing the event to Postgres and then publishing to Redis as two
//! independent steps loses events: if the process dies (or Redis errors) after
//! the commit, the event exists in Postgres but never reaches subscribers and
//! there is no replay path. Instead the event row and an `event_outbox` row are
//! written in the *same* transaction, and a relay publishes unpublished rows
//! afterwards.
//!
//! Delivery is at-least-once by design: the relay may publish a row and crash
//! before marking it published, so the same event can be delivered twice.
//! Consumers must dedupe on the event id (`event_id` here, `id` on the stream
//! entry) — exactly-once is explicitly not the target.

use serde_json::Value;
use sqlx::{PgConnection, PgPool};
use trident_common::{SorobanEvent, TridentError};
use uuid::Uuid;

/// An unpublished outbox row ready to be relayed to Redis.
#[derive(Debug, Clone)]
pub struct OutboxRecord {
    /// Monotonic sequence, also the publish order.
    pub seq: i64,
    /// Deterministic event id, used by consumers to dedupe re-deliveries.
    pub event_id: Uuid,
    /// Serialised [`SorobanEvent`].
    pub payload: Value,
}

impl OutboxRecord {
    /// Decode the stored payload back into a `SorobanEvent`.
    pub fn event(&self) -> Result<SorobanEvent, TridentError> {
        serde_json::from_value(self.payload.clone()).map_err(|e| {
            TridentError::storage(anyhow::Error::new(e).context("outbox payload decode"))
        })
    }
}

/// Claim the oldest unpublished rows in insertion order.
///
/// Rows are locked with `FOR UPDATE SKIP LOCKED` until the surrounding
/// transaction ends, so concurrent relays partition the backlog instead of
/// publishing the same rows twice (issue #740). The caller must keep the
/// transaction open through publish and [`mark_published`].
///
/// `limit` bounds the batch so the relay can never monopolise the database or
/// starve the poll loop.
pub async fn fetch_unpublished(
    conn: &mut PgConnection,
    limit: i64,
) -> Result<Vec<OutboxRecord>, TridentError> {
    let rows: Vec<(i64, Uuid, Value)> = sqlx::query_as(
        r#"
        SELECT seq, event_id, payload
        FROM event_outbox
        WHERE published = FALSE
        ORDER BY seq
        LIMIT $1
        FOR UPDATE SKIP LOCKED
        "#,
    )
    .bind(limit)
    .fetch_all(conn)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("fetch_unpublished")))?;

    Ok(rows
        .into_iter()
        .map(|(seq, event_id, payload)| OutboxRecord {
            seq,
            event_id,
            payload,
        })
        .collect())
}

/// Mark the given outbox rows as published.
///
/// Called only after a successful `XADD`. A crash between the publish and this
/// update re-delivers those events on the next relay pass, which is safe
/// because consumers dedupe by event id.
pub async fn mark_published(conn: &mut PgConnection, seqs: &[i64]) -> Result<(), TridentError> {
    if seqs.is_empty() {
        return Ok(());
    }

    sqlx::query(
        r#"
        UPDATE event_outbox
        SET published = TRUE, published_at = NOW()
        WHERE seq = ANY($1)
        "#,
    )
    .bind(seqs)
    .execute(conn)
    .await
    .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("mark_published")))?;

    Ok(())
}

/// Number of rows still awaiting publication. Exposed as a gauge so an
/// unbounded backlog (a stuck relay, a down Redis) is alertable.
pub async fn backlog(pool: &PgPool) -> Result<i64, TridentError> {
    let row: (i64,) = sqlx::query_as("SELECT COUNT(*) FROM event_outbox WHERE published = FALSE")
        .fetch_one(pool)
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("outbox backlog")))?;

    Ok(row.0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use trident_common::EventType;

    fn sample_event() -> SorobanEvent {
        SorobanEvent {
            contract_id: "CABC".to_string(),
            ledger_sequence: 42,
            ledger_timestamp: "2024-01-01T00:00:00Z".to_string(),
            transaction_hash: "txhash".to_string(),
            event_index: 3,
            event_type: EventType::Contract,
            topics: vec!["transfer".to_string()],
            data: json!({"amount": 10}),
        }
    }

    /// The payload must round-trip so the relay publishes exactly the event
    /// that was committed.
    #[test]
    fn payload_round_trips_through_json() {
        let event = sample_event();
        let record = OutboxRecord {
            seq: 1,
            event_id: Uuid::nil(),
            payload: serde_json::to_value(&event).unwrap(),
        };

        let decoded = record.event().unwrap();
        assert_eq!(decoded.contract_id, event.contract_id);
        assert_eq!(decoded.ledger_sequence, event.ledger_sequence);
        assert_eq!(decoded.event_index, event.event_index);
        assert_eq!(decoded.topics, event.topics);
        assert_eq!(decoded.data, event.data);
    }

    #[test]
    fn malformed_payload_is_a_storage_error() {
        let record = OutboxRecord {
            seq: 1,
            event_id: Uuid::nil(),
            payload: json!({"not": "an event"}),
        };
        let err = record.event().unwrap_err();
        assert!(err.to_string().contains("Storage error"), "got: {err}");
    }
}
