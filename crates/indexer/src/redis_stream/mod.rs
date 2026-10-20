pub mod relay;

#[cfg(test)]
use redis::{streams::StreamMaxlen, AsyncCommands};
use trident_common::{SorobanEvent, TridentError};

const STREAM_KEY: &str = "trident:events";

/// One `XADD` per round trip. Production publishes through [`publish_events`];
/// this is kept as the baseline for the relay throughput benchmark (#740).
#[cfg(test)]
pub async fn publish_event(
    conn: &mut redis::aio::MultiplexedConnection,
    event: &SorobanEvent,
    maxlen: u64,
    event_id: Option<&str>,
) -> Result<(), TridentError> {
    let fields = entry_fields(event, event_id)?;

    let _: String = conn
        .xadd_maxlen(
            STREAM_KEY,
            StreamMaxlen::Approx(maxlen as usize),
            "*",
            &fields,
        )
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("redis xadd")))?;

    Ok(())
}

/// Publish a batch of events in a single pipelined round trip (issue #740).
///
/// Each entry is `(event, event_id)`. The pipeline is not atomic: if it fails,
/// an unknown prefix of the batch may already be on the stream. Callers must
/// treat the whole batch as unpublished and retry, which is safe because
/// delivery is at-least-once and consumers dedupe on `event_id`.
pub async fn publish_events(
    conn: &mut redis::aio::MultiplexedConnection,
    events: &[(&SorobanEvent, &str)],
    maxlen: u64,
) -> Result<(), TridentError> {
    if events.is_empty() {
        return Ok(());
    }

    let mut pipe = redis::pipe();
    for (event, event_id) in events {
        let fields = entry_fields(event, Some(event_id))?;
        pipe.cmd("XADD")
            .arg(STREAM_KEY)
            .arg("MAXLEN")
            .arg("~")
            .arg(maxlen)
            .arg("*")
            .arg(&fields)
            .ignore();
    }

    pipe.query_async::<_, ()>(conn)
        .await
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("redis xadd pipeline")))
}

fn entry_fields(
    event: &SorobanEvent,
    event_id: Option<&str>,
) -> Result<Vec<(&'static str, String)>, TridentError> {
    let topics = serde_json::to_string(&event.topics)
        .map_err(|e| TridentError::storage(anyhow::Error::new(e).context("topics serialise")))?;
    let mut fields = vec![
        ("contract_id", event.contract_id.clone()),
        ("ledger_sequence", event.ledger_sequence.to_string()),
        ("ledger_timestamp", event.ledger_timestamp.clone()),
        ("transaction_hash", event.transaction_hash.clone()),
        ("event_index", event.event_index.to_string()),
        (
            "event_type",
            format!("{:?}", event.event_type).to_lowercase(),
        ),
        ("topics", topics),
        ("data", event.data.to_string()),
    ];
    if let Some(id) = event_id {
        fields.push(("event_id", id.to_string()));
    }
    Ok(fields)
}
