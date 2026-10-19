# Data freshness: ingest lag as a public contract

Consumers of the Trident API need to answer one question before trusting
anything else it returns: **is this data caught up with the chain?** This
document defines "freshness" precisely — the metric, the public API field,
and how the two relate — so the answer is a documented contract rather than
an implementation detail (issue #294).

## The two numbers

Freshness is expressed two ways, computed from the same underlying value:

1. **`lag_ledgers`** — the chain tip ledger sequence minus the last ledger
   the indexer has committed. The ground-truth figure; whole ledgers, exact.
2. **`lag_seconds_estimated`** — `lag_ledgers * ~5s` (Stellar's
   protocol-target ledger close time). An *estimate*, not a measurement: the
   indexer does not retain per-ledger close timestamps once a page is
   processed, so this is lag_ledgers converted to a human-scale "how stale,
   roughly, in wall-clock time" figure rather than a tracked rolling average.
   Treat it as an order-of-magnitude signal, not a precise SLA number — use
   `lag_ledgers` if you need exactness.

Both numbers are null together whenever the chain tip is unknown (RPC lookup
failed — see below) or the indexer has not indexed anything yet.

## Where each number lives

| Surface | Fields | Notes |
|---|---|---|
| Indexer Prometheus metrics (`crates/indexer/src/metrics.rs`, port 9090) | `trident_indexer_ledger_lag`, `trident_indexer_ledger_lag_seconds_estimated` | Set together by `metrics::set_ledger_lag`, computed every poll cycle from the RPC's `latestLedger` vs. the indexer's own cursor — no separate chain-tip lookup needed. |
| API service Prometheus metrics (`services/api/handlers/stats.go`, `GET /metrics`) | `trident_indexer_lag_ledgers`, `trident_indexer_lag_seconds_estimated` | Set on each `GET /v1/stats/indexer` request from the same computation the JSON response uses (see below) — not scraped independently from the indexer process. |
| Public REST API | `GET /v1/stats/indexer` → `lag_ledgers`, `lag_seconds_estimated`, `last_ledger_indexed`, `chain_tip_ledger`, `status` | The stable, documented contract external consumers should build against — see `api/openapi.yaml`. No API key required. |

The indexer's own gauge and the API's `/v1/stats/indexer` figure are computed
independently (the indexer measures lag against the RPC page it just
processed; the API measures it against a cached, separately-fetched chain
tip) and can disagree briefly — by design, since the API's view is what
external consumers see and must not depend on the indexer's internal metrics
port being reachable.

**How `lag_seconds_estimated` is computed on the API:** when
`ledger_metadata` has the close time of the last indexed ledger, the API
reports the wall-clock time since that close (`now - ledger_timestamp`).
Only when that row is missing does it fall back to
`lag_ledgers x LEDGER_CLOSE_SECONDS` (default 5.5s, the observed mainnet
average; protocol target is ~5s). The indexer's own gauge
(`AVG_LEDGER_CLOSE_SECONDS` in `crates/indexer/src/metrics.rs`) still uses a
fixed 5.0.

## `status` semantics

`GET /v1/stats/indexer`'s `status` field is the coarse, three-value summary:

| `status` | Condition | HTTP status |
|---|---|---|
| `healthy` | Last poll within the stall threshold and lag_ledgers ≤ the lag threshold (or unknown) | 200 |
| `lagging` | Last poll within the stall threshold but lag_ledgers > the lag threshold | 200 |
| `stalled` | No successful poll within the stall threshold | 503 |

The thresholds are configurable and default from the indexer's adaptive-poll
ceiling (`POLL_INTERVAL_CEILING_MS`, set it on the API as well), because a
caught-up indexer legitimately polls only once per ceiling interval, and
backs off up to 4x when the RPC is degraded:

- stall threshold: `INDEXER_STALL_THRESHOLD_SECONDS`, default
  `max(60s, 5 x ceiling)` (4x degraded backoff plus one interval of margin).
- lag threshold: `INDEXER_LAG_THRESHOLD_LEDGERS`, default
  `max(10, ceil(ceiling / LEDGER_CLOSE_SECONDS) + 10)` ledgers.

With the default 5s ceiling these are 60s and 11 ledgers; with a 600s ceiling
they are 3000s and 120 ledgers. Keep `ALERT_LAG_THRESHOLD` above the lag
threshold so alerts fire only after the status page has already flagged lag.

`stalled` is the only case that returns a non-2xx status — `lagging` is still
a 200 because the data is real, just behind, which is a materially different
situation for a consumer than "the indexer isn't running."

## Alerting

See `docs/slo.md` (SLO 1 — Ingest freshness) for the burn-rate-based alerting
built on `trident_indexer_ledger_lag`, and `observability/burn-rate-alerts.yml`
for the rule definitions, including `TridentIngestLagSustainedHigh`, a direct
threshold alert on sustained high lag independent of the SLO error-budget
calculation (issue #293).
