# Environment variable reference

This is the canonical list of every environment variable actually read by
Trident's services. It exists to keep `.env.example` (local dev) and
`.env.ci` (CI smoke tests) honest — see the enforcement mechanism below
(issue #312).

**Source of truth order**: this file documents what the code reads; if you
add or remove an `os.Getenv` / `std::env::var` call, update this file (and
`.env.example`, and `.env.ci` if CI needs a value for it) in the same change.

## Enforcement

`scripts/check-env-reference.sh` greps the indexer, backfill, gRPC API, and
Go API source for every environment variable name they read and fails if any
of them is missing from this file. It runs in CI as the `env-reference` job
in `.github/workflows/ci.yml`. It is a grep-based check (not a compiler
plugin), so it can have false positives on non-env-var ALL_CAPS identifiers —
those are allow-listed in the script itself, not silently ignored.

It only checks "does this file mention the var" — it does not verify the
description is accurate. Keep this file honest by hand.

## Shared

| Variable | Required | Default | Read by | Description |
|---|---|---|---|---|
| `DATABASE_URL` | Required | — | indexer, grpc-api, go-api | PostgreSQL connection string. Production should go through PgBouncer, not directly to Postgres. |
| `REDIS_URL` | Required | — | indexer, go-api | Redis connection string. The indexer publishes events here; the Go API consumes them for WebSocket fan-out and webhook delivery. |
| `TEST_DATABASE_URL` | Optional (test-only) | — | Rust integration tests | Real Postgres used by `#[ignore]`d integration tests; also gates `REQUIRE_TEST_SERVICES`. |
| `TEST_REDIS_URL` | Optional (test-only) | — | Rust integration tests | Real Redis used by `#[ignore]`d integration tests. |
| `REQUIRE_TEST_SERVICES` | Optional (test-only) | unset | Rust integration tests | When set, a missing `TEST_DATABASE_URL`/`TEST_REDIS_URL` is a hard test failure instead of a silent skip. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Optional | empty (tracing disabled) | indexer, grpc-api, go-api | OTLP gRPC endpoint for distributed tracing (issue #81). |
| `OTEL_SAMPLING_RATIO` | Optional | `0.1` | indexer, grpc-api, go-api | Fraction of traces sampled. |
| `TOKIO_CONSOLE_ENABLED` | Optional | `false` | indexer | Enables `tokio-console` diagnostics (only takes effect when built with the `tokio-console` cargo feature). |

## Indexer (`crates/indexer`)

| Variable | Required | Default | Description |
|---|---|---|---|
| `STELLAR_RPC_URL` | Required unless `STELLAR_RPC_URLS` set | — | Single Soroban RPC endpoint. |
| `STELLAR_RPC_URLS` | Optional | — | Prioritised, comma-separated RPC endpoints for failover; overrides `STELLAR_RPC_URL` (which stays valid as a single-value alias). |
| `INDEXER_STALL_THRESHOLD_SECONDS` | Optional | `max(60, 5 x POLL_INTERVAL_CEILING_MS)` | Seconds since the last poll after which `GET /v1/stats/indexer` reports `stalled` (issue #759). Go API; set `POLL_INTERVAL_CEILING_MS` on the API to match the indexer. |
| `INDEXER_LAG_THRESHOLD_LEDGERS` | Optional | `max(10, ceil(ceiling / LEDGER_CLOSE_SECONDS) + 10)` | Ledger lag above which the status is `lagging`. |
| `LEDGER_CLOSE_SECONDS` | Optional | `5.5` | Average ledger close time, used for the lag-seconds fallback when `ledger_metadata` has no timestamp for the last indexed ledger. |
| `NETWORK` | Optional | `testnet` | One of `mainnet` \| `testnet` \| `futurenet`. |
| `NETWORK_PASSPHRASE` | Required for non-standard networks | inferred for testnet/mainnet/pubnet | Stellar network passphrase, used to derive SAC contract ids for `TRACKED_SAC_ASSETS`. |
| `POLL_INTERVAL_MS` | Optional | `1000` (min `100`, max `60000`) | Ledger poll interval. |
| `POLL_INTERVAL_FLOOR_MS` | Optional | `250` (min `50`, max `60000`) | Adaptive-poll floor (issue #198): fastest interval, used when lag is high. |
| `POLL_INTERVAL_CEILING_MS` | Optional | `5000` (min `100`, max `600000`) | Adaptive-poll ceiling: slowest interval, used when caught up. |
| `LAG_HIGH_WATERMARK` | Optional | `100` (min `1`, max `100000000`) | Ledger lag at/above which the floor interval applies. |
| `POLL_HYSTERESIS_LEDGERS` | Optional | `10` (min `0`, max `1000000`) | Hysteresis deadband to prevent oscillation around the watermark. |
| `MAX_EVENTS_PER_POLL` | Optional | `200` (min `1`, max `10000`) | Max events fetched per `getEvents` RPC call. |
| `DB_BATCH_SIZE` | Optional | `1000` (min `1`, max `10000`) | Max rows per batched INSERT when a page commits. |
| `INDEXER_DB_POOL_SIZE` | Optional | `3` | Indexer's own Postgres pool size. |
| `INDEX_TOPIC_FILTERS` | Optional | none (no narrowing) | Comma-separated topic patterns pushed into the RPC filter alongside the contract allowlist. |
| `INDEX_DIAGNOSTIC` | Optional | `false` | Store Soroban diagnostic events (high-volume; keep `false` in production). |
| `TRACKED_SAC_ASSETS` | Optional | none | Assets to derive SAC contract ids for and track. |
| `REDIS_STREAM_MAXLEN` | Optional | `10000` | Max events kept in the Redis stream before trimming. |
| `METRICS_PORT` | Optional | `9090` | Prometheus `/metrics` port; also doubles as the indexer's liveness/readiness signal. |
| `HEALTH_PORT` | Optional | `8080` | Port serving `/healthz` and `/readyz`. |
| `RUST_LOG` | Optional | `info` | Log verbosity (`error`\|`warn`\|`info`\|`debug`\|`trace`). |
| `DB_STATEMENT_TIMEOUT_MS` | Optional | `30000` | Postgres per-statement timeout bound. |
| `DB_IDLE_IN_TRANSACTION_TIMEOUT_MS` | Optional | `10000` | Postgres `idle_in_transaction_session_timeout` bound. |
| `TOKEN_METADATA_REFRESH_INTERVAL_SECS` | Optional | `86400` | How often cached token metadata refreshes. |

### RPC transport and failover

| Variable | Required | Default | Description |
|---|---|---|---|
| `RPC_CONNECT_TIMEOUT_MS` | Optional | `5000` (min `100`, max `60000`) | TCP connect timeout. |
| `RPC_REQUEST_TIMEOUT_MS` | Optional | `30000` (min `500`, max `600000`) | Overall RPC request timeout; must be >= the connect timeout. |
| `RPC_POOL_IDLE_TIMEOUT_MS` | Optional | `90000` (min `1000`, max `600000`) | Idle pooled-connection lifetime. |
| `RPC_POOL_MAX_IDLE_PER_HOST` | Optional | `8` (min `1`, max `1024`) | Idle keep-alive connections retained per RPC host. |
| `RPC_TCP_KEEPALIVE_MS` | Optional | `60000` (min `1000`, max `600000`) | TCP keep-alive probe interval. |
| `RPC_FAILOVER_THRESHOLD` | Optional | `3` (min `1`, max `100`) | Consecutive failures before an endpoint is parked. |
| `RPC_ENDPOINT_COOLDOWN_MS` | Optional | `30000` (min `1000`, max `3600000`) | Cooldown before a parked endpoint is retried. |

### Outbox relay

| Variable | Required | Default | Description |
|---|---|---|---|
| `OUTBOX_POLL_INTERVAL_MS` | Optional | `100` (min `10`, max `60000`) | How often the relay scans for unpublished events. |
| `OUTBOX_BATCH_SIZE` | Optional | `500` (min `1`, max `10000`) | Max events published per relay pass. |
| `OUTBOX_BACKLOG_ALERT_THRESHOLD` | Optional | `10000` (min `1`, max `10000000`) | Backlog size that logs an alert-worthy warning. |

### Lag alerting

| Variable | Required | Default | Description |
|---|---|---|---|
| `ALERT_WEBHOOK_URL` | Optional | empty (disabled) | Outbound webhook URL for lag/recovery alerts. |
| `ALERT_LAG_THRESHOLD` | Optional | `200` (min `1`, max `1000000`) | Ledger lag that triggers an alert. |
| `ALERT_COOLDOWN_MINUTES` | Optional | `30` (min `1`, max `10080`) | Minimum minutes between repeated alerts. |

## Rust gRPC API (`crates/api`)

| Variable | Required | Default | Description |
|---|---|---|---|
| `GRPC_ADDR` | Required | — | Address the gRPC server binds to (also the address the Go API dials). |
| `GRPC_API_DB_POOL_SIZE` | Optional | `10` | Postgres pool size for this service. |
| `STREAM_CHANNEL_BUFFER` | Optional | `128` | In-flight event buffer per `StreamEvents` subscriber; a full buffer backpressures that subscriber's Redis consumer rather than growing an unbounded queue. |
| `GRPC_MTLS_ENABLED` | Optional | `false` | Behind-a-flag internal mTLS (issue #320): require + verify a client cert before accepting RPCs. See `docs/kubernetes.md#internal-mtls`. |
| `GRPC_MTLS_CA_CERT` | Required if `GRPC_MTLS_ENABLED=true` | — | Path to the CA bundle used to verify the Go API's client cert. |
| `GRPC_MTLS_SERVER_CERT` | Required if `GRPC_MTLS_ENABLED=true` | — | Path to this service's TLS server certificate. |
| `GRPC_MTLS_SERVER_KEY` | Required if `GRPC_MTLS_ENABLED=true` | — | Path to this service's TLS server private key. |

## Go REST API (`services/api`)

| Variable | Required | Default | Description |
|---|---|---|---|
| `API_GRPC_ADDR` | Required | — | Address of the upstream Rust gRPC API, as validated by `services/api/config`. **Known inconsistency**: `services/api/main.go` currently dials the gRPC backend using a *different*, directly-read var, `GRPC_ADDR` (falling back to `localhost:5000`), not `config.APIGrpcAddr`. `helm/trident/values.yaml`'s `goApi.env` sets `GRPC_ADDR` (matching what `main.go` actually uses); `.env.example` documents `API_GRPC_ADDR` (matching `config.go`, which is validated at startup but whose value is otherwise unused by `main.go`). Until this is unified, set both to the same value locally and in any custom Helm overrides. |
| `PORT` | Optional | `3000` | HTTP listen port. |
| `GO_API_DB_POOL_SIZE` | Optional | `5` | Postgres pool size for this service (per replica). |
| `PGBOUNCER_ADMIN_URL` | Optional | — | PgBouncer admin console connection for `GET /v1/admin/db`. |
| `ADMIN_API_KEY` | Optional | empty (admin endpoints disabled) | Shared secret for `X-Admin-Key`, gating `/v1/admin/*`. |
| `INTERNAL_API_KEY` | Required to use `/internal/status` (fails closed) | empty | Shared secret for `X-Internal-Key`, gating `GET /internal/status` (issue #316). **Unset means the endpoint rejects every request** — never treat empty as "no auth needed". Compared with `crypto/subtle.ConstantTimeCompare`. |
| `API_KEY_HASHES` | Optional | empty | Comma-separated HMAC-SHA256 hashes of accepted API keys, salted with `API_KEY_SALT`. |
| `API_KEY_SALT` | Optional but should be changed | `change-this-to-a-random-string` | Salt for API key hashing. |
| `ALLOWED_ORIGINS` | Required in production | — (dev mode allows any origin) | Comma-separated CORS allow-list (`https://` origins, or `http://localhost*`). |
| `REQUEST_TIMEOUT_MS` | Optional | `30000` | Per-request timeout middleware; excludes `/ws` and `/v1/events/stream`. |
| `MAX_WS_CONNECTIONS` | Optional | `1000` | Max concurrent WebSocket connections before new ones are rejected. |
| `REDIS_STREAM_KEY` | Optional | `trident:events` | Redis stream key used for event pub/sub and webhook consumption; must match the indexer's stream. |
| `WEBHOOK_CONSUMER_GROUP` | Optional | `trident-webhooks` | Redis Stream consumer-group name for the webhook delivery worker. |
| `WEBHOOK_CONSUMER_NAME` | Optional | `webhook-worker` | Redis Stream consumer name for the webhook delivery worker. |
| `WEBHOOK_DELIVERY_WORKERS` | Optional | `16` | Size of the delivery worker pool. Deliveries are queued in the `webhook_outbox` table and delivered independently per subscription (issue #747). |
| `WEBHOOK_RETRY_BASE_SECONDS` | Optional | `10` | First retry delay; doubles on each failed attempt (issue #748). |
| `WEBHOOK_RETRY_MAX_BACKOFF_SECONDS` | Optional | `3600` | Cap on the delay between retries. |
| `WEBHOOK_RETRY_WINDOW_HOURS` | Optional | `24` | How long after a delivery is queued it is retried before being dead-lettered. |
| `RATE_LIMIT_FREE_RPS` | Optional | `10` | Requests/sec limit, free tier. |
| `RATE_LIMIT_PRO_RPS` | Optional | `100` | Requests/sec limit, pro/standard tier. |
| `RATE_LIMIT_INTERNAL_RPS` | Optional | `1000` | Requests/sec limit, internal tier. |
| `PER_IP_RATE_LIMIT_RPS` | Optional | `20` | Pre-auth per-IP request limit, applied before any API key is checked (issue #318). |
| `PER_IP_RATE_LIMIT_WINDOW_MS` | Optional | `1000` | Window for `PER_IP_RATE_LIMIT_RPS`. |
| `MAX_IN_FLIGHT_REQUESTS` | Optional | `500` | Global concurrency cap; requests beyond it are shed rather than queued. |
| `TRUSTED_PROXY_ENABLED` | Optional | `false` | **Security-sensitive.** When `true`, the per-IP limiter attributes requests to the last hop in `X-Forwarded-For` instead of the TCP peer. Only enable when the API sits behind a proxy that *appends* to XFF (as `docker/nginx/nginx.conf` does) and is not directly reachable. Enabling it on a directly-reachable API lets any client forge its own source IP and bypass per-IP limiting. |
| `RETENTION_AUDIT_LOG_DAYS` | Optional | `90` | Days to retain audit log rows. |
| `RETENTION_PARSE_ERRORS_DAYS` | Optional | `30` | Days to retain parse-error rows. |
| `RETENTION_WEBHOOK_DELIVERIES_DAYS` | Optional | `30` | Days to retain webhook delivery records, including dead letters. This is the only webhook cleanup; there is no separate hardcoded window. |
| `RETENTION_SOROBAN_EVENTS_DAYS` | Optional | `0` (disabled) | Days to retain Soroban events; `0` disables pruning. |
| `PPROF_ENABLED` | Optional | `false` | Enables the internal-only pprof profiling server. Never exposed publicly — bind it to loopback/localhost only. |
| `PPROF_ADDR` | Optional | `127.0.0.1:6060` | Bind address for the pprof server, when enabled. |
| `GRPC_MTLS_ENABLED` / `GRPC_MTLS_CA_CERT` / `GRPC_MTLS_CLIENT_CERT` / `GRPC_MTLS_CLIENT_KEY` | Optional (see grpc-api section) | — | Client-side counterpart of the grpc-api mTLS flag (issue #320); `services/api/grpc/client.go`. |

## Docker Compose only

| Variable | Required | Default | Description |
|---|---|---|---|
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | Optional (compose only) | `trident` / `password` / `trident` | Postgres container bootstrap credentials. |
| `LOG_LEVEL` | Optional | `info` | Go API log verbosity (`.env.ci`/compose convenience; not read directly by Rust services, which use `RUST_LOG`). |

## CI-only

`.env.ci` additionally pins a fixed, publicly-known test API key/salt/hash —
documented inline in that file as "do not use outside CI".
