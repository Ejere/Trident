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
| `TESTNET_RPC_URL` | Optional (test-only) | — | Testnet correctness suite | Stellar RPC endpoint for the scheduled ingest-correctness suite (issue #419). Unset skips the suite. The public `https://soroban-testnet.stellar.org` needs no credentials. |
| `TESTNET_CONTRACT_ID` | Optional (test-only) | unset | Testnet correctness suite | Scopes the correctness proof to one known contract. Unset verifies every event in the range, which is the stronger check. |
| `TESTNET_LEDGER_SPAN` | Optional (test-only) | `400` | Testnet correctness suite | Ledger span to verify. Public testnet retains only a rolling window (~120k ledgers), so a very large span fails as out-of-range. |
| `REQUIRE_TESTNET_CORRECTNESS` | Optional (test-only) | unset | Testnet correctness suite | When set, a missing `TESTNET_RPC_URL` is a hard test failure instead of a silent skip, so the scheduled job cannot report green without testing. |
| `OTEL_EXPORTER_OTLP_ENDPOINT` | Optional | empty (tracing disabled) | indexer, grpc-api, go-api | OTLP gRPC endpoint for distributed tracing (issue #81). |
| `OTEL_SAMPLING_RATIO` | Optional | `0.1` | indexer, grpc-api, go-api | Fraction of traces sampled. |
| `TOKIO_CONSOLE_ENABLED` | Optional | `false` | indexer | Enables `tokio-console` diagnostics (only takes effect when built with the `tokio-console` cargo feature). |

## Indexer (`crates/indexer`)

| Variable | Required | Default | Description |
|---|---|---|---|
| `STELLAR_RPC_URL` | Required unless `STELLAR_RPC_URLS` set | — | Single Soroban RPC endpoint. |
| `STELLAR_RPC_URLS` | Optional | — | Prioritised, comma-separated RPC endpoints for failover; overrides `STELLAR_RPC_URL` (which stays valid as a single-value alias). |
| `STELLAR_RPC_HEADERS` | Optional | none | JSON object of HTTP headers sent with **every** RPC request, for providers that authenticate by header (issue #735), e.g. `{"Authorization":"Bearer …","x-api-key":"…"}`. Values are secrets: they are never logged, and a malformed spec fails startup naming the header only. |
| `STELLAR_RPC_HEADERS_<N>` | Optional | none | JSON object of headers for endpoint `N` (0-based, matching `STELLAR_RPC_URLS`), merged over `STELLAR_RPC_HEADERS` by name. An index with no matching endpoint is a startup error. Also read by the backfill worker (`STELLAR_RPC_HEADERS_0`) and the Go API, which call a single endpoint. |
| `NETWORK` | Optional | `testnet` | One of `mainnet` \| `testnet`, or the `pubnet` alias for `mainnet`. The value is normalised once at startup (so `pubnet` behaves identically to `mainnet` everywhere: cursor key, row tagging, passphrase and timeouts). `futurenet` is not supported by the indexer and is a startup error. |
| `NETWORK_PASSPHRASE` | Required for non-standard networks | inferred for testnet/mainnet/pubnet (see below) | Stellar network passphrase, used to derive SAC contract ids for `TRACKED_SAC_ASSETS`. |
| `POLL_INTERVAL_MS` | Removed | — | No longer read by the poll loop (it never actually was — only the adaptive floor/ceiling/watermark below control pacing). Setting it is a hard startup error naming `POLL_INTERVAL_FLOOR_MS`/`POLL_INTERVAL_CEILING_MS` instead of being silently ignored. |
| `POLL_INTERVAL_FLOOR_MS` | Optional | `250` (min `50`, max `60000`) | Adaptive-poll floor (issue #198): fastest interval, used when lag is high. |
| `POLL_INTERVAL_CEILING_MS` | Optional | `5000` (min `100`, max `600000`) | Adaptive-poll ceiling: slowest interval, used when caught up. |
| `LAG_HIGH_WATERMARK` | Optional | `100` (min `1`, max `100000000`) | Ledger lag at/above which the floor interval applies. |
| `POLL_HYSTERESIS_LEDGERS` | Optional | `10` (min `0`, max `1000000`) | Hysteresis deadband to prevent oscillation around the watermark. |
| `MAX_REORG_REWIND_DEPTH` | Removed | — | Never affected behaviour (issue #737): reorg rewinds are bounded by `MAX_REORG_DEPTH`. Setting it is a hard startup error naming the replacement instead of being silently ignored. |
| `GAP_SCAN_MAX_PER_RUN` | Optional | `100` (min `1`, max `10000`) | Maximum gaps enqueued as `backfill_jobs` rows per periodic `ledger_metadata` gap scan (issue #216). |
| `MAX_EVENTS_PER_POLL` | Optional | `200` (min `1`, max `10000`) | Max events fetched per `getEvents` RPC call. |
| `DB_BATCH_SIZE` | Optional | `1000` (min `1`, max `10000`) | Max rows per batched INSERT when a page commits. |
| `INDEXER_DB_POOL_SIZE` | Optional | `3` | Indexer's own Postgres pool size. |
| `INDEX_TOPIC_FILTERS` | Optional | none (no narrowing) | Comma-separated topic patterns pushed into the RPC filter alongside the contract allowlist. |
| `INDEX_DIAGNOSTIC` | Optional | `false` | Store Soroban diagnostic events (high-volume; keep `false` in production). |
| `EVENT_RETENTION_DAYS` | Optional | unset (no pruning) | Days of `soroban_events` history to retain before the retention job prunes older rows (issue #482). |
| `RPC_STARTUP_CHECK_ENABLED` | Optional | `true` | Verify RPC connectivity before the poll loop starts; disabling it means an unreachable endpoint only surfaces once polling begins (issue #687). |
| `TRACKED_SAC_ASSETS` | Optional | none | Assets to derive SAC contract ids for and track. |
| `REDIS_STREAM_MAXLEN` | Optional | `10000` | Max events kept in the Redis stream before trimming. |
| `METRICS_PORT` | Optional | `9090` | Prometheus `/metrics` port; also doubles as the indexer's liveness/readiness signal. |
| `HEALTH_PORT` | Optional | `8080` | Port serving `/healthz` and `/readyz`. |
| `RUST_LOG` | Optional | `info` | Log verbosity (`error`\|`warn`\|`info`\|`debug`\|`trace`). |
| `DB_STATEMENT_TIMEOUT_MS` | Optional | `30000` | Postgres per-statement timeout bound. |
| `DB_IDLE_IN_TRANSACTION_TIMEOUT_MS` | Optional | `10000` | Postgres `idle_in_transaction_session_timeout` bound. |
| `TOKEN_METADATA_REFRESH_INTERVAL_SECS` | Optional | `86400` | How often cached token metadata refreshes. |
| `EVENT_RETENTION_DAYS` | Optional | unset (pruning disabled) | Days of event history the daily pruning job keeps before deleting older `soroban_events` rows (issue #482). Unset disables pruning; a set value must be an integer in `[1, 3650]` and anything else fails startup (issue #738). |

### Inferred network passphrases

When `NETWORK_PASSPHRASE` is unset, the indexer infers it from `NETWORK`
(`crates/indexer/src/config.rs`, `default_network_passphrase`) for the two
well-known networks only:

| `NETWORK` | Inferred `NETWORK_PASSPHRASE` |
|---|---|
| `testnet` | `Test SDF Network ; September 2015` |
| `mainnet` \| `pubnet` | `Public Global Stellar Network ; September 2015` |

`futurenet` (and any other value) has no inferred passphrase — startup fails
config validation unless `NETWORK_PASSPHRASE` is set explicitly, since
guessing would silently derive wrong SAC contract ids.

### RPC transport and failover

| Variable | Required | Default | Description |
|---|---|---|---|
| `RPC_CONNECT_TIMEOUT_MS` | Optional | `5000` testnet / `10000` mainnet (min `100`, max `60000`) | TCP connect timeout. Mainnet's higher default accounts for materially higher p99 latency under real contract/event volume; see "Mainnet RPC timeout tuning" in `deployment.md`. |
| `RPC_REQUEST_TIMEOUT_MS` | Optional | `30000` testnet / `45000` mainnet (min `500`, max `600000`) | Overall RPC request timeout; must be >= the connect timeout. |
| `RPC_POOL_IDLE_TIMEOUT_MS` | Optional | `90000` (min `1000`, max `600000`) | Idle pooled-connection lifetime. |
| `RPC_POOL_MAX_IDLE_PER_HOST` | Optional | `8` (min `1`, max `1024`) | Idle keep-alive connections retained per RPC host. |
| `RPC_TCP_KEEPALIVE_MS` | Optional | `60000` (min `1000`, max `600000`) | TCP keep-alive probe interval. |
| `RPC_FAILOVER_THRESHOLD` | Removed | — | Never affected behaviour (issue #737); `RPC_BREAKER_FAILURE_THRESHOLD` is the effective knob. Setting it is a hard startup error. |
| `RPC_ENDPOINT_COOLDOWN_MS` | Removed | — | Never affected behaviour (issue #737); `RPC_BREAKER_COOLDOWN_MS` is the effective knob. Setting it is a hard startup error. |
| `RPC_BREAKER_FAILURE_THRESHOLD` | Optional | `5` (min `1`, max `1000`) | Consecutive RPC-layer poll failures before the circuit breaker opens and the run loop stops attempting polls (issue #197). |
| `RPC_BREAKER_COOLDOWN_MS` | Optional | `30000` (min `1000`, max `3600000`) | How long the breaker stays open before allowing a single probe poll through. |

### Outbox relay

| Variable | Required | Default | Description |
|---|---|---|---|
| `OUTBOX_POLL_INTERVAL_MS` | Optional | `100` (min `10`, max `60000`) | How often the relay scans for unpublished events. |
| `OUTBOX_BATCH_SIZE` | Optional | `500` (min `1`, max `10000`) | Max events published per relay pass. |
| `OUTBOX_BACKLOG_ALERT_THRESHOLD` | Optional | `10000` (min `1`, max `10000000`) | Backlog size that logs an alert-worthy warning. |
| `RECONCILE_ENABLED` | Optional | `true` | Runs the ledger-range reconciliation loop that compares indexed event counts against the RPC source (#511). Disabling it means nothing verifies indexed data against the chain. |
| `RECONCILE_INTERVAL_MS` | Optional | `600000` (min `10000`, max `86400000`) | Time between reconciliation passes. |
| `RECONCILE_LEDGER_SPAN` | Optional | `400` (min `10`, max `100000`) | Settled ledgers each pass compares. |
| `RECONCILE_TIP_MARGIN` | Optional | `100` (min `0`, max `10000`) | Distance behind the chain tip the window sits, so in-flight ledgers never read as discrepancies. |

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
| `API_KEY` | Optional | empty | Single accepted HMAC-SHA256 API key hash, salted with `API_KEY_SALT`. |
| `API_KEY_SALT` | Optional but should be changed | `change-this-to-a-random-string` | Salt for API key hashing. |
| `ALLOWED_ORIGINS` | Required in production | — (dev mode allows any origin) | Comma-separated CORS allow-list (`https://` origins, or `http://localhost*`). |
| `REQUEST_TIMEOUT_MS` | Optional | `30000` | Per-request timeout middleware; excludes `/ws` and `/v1/events/stream`. |
| `MAX_WS_CONNECTIONS` | Optional | `1000` | Max concurrent WebSocket connections before new ones are rejected. |
| `REDIS_STREAM_KEY` | Optional | `trident:events` | Redis stream key used for event pub/sub and webhook consumption; must match the indexer's stream. |
| `WEBHOOK_CONSUMER_GROUP` | Optional | `trident-webhooks` | Redis Stream consumer-group name for the webhook delivery worker. |
| `WEBHOOK_CONSUMER_NAME` | Optional | `webhook-worker` | Redis Stream consumer name for the webhook delivery worker. |
| `WEBHOOK_SECRET_OVERLAP_HOURS` | Optional | `24` | Hours a rotated webhook secret stays valid after `POST /v1/webhooks/{id}/rotate-secret`. The hourly cleanup job clears `secondary_secret` once this window passes, so a rotated secret is actually revoked. Values that are non-positive or unparseable fall back to the default. |
| `RATE_LIMIT_FREE_RPS` | Optional | `10` | Requests/sec limit, free tier. |
| `RATE_LIMIT_PRO_RPS` | Optional | `100` | Requests/sec limit, pro/standard tier. |
| `RATE_LIMIT_INTERNAL_RPS` | Optional | `1000` | Requests/sec limit, internal tier. |
| `PER_IP_RATE_LIMIT_RPS` | Optional | `20` | Pre-auth per-IP request limit, applied before any API key is checked (issue #318). |
| `PER_IP_RATE_LIMIT_WINDOW_MS` | Optional | `1000` | Window for `PER_IP_RATE_LIMIT_RPS`. |
| `MAX_IN_FLIGHT_REQUESTS` | Optional | `500` | Global concurrency cap; requests beyond it are shed rather than queued. |
| `TRUSTED_PROXY_ENABLED` | Optional | `false` | **Security-sensitive.** When `true`, the per-IP limiter attributes requests to the last hop in `X-Forwarded-For` instead of the TCP peer. Only enable when the API sits behind a proxy that *appends* to XFF (as `docker/nginx/nginx.conf` does) and is not directly reachable. Enabling it on a directly-reachable API lets any client forge its own source IP and bypass per-IP limiting. |
| `RETENTION_AUDIT_LOG_DAYS` | Optional | `90` | Days to retain audit log rows. |
| `RETENTION_PARSE_ERRORS_DAYS` | Optional | `30` | Days to retain parse-error rows. |
| `RETENTION_WEBHOOK_DELIVERIES_DAYS` | Optional | `30` | Days to retain webhook delivery records. |
| `RETENTION_SOROBAN_EVENTS_DAYS` | Optional | `0` (disabled) | Days to retain Soroban events; `0` disables pruning. |
| `RETENTION_EVENT_OUTBOX_DAYS` | Optional | `7` | Days to retain *published* event_outbox rows (issue #604). An unpublished row (`published = FALSE`) is never eligible regardless of age. |
| `PPROF_ENABLED` | Optional | `false` | Enables the internal-only pprof profiling server. Never exposed publicly — bind it to loopback/localhost only. |
| `PPROF_ADDR` | Optional | `127.0.0.1:6060` | Bind address for the pprof server, when enabled. |
| `GRPC_MTLS_ENABLED` / `GRPC_MTLS_CA_CERT` / `GRPC_MTLS_CLIENT_CERT` / `GRPC_MTLS_CLIENT_KEY` | Optional (see grpc-api section) | — | Client-side counterpart of the grpc-api mTLS flag (issue #320); `services/api/grpc/client.go`. |
| `IDEMPOTENCY_ENCRYPTION_KEY` | Required when `APP_ENV=production` | random key generated at startup | 64 hex characters (32 bytes), AES-256 key used to encrypt idempotency records at rest in Redis (issue #572) — `POST /v1/api-keys` and `POST /v1/webhooks` return a live credential in their body, and `middleware.Idempotency` stores that body for `DefaultIdempotencyTTL` (24h). `middleware.ValidateIdempotencyEncryptionConfig` refuses to start the API when `APP_ENV=production` and this is unset or malformed: with more than one replica, each replica otherwise generates its own random key and can't decrypt another replica's record, which can double-create a resource on retry. Outside production, left unset, a random key is generated per process (records then don't survive a restart). |
| `IDEMPOTENCY_ENCRYPTION_KEY_PREVIOUS` | Optional | unset | Set only while rotating `IDEMPOTENCY_ENCRYPTION_KEY` — decrypt falls back to this key for records written under it before the rotation, so they still decrypt during the overlap window. See docs/deployment.md#rotating-idempotency_encryption_key. |

## Docker Compose only

| Variable | Required | Default | Description |
|---|---|---|---|
| `POSTGRES_USER` / `POSTGRES_PASSWORD` / `POSTGRES_DB` | Optional (compose only) | `trident` / `password` / `trident` | Postgres container bootstrap credentials. |
| `LOG_LEVEL` | Optional | `info` | Go API log verbosity (`.env.ci`/compose convenience; not read directly by Rust services, which use `RUST_LOG`). |
| `APP_ENV` | Required in production | unset | Go API log *format*, not verbosity. `production` emits JSON for log aggregation; anything else (including unset) emits human-readable text. Pair with `LOG_LEVEL`, which controls the threshold. Also gates `IDEMPOTENCY_ENCRYPTION_KEY`'s startup check above — set `production` and the API refuses to start without that key configured. |

## Production secret inventory

This table is the launch inventory for values that are credentials or can
embed credentials. Non-secret configuration remains in Helm/Fly values; these
values must live in the platform's managed store and must never be committed,
passed as Docker build arguments, or printed in logs.

| Secret | Requirement and purpose | Format | Managed source | Rotation owner |
|---|---|---|---|---|
| `DATABASE_URL` | Required by every service and migrations; authenticates to PostgreSQL/PgBouncer. | PostgreSQL URI with TLS parameters where supported: `postgres://user:password@host:port/database?...` | External Secrets backend / Fly Postgres secret | Database/SRE owner |
| `REDIS_URL` | Required by indexer, gRPC API, and Go API; authenticates to Redis. | `redis://` or `rediss://` URI; use `rediss://` outside a private trusted network. | External Secrets backend / Fly secrets | Database/SRE owner |
| `STELLAR_RPC_URL` / `STELLAR_RPC_URLS` | One is required by the indexer. Treat as secret when the provider URL contains a token. | One HTTPS URL, or a comma-separated ordered list of HTTPS URLs. | External Secrets backend / Fly secrets | Indexer service owner |
| `ADMIN_API_KEY` | Required for administrative API operations. | At least 32 cryptographically random bytes, encoded as 64 hex characters or base64. | External Secrets backend / Fly secrets | Security owner |
| `INTERNAL_API_KEY` | Required when `/internal/status` is used. | At least 32 cryptographically random bytes, encoded as hex or base64. | External Secrets backend / Fly secrets | Platform/SRE owner |
| Raw API keys, including `STAGING_API_KEY` | Authenticate API clients and the staging smoke test. | At least 32 cryptographically random bytes; store only the raw client copy and the server-side HMAC/hash. | DB-backed key issuer; staging copy in GitHub Environment secrets | API service owner |
| `API_KEY_SALT` | Required in production to HMAC API keys; the built-in development value is forbidden. | 32 random bytes encoded as 64 lowercase hex characters. | External Secrets backend / Fly secrets | Security owner |
| `IDEMPOTENCY_ENCRYPTION_KEY` / `IDEMPOTENCY_ENCRYPTION_KEY_PREVIOUS` | Former required when `APP_ENV=production` (startup fails otherwise); latter set only mid-rotation. Encrypts idempotency records and lets every Go API replica decrypt records any other replica wrote. | 32 random bytes encoded as 64 lowercase hex characters. | External Secrets backend / Fly secrets | Security owner |
| `API_KEY_HASHES` | Required only for the legacy environment-backed key path. | Comma-separated lowercase 64-character HMAC-SHA256 digests. | External Secrets backend / Fly secrets | API service owner |
| `PGBOUNCER_ADMIN_URL` | Required only for the admin pool-stat endpoint; carries PgBouncer credentials. | PostgreSQL URI for the PgBouncer admin database. | External Secrets backend / Fly secrets | Database/SRE owner |
| `ALERT_WEBHOOK_URL` / `STAGING_ALERT_WEBHOOK_URL` | Optional alert destinations; webhook URLs commonly contain bearer tokens. | Provider-issued HTTPS webhook URL. | External Secrets/Fly secrets; GitHub Environment secret for staging | Observability/SRE owner |
| Credential-bearing `OTEL_EXPORTER_OTLP_ENDPOINT` | Required only when the collector authenticates through its URL. | HTTPS OTLP endpoint; prefer a separate secret header mechanism if supported. | External Secrets backend / Fly secrets | Observability/SRE owner |
| `INTERNAL_SERVER_KEY` / `INTERNAL_CLIENT_KEY` | Required when internal gRPC mTLS is enabled. | Unencrypted PKCS#8 PEM private keys, readable only by the workload identity. | External Secrets backend or cert-manager-managed Kubernetes Secret | Security/PKI owner |
| `INTERNAL_CA_CERT`, `INTERNAL_SERVER_CERT`, `INTERNAL_CLIENT_CERT` | Required with internal gRPC mTLS. Certificates are public material but are managed with their private keys. | PEM-encoded X.509 certificate/bundle with SANs matching service DNS names. | External Secrets backend or cert-manager-managed Kubernetes Secret | Security/PKI owner |
| `STAGING_KUBECONFIG` | Required for the staging deployment workflow. | Base64 of a minimal kubeconfig scoped to the staging namespace. | GitHub `staging` Environment secret | Platform/SRE owner |
| `PRODUCTION_KUBECONFIG` | Required for the production deployment workflow. | Base64 of a minimal kubeconfig scoped to the production namespace. | GitHub `production` Environment secret | Platform/SRE owner |
| `NPM_TOKEN` | Required by SDK publishing workflows. | npm automation or granular access token limited to the Trident package scope. | GitHub release Environment secret | Release engineering owner |
| `GITHUB_TOKEN` | Required for GHCR, releases, and security tooling; issued automatically per job. | GitHub-generated ephemeral token with job-minimal permissions. | GitHub Actions automatic token | Repository administrators |

Helm workloads read secrets only from `global.existingSecret`. Production
must populate that Secret from the configured `ExternalSecret`/managed
backend (or a managed CSI provider); committed `values*.yaml` files contain
only remote key references. Fly applications source the same values through
`fly secrets`; secret names and values are forbidden in each file's `[env]`
table. GitHub credentials use repository or Environment secrets, not Actions
variables. Environment secrets should be preferred so approval and branch
protection can be applied to staging and release credentials.

## Rotation procedures

All rotations must be recorded in the owning team's audit log. Verify the new
credential before revoking the old one, and never paste a value into an issue,
PR, command trace, or CI output.

### Database and Redis credentials

1. Create a new least-privilege database/Redis credential while the old one
   remains valid.
2. Update `DATABASE_URL`, `PGBOUNCER_ADMIN_URL`, or `REDIS_URL` in the managed
   backend (`fly secrets set` for Fly, or the External Secrets provider for
   Helm). Force an ExternalSecret refresh when waiting for its normal interval
   is unacceptable.
3. Restart every consuming workload because services do not hot-reload
   environment variables. Verify readiness, migrations, event publication,
   and API reads.
4. Revoke the old credential and confirm it can no longer connect.

### API and shared authentication keys

- For DB-backed API keys, issue the replacement, update the client, verify it,
  then revoke the old key. For `API_KEY_HASHES`, temporarily include both old
  and new digests, migrate clients, then remove the old digest.
- `ADMIN_API_KEY` and `INTERNAL_API_KEY` currently accept one value. Schedule a
  coordinated cutover: distribute the replacement through the consumers'
  managed stores, update the server store and restart, verify, then destroy the
  old value. Never send either key through chat or deployment logs.
- Rotating `API_KEY_SALT` invalidates every legacy `API_KEY_HASHES` digest and
  changes rate-limit/cache identities; DB-backed SHA-256 keys still
  authenticate. Generate the new legacy digests from securely held raw keys,
  or reissue those keys if the raw values are unavailable, then update the salt
  and digest list together. Expect rate-limit counters/caches to start fresh.

### Stellar RPC, alert webhook, and telemetry credentials

Create a replacement token/URL at the provider, update the managed store,
restart the consuming service or rerun the workflow, and verify one successful
RPC/export/notification before revoking the old endpoint credential. If the
provider cannot overlap credentials, use a planned cutover window.

### Internal mTLS certificates and keys

Issue a new CA/leaf set before expiry. For a CA rotation, first deploy a bundle
containing both old and new CAs, then rotate server and client leaf
certificates, verify mutual authentication, and finally remove the old CA and
private keys. Update the external backend/cert-manager Secret and restart both
the Go and gRPC API deployments after each phase.

### GitHub Actions credentials

- Rotate `STAGING_API_KEY` through the API-key overlap procedure, then replace
  the GitHub Environment secret. Replace `STAGING_KUBECONFIG` by issuing a new
  namespace-scoped identity, test a staging deploy, and revoke the old identity.
- Replace `STAGING_DATABASE_URL` and `STAGING_ALERT_WEBHOOK_URL` using their
  respective class procedure above.
- Create a new least-privilege `NPM_TOKEN`, replace the release Environment
  secret, publish/verify a test or next release, then revoke the old token.
- `GITHUB_TOKEN` is ephemeral and rotates automatically for every job; review
  workflow `permissions` whenever its use changes.

## Audit controls

- `.github/workflows/security.yml` checks out with `fetch-depth: 0` and
  runs `gitleaks detect`, scanning the complete repository history rather than
  only the pull-request diff. The 2026-08-26 launch audit scanned all 280
  commits and found no leaks.
- `.gitleaks.toml` contains the only false-positive documentation: fixed,
  publicly-known `.env.ci` test credentials, allowlisted by both value and
  path. Production-shaped findings must never be allowlisted.
- `.dockerignore` prevents local environment files and private key material
  from entering any build context. The security workflow builds all four
  shipped images and runs `scripts/check-image-history.sh`; the check inspects
  untruncated layer commands and final image Env/Labels without printing a
  matching layer that could contain a secret.
- GitHub workflow secrets are passed through masked step environments, never
  interpolated directly into shell source. Service connection errors pass
  through `redactConnErr`, and the Go tests cover connection-URL and raw API-key
  log redaction. The launch source audit found no log statement that emits a
  secret-bearing environment value.

## CI-only

### Live OpenAPI contract suite

`services/api/internal/contracttest` validates a running API's responses
against `api/openapi.yaml`. It is skipped unless `CONTRACTTEST_BASE_URL` is
set, so these are read only by the `e2e` job — never by a running service.

| Variable | Required | Description |
|---|---|---|
| `CONTRACTTEST_BASE_URL` | Yes, to run the suite | Base URL of the API under test. Unset skips the whole suite. |
| `CONTRACTTEST_API_KEY` | Yes, when the suite runs | Key for authenticated endpoints. |
| `CONTRACTTEST_ADMIN_KEY` | Yes, when the suite runs | Key for the `/v1/admin/*` endpoints. |
| `CONTRACTTEST_REDIS_URL` | Yes, when the suite runs | Redis used to seed stream fixtures before asserting on `/v1/events/stream`. |

`.env.ci` additionally pins a fixed, publicly-known test API key/salt/hash —
documented inline in that file as "do not use outside CI".
