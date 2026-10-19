# Trident Production Deployment Runbook

Trident is a Stellar blockchain event indexer. The production stack runs four services under Docker Compose: `postgres`, `redis`, `indexer` (Rust), and `api` (Go), with `nginx` providing TLS termination via a prod overlay.

---

## Prerequisites

Before deploying, ensure the following are ready on the target server:

- Docker v24 or later
- Docker Compose v2 (`docker compose`, not `docker-compose`)
- DNS A record pointing your domain to the server's public IP
- TLS certificate files: `fullchain.pem` and `privkey.pem`
- Git installed

---

## First Deployment

### 1. Clone the repository

```bash
git clone https://github.com/Telocel-Labs/Trident.git
cd Trident
```

### 2. Create `.env` from the example

```bash
cp .env.example .env
```

### 3. Configure required environment variables

Open `.env` and set every value below. Do not leave defaults in production.

| Variable | Description |
|---|---|
| `DATABASE_URL` | PostgreSQL connection string, e.g. `postgresql://trident:password@postgres:5432/trident` |
| `REDIS_URL` | Redis connection string, e.g. `redis://redis:6379` |
| `STELLAR_RPC_URL` | Soroban RPC endpoint (`https://soroban-testnet.stellar.org` for testnet) |
| `NETWORK` | One of `mainnet`, `testnet`, or `futurenet` |
| `POLL_INTERVAL_MS` | Ledger poll interval in milliseconds (default: `5000`) |
| `INDEX_DIAGNOSTIC` | Set `false` in production (diagnostic events are high-volume) |
| `LOG_LEVEL` | One of `error`, `warn`, `info`, `debug`, `trace` (use `info` in production) |
| `PORT` | API listen port (default: `3000`) |
| `API_KEY_SALT` | Random secret for hashing API keys — **must be changed** |
| `POSTGRES_USER` | PostgreSQL username |
| `POSTGRES_PASSWORD` | PostgreSQL password |
| `POSTGRES_DB` | PostgreSQL database name |
| `ALLOWED_ORIGINS` | Comma-separated allowed CORS origins, or `*` to allow all |
| `REQUEST_TIMEOUT_MS` | HTTP request timeout in milliseconds (default: `30000`) |
| `MAX_REQUEST_BODY_BYTES` | Maximum request body size for POST/PUT/PATCH, in bytes (default: `1048576`, 1 MiB); oversized bodies get `413` |
| `MAX_BATCH_BODY_BYTES` | Maximum request body size specifically for `POST /v1/events/batch`, in bytes (default: `2097152`, 2 MiB) |
| `PER_IP_RATE_LIMIT_RPS` | Per-IP sliding-window request limit, applied before auth on public paths (default: `20`) |
| `PER_IP_RATE_LIMIT_WINDOW_MS` | Window for the per-IP limit above, in milliseconds (default: `1000`) |
| `TRUSTED_PROXY_ENABLED` | Set `true` **only** when the API is known to sit entirely behind the provided nginx config (or an equivalent proxy) that is the sole path reachable by clients — resolves the per-IP rate limiter's client IP from the last hop of `X-Forwarded-For` instead of the raw TCP peer address. Leaving this unset/`false` is always safe; enabling it when untrusted clients can reach the API directly lets them spoof their rate-limit bucket via a forged header. See `services/api/middleware/abuse.go` (`trustedClientIP`) and `docs/threat-model.md`. |
| `MAX_IN_FLIGHT_REQUESTS` | Global concurrency cap — requests beyond this many in-flight get `503` to shed load (default: `500`) |

#### Indexer RPC transport and failover

| Variable | Description |
|---|---|
| `STELLAR_RPC_URLS` | Prioritised, comma-separated RPC endpoints; the first is the primary. Overrides `STELLAR_RPC_URL`, which stays valid as a single-value alias |
| `RPC_CONNECT_TIMEOUT_MS` | TCP connect timeout for RPC calls (default: `5000`) |
| `RPC_REQUEST_TIMEOUT_MS` | Overall RPC request timeout; must be >= the connect timeout (default: `30000`) |
| `RPC_POOL_IDLE_TIMEOUT_MS` | How long an idle pooled connection is kept (default: `90000`) |
| `RPC_POOL_MAX_IDLE_PER_HOST` | Idle keep-alive connections retained per RPC host (default: `8`) |
| `RPC_TCP_KEEPALIVE_MS` | TCP keep-alive probe interval (default: `60000`) |
| `RPC_FAILOVER_THRESHOLD` | Consecutive failures before the active endpoint is parked (default: `3`) |
| `RPC_ENDPOINT_COOLDOWN_MS` | How long a parked endpoint waits before it is tried again (default: `30000`) |

Without an explicit request timeout a stalled RPC connection blocks a poll
indefinitely: the retry wrapper only reacts to returned errors, never to a call
that never returns. Timeouts are classified retryable, so they engage backoff
and count toward the failover threshold.

#### Indexer outbox relay

| Variable | Description |
|---|---|
| `OUTBOX_POLL_INTERVAL_MS` | How often the relay scans for unpublished events (default: `100`) |
| `OUTBOX_BATCH_SIZE` | Maximum events published per relay pass (default: `500`) |
| `OUTBOX_BACKLOG_ALERT_THRESHOLD` | Backlog size at which the relay logs an alert-worthy warning (default: `10000`) |

Generate a secure `API_KEY_SALT`:

```bash
openssl rand -hex 32
```

### 4. Place TLS certificates

The nginx service expects certificates in the `nginx_certs` Docker volume.

```bash
docker volume create trident_nginx_certs
```

> **Note**: Docker Compose prefixes volume names with the project name (directory name by default).
> If your working directory is not named `trident`, use
> `docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml config --volumes`
> to find the actual volume name, then substitute it in the `docker volume create` and `docker run` commands above.

```bash
docker run --rm \
  -v trident_nginx_certs:/certs \
  -v $(pwd)/certs:/src \
  alpine \
  sh -c "cp /src/fullchain.pem /certs/ && cp /src/privkey.pem /certs/"
```

Replace `$(pwd)/certs` with the directory containing your certificate files.

### 5. Start PostgreSQL and run database migrations

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml up -d postgres
```

Wait for the health check to pass (postgres has a 15 s start period, 10 retries):

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
  ps postgres
```

Apply migrations in order:

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
  exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB \
  -f /docker-entrypoint-initdb.d/0001_init.sql

docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
  exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB \
  -f /docker-entrypoint-initdb.d/0002_system_state_health.sql
```

### 6. Start all services

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml up -d
```

### 7. Verify health

```bash
curl https://your-domain.com/v1/ready
```

Expected response (Postgres/Redis/gRPC all reachable):

```json
{"status":"ok","indexer_lag":0,"checks":{"postgres":"ok","redis":"ok","grpc_api":"ok"}}
```

---

## Updating (Rolling Update)

### 1. Pull latest images and rebuild

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml pull
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml build
```

### 2. Check for new migrations

Inspect `database/migrations/` for any files added since the last deploy. Apply each new file in ascending numeric order:

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
  exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB \
  -f /docker-entrypoint-initdb.d/<new-migration-file>.sql
```

Current migration files:
- `0001_init.sql`
- `0002_system_state_health.sql`

### 3. Restart the API service (zero-downtime)

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
  up -d --no-deps api
```

To also restart the indexer:

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
  up -d --no-deps indexer
```

### 4. Verify deployment

```bash
curl https://your-domain.com/v1/ready
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
  logs --tail=50 api
```

---

## Rollback

### 1. Identify the previous image

```bash
docker images | grep trident
```

### 2. Update the image tag in compose or re-tag, then restart

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
  up -d --no-deps api
```

### 3. Handle migration rollback (if applicable)

Migrations in `database/migrations/` are plain SQL and have no automated down path. If a schema change must be reversed, write and apply the inverse SQL manually. Review the relevant migration file before making any irreversible changes in production.

### 4. Verify health after rollback

```bash
curl https://your-domain.com/v1/ready
```

---

## Secret Rotation

### `API_KEY_SALT`

> **Warning:** Rotating `API_KEY_SALT` invalidates all existing API keys. Clients must re-authenticate after rotation.

1. Generate a new salt:
   ```bash
   openssl rand -hex 32
   ```
2. Update `API_KEY_SALT` in `.env`.
3. Restart the API service:
   ```bash
   docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
     up -d --no-deps api
   ```

### `POSTGRES_PASSWORD`

1. Connect to PostgreSQL and change the password:
   ```bash
   docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
     exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB
   ```
   ```sql
   ALTER USER trident WITH PASSWORD 'new-password';
   \q
   ```
2. Update `DATABASE_URL` and `POSTGRES_PASSWORD` in `.env`.
3. Restart all services that connect to the database:
   ```bash
   docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
     up -d --no-deps api indexer
   ```

### `REDIS_PASSWORD`

1. Update the Redis ACL or `requirepass` setting in your Redis config.
2. Update `REDIS_URL` in `.env` to include the new password (e.g. `redis://:new-password@redis:6379`).
3. Restart the services that use Redis:
   ```bash
   docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
     up -d --no-deps api indexer
   ```

---

## Monitoring

### Health Endpoints

| Endpoint | Description |
|---|---|
| `GET /v1/health` | Public liveness check. No dependency calls — just confirms the process is up (issue #243). |
| `GET /v1/ready` | Public readiness check. Verifies Postgres, Redis, and the gRPC backend (issue #243). 503 only if Postgres or the gRPC backend is unavailable; a Redis failure is reported as `degraded` with a 200 (issue #758). |
| `GET /internal/status` | Internal metrics endpoint (planned for a future release). |

`/v1/health` response shape (always 200 while the process is alive):

```json
{"status":"ok"}
```

`/v1/ready` response shapes:

```json
{"status":"ok","indexer_lag":3,"checks":{"postgres":"ok","redis":"ok","grpc_api":"ok"}}
```

```json
{"status":"degraded","indexer_lag":null,"checks":{"postgres":"unavailable","redis":"ok","grpc_api":"ok"}}
```

```json
{"status":"degraded","indexer_lag":3,"checks":{"postgres":"ok","redis":"degraded","grpc_api":"ok"}}
```
`indexer_lag` is null whenever Postgres is unreachable or the chain-tip cache hasn't been populated yet. `postgres` and `grpc_api` are required: `unavailable` returns HTTP 503. `redis` is optional: `degraded` still returns HTTP 200. The public body never includes hostnames or driver errors; those are logged by the API (`readiness check failed`).

### PostgreSQL Disk Usage

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
  exec postgres psql -U $POSTGRES_USER -d $POSTGRES_DB \
  -c "SELECT pg_size_pretty(pg_database_size('$POSTGRES_DB'));"
```

Alert when disk usage exceeds 80% of available space.

### Redis Stream Backlog

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
  exec redis redis-cli XLEN trident:events
```

A growing `trident:events` stream length indicates consumer lag. Investigate the `api` service logs if the stream is not draining.

### Indexer Lag

Check `indexer_lag` in the readiness response, or query `GET /v1/stats/indexer` directly for `last_poll_at` and `status`. If `indexer_lag` is large or `checks.postgres` is not `ok`, investigate the indexer/database.

```bash
curl https://your-domain.com/v1/ready | jq .
```

### nginx / WebSocket Connections

WebSocket connections arrive at `/ws` through nginx. Monitor active connections in nginx access logs:

```bash
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml \
  logs nginx | grep "/ws" | tail -20
```

### View Service Logs

```bash
# All services
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml logs -f

# Single service
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml logs -f api
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml logs -f indexer
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml logs -f postgres
docker compose -f docker/docker-compose.yml -f docker/docker-compose.prod.yml logs -f nginx
```

---

## Connection Topology

Trident runs three database clients:

| Service              | Role                         | Pool env var            | Default |
| -------------------- | ---------------------------- | ----------------------- | ------- |
| Indexer (Rust)       | single writer, low write QPS | `INDEXER_DB_POOL_SIZE`  | 3       |
| gRPC API (Rust)      | read-heavy, moderate QPS     | `GRPC_API_DB_POOL_SIZE` | 10      |
| REST API (Go)        | per-replica request handler  | `GO_API_DB_POOL_SIZE`   | 5       |

In production every service connects to **PgBouncer**, never to Postgres
directly. PgBouncer (transaction pooling mode) multiplexes the many short-lived
application connections over a small set of real Postgres connections, so
Postgres never approaches its `max_connections` limit even as the Go API scales
to multiple replicas.

```
indexer  ─┐
gRPC API ─┼─▶  PgBouncer (pgbouncer:6432, transaction mode)  ─▶  Postgres :5432
Go API   ─┘        default_pool_size = 20
(N replicas)
```

### PgBouncer Transaction Mode: Common Pitfalls

Transaction pooling is efficient but means **no session state survives across transaction boundaries**. The following do **not** work in transaction mode:

1. **Named/server-side prepared statements** — A prepared statement lives on one server connection; the next transaction may land on another.
2. **`SET SESSION` variables** — Not preserved across transactions.
3. **Session-level advisory locks** — Behave unexpectedly because the "session" is not stable.

Trident's clients are configured to avoid (1):

- **Rust (sqlx):** `PgConnectOptions::statement_cache_capacity(0)`
- **Go (pgx v5):** `cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol`

### Schema Migrations

Run migrations against a **direct** Postgres connection, not the transaction-mode pooler. Keep a direct DSN available for that purpose; do not point your migration tool at `pgbouncer:6432`.

## Admin Stats Endpoint

The Go API exposes `GET /v1/admin/db` for capacity planning. Set `ADMIN_API_KEY` and `PGBOUNCER_ADMIN_URL` in `.env`, then:

```bash
curl -H "X-Admin-Key: $ADMIN_API_KEY" http://localhost:3000/v1/admin/db
```

A missing or wrong key returns `401`; an unreachable PgBouncer returns `502`.

## Load Testing

`load-tests/pgbouncer-validation.js` is a [k6](https://k6.io) script that drives
100 concurrent clients, each issuing 10 requests to `GET /v1/events`, and asserts
no `too many connections` errors with p99 latency under 500ms.

```bash
BASE_URL=http://localhost:3000 k6 run load-tests/pgbouncer-validation.js
```

`load-tests/` also has k6 scenarios for the top API endpoints (events list/get,
batch, stats, SSE stream) with SLO-derived thresholds, and a sustained ingest
soak test — see [`load-tests/README.md`](../load-tests/README.md) for the
full runbook. These run in CI only via `workflow_dispatch`/a weekly schedule
(`.github/workflows/load-tests.yml`), not on every PR.

---

## Fly.io Deployment

Trident can be deployed to [Fly.io](https://fly.io) as three separate apps sharing a private network (6PN). Configuration files live in `fly/`.

### Prerequisites

- [flyctl](https://fly.io/docs/flyctl/installing/) installed and authenticated (`fly auth login`)
- A Fly.io organization with Fly Postgres and Fly Redis provisioned

### App topology

| App name | Config | Description |
|---|---|---|
| `trident-grpc-api` | `fly/grpc-api.toml` | Rust gRPC API — event query backend |
| `trident-indexer` | `fly/indexer.toml` | Rust Stellar event indexer |
| `trident-api` | `fly/api.toml` | Go REST API — public-facing |

Services communicate over Fly's private 6PN network:
- Go API → gRPC API at `trident-grpc-api.internal:50051`
- Indexer → database and Redis directly (no external exposure needed)

### First-time setup

#### 1. Create the Fly apps

```bash
fly apps create trident-grpc-api
fly apps create trident-indexer
fly apps create trident-api
```

#### 2. Provision Fly Postgres

```bash
fly postgres create --name trident-db --region iad
fly postgres attach trident-db -a trident-grpc-api
fly postgres attach trident-db -a trident-indexer
fly postgres attach trident-db -a trident-api
```

`fly postgres attach` automatically sets `DATABASE_URL` as a secret on each app.

#### 3. Provision Fly Redis

```bash
fly redis create --name trident-redis --region iad
```

Note the Redis URL from the output, then set it on the apps that need it:

```bash
fly secrets set -a trident-indexer REDIS_URL="redis://..."
fly secrets set -a trident-api     REDIS_URL="redis://..."
```

#### 4. Set required secrets

Each service reads its secrets from process environment; the lists below are
cross-checked against the actual `env::var`/`os.Getenv` calls in each
service's source, not just the comments in the `fly/*.toml` files.

**gRPC API** (`trident-grpc-api`, Rust, `crates/api`):
```bash
# DATABASE_URL is set automatically by `fly postgres attach` (step 2).
fly secrets set -a trident-grpc-api \
  REDIS_URL="redis://..."
```
`crates/api/src/config.rs` requires `DATABASE_URL` and `GRPC_ADDR` at startup
(missing either one makes the process exit immediately); `GRPC_ADDR` is set as
a plain (non-secret) `[env]` value in `fly/grpc-api.toml` because it is not
sensitive, only internal-network configuration. `REDIS_URL` is read directly
in `crates/api/src/main.rs` via `.expect(...)`, so it is just as required in
practice even though it is not part of `Config::from_env`.

**Indexer** (`trident-indexer`, Rust, `crates/indexer`):
```bash
fly secrets set -a trident-indexer \
  REDIS_URL="redis://..." \
  STELLAR_RPC_URL="https://soroban-testnet.stellar.org"
```
`crates/indexer/src/config.rs` requires `DATABASE_URL`, `REDIS_URL`, and
`STELLAR_RPC_URL` (or the multi-endpoint `STELLAR_RPC_URLS`). `NETWORK` is
optional and defaults to `"testnet"` if unset — set it explicitly for
mainnet deployments:
```bash
fly secrets set -a trident-indexer NETWORK="mainnet"
```

**Go REST API** (`trident-api`, Go, `services/api`):
```bash
fly secrets set -a trident-api \
  API_KEY_SALT="$(openssl rand -hex 32)" \
  ADMIN_API_KEY="$(openssl rand -hex 32)" \
  API_KEY_HASHES="<hex(hmac_sha256(salt, key)) list, comma-separated>"
```
`services/api/main.go` does not hard-exit on a missing `DATABASE_URL` or
`REDIS_URL` — it falls back to `redis://localhost:6379` and serves
DB-backed endpoints as `503` — so those failure modes are silent unless you
check logs; always set both explicitly in production. `API_KEY_SALT` is
read by `services/api/middleware/auth.go` to HMAC every incoming API key.
`API_KEY_HASHES` is the legacy env-var auth allowlist; skip it only if every
API key is issued through the DB-backed `/v1/admin/keys` endpoints instead.
Optional secrets: `STELLAR_RPC_URL` (enables the admin contract-call
endpoint) and `PGBOUNCER_ADMIN_URL` (enables `GET /v1/admin/db`).

#### 5. Run database migrations

Attach to a temporary machine in the Trident private network and run migrations directly against Postgres (not through PgBouncer):

```bash
fly ssh console -a trident-grpc-api -C \
  "psql \$DATABASE_URL -f /path/to/migrations/0001_init.sql"
```

Or use a local `psql` with the direct Postgres URL (bypassing PgBouncer).

#### 6. Deploy

```bash
make deploy
```

This deploys in dependency order: gRPC API → Indexer → Go REST API.

To deploy a single service:

```bash
fly deploy -c fly/grpc-api.toml --remote-only
fly deploy -c fly/indexer.toml --remote-only
fly deploy -c fly/api.toml     --remote-only
```

### Scaling

```bash
fly scale count 2 -a trident-api               # scale Go API to 2 instances
fly scale count 2 -a trident-grpc-api          # scale gRPC API to 2 instances
fly scale vm shared-cpu-2x -a trident-indexer  # upgrade indexer VM
fly scale show -a trident-api                  # show current VM size / count
```

The indexer should normally stay at `count = 1` — it is a single writer
(issue #87); running more than one instance against the same database causes
duplicate polling, not higher throughput. `trident-api` and `trident-grpc-api`
are stateless request handlers and scale horizontally.

Each `fly/*.toml` also pins a starting VM size under `[[vm]]` and
`min_machines_running` under the service block:

| App | VM size (`fly/*.toml`) | `min_machines_running` |
|---|---|---|
| `trident-api` | `shared-cpu-1x` / 512mb | 1 (always at least one machine up) |
| `trident-grpc-api` | `shared-cpu-1x` / 256mb | not set (services block has no `http_service`; Fly autostarts on demand) |
| `trident-indexer` | `shared-cpu-1x` / 512mb | not set — this is a worker, not scaled to zero on request traffic |

Adjust the `[[vm]]` block or pass `fly scale vm <size> -a <app>` for a one-off
resize; edit `min_machines_running` in the relevant toml and redeploy for a
lasting change.

### Monitoring

- **Indexer metrics**: accessible on the 6PN at `trident-indexer.internal:9090/metrics`
- **Indexer health/readiness**: accessible on the 6PN at
  `trident-indexer.internal:8080/healthz` (liveness) and `.../readyz`
  (readiness — checks both Postgres and Redis connectivity; see
  `crates/indexer/src/health.rs`). These are also wired into `fly/indexer.toml`
  under the top-level `[checks]` section so Fly restarts the machine if
  readiness fails, not just if the TCP port stops accepting connections.
- **Go API metrics**: `GET /metrics` on the public `trident-api` endpoint
- **Go API health check**: `GET /v1/ready` (used by Fly's HTTP service check
  in `fly/api.toml` to gate traffic routing). `/v1/ready` verifies
  Postgres/gRPC reachability (and reports Redis as degraded), unlike `/v1/health`, which is a cheap
  liveness check that says only that the process is up (issue #243).
- **gRPC API**: no HTTP health endpoint exists in `crates/api` today; Fly's
  check in `fly/grpc-api.toml` is a plain TCP check against port 50051. This
  proves the socket is listening, not that gRPC calls actually succeed — if a
  future gRPC health-checking service (`grpc.health.v1.Health`) is added to
  `crates/api`, upgrade this to a real RPC-level check.

### Configuration validation

`flyctl config validate` requires an authenticated Fly session (an API access
token), which is not available in every environment (for example, a CI
sandbox with no Fly account). Where a live token is not available, at minimum
confirm the TOML itself is well-formed, e.g.:

```bash
python3 -c "import tomllib; tomllib.load(open('fly/api.toml','rb'))"
```

Note: `fly/*.toml` are saved with a UTF-8 BOM; strip it first if your TOML
parser rejects a leading BOM (Fly's own TOML parser accepts it fine). Once
authenticated, always confirm with the real thing before deploying:

```bash
fly auth login
fly config validate -c fly/api.toml
fly config validate -c fly/grpc-api.toml
fly config validate -c fly/indexer.toml
```
>>>>>>> origin/dev

### Updating secrets

```bash
fly secrets set -a <app-name> KEY=new_value
```

Fly automatically redeploys the app when secrets change.

### Rollback

```bash
fly releases -a trident-api          # list releases
fly deploy --image-label <version> -a trident-api  # roll back to a specific release
```

---

## Distributed Tracing (OpenTelemetry)

All three Trident services — `trident-go-api`, `trident-grpc-api`, and `trident-indexer` — are instrumented with OpenTelemetry. Tracing is **opt-in**: set `OTEL_EXPORTER_OTLP_ENDPOINT` to enable it; leave it empty for zero overhead.

### Local development with Jaeger

`docker/docker-compose.dev.yml` includes a Jaeger all-in-one container. Start it alongside the other infrastructure dependencies:

```bash
docker compose -f docker/docker-compose.dev.yml up -d
```

Then set the following in your `.env` before starting the services:

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
OTEL_SAMPLING_RATIO=1.0
```

Open the Jaeger UI at **http://localhost:16686** and search by service name:

- `trident-go-api` — HTTP handler spans and outbound gRPC call spans
- `trident-grpc-api` — inbound gRPC handler spans and SQL query spans
- `trident-indexer` — poll cycle, RPC, parse, DB insert, and Redis publish spans

A single `GET /v1/events` request produces a trace with spans linked across all three services via the W3C `traceparent` header.

### Production (Grafana Tempo on Fly.io)

Set `OTEL_EXPORTER_OTLP_ENDPOINT` to your Grafana Tempo OTLP gRPC endpoint on each app:

```bash
fly secrets set -a trident-api       OTEL_EXPORTER_OTLP_ENDPOINT="https://tempo.your-org.grafana.net:443"
fly secrets set -a trident-grpc-api  OTEL_EXPORTER_OTLP_ENDPOINT="https://tempo.your-org.grafana.net:443"
fly secrets set -a trident-indexer   OTEL_EXPORTER_OTLP_ENDPOINT="https://tempo.your-org.grafana.net:443"
```

Set the sampling ratio (default 10% in production):

```bash
fly secrets set -a trident-api       OTEL_SAMPLING_RATIO=0.1
fly secrets set -a trident-grpc-api  OTEL_SAMPLING_RATIO=0.1
fly secrets set -a trident-indexer   OTEL_SAMPLING_RATIO=0.1
```

### Span attributes

| Service | Span name | Key attributes |
|---|---|---|
| `trident-go-api` | HTTP handler (auto via `otelhttp`) | `http.method`, `http.status_code`, `http.route` |
| `trident-go-api` | gRPC client call (auto via `otelgrpc`) | `rpc.system`, `rpc.method` |
| `trident-grpc-api` | `list_events` | `rpc.system`, `contract_id` |
| `trident-grpc-api` | `get_event` | `rpc.system` |
| `trident-grpc-api` | `stream_events` | `rpc.system` |
| `trident-indexer` | `poll_cycle` | `cursor` |
| `trident-indexer` | `rpc_get_events` | — |
| `trident-indexer` | `parse_events` | — |
| `trident-indexer` | `db_insert_events` | `contract_id` |
| `trident-indexer` | `redis_xadd` | — |

## Staging Environment {#staging}

Issue #313: `.github/workflows/staging-deploy.yml` builds and deploys the
`dev` branch to a staging environment automatically on every push to `dev`,
then runs a post-deploy smoke test before staging is considered promotable.

### What's fully working

- **Build + push**: every push to `dev` builds all three service images
  (indexer, grpc-api, go-api) and pushes them to GHCR tagged `staging` and
  `staging-<short-sha>`, reusing the same Dockerfiles and Buildx setup as the
  tag-triggered `release.yml` (issue #302).
- **Helm deploy**: `helm upgrade --install trident-staging` using
  `helm/trident/values-staging.yaml` (a scaled-down overlay: single replicas,
  smaller resource requests, ingress disabled) plus the new image tags.
- **Smoke test**: health check, a real `GET /v1/events` query, a bounded read
  of the `GET /v1/events/stream` SSE endpoint, and an error-path check
  (invalid API key -> 401) — mirroring the shape of the existing CI `e2e` job
  (issue #301) but against the deployed staging URL.
- **Failure alerting**: a failed deploy or smoke test posts to
  `STAGING_ALERT_WEBHOOK_URL` if configured.
- **Env reference lint** (`scripts/check-env-reference.sh`, issue #312) is
  wired into `ci.yml` as the `env-reference` job so config drift on any
  branch — including `dev` — is caught before it reaches staging.

### What's best-effort / requires setup this environment couldn't provision

This was written and validated (workflow YAML, Helm values, `helm lint`)
without access to a real Kubernetes cluster or cloud credentials — the
following require you to provision them once:

- **Secrets**: `STAGING_KUBECONFIG` (repo/org secret, base64-encoded
  kubeconfig scoped to the staging namespace) and `STAGING_API_KEY` must be
  set for `deploy-staging`/`smoke-test-staging` to run at all. Without them,
  those jobs are skipped (not failed) — see `check-staging-config` in the
  workflow.
- **Variables**: `STAGING_NAMESPACE` (defaults to `trident-staging`),
  `STAGING_URL` (public URL of the deployed staging stack, used by the smoke
  test), `STAGING_DATABASE_URL` (for the migration step, if runner-reachable),
  and `STAGING_ALERT_WEBHOOK_URL` (optional).
- **Migrations are stubbed pending issue #308** ("database migration job as
  a Helm hook", still open): the workflow currently runs
  `sqlx migrate run` directly from the GitHub Actions runner against
  `STAGING_DATABASE_URL`, the same mechanism CI's `rust` job already uses.
  This is a reasonable stand-in but is **not** the in-cluster,
  chart-versioned Helm pre-upgrade hook Job that #308 calls for — that hook
  would run migrations from inside the cluster (no runner network path to
  the DB required) and block the Helm rollout itself on migration failure.
  Once #308 lands, replace the "Run database migrations" step with
  `helm upgrade --wait` relying on the hook's own `helm.sh/hook-weight`
  ordering, and delete the runner-side `sqlx migrate run` step.
- **Blocking promotion on smoke test failure**: the workflow file makes the
  failure visible (job fails, `alert-on-failure` fires) and is a natural gate
  for a required status check, but GitHub Environment protection rules
  (Settings -> Environments -> `staging` -> required reviewers / deployment
  branch policies) must be configured in the repository UI — a workflow file
  alone cannot set repo-level branch protection.

### Promotion path: staging -> prod (dev -> main)

1. Changes land on `dev` via PR, triggering `staging-deploy.yml` automatically.
2. Once staging's smoke test passes (green `smoke-test-staging` job), open a
   PR from `dev` into `main`.
3. Merging into `main` does not itself deploy anything — production deploys
   are tag-triggered: `git tag vX.Y.Z && git push origin vX.Y.Z` runs
   `release.yml`, which builds and pushes the same three images tagged with
   that version and `latest` (issue #302).
4. Roll out to production the same way as any other release — see
   [Updating (Rolling Update)](#updating-rolling-update) above — pointing
   `helm/trident/values.yaml` (production defaults, no `-staging` overlay) at
   the new tag.
5. If a smoke test on `dev` fails, treat the branch as un-promotable until
   fixed — do not cut a `main` PR or tag from a red `dev`.
