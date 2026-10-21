# Threat Model

A lightweight threat model for Trident: what's worth protecting, where an
attacker can touch the system, where trust boundaries sit, and the top
threats we've identified with their current mitigations (issue #321).

This is not a formal STRIDE/DREAD exercise or a substitute for an external
pen-test (explicitly out of scope for the MVP per issue #321) — it's meant
to be a shared, living reference for contributors and reviewers.

## Assets

What's worth protecting, roughly in order of blast radius if compromised:

| Asset | Where it lives | Why it matters |
|---|---|---|
| Admin API key (`ADMIN_API_KEY`) | env var, compared in `handlers.requireAdmin` / webhook + contract admin handlers | Full control over API-key issuance, contract registration, and admin stats |
| Per-consumer API keys | `api_keys` table (hashed, see `services/api/handlers/apikeys.go`); plaintext returned once at creation | Gate read access to indexed event data; tied to a rate-limit tier |
| Postgres data (events, audit log, api_keys, webhooks) | `postgres` service, reached via pgx (Go) and sqlx (Rust) | Indexed on-chain event data, audit trail, and credentials-adjacent metadata (key hashes, webhook target URLs) |
| Event/ledger data itself | `soroban_events` table, populated by `crates/indexer` from Soroban RPC | The actual product — integrity (not just confidentiality) matters: a tampered or incomplete event feed misleads every downstream consumer |
| Webhook target URLs + delivery secrets | `webhooks` table | An attacker who can register a webhook can potentially use Trident as an SSRF launchpad against the target URL's network |
| Redis (rate-limit state, WS pub/sub stream, cache) | `redis` service | Availability-critical (rate limiting fails open without it — see `ratelimit.go`), not confidentiality-critical (no secrets stored there directly) |
| TLS certificates | `docker/nginx/certs` | Transport confidentiality/integrity for the whole public surface |

## Entry Points

Every place an external actor's input reaches the system:

- **Public Go API** (`services/api`, fronted by nginx) — `GET/POST /v1/events*`,
  `GET /v1/stats/*`, `POST /v1/contracts/{id}/call`, `POST/GET/PATCH/DELETE
  /v1/webhooks*`, admin endpoints under `/v1/admin/*` and `/v1/api-keys*`.
  The highest-traffic, most attacker-reachable surface.
- **WebSocket / GraphQL-over-WS** (`/ws`, `/graphql` — `services/api/ws`) —
  a long-lived, stateful connection per client; a different resource-exhaustion
  shape than a request/response REST call (see "Top threats" below).
- **gRPC API** (`crates/api`) — not directly internet-facing in the reference
  deployment (the Go API is the public front door and calls gRPC internally,
  `GRPC_ADDR`), but is a real entry point in any topology where it's exposed
  directly (e.g. a service mesh) or where the Go API's authorization is
  bypassed.
- **Indexer's RPC ingestion path** (`crates/indexer`) — technically an
  *outbound* connection to Soroban RPC (`STELLAR_RPC_URL`/`STELLAR_RPC_URLS`),
  but the RPC endpoint is the entry point for anything malicious making it
  into the event feed: a malicious or compromised RPC node could serve
  fabricated ledger/event data. Mitigated by using well-known RPC providers
  and, longer-term, by cross-validating against multiple endpoints
  (`RPC_FAILOVER_THRESHOLD` today provides failover, not cross-validation).
- **Internal-only endpoints** — `GET /internal/status` (process/dependency
  health with more detail than the public `/v1/health`) and the Prometheus
  metrics. `/internal/status` is mounted on the same mux as the public routes
  in `main.go` and is **not** protected by network policy at the application
  layer today — it relies on the deployment topology (nginx denying
  `/internal/`; see `docker/nginx/nginx.conf`) keeping it off the public
  network. Metrics are not on the public mux at all: `GET /metrics` and
  `GET /metrics/legacy` are served only by the separate `METRICS_PORT` server
  (default 9091), and nginx also returns 404 for `/metrics` as defence in
  depth. `PPROF_ENABLED` (see
  `services/api/internal/profiling/profiling.go`) is opt-in and, when
  enabled, must never be exposed publicly — it allows arbitrary memory/goroutine
  introspection and CPU profiling of the live process.

## Trust Boundaries

```
Untrusted client
      │  (HTTPS, WSS)
      ▼
nginx edge (docker/nginx/nginx.conf)
      │  X-Forwarded-For appended, TLS terminated
      ▼
Go API (services/api) ── trust boundary: auth (API key / admin key) enforced here
      │  gRPC (internal network / same host)
      ▼
gRPC API (crates/api) ── trust boundary: assumes callers are the Go API, not re-authenticated
      │  SQL (parameterized, pgx/sqlx)
      ▼
Postgres ── trust boundary: only the Go API and gRPC API (and the indexer) hold credentials

Soroban RPC (external, semi-trusted)
      │  JSON-RPC
      ▼
Indexer (crates/indexer) ── trust boundary: RPC responses are parsed/validated before being persisted
      │  SQL (parameterized, sqlx)
      ▼
Postgres
```

Key implications:

- **Client → nginx → Go API** is the only boundary a fully untrusted actor
  crosses. Everything downstream of the Go API assumes it has already
  authenticated/authorized/rate-limited the request.
- **Go API → gRPC API** is *not* re-authenticated at the gRPC layer — the
  gRPC API trusts that anything calling it is the Go API (or another
  equally-trusted internal caller). Exposing the gRPC port directly to
  untrusted networks would collapse this boundary; it must stay
  internal-only in any deployment.
- **nginx's `X-Forwarded-For`** is a boundary in itself: nginx *appends* to
  any client-supplied XFF rather than replacing it
  (`proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for`). The
  per-IP rate limiter (`services/api/middleware/abuse.go`,
  `trustedClientIP`) only reads XFF at all when `TRUSTED_PROXY_ENABLED=true`,
  and (issue #779) walks the chain from the right, skipping only hops whose
  address falls inside `TRUSTED_PROXY_CIDRS` — the immediate TCP peer must
  itself be a trusted proxy or the whole header is ignored, so a client that
  connects directly cannot forge a trusted-looking chain and pick its own
  rate-limit bucket. On Fly, `Fly-Client-IP` (set by Fly's own edge, not
  forgeable by the client) is used instead and no CIDR list is needed.
  `TRUSTED_PROXY_ENABLED` must only be set when nginx/Fly (or an equivalent
  proxy) is confirmed to be the sole path to the API.
- **Indexer → Soroban RPC** crosses into semi-trusted territory: the RPC
  endpoint is operator-configured (not arbitrary/attacker-supplied), but its
  *responses* are still untrusted input and are parsed defensively
  (`crates/indexer` event/ledger parsing, with retries/failover rather than
  blind trust — see `docs/deployment.md`'s RPC transport section).

## Top Threats and Mitigations

| # | Threat | Mitigation | Where |
|---|---|---|---|
| 1 | SQL injection via string-built queries | Audited (issue #317): all Go DB access goes through pgx with parameterized queries (`$1`, `$2`, ...); all Rust DB access goes through sqlx. The only `format!`-built SQL found (`crates/indexer/src/main.rs`, setting `statement_timeout`/`idle_in_transaction_session_timeout`) interpolates operator-configured numeric config, not per-request/attacker-controlled input, and `SET` statements cannot be parameterized in Postgres — reviewed and accepted as safe. No changes needed. | `services/api/**/*.go`, `crates/**/*.rs` |
| 2 | Oversized request bodies exhausting memory | `http.MaxBytesReader`-backed body-size middleware (413 on exceed), plus `http.Server.MaxHeaderBytes` and nginx `large_client_header_buffers` for headers/URLs | `services/api/middleware/bodysize.go`, `main.go`, `docker/nginx/nginx.conf` (#317) |
| 3 | Abusive/hostile WebSocket-GraphQL subscriptions (oversized query strings, unbounded concurrent subscriptions per connection) | Frame size cap (`gqlMaxFrameSize`, pre-existing), plus a query-length cap and a per-connection subscription count cap — the practical equivalent of depth/complexity limits for a protocol with no general query executor | `services/api/ws/graphql.go` (#317) |
| 4 | A single IP or unauthenticated client flooding public endpoints or the auth path itself | Per-IP sliding-window rate limit, applied before auth on public paths, resolved from a trusted proxy hop only when explicitly configured | `services/api/middleware/abuse.go` (#318) |
| 5 | Traffic spike or attack degrading the whole process regardless of per-key/per-IP limits | Global in-flight-request concurrency cap shedding load (503) before other work happens | `services/api/middleware/abuse.go` (#318) |
| 6 | Excessive per-key request volume | Per-API-key tiered sliding-window rate limiter (Redis-backed) | `services/api/middleware/ratelimit.go` (#229) |
| 7 | Credential/API-key misuse or unauthorized access | DB-backed auth with Redis caching (`NewDBAuth`), HMAC-hashed legacy env-var keys, admin-key-gated admin endpoints | `services/api/middleware/auth.go` |
| 8 | Cross-origin abuse of authenticated endpoints from a malicious web page | Explicit CORS allowlist validated at startup (`ValidateAllowedOrigins`), no silent wildcard fallback in production | `services/api/middleware/cors.go`, `security.go` |
| 9 | Response-side injection / clickjacking / MIME sniffing | Standard security headers (`X-Content-Type-Options`, `X-Frame-Options`, HSTS in production, `Referrer-Policy`) | `services/api/middleware/security.go` |
| 10 | No audit trail for sensitive operations | Async, batched audit log writer capturing admin/API-key operations | `services/api/middleware/audit.go` |
| 11 | Known-vulnerable dependencies shipping in a release | SBOM generation + image scanning (trivy) and dependency audits (`cargo audit`, `govulncheck`, `npm audit`, `pip-audit`) gating CI on push/PR to `dev`/`main` plus a weekly cron | `.github/workflows/security-scan.yml`, `docs/security-triage.md` (#311) |
| 12 | Leaked secrets in commits/history | Dedicated secrets-scanning workflow | `.github/workflows/security.yml` |
| 13 | Connection-string credentials leaking into logs on a DB/Redis error | DSN userinfo redaction applied to logged connection errors | `services/api/main.go` (`redactConnErr`, issue #305) |
| 14 | Malicious or compromised Soroban RPC endpoint feeding fabricated event data | RPC failover across configured endpoints (`RPC_FAILOVER_THRESHOLD`), defensive parsing of RPC responses; cross-endpoint response validation is not yet implemented (see "Open gaps" below) | `crates/indexer` |
| 15 | SSRF via a registered webhook target URL | Not yet enforced at the application layer — see "Open gaps" | `services/api/webhooks.go` |

## Open Gaps (Known, Not Yet Mitigated)

Documented rather than silently left out, so they're tracked instead of
forgotten:

- **Webhook SSRF:** a user with a valid API key can register a webhook
  pointing at an internal address (e.g. a cloud metadata endpoint or another
  service on the deployment's private network). No allowlist/denylist or
  private-IP-range rejection is applied to `targetUrl` today. Worth a
  dedicated follow-up issue.
- **Cross-validation of RPC responses:** the indexer fails over between
  configured RPC endpoints on error but does not cross-check ledger data
  against multiple endpoints for consistency (defense against a single
  compromised/malicious RPC provider serving plausible-but-wrong data).
- **gRPC API has no independent authentication layer:** it trusts network
  placement entirely. Fine for the reference single-host/compose deployment;
  worth revisiting before a multi-tenant or less-trusted internal-network
  deployment topology.

## Review Cadence

This threat model is reviewed:

- **Quarterly**, alongside the dependency-audit allowlist review cadence in
  `docs/security-triage.md`.
- **Whenever a new public entry point is added** (a new route, a new
  protocol like the WS/GraphQL endpoint was, a new externally-reachable
  service).
- **Whenever a trust boundary changes** — e.g. exposing the gRPC API
  directly, changing the nginx topology, or adding a new internal-only
  endpoint.

See also `SECURITY.md` for the vulnerability disclosure process this
document feeds into.
