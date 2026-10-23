package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"
	"time"

	"github.com/Depo-dev/trident/services/api/grpc"
	"github.com/Depo-dev/trident/services/api/handlers"
	"github.com/Depo-dev/trident/services/api/internal/metrics"
	"github.com/Depo-dev/trident/services/api/internal/profiling"
	"github.com/Depo-dev/trident/services/api/internal/sorobanrpc"
	"github.com/Depo-dev/trident/services/api/middleware"
	"github.com/Depo-dev/trident/services/api/ws"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
)

// How often contract_stats_rollup is recomputed from soroban_events (issue
// #257). Matches the Redis response cache TTL in handlers.ContractsStats, so
// a rollup-backed response is never staler than the cache would already
// allow it to be.
const contractStatsRollupRefreshInterval = 60 * time.Second

// How often pgxpool saturation stats are polled into Prometheus gauges
// (issue #238).
const dbPoolMetricsPollInterval = 15 * time.Second

// Usage rollup: re-aggregate audit_log into usage_rollup every 5 minutes,
// covering the last 48h so late-arriving audit rows (the writer batches
// asynchronously) and the UTC day boundary are always caught by the next run.
const (
	usageRollupInterval        = 5 * time.Minute
	usageRollupLookback        = 48 * time.Hour
	usageRollupRetention       = 400 * 24 * time.Hour
	usageRollupCleanupInterval = 24 * time.Hour
)

const defaultDBPoolSize = 5

// Pool lifecycle defaults (issue #238). Applied in buildPoolConfig unless
// overridden by env vars.
const (
	defaultDBPoolMinConns              = 0
	defaultDBPoolMaxConnLifetimeMS     = 1_800_000 // 30 min
	defaultDBPoolMaxConnIdleTimeMS     = 300_000   // 5 min
	defaultDBPoolHealthCheckPeriodMS   = 60_000    // 1 min, matches pgxpool's own default
	dbPoolMaxConnLifetimeJitterPercent = 10        // spreads reconnects so the pool doesn't empty all at once
)

// Statement-timeout defaults (issue #238), shared with the Rust indexer
// (crates/indexer/src/config.rs) via the same env vars so both services agree
// on how long a query or idle transaction may hold a connection.
const (
	defaultStatementTimeoutMS         = 30_000
	defaultIdleInTransactionTimeoutMS = 10_000
	statementTimeoutMinMS             = 100
	statementTimeoutMaxMS             = 3_600_000
)

// connErrRegexp matches a userinfo-bearing connection URI (scheme://user:pass@host)
// so DB/Redis connection errors — which some drivers embed the DSN in — never
// leak the credential portion to logs (issue #305).
var connErrRegexp = regexp.MustCompile(`([a-zA-Z][a-zA-Z0-9+.-]*)://[^@\s]+@`)

// redactConnErr strips any embedded userinfo from a connection error's
// message before logging it. Safe to call on any error, not just ones that
// actually embed a DSN — a no-op when the pattern doesn't match.
func redactConnErr(err error) string {
	if err == nil {
		return ""
	}
	return connErrRegexp.ReplaceAllString(err.Error(), "${1}://[redacted]@")
}

// parseOTLPHeaders parses the standard OTel OTEL_EXPORTER_OTLP_HEADERS
// format (comma-separated key=value pairs, e.g. "api-key=secret,x-foo=bar")
// used to authenticate against a hosted collector (issue #762). Malformed
// entries are skipped rather than failing exporter init.
func parseOTLPHeaders(raw string) map[string]string {
	if raw == "" {
		return nil
	}
	headers := make(map[string]string)
	for _, pair := range strings.Split(raw, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		kv := strings.SplitN(pair, "=", 2)
		if len(kv) != 2 {
			slog.Warn("skipping malformed OTEL_EXPORTER_OTLP_HEADERS entry", "entry", pair)
			continue
		}
		key := strings.TrimSpace(kv[0])
		value := strings.TrimSpace(kv[1])
		if key == "" {
			continue
		}
		headers[key] = value
	}
	return headers
}

func initTracer(ctx context.Context) func() {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return func() {}
	}

	samplingRatio := 0.1
	if r := os.Getenv("OTEL_SAMPLING_RATIO"); r != "" {
		f, err := strconv.ParseFloat(r, 64)
		if err != nil || f < 0 || f > 1 {
			slog.Warn("invalid OTEL_SAMPLING_RATIO, must be a float in [0,1]; falling back to default", "value", r, "default", samplingRatio)
		} else {
			samplingRatio = f
		}
	}

	// Issue #762: traces include request paths, contract ids, key ids and
	// error messages, so the exporter connection defaults to TLS. Insecure
	// (plaintext) requires an explicit opt-in via the standard OTel env var
	// — e.g. for a local collector sidecar in development.
	opts := []otlptracegrpc.Option{
		otlptracegrpc.WithEndpoint(endpoint),
	}
	if insecure, _ := strconv.ParseBool(os.Getenv("OTEL_EXPORTER_OTLP_INSECURE")); insecure {
		opts = append(opts, otlptracegrpc.WithInsecure())
	}
	if headers := parseOTLPHeaders(os.Getenv("OTEL_EXPORTER_OTLP_HEADERS")); len(headers) > 0 {
		opts = append(opts, otlptracegrpc.WithHeaders(headers))
	}

	exporter, err := otlptracegrpc.New(ctx, opts...)
	if err != nil {
		slog.Warn("failed to create OTLP trace exporter", "err", err)
		return func() {}
	}

	res, err := resource.New(ctx,
		resource.WithAttributes(semconv.ServiceName("trident-go-api")),
	)
	if err != nil {
		slog.Warn("failed to create OTel resource", "err", err)
		res = resource.Default()
	}

	// Issue #457: a fixed low ratio alone means the one failing/slow
	// request is almost never the one sampled. newAlwaysRecordSampler +
	// newAlwaysKeepExporter together guarantee every error and every
	// slow (>2s) request is exported regardless of the ratio, while the
	// rest of traffic still samples at samplingRatio.
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(newAlwaysKeepExporter(exporter)),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(newAlwaysRecordSampler(samplingRatio)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return func() { _ = tp.Shutdown(context.Background()) }
}

func main() {
	initLogger()
	shutdownTracer := initTracer(context.Background())
	defer shutdownTracer()

	port := os.Getenv("PORT")
	if port == "" {
		port = "3000"
	}

	grpcAddr := os.Getenv("GRPC_ADDR")
	if grpcAddr == "" {
		grpcAddr = "localhost:5000"
	}
	grpcClient, err := grpc.NewClient(context.Background(), grpcAddr)
	if err != nil {
		slog.Error("failed to connect to gRPC backend", "err", err)
		os.Exit(1)
	}
	defer func() { _ = grpcClient.Close() }()
	handlers.SetEventsClient(grpcClient)

	var pool *pgxpool.Pool
	if dsn := os.Getenv("DATABASE_URL"); dsn != "" {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		p, err := newDBPool(ctx, dsn, dbPoolSizeFromEnv())
		cancel()
		if err != nil {
			// Only a malformed DSN lands here; an unreachable server does not,
			// because the pool is created lazily (see newDBPool).
			slog.Warn("invalid database configuration; DB-backed endpoints will return 503", "err", redactConnErr(err))
		} else {
			pool = p
			defer pool.Close()
		}
	} else {
		slog.Warn("DATABASE_URL not set; DB-backed endpoints will return 503")
	}

	var healthDB handlers.DBPool
	if pool != nil {
		healthDB = pool
	}

	var schemaRegistryDB handlers.SchemaRegistryDB
	if pool != nil {
		schemaRegistryDB = pool
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379"
	}
	redisOpts, err := redis.ParseURL(redisURL)
	if err != nil {
		slog.Error("invalid REDIS_URL", "err", redactConnErr(err))
		os.Exit(1)
	}
	redisClient := redis.NewClient(redisOpts)
	defer func() { _ = redisClient.Close() }()

	hub := ws.NewHub()
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go ws.StartConsumer(ctx, redisClient, hub)
	// Best-effort cache invalidation (issue #221): bumps a contract's cache
	// version the moment a new event for it arrives, so ResponseCache-wrapped
	// endpoints (contract spec below) stop serving stale responses
	// immediately rather than waiting out their TTL.
	go middleware.StartCacheInvalidator(ctx, redisClient, ws.StreamKey)

	// Start API-key usage tracker (issue #139). Flushes request_count /
	// last_used_at to postgres in batches every 5s so auth never blocks.
	var usageTrack chan<- string
	var usageStop func()
	if pool != nil {
		usageTrack, usageStop = handlers.NewAPIKeyUsageTracker(pool, 5*time.Second)
		defer usageStop()
	}

	// Start async audit log writer (issue #162). Batches entries every 500ms
	// and inserts them in bulk — zero latency added to the request path.
	var auditWriter *middleware.AuditWriter
	if pool != nil {
		auditWriter = middleware.NewAuditWriter(
			pool, slog.Default(), 500*time.Millisecond, 100, 10000,
		)
		defer auditWriter.Close()
	}

	// Start automated retention job (issue #245). Replaces the ad-hoc audit
	// cleanup with a configurable per-table retention policy.
	if pool != nil {
		startRetentionJob(ctx, pool)
	}

	// Periodically recompute contract_stats_rollup from soroban_events so
	// GET /v1/stats/contracts can read a small pre-aggregated table instead
	// of a live GROUP BY on every cache miss (issue #257).
	if pool != nil {
		go runContractStatsRollupRefresh(ctx, pool)
	}

	// Periodically export pgxpool saturation stats (issue #238) — total/idle/
	// acquired conns and acquire-wait time are the direct signal that a burst
	// of slow queries is starving the pool.
	if pool != nil {
		go metrics.PollDBPool(ctx, pool, dbPoolMetricsPollInterval)
	}

	// Re-aggregate audit_log into usage_rollup so GET /v1/usage reads a
	// pre-aggregated table, and bound that table's growth. Both loops and
	// their four interval constants already existed but were never started —
	// RunUsageRollupLoop's own doc comment says it is "called from main() as
	// a background goroutine", and it was not.
	if pool != nil {
		go handlers.RunUsageRollupLoop(ctx, pool, usageRollupInterval, usageRollupLookback)
		go runUsageRollupCleanup(ctx, pool)
	}

	adminCfg := handlers.AdminConfig{
		AdminKey: os.Getenv("ADMIN_API_KEY"),
		DB:       pool,
	}
	if adminURL := os.Getenv("PGBOUNCER_ADMIN_URL"); adminURL != "" {
		adminCfg.StatsFunc = newPgbouncerStats(adminURL)
	}

	// Validate CORS allowlist at startup (issue #234).
	allowedOrigins, err := middleware.ValidateAllowedOrigins()
	if err != nil {
		slog.Error("invalid CORS configuration", "err", err)
		os.Exit(1)
	}

	// Require a pinned idempotency encryption key in production: unset, each
	// replica generates its own random key and a retried create landing on a
	// different replica can't decrypt the first attempt's record, which can
	// double-create a resource (see idempotency.go).
	if err := middleware.ValidateIdempotencyEncryptionConfig(); err != nil {
		slog.Error("invalid idempotency encryption configuration", "err", err)
		os.Exit(1)
	}

	// Shared tier cache so an admin tier change (PATCH /v1/api-keys/{id}) can
	// evict the stale entry immediately instead of waiting for the TTL (#229).
	tierCache := middleware.NewTierCache()

	apiKeyCfg := handlers.APIKeyConfig{
		AdminKey:       os.Getenv("ADMIN_API_KEY"),
		DB:             pool,
		Redis:          redisClient,
		InvalidateTier: tierCache.Invalidate,
	}

	webhookDB, err := newDB()
	if err != nil {
		slog.Warn("database unavailable for webhook handlers", "err", err)
	} else {
		defer func() { _ = webhookDB.Close() }()
	}
	startWebhookWorker(ctx, webhookDB, redisClient)
	startWebhookCleanupJob(ctx, webhookDB)

	// Configure internal status handler with dependencies.
	handlers.SetInternalStatusDeps(pool, redisClient, hub)

	mux := http.NewServeMux()
	// Route registration lives in routes.go as a single table shared with the
	// OpenAPI inventory contract test (issue #513): every route is either
	// documented in api/openapi.yaml or carries an explicit exemption, and the
	// test fails on any drift in either direction.
	// nil (untyped) when STELLAR_RPC_URL is unset, so CallContract's `rpc ==
	// nil` check reports 503 rather than a typed-nil interface slipping
	// through and panicking on first use.
	var sorobanCaller handlers.SorobanRPCCaller
	if rpcURL := os.Getenv("STELLAR_RPC_URL"); rpcURL != "" {
		rpcClient, err := sorobanrpc.NewClient(rpcURL)
		if err != nil {
			// A malformed STELLAR_RPC_HEADERS must not silently start an
			// unauthenticated client that 401s every contract call.
			slog.Error("invalid Stellar RPC configuration", "err", err)
			os.Exit(1)
		}
		sorobanCaller = rpcClient
	}
	var rlDB middleware.TierDB
	if pool != nil {
		rlDB = pool
	}
	rlCfg := middleware.RateLimitConfig{Redis: redisClient, DB: rlDB, Cache: tierCache}

	// DB-backed auth middleware with Redis caching and env-var fallback.
	var authDB middleware.DBAuthConfig
	if pool != nil {
		authDB.DB = pool
	}
	authDB.Redis = redisClient
	authDB.UsageTrack = usageTrack

	registerRoutes(mux, routeDeps{
		rlCfg:            rlCfg,
		authDB:           authDB,
		pool:             pool,
		healthDB:         healthDB,
		schemaRegistryDB: schemaRegistryDB,
		redisClient:      redisClient,
		grpcClient:       grpcClient,
		adminCfg:         adminCfg,
		contractCfg:      handlers.ContractConfig{AdminKey: os.Getenv("ADMIN_API_KEY"), DB: pool},
		apiKeyCfg:        apiKeyCfg,
		sorobanCaller:    sorobanCaller,
		webhookDB:        webhookDB,
		hub:              hub,
	})

	handler := middleware.NewBodySizeLimitFromEnv()(mux)
	handler = middleware.TieredRateLimit(rlCfg)(handler)
	handler = middleware.NewDBAuth(authDB)(handler)
	// AuditMiddleware wraps NewDBAuth (issue #609), not the other way around:
	// it must run on every request regardless of auth outcome, so a rejected
	// 401 is still written to audit_log. With AuditMiddleware inside NewDBAuth,
	// NewDBAuth's early return on a missing/invalid key never reached
	// next.ServeHTTP, so AuditMiddleware's write never ran and failed auth
	// attempts left no audit trail at all.
	if auditWriter != nil {
		handler = middleware.AuditMiddleware(auditWriter)(handler)
	}
	handler = middleware.NewCompression()(handler)
	// Per-IP rate limit runs BEFORE auth (issue #318): it wraps the handler
	// chain built so far, so it executes ahead of NewDBAuth for every
	// request, containing abusive/unauthenticated traffic before a DB/Redis
	// lookup is spent on it.
	handler = middleware.NewPerIPRateLimitFromEnv(redisClient)(handler)
	handler = middleware.NewCORSFromEnv(allowedOrigins)(middleware.NewTimeoutFromEnv()(handler))
	handler = middleware.SecurityHeaders(true)(handler)
	handler = middleware.TracingMiddleware(handler)
	// RequestID + StructuredLogging are outermost so every response — including
	// auth and rate-limit rejections — is assigned a request id, echoes it on
	// X-Request-ID, and is captured in structured logs (issue #226). RequestID
	// must precede StructuredLogging so the id is in context when the log line
	// is emitted.
	handler = middleware.Chain(handler, middleware.RequestID, middleware.StructuredLogging(mux))
	// Global concurrency cap is the outermost middleware of all (issue #318):
	// it must shed load before any other work — auth lookups, rate-limit
	// Redis calls, logging — is spent on a request that's going to be
	// rejected anyway.
	handler = middleware.NewGlobalConcurrencyLimitFromEnv()(handler)
	// Metrics middleware wraps everything up to this point (issue #58): it
	// must see every response, including ones shed by GlobalConcurrencyLimit,
	// to report accurate per-endpoint counts/latency.
	handler = middleware.NewMetrics(mux)(handler)
	// Recover is the true outermost wrap (issue #610): it must sit ahead of
	// every other middleware, including Metrics, so a panic anywhere in the
	// chain — not just in a leaf handler — is caught, logged, counted, and
	// answered with a 500 instead of dropping the connection uncontained.
	handler = middleware.Recover(handler)
	// Opt-in, internal-only pprof server (off unless PPROF_ENABLED=true). It is
	// never mounted on the public mux above (#299).
	pprofSrv := profiling.Start()
	defer profiling.Shutdown(pprofSrv)

	// Internal Prometheus metrics server on METRICS_PORT (default 9091,
	// issue #58) — the only place metrics are served. The public listener has
	// no /metrics route; the hand-rolled legacy metrics live at
	// /metrics/legacy on this port.
	metricsSrv := metrics.Start(handlers.MetricsHandler(pool, redisClient))
	defer metrics.Shutdown(metricsSrv)

	// Grace period mirrors Helm terminationGracePeriodSeconds (default 30s).
	const shutdownGrace = 30 * time.Second

	server := &http.Server{
		Addr:         fmt.Sprintf(":%s", port),
		Handler:      handler,
		ReadTimeout:  10 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  120 * time.Second,
		// Bounds total request-line + header size (issue #317). nginx also
		// enforces large_client_header_buffers in front of this in the
		// docker-compose deployment, but MaxHeaderBytes is set independently
		// since nginx is not guaranteed to be in front of every deployment
		// (e.g. Fly.io apps hit the Go server directly).
		MaxHeaderBytes: 1 << 20, // 1 MiB
	}
	go func() {
		slog.Info("Trident API server listening", "port", port)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "err", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down", "grace", shutdownGrace)

	// Stop accepting new connections and begin draining in-flight requests.
	shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownGrace)
	defer cancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		slog.Error("graceful shutdown failed", "err", err)
	}

	// After the HTTP server stops accepting requests, close active SSE/WS
	// streams so connected clients receive a clean close instead of a TCP RST.
	hub.ShutdownAll()

	slog.Info("shutdown complete")
}

// buildPoolConfig parses dsn and applies pool sizing, lifecycle, and
// statement-timeout settings (issue #238). It does not connect — safe to
// call from tests without a live database.
func buildPoolConfig(dsn string, poolSize int32) (*pgxpool.Config, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse DATABASE_URL: %w", err)
	}
	cfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol

	cfg.MaxConns = poolSize
	cfg.MinConns = envInt32("GO_API_DB_POOL_MIN_CONNS", defaultDBPoolMinConns)
	cfg.MaxConnLifetime = envDurationMS("GO_API_DB_POOL_MAX_CONN_LIFETIME_MS", defaultDBPoolMaxConnLifetimeMS)
	cfg.MaxConnLifetimeJitter = cfg.MaxConnLifetime * dbPoolMaxConnLifetimeJitterPercent / 100
	cfg.MaxConnIdleTime = envDurationMS("GO_API_DB_POOL_MAX_CONN_IDLE_TIME_MS", defaultDBPoolMaxConnIdleTimeMS)
	cfg.HealthCheckPeriod = envDurationMS("GO_API_DB_POOL_HEALTH_CHECK_PERIOD_MS", defaultDBPoolHealthCheckPeriodMS)

	// Bound how long a single statement or an idle-in-transaction connection
	// may hold a pool slot (issue #238) — a runaway query or a leaked
	// transaction must not be able to stall the whole pool. Shared env vars
	// with the Rust indexer (crates/indexer/src/config.rs) so both services
	// agree; see #249 for database-level (role/cluster) coordination.
	//
	// These SETs run once per physical connection at AfterConnect time,
	// before any transaction begins — safe for a direct DATABASE_URL
	// connection. If DATABASE_URL is ever pointed at PgBouncer in
	// transaction-pooling mode (today only PGBOUNCER_ADMIN_URL, the admin
	// console, is used — see pgbouncer.go), this needs revisiting against
	// PgBouncer's parameter-tracking behavior (#249, #256).
	stmtTimeoutMS := envIntBounded("DB_STATEMENT_TIMEOUT_MS", defaultStatementTimeoutMS, statementTimeoutMinMS, statementTimeoutMaxMS)
	idleTimeoutMS := envIntBounded("DB_IDLE_IN_TRANSACTION_TIMEOUT_MS", defaultIdleInTransactionTimeoutMS, statementTimeoutMinMS, statementTimeoutMaxMS)
	cfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = '%dms'", stmtTimeoutMS)); err != nil {
			return fmt.Errorf("set statement_timeout: %w", err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("SET idle_in_transaction_session_timeout = '%dms'", idleTimeoutMS)); err != nil {
			return fmt.Errorf("set idle_in_transaction_session_timeout: %w", err)
		}
		return nil
	}

	return cfg, nil
}

// newDBPool builds the pool without requiring Postgres to be reachable.
// pgxpool dials on demand and re-dials after failures, so a database that is
// down or failing over at boot recovers on its own once it is back — the pod
// does not have to be restarted, and every DB-backed component stays wired.
// While the database is unreachable, requests fail (and /v1/ready reports it)
// rather than the components being disabled for the life of the process.
func newDBPool(ctx context.Context, dsn string, poolSize int32) (*pgxpool.Pool, error) {
	cfg, err := buildPoolConfig(dsn, poolSize)
	if err != nil {
		return nil, err
	}
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, err
	}
	if err := pool.Ping(ctx); err != nil {
		slog.Warn("database not reachable at startup; will connect when it becomes available", "err", redactConnErr(err))
	}
	return pool, nil
}

func dbPoolSizeFromEnv() int32 {
	if raw := os.Getenv("GO_API_DB_POOL_SIZE"); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n > 0 {
			return int32(n)
		}
		slog.Warn("invalid GO_API_DB_POOL_SIZE; using default", "value", raw, "default", defaultDBPoolSize)
	}
	return defaultDBPoolSize
}

// envInt32 reads a non-negative int32 env var, falling back to def on
// missing/invalid input (issue #238).
func envInt32(key string, def int32) int32 {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		slog.Warn("invalid env value; using default", "key", key, "value", raw, "default", def)
		return def
	}
	return int32(n)
}

// envDurationMS reads a millisecond duration env var, falling back to defMS
// on missing/invalid input (issue #238).
func envDurationMS(key string, defMS int) time.Duration {
	raw := os.Getenv(key)
	if raw == "" {
		return time.Duration(defMS) * time.Millisecond
	}
	n, err := strconv.Atoi(raw)
	if err != nil || n < 0 {
		slog.Warn("invalid env value; using default", "key", key, "value", raw, "default_ms", defMS)
		return time.Duration(defMS) * time.Millisecond
	}
	return time.Duration(n) * time.Millisecond
}

// envIntBounded reads an int env var clamped to [min, max], falling back to
// def on missing/invalid input (issue #238). Mirrors the Rust indexer's
// parse_bounded_u64 (crates/indexer/src/config.rs) so both services validate
// DB_STATEMENT_TIMEOUT_MS/DB_IDLE_IN_TRANSACTION_TIMEOUT_MS the same way.
func envIntBounded(key string, def, min, max int) int {
	raw := os.Getenv(key)
	if raw == "" {
		return def
	}
	n, err := strconv.Atoi(raw)
	if err != nil {
		slog.Warn("invalid env value; using default", "key", key, "value", raw, "default", def)
		return def
	}
	if n < min || n > max {
		slog.Warn("env value out of range; clamping", "key", key, "value", n, "min", min, "max", max)
		if n < min {
			return min
		}
		return max
	}
	return n
}

// runContractStatsRollupRefresh recomputes contract_stats_rollup on a fixed
// interval until ctx is cancelled (issue #257). Runs once immediately so the
// rollup is populated shortly after startup rather than only after the first
// tick.
func runContractStatsRollupRefresh(ctx context.Context, pool *pgxpool.Pool) {
	refresh := func() {
		refreshCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		if err := handlers.RefreshContractStatsRollup(refreshCtx, pool); err != nil {
			slog.Warn("contract stats rollup refresh failed", "err", err)
		}
	}

	// Delay first run, and use advisory lock (issue #731)
	time.Sleep(60 * time.Second)
	var locked bool
	if err := pool.QueryRow(ctx, "SELECT pg_try_advisory_lock(123456789, 101)").Scan(&locked); err == nil && locked {
		refresh()
		_, _ = pool.Exec(ctx, "SELECT pg_advisory_unlock(123456789, 101)")
	}

	ticker := time.NewTicker(contractStatsRollupRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			var locked bool
			if err := pool.QueryRow(ctx, "SELECT pg_try_advisory_lock(123456789, 101)").Scan(&locked); err == nil && locked {
				refresh()
				_, _ = pool.Exec(ctx, "SELECT pg_advisory_unlock(123456789, 101)")
			}
		}
	}
}

// retentionConfig holds per-table retention windows (in days).
// Configured via env vars with sensible defaults.
type retentionConfig struct {
	AuditLogDays          int
	ParseErrorsDays       int
	WebhookDeliveriesDays int
	SorobanEventsDays     int
	EventOutboxDays       int
}

func loadRetentionConfig() retentionConfig {
	return retentionConfig{
		AuditLogDays:          envInt("RETENTION_AUDIT_LOG_DAYS", 90),
		ParseErrorsDays:       envInt("RETENTION_PARSE_ERRORS_DAYS", 30),
		WebhookDeliveriesDays: envInt("RETENTION_WEBHOOK_DELIVERIES_DAYS", 30),
		// Highest-volume table; unlike audit_log/parse_errors/webhook_deliveries
		// it previously defaulted to 0 (disabled), letting it grow unbounded
		// with growing indexes/WAL/vacuum pressure unless an operator opted in
		// explicitly (#645). 90 days matches the audit log's window.
		SorobanEventsDays: envInt("RETENTION_SOROBAN_EVENTS_DAYS", 90),
		// event_outbox is the fastest-growing unbounded table (issue #604):
		// rows are only ever flipped published = TRUE, never deleted, and
		// each carries a full JSONB copy of the event. 7 days is generous
		// relative to the relay's normal publish latency (seconds), while
		// still giving an operator a window to notice and recover a stuck
		// relay before its backlog is pruned out from under it.
		EventOutboxDays: envInt("RETENTION_EVENT_OUTBOX_DAYS", 7),
	}
}

func envInt(key string, defaultVal int) int {
	if raw := os.Getenv(key); raw != "" {
		if n, err := strconv.Atoi(raw); err == nil && n >= 0 {
			return n
		}
	}
	return defaultVal
}

// startRetentionJob runs a periodic retention cleanup loop (issue #245).
// It replaces the ad-hoc audit cleanup with a configurable per-table policy.
func startRetentionJob(ctx context.Context, pool *pgxpool.Pool) {
	cfg := loadRetentionConfig()
	interval := 6 * time.Hour

	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()

		// Delay first run to avoid re-running on restart/crash-loop
		time.Sleep(60 * time.Second)

		run := func() {
			// Leader election using advisory lock (issue #731)
			var locked bool
			if err := pool.QueryRow(ctx, "SELECT pg_try_advisory_lock(123456789, 100)").Scan(&locked); err != nil || !locked {
				return
			}
			defer pool.Exec(context.Background(), "SELECT pg_advisory_unlock(123456789, 100)")
			cleanupCtx, cancel := context.WithTimeout(ctx, 60*time.Second)
			defer cancel()

			tables := []struct {
				name  string
				days  int
				query string
			}{
				{"audit_log", cfg.AuditLogDays,
					`DELETE FROM audit_log WHERE ts < NOW() - ($1 || ' days')::INTERVAL AND ctid IN (
						SELECT ctid FROM audit_log WHERE ts < NOW() - ($1 || ' days')::INTERVAL LIMIT 1000
					)`},
				{"parse_errors", cfg.ParseErrorsDays,
					`DELETE FROM parse_errors WHERE occurred_at < NOW() - ($1 || ' days')::INTERVAL AND ctid IN (
						SELECT ctid FROM parse_errors WHERE occurred_at < NOW() - ($1 || ' days')::INTERVAL LIMIT 1000
					)`},
				{"webhook_deliveries", cfg.WebhookDeliveriesDays,
					`DELETE FROM webhook_deliveries WHERE delivered_at < NOW() - ($1 || ' days')::INTERVAL AND ctid IN (
						SELECT ctid FROM webhook_deliveries WHERE delivered_at < NOW() - ($1 || ' days')::INTERVAL LIMIT 1000
					)`},
				{"soroban_events", cfg.SorobanEventsDays,
					`DELETE FROM soroban_events WHERE created_at < NOW() - ($1 || ' days')::INTERVAL AND ctid IN (
						SELECT ctid FROM soroban_events WHERE created_at < NOW() - ($1 || ' days')::INTERVAL LIMIT 1000
					)`},
				// event_outbox additionally requires published = TRUE: an
				// unpublished row must never be deleted regardless of age,
				// since the relay has not yet delivered it (issue #604).
				{"event_outbox", cfg.EventOutboxDays,
					`DELETE FROM event_outbox WHERE published = TRUE AND published_at < NOW() - ($1 || ' days')::INTERVAL AND ctid IN (
						SELECT ctid FROM event_outbox WHERE published = TRUE AND published_at < NOW() - ($1 || ' days')::INTERVAL LIMIT 1000
					)`},
			}

			for _, t := range tables {
				if t.days <= 0 {
					continue
				}
				for {
					tag, err := pool.Exec(cleanupCtx, t.query, fmt.Sprintf("%d", t.days))
					if err != nil {
						slog.Warn("retention: cleanup failed", "table", t.name, "err", err)
						break
					}
					if tag.RowsAffected() > 0 {
						metrics.RetentionRowsDeletedTotal.WithLabelValues(t.name).Add(float64(tag.RowsAffected()))
					}
					if tag.RowsAffected() == 0 {
						break
					}
				}
			}
		}

		// Run once at startup, then on ticker.
		run()

	for {
		select {
		case <-ctx.Done():
			// Don't run on exit with locks
			return
		case <-ticker.C:
			run()
		}
	}
	}()
}

// runUsageRollupCleanup bounds usage_rollup storage by deleting daily buckets
// older than usageRollupRetention (~13 months) — generous relative to the
// 90-day audit_log retention since usage_rollup is O(keys * days), not
// O(requests).
func runUsageRollupCleanup(ctx context.Context, pool *pgxpool.Pool) {
	ticker := time.NewTicker(usageRollupCleanupInterval)
	defer ticker.Stop()

	cleanup := func() {
		cleanupCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		var locked bool
		if err := pool.QueryRow(cleanupCtx, "SELECT pg_try_advisory_lock(123456789, 103)").Scan(&locked); err != nil || !locked {
			return
		}
		defer pool.Exec(context.Background(), "SELECT pg_advisory_unlock(123456789, 103)")
		if _, err := pool.Exec(cleanupCtx,
			`DELETE FROM usage_rollup WHERE period_start < NOW() - $1::interval`,
			fmt.Sprintf("%d seconds", int64(usageRollupRetention.Seconds())),
		); err != nil {
			slog.Warn("usage rollup cleanup failed", "err", err)
		}
	}

	time.Sleep(120 * time.Second)
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			cleanup()
		}
	}
}
