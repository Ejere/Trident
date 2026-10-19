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

func initTracer(ctx context.Context) func() {
	endpoint := os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")
	if endpoint == "" {
		return func() {}
	}

	samplingRatio := 0.1
	if r := os.Getenv("OTEL_SAMPLING_RATIO"); r != "" {
		if f, err := strconv.ParseFloat(r, 64); err == nil {
			samplingRatio = f
		}
	}

	exporter, err := otlptracegrpc.New(ctx,
		otlptracegrpc.WithEndpoint(endpoint),
		otlptracegrpc.WithInsecure(),
	)
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

	tp := sdktrace.NewTracerProvider(
		sdktrace.WithBatcher(exporter),
		sdktrace.WithResource(res),
		sdktrace.WithSampler(sdktrace.TraceIDRatioBased(samplingRatio)),
	)
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return func() { _ = tp.Shutdown(context.Background()) }
}

func main() {
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
			slog.Warn("could not connect to database; DB-backed endpoints will return 503", "err", redactConnErr(err))
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

	// Configure internal status handler with dependencies.
	handlers.SetInternalStatusDeps(pool, redisClient, hub)

	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/health", handlers.Health())
	mux.HandleFunc("GET /v1/ready", handlers.Ready(healthDB, redisClient, grpcClient))
	mux.HandleFunc("GET /v1/events", handlers.ListEvents)
	mux.HandleFunc("POST /v1/events/batch", handlers.BatchGetEvents)
	mux.HandleFunc("GET /v1/events/{id}", handlers.GetEvent)
	mux.HandleFunc("GET /v1/events/stream", handlers.Stream(redisClient))
	mux.HandleFunc("GET /v1/admin/db", handlers.AdminDB(adminCfg))
	mux.HandleFunc("GET /v1/admin/keys/{id}/usage", handlers.AdminKeyUsage(adminCfg))
	// Admin contract registration CRUD (issue #230)
	contractCfg := handlers.ContractConfig{AdminKey: os.Getenv("ADMIN_API_KEY"), DB: pool}
	mux.HandleFunc("POST /v1/admin/contracts", handlers.CreateContract(contractCfg))
	mux.HandleFunc("GET /v1/admin/contracts", handlers.ListContracts(contractCfg))
	mux.HandleFunc("DELETE /v1/admin/contracts/{id}", handlers.DeleteContract(contractCfg))
	// API key management (admin-only via X-Admin-Key header)
	mux.HandleFunc("POST /v1/api-keys", handlers.CreateAPIKey(apiKeyCfg))
	mux.HandleFunc("GET /v1/api-keys", handlers.ListAPIKeys(apiKeyCfg))
	mux.HandleFunc("PATCH /v1/api-keys/{id}", handlers.UpdateAPIKey(apiKeyCfg))
	mux.HandleFunc("DELETE /v1/api-keys/{id}", handlers.DeleteAPIKey(apiKeyCfg))
	mux.HandleFunc("GET /v1/stats/indexer", handlers.IndexerStats(healthDB))
	mux.HandleFunc("GET /v1/contracts/{id}/events/schema", handlers.ContractEventSchemas(schemaRegistryDB))
	mux.HandleFunc("GET /v1/contracts/{id}/spec", handlers.ContractSpec(schemaRegistryDB))
	mux.HandleFunc("GET /v1/contracts/{id}/storage", handlers.ContractStorageLatest(schemaRegistryDB))
	mux.HandleFunc("GET /v1/contracts/{id}/storage/history", handlers.ContractStorageHistory(schemaRegistryDB))
	mux.HandleFunc("GET /v1/stats/contracts", handlers.ContractsStats(pool, redisClient))
	// nil (untyped) when STELLAR_RPC_URL is unset, so CallContract's `rpc ==
	// nil` check reports 503 rather than a typed-nil interface slipping
	// through and panicking on first use.
	var sorobanCaller handlers.SorobanRPCCaller
	if rpcURL := os.Getenv("STELLAR_RPC_URL"); rpcURL != "" {
		sorobanCaller = sorobanrpc.NewClient(rpcURL)
	}
	mux.HandleFunc("POST /v1/contracts/{id}/call", handlers.CallContract(sorobanCaller))
	mux.HandleFunc("GET /v1/webhooks", listWebhooksHandler(webhookDB))
	mux.HandleFunc("POST /v1/webhooks", createWebhookHandler(webhookDB))
	mux.HandleFunc("DELETE /v1/webhooks/{id}", deleteWebhookHandler(webhookDB))
	mux.HandleFunc("PATCH /v1/webhooks/{id}/pause", pauseWebhookHandler(webhookDB))
	mux.HandleFunc("PATCH /v1/webhooks/{id}/resume", resumeWebhookHandler(webhookDB))
	mux.HandleFunc("GET /v1/webhooks/{id}/deliveries", deliveriesWebhookHandler(webhookDB))
	mux.HandleFunc("GET /v1/webhooks/{id}/dead-letters", deadLettersWebhookHandler(webhookDB))
	mux.HandleFunc("POST /v1/webhooks/{id}/dead-letters/{deliveryId}/replay", replayDeadLetterHandler(webhookDB))
	mux.HandleFunc("GET /metrics", handlers.MetricsHandler(pool, redisClient))
	mux.HandleFunc("GET /internal/status", handlers.InternalStatus())
	mux.Handle("/ws", middleware.WSConnectionLimit(ws.Handler(hub)))
	keyValidator := middleware.Validator(middleware.ParseKeyHashes(os.Getenv("API_KEY_HASHES")))
	mux.Handle("/graphql", middleware.WSConnectionLimit(ws.GraphQLHandler(hub, keyValidator)))

	_ = usageTrack // passed to middleware in future; declared for shutdown ordering

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

	handler := middleware.NewBodySizeLimitFromEnv()(mux)
	handler = middleware.TieredRateLimit(rlCfg)(handler)
	if auditWriter != nil {
		handler = middleware.AuditMiddleware(auditWriter)(handler)
	}
	handler = middleware.NewDBAuth(authDB)(handler)
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
	handler = middleware.Chain(handler, middleware.RequestID, middleware.StructuredLogging)
	// Global concurrency cap is the outermost middleware of all (issue #318):
	// it must shed load before any other work — auth lookups, rate-limit
	// Redis calls, logging — is spent on a request that's going to be
	// rejected anyway.
	handler = middleware.NewGlobalConcurrencyLimitFromEnv()(handler)
	// Metrics middleware is the absolute outermost wrap (issue #58): it must
	// see every response, including ones shed by GlobalConcurrencyLimit, to
	// report accurate per-endpoint counts/latency.
	handler = middleware.NewMetrics(mux)(handler)
	// Opt-in, internal-only pprof server (off unless PPROF_ENABLED=true). It is
	// never mounted on the public mux above (#299).
	pprofSrv := profiling.Start()
	defer profiling.Shutdown(pprofSrv)

	// Internal Prometheus metrics server on METRICS_PORT (default 9091,
	// issue #58) — separate port from the public API and from the legacy
	// /metrics route mounted above.
	metricsSrv := metrics.Start()
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
		pool.Close()
		return nil, fmt.Errorf("ping database: %w", err)
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

	refresh()

	ticker := time.NewTicker(contractStatsRollupRefreshInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			refresh()
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
}

func loadRetentionConfig() retentionConfig {
	return retentionConfig{
		AuditLogDays:          envInt("RETENTION_AUDIT_LOG_DAYS", 90),
		ParseErrorsDays:       envInt("RETENTION_PARSE_ERRORS_DAYS", 30),
		WebhookDeliveriesDays: envInt("RETENTION_WEBHOOK_DELIVERIES_DAYS", 30),
		SorobanEventsDays:     envInt("RETENTION_SOROBAN_EVENTS_DAYS", 0), // 0 = disabled
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

		run := func() {
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
				run()
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
		if _, err := pool.Exec(cleanupCtx,
			`DELETE FROM usage_rollup WHERE period_start < NOW() - $1::interval`,
			fmt.Sprintf("%d seconds", int64(usageRollupRetention.Seconds())),
		); err != nil {
			slog.Warn("usage rollup cleanup failed", "err", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			cleanup()
			return
		case <-ticker.C:
			cleanup()
		}
	}
}
