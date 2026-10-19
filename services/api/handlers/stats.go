package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	apigrpc "github.com/Depo-dev/trident/services/api/grpc"
	"github.com/Depo-dev/trident/services/api/internal/httputil"
	"github.com/Depo-dev/trident/services/api/middleware"
	"github.com/Depo-dev/trident/services/api/validation"
	"github.com/Depo-dev/trident/services/api/ws"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// Lightweight Prometheus gauge registry (no external dependency)
// ---------------------------------------------------------------------------

type atomicGauge struct {
	bits atomic.Uint64 // stores float64 bits
}

func (g *atomicGauge) Set(v float64) {
	g.bits.Store(math.Float64bits(v))
}

func (g *atomicGauge) Get() float64 {
	return math.Float64frombits(g.bits.Load())
}

var (
	metricLagLedgers          atomicGauge
	metricLagSecondsEstimated atomicGauge
	metricLastPollTimestamp   atomicGauge
	metricEventsTotal         atomicGauge

	// Webhook delivery counters (#241).
	metricWebhookSuccess    atomic.Int64
	metricWebhookFailed     atomic.Int64
	metricWebhookDeadLetter atomic.Int64
	metricWebhookDurationMs atomic.Int64
	metricWebhookTotal      atomic.Int64

	// SSE slow-consumer disconnects (#224). SSE has no shared per-message
	// drop path like the WS hub — a stalled SSE client is detected by the
	// write deadline in Stream() failing, which always means "disconnect",
	// so there is no separate drop counter to pair this with.
	metricSSESlowConsumerDisconnects atomic.Int64
)

// RecordWebhookDelivery records the outcome and round-trip latency of a single
// webhook delivery attempt. Called from the webhook worker (#241).
func RecordWebhookDelivery(success bool, deadLetter bool, durationMs int64) {
	metricWebhookDurationMs.Add(durationMs)
	metricWebhookTotal.Add(1)
	switch {
	case deadLetter:
		metricWebhookDeadLetter.Add(1)
	case success:
		metricWebhookSuccess.Add(1)
	default:
		metricWebhookFailed.Add(1)
	}
}

// MetricsHandler exposes the Go API's Prometheus metrics in text format:
// indexer-status gauges (populated as a side effect of GET /v1/stats/indexer;
// note the trident_api_indexer_* prefix, which avoids colliding with the
// indexer's own same-named counters/gauges of a different type), HTTP
// request count/latency (all routes, via middleware.PrometheusHTTP), gRPC
// client call count/latency, DB connection pool utilisation, and the
// trident:events Redis Stream length. Mount at GET /metrics.
func MetricsHandler(pool *pgxpool.Pool, rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
		_, _ = fmt.Fprintf(w, "# HELP trident_indexer_lag_ledgers Number of ledgers the indexer is behind the chain tip.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_indexer_lag_ledgers gauge\n")
		_, _ = fmt.Fprintf(w, "trident_indexer_lag_ledgers %g\n", metricLagLedgers.Get())
		_, _ = fmt.Fprintf(w, "# HELP trident_indexer_lag_seconds_estimated Estimated wall-clock lag: lag_ledgers * average ledger close time (issue #294).\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_indexer_lag_seconds_estimated gauge\n")
		_, _ = fmt.Fprintf(w, "trident_indexer_lag_seconds_estimated %g\n", metricLagSecondsEstimated.Get())
		_, _ = fmt.Fprintf(w, "# HELP trident_indexer_last_poll_timestamp_seconds Unix timestamp of the last successful indexer poll.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_indexer_last_poll_timestamp_seconds gauge\n")
		_, _ = fmt.Fprintf(w, "trident_indexer_last_poll_timestamp_seconds %g\n", metricLastPollTimestamp.Get())
		_, _ = fmt.Fprintf(w, "# HELP trident_indexer_events_total Cumulative events indexed.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_indexer_events_total gauge\n")
		_, _ = fmt.Fprintf(w, "trident_indexer_events_total %g\n", metricEventsTotal.Get())
		apigrpc.WriteClientMetrics(w)

		// Webhook delivery metrics (#241).
		_, _ = fmt.Fprintf(w, "# HELP trident_webhook_deliveries_success_total Successful webhook deliveries since startup.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_webhook_deliveries_success_total counter\n")
		_, _ = fmt.Fprintf(w, "trident_webhook_deliveries_success_total %d\n", metricWebhookSuccess.Load())
		_, _ = fmt.Fprintf(w, "# HELP trident_webhook_deliveries_failed_total Failed (retryable) webhook deliveries since startup.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_webhook_deliveries_failed_total counter\n")
		_, _ = fmt.Fprintf(w, "trident_webhook_deliveries_failed_total %d\n", metricWebhookFailed.Load())
		_, _ = fmt.Fprintf(w, "# HELP trident_webhook_deliveries_dead_lettered_total Webhook deliveries exhausted all retries.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_webhook_deliveries_dead_lettered_total counter\n")
		_, _ = fmt.Fprintf(w, "trident_webhook_deliveries_dead_lettered_total %d\n", metricWebhookDeadLetter.Load())
		total := metricWebhookTotal.Load()
		var meanMs float64
		if total > 0 {
			meanMs = float64(metricWebhookDurationMs.Load()) / float64(total)
		}
		_, _ = fmt.Fprintf(w, "# HELP trident_webhook_delivery_mean_duration_ms Mean delivery round-trip latency in milliseconds.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_webhook_delivery_mean_duration_ms gauge\n")
		_, _ = fmt.Fprintf(w, "trident_webhook_delivery_mean_duration_ms %g\n", meanMs)

		_, _ = fmt.Fprint(w, "# HELP trident_api_indexer_lag_ledgers Number of ledgers the indexer is behind the chain tip, as last observed via GET /v1/stats/indexer.\n")
		_, _ = fmt.Fprint(w, "# TYPE trident_api_indexer_lag_ledgers gauge\n")
		_, _ = fmt.Fprintf(w, "trident_api_indexer_lag_ledgers %g\n", metricLagLedgers.Get())
		_, _ = fmt.Fprint(w, "# HELP trident_api_indexer_last_poll_timestamp_seconds Unix timestamp of the last successful indexer poll, as last observed via GET /v1/stats/indexer.\n")
		_, _ = fmt.Fprint(w, "# TYPE trident_api_indexer_last_poll_timestamp_seconds gauge\n")
		_, _ = fmt.Fprintf(w, "trident_api_indexer_last_poll_timestamp_seconds %g\n", metricLastPollTimestamp.Get())
		_, _ = fmt.Fprint(w, "# HELP trident_api_indexer_events_indexed Cumulative events indexed, as last observed via GET /v1/stats/indexer.\n")
		_, _ = fmt.Fprint(w, "# TYPE trident_api_indexer_events_indexed gauge\n")
		_, _ = fmt.Fprintf(w, "trident_api_indexer_events_indexed %g\n", metricEventsTotal.Get())

		middleware.WriteHTTPMetrics(w)
		middleware.WriteGRPCClientMetrics(w)

		if pool != nil {
			stat := pool.Stat()
			writeGauge(w, "trident_api_db_pool_acquired_connections", "Connections currently checked out of the API's Postgres pool.", float64(stat.AcquiredConns()))
			writeGauge(w, "trident_api_db_pool_idle_connections", "Idle (available) connections in the API's Postgres pool.", float64(stat.IdleConns()))
			writeGauge(w, "trident_api_db_pool_total_connections", "Total connections (idle + acquired) currently open in the API's Postgres pool.", float64(stat.TotalConns()))
			writeGauge(w, "trident_api_db_pool_max_connections", "Configured maximum size of the API's Postgres pool.", float64(stat.MaxConns()))
		}

		if rdb != nil {
			ctx, cancel := context.WithTimeout(r.Context(), 2*time.Second)
			defer cancel()
			if length, err := rdb.XLen(ctx, eventStreamKey).Result(); err == nil {
				writeGauge(w, "trident_api_redis_stream_length", "Length of the trident:events Redis Stream (indexer -> API consumer backlog).", float64(length))
			}
		}

		// SSE + WS/GraphQL slow-consumer backpressure metrics (#224).
		_, _ = fmt.Fprintf(w, "# HELP trident_sse_slow_consumer_disconnects_total SSE connections closed because a write exceeded the write deadline.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_sse_slow_consumer_disconnects_total counter\n")
		_, _ = fmt.Fprintf(w, "trident_sse_slow_consumer_disconnects_total %d\n", metricSSESlowConsumerDisconnects.Load())
		ws.WriteMetrics(w)

		// Abuse-protection rejection counters, split by reason (issue #318):
		// per-key (existing TieredRateLimit), per-IP, and global concurrency
		// shedding.
		rlAllowedN, rlRejectedN := middleware.RateLimitMetrics()
		_, _ = fmt.Fprintf(w, "# HELP trident_ratelimit_key_allowed_total Requests allowed by the per-API-key tiered rate limiter.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_ratelimit_key_allowed_total counter\n")
		_, _ = fmt.Fprintf(w, "trident_ratelimit_key_allowed_total %d\n", rlAllowedN)
		_, _ = fmt.Fprintf(w, "# HELP trident_ratelimit_key_rejected_total Requests rejected by the per-API-key tiered rate limiter.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_ratelimit_key_rejected_total counter\n")
		_, _ = fmt.Fprintf(w, "trident_ratelimit_key_rejected_total %d\n", rlRejectedN)

		ipAllowedN, ipRejectedN, globalAllowedN, globalRejectedN := middleware.AbuseMetrics()
		_, _ = fmt.Fprintf(w, "# HELP trident_ratelimit_ip_allowed_total Requests allowed by the pre-auth per-IP rate limiter.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_ratelimit_ip_allowed_total counter\n")
		_, _ = fmt.Fprintf(w, "trident_ratelimit_ip_allowed_total %d\n", ipAllowedN)
		_, _ = fmt.Fprintf(w, "# HELP trident_ratelimit_ip_rejected_total Requests rejected by the pre-auth per-IP rate limiter.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_ratelimit_ip_rejected_total counter\n")
		_, _ = fmt.Fprintf(w, "trident_ratelimit_ip_rejected_total %d\n", ipRejectedN)
		_, _ = fmt.Fprintf(w, "# HELP trident_concurrency_allowed_total Requests allowed through the global concurrency cap.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_concurrency_allowed_total counter\n")
		_, _ = fmt.Fprintf(w, "trident_concurrency_allowed_total %d\n", globalAllowedN)
		_, _ = fmt.Fprintf(w, "# HELP trident_concurrency_rejected_total Requests shed by the global concurrency cap.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_concurrency_rejected_total counter\n")
		_, _ = fmt.Fprintf(w, "trident_concurrency_rejected_total %d\n", globalRejectedN)
		_, _ = fmt.Fprintf(w, "# HELP trident_concurrency_in_flight Requests currently in flight.\n")
		_, _ = fmt.Fprintf(w, "# TYPE trident_concurrency_in_flight gauge\n")
		_, _ = fmt.Fprintf(w, "trident_concurrency_in_flight %d\n", middleware.InFlightRequests())
	}
}

func writeGauge(w http.ResponseWriter, name, help string, value float64) {
	_, _ = fmt.Fprintf(w, "# HELP %s %s\n", name, help)
	_, _ = fmt.Fprintf(w, "# TYPE %s gauge\n", name)
	_, _ = fmt.Fprintf(w, "%s %g\n", name, value)
}

// ---------------------------------------------------------------------------
// Chain-tip cache (10-second TTL, 2-second RPC timeout, null on failure)
// ---------------------------------------------------------------------------

type chainTipCache struct {
	mu        sync.Mutex
	ledger    *int64
	fetchedAt time.Time
}

var globalChainTipCache chainTipCache

func (c *chainTipCache) get(ctx context.Context) *int64 {
	c.mu.Lock()
	defer c.mu.Unlock()

	if c.ledger != nil && time.Since(c.fetchedAt) < 10*time.Second {
		return c.ledger
	}

	rpcURL := os.Getenv("STELLAR_RPC_URL")
	if rpcURL == "" {
		return nil
	}

	fetchCtx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	ledger := fetchLatestLedger(fetchCtx, rpcURL)
	if ledger != nil {
		c.ledger = ledger
		c.fetchedAt = time.Now()
	}
	return ledger
}

func fetchLatestLedger(ctx context.Context, rpcURL string) *int64 {
	body := bytes.NewReader([]byte(`{"jsonrpc":"2.0","id":1,"method":"getLatestLedger"}`))
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, rpcURL, body)
	if err != nil {
		slog.Debug("stats: build RPC request", "err", err)
		return nil
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		slog.Debug("stats: RPC call failed", "err", err)
		return nil
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		Result struct {
			Sequence int64 `json:"sequence"`
		} `json:"result"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		slog.Debug("stats: RPC decode failed", "err", err)
		return nil
	}
	if result.Result.Sequence == 0 {
		return nil
	}
	seq := result.Result.Sequence
	return &seq
}

// ---------------------------------------------------------------------------
// DB query
// ---------------------------------------------------------------------------

type indexerStatsRow struct {
	lastLedgerIndexed  *int64
	eventsIndexedTotal *int64
	eventsLastPoll     *int64
	pollDurationMs     *int64
	lastPollAt         *time.Time
	lastLedgerTime     *time.Time
}

func queryIndexerStats(ctx context.Context, db DBPool) (indexerStatsRow, error) {
	var row indexerStatsRow
	err := db.QueryRow(ctx,
		`SELECT last_ledger_indexed,
		        events_indexed_total,
		        events_in_last_poll,
		        poll_duration_ms,
		        last_poll_at,
		        (SELECT lm.ledger_timestamp FROM ledger_metadata lm
		          WHERE lm.ledger_sequence = system_state.last_ledger_indexed)
		   FROM system_state
		  WHERE key = 'latest_ledger_cursor'`,
	).Scan(
		&row.lastLedgerIndexed,
		&row.eventsIndexedTotal,
		&row.eventsLastPoll,
		&row.pollDurationMs,
		&row.lastPollAt,
		&row.lastLedgerTime,
	)
	if err != nil && err != pgx.ErrNoRows {
		return row, err
	}
	return row, nil
}

// ---------------------------------------------------------------------------
// Status logic
// ---------------------------------------------------------------------------

func indexerStatus(lastPollAt *time.Time, lagLedgers *int64, th IndexerThresholds) string {
	if lastPollAt == nil || time.Since(*lastPollAt) > th.Stall {
		return "stalled"
	}
	if lagLedgers != nil && *lagLedgers > th.LagLedgers {
		return "lagging"
	}
	return "healthy"
}

// ---------------------------------------------------------------------------
// Response
// ---------------------------------------------------------------------------

// IndexerStatsResponse is the JSON body for GET /v1/stats/indexer.
type IndexerStatsResponse struct {
	LastLedgerIndexed   *int64   `json:"last_ledger_indexed"`
	ChainTipLedger      *int64   `json:"chain_tip_ledger"`
	LagLedgers          *int64   `json:"lag_ledgers"`
	LagSecondsEstimated *float64 `json:"lag_seconds_estimated"`
	EventsIndexedTotal  *int64   `json:"events_indexed_total"`
	EventsLastPoll      *int64   `json:"events_last_poll"`
	AvgPollDurationMs   *int64   `json:"avg_poll_duration_ms"`
	LastPollAt          *string  `json:"last_poll_at"`
	Status              string   `json:"status"`
	Network             string   `json:"network"`
}

// ---------------------------------------------------------------------------
// Handler
// ---------------------------------------------------------------------------

// IndexerStats handles GET /v1/stats/indexer.
//
// Returns real-time indexer health and throughput metrics sourced from
// system_state. Chain tip is fetched from STELLAR_RPC_URL with a 2-second
// timeout and cached for 10 seconds; it is null on RPC failure. HTTP 503 is
// returned only when status == "stalled". No API key is required.
func IndexerStats(db DBPool) http.HandlerFunc {
	th := loadIndexerThresholds()
	return func(w http.ResponseWriter, r *http.Request) {
		if db == nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusServiceUnavailable, httputil.UNAVAILABLE, "database unavailable")
			return
		}

		ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
		defer cancel()

		stats, err := queryIndexerStats(ctx, db)
		if err != nil {
			slog.Error("stats: DB query failed", "err", err)
			httputil.WriteErrorCtx(r.Context(), w, http.StatusServiceUnavailable, httputil.UNAVAILABLE, "database query failed")
			return
		}

		chainTip := globalChainTipCache.get(r.Context())

		var lagLedgers *int64
		var lagSecondsEstimated *float64
		if chainTip != nil && stats.lastLedgerIndexed != nil {
			lag := *chainTip - *stats.lastLedgerIndexed
			lagLedgers = &lag
			estimated := float64(lag) * th.AvgLedgerCloseSeconds
			if stats.lastLedgerTime != nil {
				estimated = math.Max(0, time.Since(*stats.lastLedgerTime).Seconds())
			}
			lagSecondsEstimated = &estimated
		}

		status := indexerStatus(stats.lastPollAt, lagLedgers, th)

		// Update Prometheus gauges with latest observed values.
		if lagLedgers != nil {
			metricLagLedgers.Set(float64(*lagLedgers))
		}
		if lagSecondsEstimated != nil {
			metricLagSecondsEstimated.Set(*lagSecondsEstimated)
		}
		if stats.lastPollAt != nil {
			metricLastPollTimestamp.Set(float64(stats.lastPollAt.Unix()))
		}
		if stats.eventsIndexedTotal != nil {
			metricEventsTotal.Set(float64(*stats.eventsIndexedTotal))
		}

		var lastPollAtStr *string
		if stats.lastPollAt != nil {
			s := stats.lastPollAt.UTC().Format(time.RFC3339)
			lastPollAtStr = &s
		}

		resp := IndexerStatsResponse{
			LastLedgerIndexed:   stats.lastLedgerIndexed,
			ChainTipLedger:      chainTip,
			LagLedgers:          lagLedgers,
			LagSecondsEstimated: lagSecondsEstimated,
			EventsIndexedTotal:  stats.eventsIndexedTotal,
			EventsLastPoll:      stats.eventsLastPoll,
			AvgPollDurationMs:   stats.pollDurationMs,
			LastPollAt:          lastPollAtStr,
			Status:              status,
			Network:             os.Getenv("NETWORK"),
		}

		httpStatus := http.StatusOK
		if status == "stalled" {
			httpStatus = http.StatusServiceUnavailable
		}
		writeJSON(w, httpStatus, resp)
	}
}

// ---------------------------------------------------------------------------
// Contract Analytics Endpoint
// ---------------------------------------------------------------------------

// ContractStats represents a single contract's activity metrics
type ContractStats struct {
	ContractID     string `json:"contract_id"`
	EventCount     int64  `json:"event_count"`
	LastSeenLedger int64  `json:"last_seen_ledger"`
	LastSeenAt     string `json:"last_seen_at"`

	// Per-invocation fee + declared-resource metering (issue #266), sourced
	// from contract_invocation_metrics. Null when the contract has no metered
	// invocations in range — either it isn't on the tracked-contract
	// allowlist, or it has not yet been invoked since metering was added.
	InvocationCount    *int64   `json:"invocation_count"`
	TotalFeeCharged    *int64   `json:"total_fee_charged"`
	AvgFeeCharged      *float64 `json:"avg_fee_charged"`
	AvgCpuInstructions *float64 `json:"avg_cpu_instructions"`
	AvgReadBytes       *float64 `json:"avg_read_bytes"`
	AvgWriteBytes      *float64 `json:"avg_write_bytes"`
}

// ContractsStatsResponse is the JSON response for GET /v1/stats/contracts
type ContractsStatsResponse struct {
	Contracts   []*ContractStats `json:"contracts"`
	FromLedger  int64            `json:"from_ledger"`
	ToLedger    int64            `json:"to_ledger"`
	Network     string           `json:"network"`
	GeneratedAt string           `json:"generated_at"`
}

// ContractsStats handles GET /v1/stats/contracts (analytics endpoint).
//
// Query parameters:
//   - from_ledger (optional): lower bound, inclusive. Default: 0 (all time)
//   - to_ledger (optional): upper bound, inclusive. Default: latest indexed ledger
//   - network (optional): "testnet" or "mainnet". Default: "testnet"
//   - limit (optional): 1-100, number of contracts to return. Default: 50
//
// Response is cached in Redis for 60 seconds using key:
// stats:contracts:{network}:{from}:{to}:{limit}
//
// Returns results ordered by event_count DESC (highest volume first).
func ContractsStats(db DBPool, rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if db == nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusServiceUnavailable, httputil.UNAVAILABLE, "database unavailable")
			return
		}

		q := r.URL.Query()
		if verr := validation.RejectUnknownParams(
			q, "from_ledger", "to_ledger", "network", "limit",
		); verr != nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusBadRequest, httputil.INVALID_ARGUMENT, verr.Message)
			return
		}

		// Validate and parse query parameters
		params, verr := validation.ValidateQueryStats(
			q.Get("from_ledger"),
			q.Get("to_ledger"),
			q.Get("network"),
			q.Get("limit"),
		)
		if verr != nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusBadRequest, httputil.INVALID_ARGUMENT, verr.Message)
			return
		}

		// Build cache key
		cacheKey := fmt.Sprintf("stats:contracts:%s:%d:%d:%d", params.Network, params.FromLedger, params.ToLedger, params.Limit)

		// Try Redis cache first
		if cached, err := rdb.Get(r.Context(), cacheKey).Result(); err == nil {
			w.Header().Set("Content-Type", "application/json")
			w.Header().Set("Cache-Control", "public, max-age=60")
			w.Header().Set("X-Cache", "HIT")
			_, _ = w.Write([]byte(cached))
			return
		} else if err != redis.Nil {
			// Log Redis error but continue (cache is best-effort)
			slog.ErrorContext(r.Context(), "redis cache get failed", "err", err)
		}

		// Query the database
		ctx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()

		// The maintained rollup (issue #257) only represents the full,
		// unfiltered event history per contract, so it can only answer the
		// default "all time" query. Any explicit ledger-range filter falls
		// back to the live aggregate below, which the rollup cannot cover.
		isDefaultRange := q.Get("from_ledger") == "" && q.Get("to_ledger") == ""

		var stats []*ContractStats
		var err error
		usedRollup := false
		if isDefaultRange {
			stats, usedRollup, err = queryContractStatsFromRollup(ctx, db, params)
			if err != nil {
				slog.ErrorContext(r.Context(), "rollup query failed; falling back to live aggregation", "err", err)
				usedRollup = false
			}
		}
		if !usedRollup {
			stats, err = queryContractStats(ctx, db, params)
			if err != nil {
				slog.ErrorContext(r.Context(), "database query failed", "err", err)
				httputil.WriteErrorCtx(r.Context(), w, http.StatusInternalServerError, httputil.INTERNAL, "failed to fetch statistics")
				return
			}
		}

		// Get the latest ledger for the response metadata if to_ledger was not explicitly set
		toLedger := params.ToLedger
		if q.Get("to_ledger") == "" {
			latestLedger, err := getLatestIndexedLedger(ctx, db)
			if err != nil {
				slog.ErrorContext(r.Context(), "failed to get latest ledger", "err", err)
				// Continue anyway; use 0 as fallback
				latestLedger = 0
			}
			toLedger = latestLedger
		}

		response := &ContractsStatsResponse{
			Contracts:   stats,
			FromLedger:  params.FromLedger,
			ToLedger:    toLedger,
			Network:     params.Network,
			GeneratedAt: time.Now().UTC().Format(time.RFC3339),
		}

		// Marshal to JSON for caching and response
		body, err := json.Marshal(response)
		if err != nil {
			slog.ErrorContext(r.Context(), "json marshal failed", "err", err)
			httputil.WriteErrorCtx(r.Context(), w, http.StatusInternalServerError, httputil.INTERNAL, "internal error")
			return
		}

		// Cache in Redis for 60 seconds (best-effort; ignore errors)
		_ = rdb.Set(r.Context(), cacheKey, body, 60*time.Second).Err()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Cache-Control", "public, max-age=60")
		w.Header().Set("X-Cache", "MISS")
		_, _ = w.Write(body)
	}
}

// queryContractStats executes the aggregation query against the database.
// Requires compound index (contract_id, ledger_sequence DESC) from #61.
//
// Invocation fee/resource metering (issue #266) is joined in from
// contract_invocation_metrics via a pre-aggregated subquery rather than a
// row-level join against soroban_events: the two tables share no per-row key
// (metering is one row per transaction, events are one row per emitted
// event), so the join key is contract_id + the same ledger range.
//
// Index usage: the WHERE clause on (network, ledger_sequence) uses
// idx_soroban_events_network_contract (migration 0004) for partition pruning
// when a ledger range is supplied; the GROUP BY + ORDER BY event_count DESC is
// a computed aggregate and is not index-backed — this is expected for an
// aggregation query. The LIMIT cap prevents runaway result sets (#255).
func queryContractStats(ctx context.Context, db DBPool, params *validation.QueryStatsParams) ([]*ContractStats, error) {
	// Belt-and-suspenders: clamp limit even if the caller skips ValidateQueryStats.
	limit := params.Limit
	if limit <= 0 || limit > validation.StatsLimitMax {
		limit = validation.StatsLimitDefault
	}

	query := `
	SELECT
		e.contract_id,
		COUNT(*) AS event_count,
		MAX(e.ledger_sequence) AS last_seen_ledger,
		MAX(e.ledger_timestamp) AS last_seen_at,
		m.invocation_count,
		m.total_fee_charged,
		m.avg_fee_charged,
		m.avg_cpu_instructions,
		m.avg_read_bytes,
		m.avg_write_bytes
	FROM soroban_events e
	LEFT JOIN (
		SELECT
			contract_id,
			COUNT(*) AS invocation_count,
			SUM(fee_charged) AS total_fee_charged,
			AVG(fee_charged) AS avg_fee_charged,
			AVG(cpu_instructions) AS avg_cpu_instructions,
			AVG(read_bytes) AS avg_read_bytes,
			AVG(write_bytes) AS avg_write_bytes
		FROM contract_invocation_metrics
		WHERE
			network = $1
			AND ($2::BIGINT IS NULL OR ledger_sequence >= $2)
			AND ($3::BIGINT IS NULL OR ledger_sequence <= $3)
		GROUP BY contract_id
	) m ON m.contract_id = e.contract_id
	WHERE
		e.network = $1
		AND ($2::BIGINT IS NULL OR e.ledger_sequence >= $2)
		AND ($3::BIGINT IS NULL OR e.ledger_sequence <= $3)
	GROUP BY
		e.contract_id, m.invocation_count, m.total_fee_charged,
		m.avg_fee_charged, m.avg_cpu_instructions, m.avg_read_bytes, m.avg_write_bytes
	ORDER BY event_count DESC
	LIMIT $4
	`

	rows, err := db.Query(ctx, query, params.Network, params.FromLedgerPtr, params.ToLedgerPtr, limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	// Non-nil so a zero-row result serializes as JSON [] rather than null
	// (issue #242) — the OpenAPI spec documents ContractStatsResponse.contracts
	// as a non-nullable array.
	stats := []*ContractStats{}
	for rows.Next() {
		var cs ContractStats
		var lastSeenAt time.Time

		err := rows.Scan(
			&cs.ContractID,
			&cs.EventCount,
			&cs.LastSeenLedger,
			&lastSeenAt,
			&cs.InvocationCount,
			&cs.TotalFeeCharged,
			&cs.AvgFeeCharged,
			&cs.AvgCpuInstructions,
			&cs.AvgReadBytes,
			&cs.AvgWriteBytes,
		)
		if err != nil {
			return nil, err
		}

		cs.LastSeenAt = lastSeenAt.UTC().Format(time.RFC3339)
		stats = append(stats, &cs)
	}

	if err = rows.Err(); err != nil {
		return nil, err
	}

	return stats, nil
}

// queryContractStatsFromRollup serves the default "all time" contract stats
// query from the maintained contract_stats_rollup table (issue #257) instead
// of aggregating soroban_events live. Invocation metering (issue #266) is
// still joined live from contract_invocation_metrics — that table is small
// and indexed by contract_id, so joining it against at most `limit` rollup
// rows stays cheap.
//
// The second return value is false when the rollup has not been populated
// for this network yet (e.g. before the first periodic refresh completes),
// signalling the caller to fall back to queryContractStats.
func queryContractStatsFromRollup(ctx context.Context, db DBPool, params *validation.QueryStatsParams) ([]*ContractStats, bool, error) {
	query := `
	SELECT
		r.contract_id,
		r.event_count,
		r.last_seen_ledger,
		r.last_seen_at,
		m.invocation_count,
		m.total_fee_charged,
		m.avg_fee_charged,
		m.avg_cpu_instructions,
		m.avg_read_bytes,
		m.avg_write_bytes
	FROM contract_stats_rollup r
	LEFT JOIN (
		SELECT
			contract_id,
			COUNT(*) AS invocation_count,
			SUM(fee_charged) AS total_fee_charged,
			AVG(fee_charged) AS avg_fee_charged,
			AVG(cpu_instructions) AS avg_cpu_instructions,
			AVG(read_bytes) AS avg_read_bytes,
			AVG(write_bytes) AS avg_write_bytes
		FROM contract_invocation_metrics
		WHERE network = $1
		GROUP BY contract_id
	) m ON m.contract_id = r.contract_id
	WHERE r.network = $1
	ORDER BY r.event_count DESC
	LIMIT $2
	`

	rows, err := db.Query(ctx, query, params.Network, params.Limit)
	if err != nil {
		return nil, false, err
	}
	defer rows.Close()

	// Non-nil so a zero-row result serializes as JSON [] rather than null
	// (issue #242) — the OpenAPI spec documents ContractStatsResponse.contracts
	// as a non-nullable array.
	stats := []*ContractStats{}
	for rows.Next() {
		var cs ContractStats
		var lastSeenAt time.Time

		err := rows.Scan(
			&cs.ContractID,
			&cs.EventCount,
			&cs.LastSeenLedger,
			&lastSeenAt,
			&cs.InvocationCount,
			&cs.TotalFeeCharged,
			&cs.AvgFeeCharged,
			&cs.AvgCpuInstructions,
			&cs.AvgReadBytes,
			&cs.AvgWriteBytes,
		)
		if err != nil {
			return nil, false, err
		}

		cs.LastSeenAt = lastSeenAt.UTC().Format(time.RFC3339)
		stats = append(stats, &cs)
	}
	if err = rows.Err(); err != nil {
		return nil, false, err
	}

	if len(stats) > 0 {
		return stats, true, nil
	}

	// Empty result: indistinguishable between "no activity on this network"
	// and "the rollup has not been refreshed yet". Check whether the rollup
	// has ever been populated for this network at all; if not, tell the
	// caller to fall back to the live aggregate rather than serve a
	// possibly-wrong empty response.
	var populated bool
	if err := db.QueryRow(ctx,
		"SELECT EXISTS(SELECT 1 FROM contract_stats_rollup WHERE network = $1)",
		params.Network,
	).Scan(&populated); err != nil {
		return nil, false, err
	}

	return stats, populated, nil
}

// contractStatsRollupRefreshSQL recomputes contract_stats_rollup in full from
// soroban_events (issue #257). Run periodically rather than incrementally on
// ingest, so the rollup stays independent of the Rust indexer's write path —
// see the ticker started in main.go and the freshness note in
// database/migrations/0019_contract_stats_rollup.sql.
const contractStatsRollupRefreshSQL = `
	INSERT INTO contract_stats_rollup (
		contract_id, network, event_count, contract_event_count, system_event_count,
		diagnostic_event_count, first_seen_ledger, last_seen_ledger, last_seen_at, refreshed_at
	)
	SELECT
		contract_id,
		network,
		COUNT(*),
		COUNT(*) FILTER (WHERE event_type = 'contract'),
		COUNT(*) FILTER (WHERE event_type = 'system'),
		COUNT(*) FILTER (WHERE event_type = 'diagnostic'),
		MIN(ledger_sequence),
		MAX(ledger_sequence),
		MAX(ledger_timestamp),
		NOW()
	FROM soroban_events
	GROUP BY contract_id, network
	ON CONFLICT (contract_id, network) DO UPDATE SET
		event_count            = EXCLUDED.event_count,
		contract_event_count   = EXCLUDED.contract_event_count,
		system_event_count     = EXCLUDED.system_event_count,
		diagnostic_event_count = EXCLUDED.diagnostic_event_count,
		first_seen_ledger      = EXCLUDED.first_seen_ledger,
		last_seen_ledger       = EXCLUDED.last_seen_ledger,
		last_seen_at           = EXCLUDED.last_seen_at,
		refreshed_at           = EXCLUDED.refreshed_at
`

// RefreshContractStatsRollup recomputes contract_stats_rollup from
// soroban_events (issue #257). Exported so main.go's periodic ticker and
// tests can both call it.
func RefreshContractStatsRollup(ctx context.Context, db SchemaRegistryDB) error {
	_, err := db.Exec(ctx, contractStatsRollupRefreshSQL)
	return err
}

// getLatestIndexedLedger queries the database for the highest indexed ledger sequence.
func getLatestIndexedLedger(ctx context.Context, db DBPool) (int64, error) {
	var latest int64
	err := db.QueryRow(ctx, "SELECT COALESCE(MAX(ledger_sequence), 0) FROM soroban_events").Scan(&latest)
	return latest, err
}
