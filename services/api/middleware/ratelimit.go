package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Depo-dev/trident/services/api/internal/httputil"
	"github.com/Depo-dev/trident/services/api/internal/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// Public types
// ---------------------------------------------------------------------------

// TierConfig defines the sliding-window rate limit for a single tier.
type TierConfig struct {
	RPS    int
	Window time.Duration
}

// TierDB can query the rate_limit_tier for an API key hash.
type TierDB interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// RateLimitConfig configures TieredRateLimit.
type RateLimitConfig struct {
	Redis *redis.Client
	DB    TierDB
	Tiers map[string]TierConfig
	// Cache holds resolved key->tier entries. Share a single instance (see
	// NewTierCache) with the api-key admin handler so a tier change can evict
	// the affected entry immediately instead of waiting for the TTL. When nil,
	// TieredRateLimit creates a private cache.
	Cache *TierCache
	// Quotas holds per-tier daily/monthly request quotas (zero = unlimited).
	// Defaults come from QUOTA_<TIER>_DAILY/MONTHLY when nil.
	Quotas map[string]QuotaConfig
	// QuotaFn overrides the Redis quota counter. Set in tests. Quotas are
	// only enforced when this or Redis is configured.
	QuotaFn QuotaCounterFn
	// SliderFn overrides the Redis sliding window. Set in tests.
	SliderFn func(ctx context.Context, key string, limit, windowMs int64) (allowed bool, count int64, err error)
}

// ---------------------------------------------------------------------------
// Prometheus-compatible counters
// ---------------------------------------------------------------------------

var rlAllowed, rlRejected atomic.Int64

// RateLimitMetrics returns (allowed, rejected) request counters since startup.
func RateLimitMetrics() (allowed, rejected int64) {
	return rlAllowed.Load(), rlRejected.Load()
}

// ---------------------------------------------------------------------------
// Tier cache
// ---------------------------------------------------------------------------

type tierEntry struct {
	tier string
	exp  time.Time
}

// TierCache caches the resolved rate-limit tier per API-key hash for a short
// TTL. It is safe for concurrent use. Share one instance between the rate-limit
// middleware and the api-key admin handler so an admin tier change can evict the
// stale entry with Invalidate — otherwise a new tier would not take effect until
// the TTL expires (up to tierCacheTTL later).
type TierCache struct {
	mu        sync.RWMutex
	entries   map[string]tierEntry
	lastSweep time.Time
}

const (
	tierCacheTTL = 5 * time.Minute
	// tierStaleGrace is how long an expired entry is kept as the "previous
	// known tier" fallback for a failed lookup before it is evicted.
	tierStaleGrace = 10 * time.Minute
	// tierSweepInterval bounds how often set() scans for evictable entries.
	tierSweepInterval = time.Minute
)

// NewTierCache returns an empty, ready-to-use tier cache.
func NewTierCache() *TierCache {
	return &TierCache{entries: map[string]tierEntry{}}
}

func (tc *TierCache) get(hash string) (string, bool) {
	tc.mu.RLock()
	e, ok := tc.entries[hash]
	tc.mu.RUnlock()
	return e.tier, ok && time.Now().Before(e.exp)
}

func (tc *TierCache) set(hash, tier string) {
	now := time.Now()
	tc.mu.Lock()
	tc.entries[hash] = tierEntry{tier: tier, exp: now.Add(tierCacheTTL)}
	if now.Sub(tc.lastSweep) >= tierSweepInterval {
		tc.lastSweep = now
		tc.evictExpiredLocked(now)
	}
	tc.mu.Unlock()
}

// evictExpired drops entries that expired more than tierStaleGrace ago.
func (tc *TierCache) evictExpired(now time.Time) {
	tc.mu.Lock()
	tc.evictExpiredLocked(now)
	tc.mu.Unlock()
}

func (tc *TierCache) evictExpiredLocked(now time.Time) {
	for k, e := range tc.entries {
		if now.After(e.exp.Add(tierStaleGrace)) {
			delete(tc.entries, k)
		}
	}
}

// stale returns the last known tier for hash even if its TTL has lapsed.
func (tc *TierCache) stale(hash string) (string, bool) {
	tc.mu.RLock()
	e, ok := tc.entries[hash]
	tc.mu.RUnlock()
	return e.tier, ok
}

// Invalidate drops the cached tier for the given API-key hash so the next
// request re-resolves it from the database. Called by the admin api-key handler
// after a rate_limit_tier change so the new limit applies promptly.
func (tc *TierCache) Invalidate(hash string) {
	tc.mu.Lock()
	delete(tc.entries, hash)
	tc.mu.Unlock()
}

func (tc *TierCache) resolve(ctx context.Context, apiKey string, db TierDB) string {
	hash := hashKey(apiKey)
	if t, ok := tc.get(hash); ok {
		return t
	}
	tier := "free"
	if db != nil {
		ctx2, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
		defer cancel()
		var t string
		if err := db.QueryRow(ctx2,
			`SELECT rate_limit_tier FROM api_keys WHERE key_hash = $1`, hash,
		).Scan(&t); err != nil {
			// A failed lookup must not be cached: keep the last known tier
			// (or free) for this request only and retry on the next one.
			if prev, ok := tc.stale(hash); ok {
				return prev
			}
			return tier
		} else if t != "" {
			tier = t
		}
	}
	tc.set(hash, tier)
	return tier
}

// ---------------------------------------------------------------------------
// Redis sliding-window Lua script
// ---------------------------------------------------------------------------

const slidingWindowLua = `
local key   = KEYS[1]
local now   = tonumber(ARGV[1])
local wms   = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, 0, now - wms)
local cnt = redis.call('ZCARD', key)
if cnt >= limit then
  return {0, cnt}
end
redis.call('ZADD', key, now, now .. ':' .. math.random(1, 2147483647))
redis.call('EXPIRE', key, math.ceil(wms / 1000) + 1)
return {1, cnt + 1}
`

func redisSlider(rc *redis.Client) func(ctx context.Context, key string, limit, windowMs int64) (bool, int64, error) {
	return func(ctx context.Context, key string, limit, windowMs int64) (bool, int64, error) {
		nowMs := time.Now().UnixMilli()
		res, err := rc.Eval(ctx, slidingWindowLua, []string{key}, nowMs, windowMs, limit).Slice()
		if err != nil {
			return false, 0, fmt.Errorf("redis eval: %w", err)
		}
		return res[0].(int64) == 1, res[1].(int64), nil
	}
}

// ---------------------------------------------------------------------------
// Default tier configuration
// ---------------------------------------------------------------------------

func defaultTiers() map[string]TierConfig {
	w := time.Second
	return map[string]TierConfig{
		"free":     {RPS: envInt("RATE_LIMIT_FREE_RPS", 10), Window: w},
		"pro":      {RPS: envInt("RATE_LIMIT_PRO_RPS", 100), Window: w},
		"internal": {RPS: envInt("RATE_LIMIT_INTERNAL_RPS", 1000), Window: w},
		"standard": {RPS: envInt("RATE_LIMIT_PRO_RPS", 100), Window: w},
	}
}

// ---------------------------------------------------------------------------
// TieredRateLimit middleware
// ---------------------------------------------------------------------------

// TieredRateLimit enforces a Redis sliding-window rate limit per API key,
// resolved to a tier via the database.
//
// Fail-open policy (intentional): if the Redis sliding-window check errors
// (Redis unavailable, timeout, script failure) the request is ALLOWED and the
// event is logged at warn level. Availability is prioritised over strict
// enforcement — a Redis outage must not take the whole API down. The tradeoff
// is that limits are not enforced during such an outage. Tier resolution
// likewise degrades gracefully: a DB error falls back to the "free" tier.
func TieredRateLimit(cfg RateLimitConfig) func(http.Handler) http.Handler {
	if cfg.Tiers == nil {
		cfg.Tiers = defaultTiers()
	}
	slide := cfg.SliderFn
	if slide == nil {
		if cfg.Redis != nil {
			slide = redisSlider(cfg.Redis)
		} else {
			slide = func(_ context.Context, _ string, _, _ int64) (bool, int64, error) {
				return true, 0, nil
			}
		}
	}
	if cfg.Quotas == nil {
		cfg.Quotas = defaultQuotas()
	}
	incr := cfg.QuotaFn
	if incr == nil && cfg.Redis != nil {
		incr = redisQuotaCounter(cfg.Redis)
	}
	tc := cfg.Cache
	if tc == nil {
		tc = NewTierCache()
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("X-API-Key")
			if key == "" {
				next.ServeHTTP(w, r)
				return
			}

			tier := tc.resolve(r.Context(), key, cfg.DB)
			tcfg, ok := cfg.Tiers[tier]
			if !ok {
				tcfg = cfg.Tiers["free"]
			}

			redisKey := fmt.Sprintf("ratelimit:%s", hashKey(key))
			windowMs := int64(tcfg.Window / time.Millisecond)
			limit := int64(tcfg.RPS)

			allowed, count, err := slide(r.Context(), redisKey, limit, windowMs)
			if err != nil {
				slog.Warn("rate limit check failed; failing open", "err", err)
				rlAllowed.Add(1)
				next.ServeHTTP(w, r)
				return
			}

			resetAt := time.Now().Add(tcfg.Window).Unix()
			remaining := max(int64(0), limit-count)
			w.Header().Set("X-RateLimit-Limit", strconv.FormatInt(limit, 10))
			w.Header().Set("X-RateLimit-Remaining", strconv.FormatInt(remaining, 10))
			w.Header().Set("X-RateLimit-Reset", strconv.FormatInt(resetAt, 10))

			if !allowed {
				rlRejected.Add(1)
				metrics.RateLimitRejectionsTotal.WithLabelValues("per_key").Inc()
				retryAfter := int64(math.Ceil(tcfg.Window.Seconds()))
				w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
				httputil.WriteErrorCtx(r.Context(), w, http.StatusTooManyRequests, httputil.RATE_LIMITED, "rate limit exceeded")
				return
			}

			if incr != nil {
				if !checkQuota(w, r, incr, cfg.Quotas[tier], hashKey(key)) {
					return
				}
			}

			rlAllowed.Add(1)
			next.ServeHTTP(w, r)
		})
	}
}

// ---------------------------------------------------------------------------
// WebSocket connection limiter
// ---------------------------------------------------------------------------

var wsConns atomic.Int64

// WSConnectionLimit rejects new WebSocket connections once active connections
// exceed MAX_WS_CONNECTIONS (default 1000).
func WSConnectionLimit(next http.Handler) http.Handler {
	limit := int64(envInt("MAX_WS_CONNECTIONS", 1000))
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := wsConns.Add(1)
		defer wsConns.Add(-1)
		if n > limit {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusTooManyRequests, httputil.RATE_LIMITED, "too many WebSocket connections")
			return
		}
		next.ServeHTTP(w, r)
	})
}

// WSActiveConnections returns the current number of active WebSocket connections.
func WSActiveConnections() int64 { return wsConns.Load() }

func envInt(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}
