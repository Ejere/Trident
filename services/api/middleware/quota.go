package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"math"
	"net/http"
	"strconv"
	"time"

	"github.com/Depo-dev/trident/services/api/internal/httputil"
	"github.com/Depo-dev/trident/services/api/internal/metrics"
	"github.com/redis/go-redis/v9"
)

// QuotaConfig defines per-tier request quotas. A zero value means unlimited.
type QuotaConfig struct {
	Daily   int64
	Monthly int64
}

// QuotaCounterFn atomically increments the counter at key and returns the new
// value. The counter must expire at expireAt (the end of the quota period).
type QuotaCounterFn func(ctx context.Context, key string, expireAt time.Time) (int64, error)

func defaultQuotas() map[string]QuotaConfig {
	pro := QuotaConfig{
		Daily:   int64(envInt("QUOTA_PRO_DAILY", 1_000_000)),
		Monthly: int64(envInt("QUOTA_PRO_MONTHLY", 20_000_000)),
	}
	return map[string]QuotaConfig{
		"free": {
			Daily:   int64(envInt("QUOTA_FREE_DAILY", 10_000)),
			Monthly: int64(envInt("QUOTA_FREE_MONTHLY", 100_000)),
		},
		"pro":      pro,
		"standard": pro,
		"internal": {
			Daily:   int64(envInt("QUOTA_INTERNAL_DAILY", 0)),
			Monthly: int64(envInt("QUOTA_INTERNAL_MONTHLY", 0)),
		},
	}
}

const incrExpireLua = `
local c = redis.call('INCR', KEYS[1])
if c == 1 then redis.call('PEXPIREAT', KEYS[1], ARGV[1]) end
return c
`

// redisQuotaCounter returns a QuotaCounterFn backed by Redis so counts are
// shared across API replicas.
func redisQuotaCounter(rc *redis.Client) QuotaCounterFn {
	return func(ctx context.Context, key string, expireAt time.Time) (int64, error) {
		n, err := rc.Eval(ctx, incrExpireLua, []string{key}, expireAt.UnixMilli()).Int64()
		if err != nil {
			return 0, fmt.Errorf("redis quota incr: %w", err)
		}
		return n, nil
	}
}

func nextDayStart(now time.Time) time.Time {
	n := now.UTC()
	return time.Date(n.Year(), n.Month(), n.Day()+1, 0, 0, 0, 0, time.UTC)
}

func nextMonthStart(now time.Time) time.Time {
	n := now.UTC()
	return time.Date(n.Year(), n.Month()+1, 1, 0, 0, 0, 0, time.UTC)
}

type quotaPeriod struct {
	name    string
	limit   int64
	resetAt time.Time
	count   int64
}

// checkQuota counts the request against the key's daily and monthly quotas,
// sets X-Quota-* headers, and reports whether the request may proceed. On a
// counter error it fails open, matching the sliding-window policy.
func checkQuota(w http.ResponseWriter, r *http.Request, incr QuotaCounterFn, q QuotaConfig, keyHash string) bool {
	now := time.Now()
	periods := []*quotaPeriod{
		{name: "Daily", limit: q.Daily, resetAt: nextDayStart(now)},
		{name: "Monthly", limit: q.Monthly, resetAt: nextMonthStart(now)},
	}
	var exceeded *quotaPeriod
	for _, p := range periods {
		if p.limit <= 0 {
			continue
		}
		key := fmt.Sprintf("quota:%s:%s:%s", p.name, keyHash, p.resetAt.Format("20060102"))
		n, err := incr(r.Context(), key, p.resetAt.Add(time.Hour))
		if err != nil {
			slog.Warn("quota check failed; failing open", "err", err)
			return true
		}
		p.count = n
		w.Header().Set("X-Quota-"+p.name+"-Limit", strconv.FormatInt(p.limit, 10))
		w.Header().Set("X-Quota-"+p.name+"-Remaining", strconv.FormatInt(max(0, p.limit-n), 10))
		w.Header().Set("X-Quota-"+p.name+"-Reset", strconv.FormatInt(p.resetAt.Unix(), 10))
		if n > p.limit && exceeded == nil {
			exceeded = p
		}
	}
	if exceeded == nil {
		return true
	}
	rlRejected.Add(1)
	metrics.RateLimitRejectionsTotal.WithLabelValues("quota").Inc()
	retryAfter := int64(math.Ceil(time.Until(exceeded.resetAt).Seconds()))
	w.Header().Set("Retry-After", strconv.FormatInt(max(1, retryAfter), 10))
	httputil.WriteErrorCtx(r.Context(), w, http.StatusTooManyRequests, httputil.QUOTA_EXCEEDED,
		fmt.Sprintf("%s request quota exceeded; resets at %s", lower(exceeded.name), exceeded.resetAt.Format(time.RFC3339)))
	return false
}

func lower(s string) string {
	if s == "Daily" {
		return "daily"
	}
	return "monthly"
}

// NewSimulationBudget returns a per-key budget of perMinute upstream
// simulations, counted in Redis so it holds across replicas. Pass it as
// handlers.ContractCallConfig.SimulationAllowed.
func NewSimulationBudget(rc *redis.Client, perMinute int64) func(ctx context.Context, apiKeyID string) (bool, error) {
	return newSimulationBudget(redisQuotaCounter(rc), perMinute)
}

func newSimulationBudget(incr QuotaCounterFn, perMinute int64) func(ctx context.Context, apiKeyID string) (bool, error) {
	return func(ctx context.Context, apiKeyID string) (bool, error) {
		window := time.Now().UTC().Truncate(time.Minute)
		key := fmt.Sprintf("simbudget:%s:%d", apiKeyID, window.Unix())
		n, err := incr(ctx, key, window.Add(2*time.Minute))
		if err != nil {
			return false, err
		}
		return n <= perMinute, nil
	}
}
