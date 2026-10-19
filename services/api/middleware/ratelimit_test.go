package middleware

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/Depo-dev/trident/services/api/internal/metrics"
	"github.com/jackc/pgx/v5"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// ---------------------------------------------------------------------------
// Test doubles
// ---------------------------------------------------------------------------

type mockTierDB struct {
	tier string
	err  error
}

func (m *mockTierDB) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return &mockTierRow{tier: m.tier, err: m.err}
}

type mockTierRow struct {
	tier string
	err  error
}

func (r *mockTierRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	*dest[0].(*string) = r.tier
	return nil
}

func noop() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})
}

func apiKeyReq(key string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v1/events", nil)
	if key != "" {
		r.Header.Set("X-API-Key", key)
	}
	return r
}

func resetCounters() {
	rlAllowed.Store(0)
	rlRejected.Store(0)
}

// alwaysAllow is a SliderFn that always allows with count=1.
func alwaysAllow(_ context.Context, _ string, _ int64, _ int64) (bool, int64, error) {
	return true, 1, nil
}

// alwaysReject is a SliderFn that always rejects at the limit.
func alwaysReject(_ context.Context, _ string, limit int64, _ int64) (bool, int64, error) {
	return false, limit, nil
}

func testTiers() map[string]TierConfig {
	return map[string]TierConfig{
		"free":     {RPS: 5, Window: time.Second},
		"pro":      {RPS: 50, Window: time.Second},
		"internal": {RPS: 500, Window: time.Second},
	}
}

// ---------------------------------------------------------------------------
// TieredRateLimit tests
// ---------------------------------------------------------------------------

func TestTieredRateLimit_NoAPIKey_Passthrough(t *testing.T) {
	resetCounters()
	mw := TieredRateLimit(RateLimitConfig{Tiers: testTiers()})(noop())
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, apiKeyReq(""))
	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	if rec.Header().Get("X-RateLimit-Limit") != "" {
		t.Error("rate limit headers must not be set when no API key present")
	}
}

func TestTieredRateLimit_NilRedis_FailOpen(t *testing.T) {
	resetCounters()
	cfg := RateLimitConfig{Redis: nil, DB: nil, Tiers: testTiers()}
	mw := TieredRateLimit(cfg)(noop())
	for i := 0; i < 200; i++ {
		rec := httptest.NewRecorder()
		mw.ServeHTTP(rec, apiKeyReq("any-key"))
		if rec.Code != http.StatusOK {
			t.Fatalf("nil Redis must fail open: want 200, got %d (i=%d)", rec.Code, i)
		}
	}
}

func TestTieredRateLimit_Allows_SetsHeaders(t *testing.T) {
	resetCounters()
	cfg := RateLimitConfig{SliderFn: alwaysAllow, Tiers: testTiers()}
	mw := TieredRateLimit(cfg)(noop())
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, apiKeyReq("my-key"))

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "5" {
		t.Errorf("X-RateLimit-Limit: want 5, got %q", got)
	}
	if rec.Header().Get("X-RateLimit-Remaining") == "" {
		t.Error("X-RateLimit-Remaining must be set")
	}
	if rec.Header().Get("X-RateLimit-Reset") == "" {
		t.Error("X-RateLimit-Reset must be set")
	}
	if rec.Header().Get("Retry-After") != "" {
		t.Error("Retry-After must not be set on 200")
	}
}

func TestTieredRateLimit_Rejects_Returns429WithHeaders(t *testing.T) {
	resetCounters()
	cfg := RateLimitConfig{SliderFn: alwaysReject, Tiers: testTiers()}
	mw := TieredRateLimit(cfg)(noop())
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, apiKeyReq("key"))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("want 429, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("Retry-After must be set on 429")
	}
	if got := rec.Header().Get("X-RateLimit-Limit"); got != "5" {
		t.Errorf("X-RateLimit-Limit: want 5, got %q", got)
	}
	if got := rec.Header().Get("X-RateLimit-Remaining"); got != "0" {
		t.Errorf("X-RateLimit-Remaining: want 0, got %q", got)
	}
	if rec.Header().Get("X-RateLimit-Reset") == "" {
		t.Error("X-RateLimit-Reset must be set on 429")
	}
}

// TestTieredRateLimit_Rejects_RecordsPrometheusMetric verifies a 429 from the
// per-key tiered limiter increments trident_ratelimit_rejections_total{limiter="per_key"}
// (issue #58).
func TestTieredRateLimit_Rejects_RecordsPrometheusMetric(t *testing.T) {
	resetCounters()
	before := testutil.ToFloat64(metrics.RateLimitRejectionsTotal.WithLabelValues("per_key"))

	cfg := RateLimitConfig{SliderFn: alwaysReject, Tiers: testTiers()}
	mw := TieredRateLimit(cfg)(noop())
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, apiKeyReq("key"))

	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("want 429, got %d", rec.Code)
	}
	if got := testutil.ToFloat64(metrics.RateLimitRejectionsTotal.WithLabelValues("per_key")); got != before+1 {
		t.Errorf("per_key rejections total: want %v, got %v", before+1, got)
	}
}

func TestTieredRateLimit_FailOpen_OnSliderError(t *testing.T) {
	resetCounters()
	errSlider := func(_ context.Context, _ string, _, _ int64) (bool, int64, error) {
		return false, 0, fmt.Errorf("redis: connection refused")
	}
	cfg := RateLimitConfig{SliderFn: errSlider, Tiers: testTiers()}
	mw := TieredRateLimit(cfg)(noop())
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, apiKeyReq("key"))
	if rec.Code != http.StatusOK {
		t.Fatalf("slider error must fail open: want 200, got %d", rec.Code)
	}
}

func TestTieredRateLimit_TierFromDB_UsesCorrectLimit(t *testing.T) {
	resetCounters()
	db := &mockTierDB{tier: "internal"}
	var capturedLimit int64
	captureSlider := func(_ context.Context, _ string, limit, _ int64) (bool, int64, error) {
		capturedLimit = limit
		return true, 1, nil
	}
	cfg := RateLimitConfig{
		DB:       db,
		SliderFn: captureSlider,
		Tiers: map[string]TierConfig{
			"free":     {RPS: 5, Window: time.Second},
			"internal": {RPS: 500, Window: time.Second},
		},
	}
	mw := TieredRateLimit(cfg)(noop())
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, apiKeyReq("internal-key"))
	if capturedLimit != 500 {
		t.Errorf("internal tier: want limit=500, got %d", capturedLimit)
	}
}

func TestTieredRateLimit_DBError_DefaultsToFree(t *testing.T) {
	resetCounters()
	db := &mockTierDB{err: fmt.Errorf("db down")}
	var capturedLimit int64
	captureSlider := func(_ context.Context, _ string, limit, _ int64) (bool, int64, error) {
		capturedLimit = limit
		return true, 1, nil
	}
	cfg := RateLimitConfig{
		DB:       db,
		SliderFn: captureSlider,
		Tiers: map[string]TierConfig{
			"free": {RPS: 10, Window: time.Second},
			"pro":  {RPS: 100, Window: time.Second},
		},
	}
	mw := TieredRateLimit(cfg)(noop())
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, apiKeyReq("unknown-key"))
	if capturedLimit != 10 {
		t.Errorf("DB error should default to free (10), got %d", capturedLimit)
	}
}

func TestTieredRateLimit_Counters(t *testing.T) {
	resetCounters()
	call := 0
	var mu sync.Mutex
	countingSlider := func(_ context.Context, _ string, limit, _ int64) (bool, int64, error) {
		mu.Lock()
		defer mu.Unlock()
		call++
		if call <= 3 {
			return true, int64(call), nil
		}
		return false, limit, nil
	}
	cfg := RateLimitConfig{SliderFn: countingSlider, Tiers: testTiers()}
	mw := TieredRateLimit(cfg)(noop())

	for i := 0; i < 5; i++ {
		mw.ServeHTTP(httptest.NewRecorder(), apiKeyReq("ctr-key"))
	}

	allowed, rejected := RateLimitMetrics()
	if allowed != 3 {
		t.Errorf("allowed counter: want 3, got %d", allowed)
	}
	if rejected != 2 {
		t.Errorf("rejected counter: want 2, got %d", rejected)
	}
}

// TestTierCache_Invalidate_AppliesNewTierWithoutTTL asserts that after an admin
// tier change, invalidating the shared cache makes the new tier take effect on
// the next request instead of waiting for the 5-minute TTL (issue #229).
func TestTierCache_Invalidate_AppliesNewTierWithoutTTL(t *testing.T) {
	resetCounters()
	db := &mockTierDB{tier: "free"}
	cache := NewTierCache()

	var capturedLimit int64
	captureSlider := func(_ context.Context, _ string, limit, _ int64) (bool, int64, error) {
		capturedLimit = limit
		return true, 1, nil
	}
	cfg := RateLimitConfig{
		DB:       db,
		Cache:    cache,
		SliderFn: captureSlider,
		Tiers: map[string]TierConfig{
			"free": {RPS: 10, Window: time.Second},
			"pro":  {RPS: 100, Window: time.Second},
		},
	}
	mw := TieredRateLimit(cfg)(noop())
	const key = "switch-key"

	// First request resolves and caches the "free" tier.
	mw.ServeHTTP(httptest.NewRecorder(), apiKeyReq(key))
	if capturedLimit != 10 {
		t.Fatalf("initial tier: want limit 10 (free), got %d", capturedLimit)
	}

	// Admin promotes the key to "pro" in the DB. Without invalidation the cache
	// still serves the stale "free" tier.
	db.tier = "pro"
	mw.ServeHTTP(httptest.NewRecorder(), apiKeyReq(key))
	if capturedLimit != 10 {
		t.Fatalf("before invalidation: cached tier should still apply (10), got %d", capturedLimit)
	}

	// Invalidate the entry (as UpdateAPIKey does) — the new tier applies now.
	cache.Invalidate(hashKey(key))
	mw.ServeHTTP(httptest.NewRecorder(), apiKeyReq(key))
	if capturedLimit != 100 {
		t.Fatalf("after invalidation: want new tier limit 100 (pro), got %d", capturedLimit)
	}
}

// TestTieredRateLimit_BoundaryExactLimit asserts the request that exactly hits
// the limit is allowed and the next one is rejected (issue #229 boundary case).
func TestTieredRateLimit_BoundaryExactLimit(t *testing.T) {
	resetCounters()
	const limit = 3
	var mu sync.Mutex
	calls := 0
	// Mimic the sliding window: allow while the pre-increment count is below the
	// limit, returning the post-increment count; reject once the window is full.
	slider := func(_ context.Context, _ string, lim, _ int64) (bool, int64, error) {
		mu.Lock()
		defer mu.Unlock()
		if int64(calls) >= lim {
			return false, lim, nil
		}
		calls++
		return true, int64(calls), nil
	}
	cfg := RateLimitConfig{
		SliderFn: slider,
		Tiers:    map[string]TierConfig{"free": {RPS: limit, Window: time.Second}},
	}
	mw := TieredRateLimit(cfg)(noop())
	const key = "boundary-key"

	// The first `limit` requests are allowed; the last exactly hits the cap.
	for i := 1; i <= limit; i++ {
		rec := httptest.NewRecorder()
		mw.ServeHTTP(rec, apiKeyReq(key))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d/%d: want 200, got %d", i, limit, rec.Code)
		}
		if i == limit && rec.Header().Get("X-RateLimit-Remaining") != "0" {
			t.Errorf("at exact limit: want remaining 0, got %q", rec.Header().Get("X-RateLimit-Remaining"))
		}
	}

	// The next request over the limit is rejected.
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, apiKeyReq(key))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over limit: want 429, got %d", rec.Code)
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("429 must carry Retry-After")
	}
}

// ---------------------------------------------------------------------------
// WSConnectionLimit tests
// ---------------------------------------------------------------------------

func TestWSConnectionLimit_UnderLimit_Passes(t *testing.T) {
	wsConns.Store(0)
	defer wsConns.Store(0)
	mw := WSConnectionLimit(noop())
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ws", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("under limit: want 200, got %d", rec.Code)
	}
}

func TestWSConnectionLimit_OverLimit_Returns429(t *testing.T) {
	wsConns.Store(1000) // already AT the limit
	defer wsConns.Store(0)
	mw := WSConnectionLimit(noop())
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/ws", nil))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over limit: want 429, got %d", rec.Code)
	}
}

// ---------------------------------------------------------------------------
// RateLimitMetrics helper
// ---------------------------------------------------------------------------

func TestRateLimitMetrics_InitiallyZero(t *testing.T) {
	resetCounters()
	allowed, rejected := RateLimitMetrics()
	if allowed != 0 || rejected != 0 {
		t.Errorf("want (0,0), got (%d,%d)", allowed, rejected)
	}
}

// ---------------------------------------------------------------------------
// Tier cache: lookup errors must not poison the cache (issue #754)
// ---------------------------------------------------------------------------

type flakyTierDB struct {
	mu    sync.Mutex
	calls int
	tier  string
}

func (f *flakyTierDB) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.calls == 1 {
		return &mockTierRow{err: context.DeadlineExceeded}
	}
	return &mockTierRow{tier: f.tier}
}

func TestTierCache_LookupErrorIsNotCached(t *testing.T) {
	tc := NewTierCache()
	db := &flakyTierDB{tier: "pro"}

	if got := tc.resolve(context.Background(), "k", db); got != "free" {
		t.Fatalf("first lookup (db timeout) = %q, want free fallback", got)
	}
	if got := tc.resolve(context.Background(), "k", db); got != "pro" {
		t.Fatalf("next request tier = %q, want pro", got)
	}
}

func TestTierCache_LookupErrorKeepsPreviousKnownTier(t *testing.T) {
	tc := NewTierCache()
	tc.set(hashKey("k"), "pro")
	tc.mu.Lock()
	e := tc.entries[hashKey("k")]
	e.exp = time.Now().Add(-time.Second)
	tc.entries[hashKey("k")] = e
	tc.mu.Unlock()

	got := tc.resolve(context.Background(), "k", &mockTierDB{err: context.DeadlineExceeded})
	if got != "pro" {
		t.Fatalf("tier on lookup error = %q, want previously known pro", got)
	}
}

func TestTierCache_EvictsExpiredEntries(t *testing.T) {
	tc := NewTierCache()
	tc.mu.Lock()
	for i := 0; i < 50; i++ {
		tc.entries[fmt.Sprintf("old-%d", i)] = tierEntry{tier: "free", exp: time.Now().Add(-time.Hour)}
	}
	tc.mu.Unlock()

	tc.evictExpired(time.Now())

	tc.mu.RLock()
	n := len(tc.entries)
	tc.mu.RUnlock()
	if n != 0 {
		t.Fatalf("entries after eviction = %d, want 0", n)
	}
}
