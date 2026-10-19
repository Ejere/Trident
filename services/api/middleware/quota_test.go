package middleware

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Depo-dev/trident/services/api/internal/httputil"
)

type memQuota struct {
	mu sync.Mutex
	n  map[string]int64
}

func newMemQuota() *memQuota { return &memQuota{n: map[string]int64{}} }

func (m *memQuota) incr(_ context.Context, key string, _ time.Time) (int64, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.n[key]++
	return m.n[key], nil
}

func quotaCfg(q *memQuota) RateLimitConfig {
	return RateLimitConfig{
		SliderFn: alwaysAllow,
		Tiers:    testTiers(),
		Quotas:   map[string]QuotaConfig{"free": {Daily: 2, Monthly: 100}, "pro": {Daily: 0, Monthly: 0}},
		QuotaFn:  q.incr,
	}
}

func TestQuota_DailyExceeded_Returns429QuotaExceeded(t *testing.T) {
	resetCounters()
	mw := TieredRateLimit(quotaCfg(newMemQuota()))(noop())

	for i := 0; i < 2; i++ {
		rec := httptest.NewRecorder()
		mw.ServeHTTP(rec, apiKeyReq("free-key"))
		if rec.Code != http.StatusOK {
			t.Fatalf("request %d: want 200, got %d", i, rec.Code)
		}
		if rec.Header().Get("X-Quota-Daily-Limit") != "2" {
			t.Errorf("X-Quota-Daily-Limit = %q", rec.Header().Get("X-Quota-Daily-Limit"))
		}
	}

	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, apiKeyReq("free-key"))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("want 429, got %d", rec.Code)
	}
	var body httputil.ErrorResponse
	if err := json.NewDecoder(rec.Body).Decode(&body); err != nil {
		t.Fatal(err)
	}
	if body.Error.Code != httputil.QUOTA_EXCEEDED {
		t.Errorf("code = %q, want QUOTA_EXCEEDED", body.Error.Code)
	}
	if rec.Header().Get("X-Quota-Daily-Remaining") != "0" {
		t.Errorf("remaining = %q, want 0", rec.Header().Get("X-Quota-Daily-Remaining"))
	}
	reset, err := strconv.ParseInt(rec.Header().Get("X-Quota-Daily-Reset"), 10, 64)
	if err != nil || reset <= time.Now().Unix() {
		t.Errorf("X-Quota-Daily-Reset = %q, want a future unix time", rec.Header().Get("X-Quota-Daily-Reset"))
	}
	if rec.Header().Get("Retry-After") == "" {
		t.Error("Retry-After must be set on quota rejection")
	}
}

func TestQuota_OtherKeyUnaffected(t *testing.T) {
	resetCounters()
	mw := TieredRateLimit(quotaCfg(newMemQuota()))(noop())
	for i := 0; i < 5; i++ {
		mw.ServeHTTP(httptest.NewRecorder(), apiKeyReq("greedy"))
	}
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, apiKeyReq("polite"))
	if rec.Code != http.StatusOK {
		t.Fatalf("other key: want 200, got %d", rec.Code)
	}
}

func TestQuota_MonthlyExceeded(t *testing.T) {
	resetCounters()
	cfg := quotaCfg(newMemQuota())
	cfg.Quotas = map[string]QuotaConfig{"free": {Daily: 0, Monthly: 1}}
	mw := TieredRateLimit(cfg)(noop())
	mw.ServeHTTP(httptest.NewRecorder(), apiKeyReq("k"))
	rec := httptest.NewRecorder()
	mw.ServeHTTP(rec, apiKeyReq("k"))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("want 429, got %d", rec.Code)
	}
	if rec.Header().Get("X-Quota-Monthly-Reset") == "" {
		t.Error("X-Quota-Monthly-Reset must be set")
	}
}

func TestQuota_ZeroMeansUnlimited(t *testing.T) {
	resetCounters()
	cfg := quotaCfg(newMemQuota())
	cfg.Quotas = map[string]QuotaConfig{"free": {}}
	mw := TieredRateLimit(cfg)(noop())
	for i := 0; i < 20; i++ {
		rec := httptest.NewRecorder()
		mw.ServeHTTP(rec, apiKeyReq("k"))
		if rec.Code != http.StatusOK {
			t.Fatalf("want 200, got %d", rec.Code)
		}
	}
}

func TestQuota_CounterErrorFailsOpen(t *testing.T) {
	resetCounters()
	cfg := quotaCfg(newMemQuota())
	cfg.QuotaFn = func(context.Context, string, time.Time) (int64, error) { return 0, context.DeadlineExceeded }
	rec := httptest.NewRecorder()
	TieredRateLimit(cfg)(noop()).ServeHTTP(rec, apiKeyReq("k"))
	if rec.Code != http.StatusOK {
		t.Fatalf("want fail-open 200, got %d", rec.Code)
	}
}

func TestQuota_PeriodResets(t *testing.T) {
	now := time.Date(2026, 12, 31, 23, 0, 0, 0, time.UTC)
	if got := nextDayStart(now); !got.Equal(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("nextDayStart = %v", got)
	}
	if got := nextMonthStart(now); !got.Equal(time.Date(2027, 1, 1, 0, 0, 0, 0, time.UTC)) {
		t.Errorf("nextMonthStart = %v", got)
	}
}

func TestSimulationBudget_PerKeyPerMinute(t *testing.T) {
	q := newMemQuota()
	allowed := newSimulationBudget(q.incr, 2)
	ctx := context.Background()
	for i := 0; i < 2; i++ {
		if ok, _ := allowed(ctx, "a"); !ok {
			t.Fatalf("call %d should be allowed", i)
		}
	}
	if ok, _ := allowed(ctx, "a"); ok {
		t.Fatal("third call in the minute must be rejected")
	}
	if ok, _ := allowed(ctx, "b"); !ok {
		t.Fatal("other key must be unaffected")
	}
}
