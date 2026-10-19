package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/Depo-dev/trident/services/api/validation"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// mockStatsDB implements DBPool for stats handler tests.
type mockStatsDB struct {
	lastLedger  *int64
	eventsTotal *int64
	eventsLast  *int64
	pollMs      *int64
	lastPollAt  *time.Time
	lastLedgerT *time.Time
	scanErr     error
}

func (m *mockStatsDB) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return &mockStatsRow{m: m}
}

func (m *mockStatsDB) Ping(_ context.Context) error {
	return nil
}

func (m *mockStatsDB) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return nil, nil
}

type mockStatsRow struct{ m *mockStatsDB }

func (r *mockStatsRow) Scan(dest ...any) error {
	if r.m.scanErr != nil {
		return r.m.scanErr
	}
	if len(dest) != 6 {
		return fmt.Errorf("expected 6 dest, got %d", len(dest))
	}
	*dest[0].(**int64) = r.m.lastLedger
	*dest[1].(**int64) = r.m.eventsTotal
	*dest[2].(**int64) = r.m.eventsLast
	*dest[3].(**int64) = r.m.pollMs
	*dest[4].(**time.Time) = r.m.lastPollAt
	*dest[5].(**time.Time) = r.m.lastLedgerT
	return nil
}

func resetChainTipCache() {
	globalChainTipCache.mu.Lock()
	globalChainTipCache.ledger = nil
	globalChainTipCache.fetchedAt = time.Time{}
	globalChainTipCache.mu.Unlock()
}

func setChainTip(seq int64) {
	globalChainTipCache.mu.Lock()
	globalChainTipCache.ledger = &seq
	globalChainTipCache.fetchedAt = time.Now()
	globalChainTipCache.mu.Unlock()
}

func statsReq() *http.Request {
	return httptest.NewRequest(http.MethodGet, "/v1/stats/indexer", nil)
}

func TestIndexerStats_NilDB_Returns503(t *testing.T) {
	rec := httptest.NewRecorder()
	IndexerStats(nil).ServeHTTP(rec, statsReq())
	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", rec.Code)
	}
}

func TestIndexerStats_Stalled_Returns503(t *testing.T) {
	stale := time.Now().Add(-90 * time.Second)
	resetChainTipCache()

	rec := httptest.NewRecorder()
	IndexerStats(&mockStatsDB{lastPollAt: &stale}).ServeHTTP(rec, statsReq())

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("stalled: want 503, got %d", rec.Code)
	}
	var resp IndexerStatsResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Status != "stalled" {
		t.Errorf("want status=stalled, got %q", resp.Status)
	}
}

func TestIndexerStats_NullLastPollAt_IsStalled(t *testing.T) {
	resetChainTipCache()
	rec := httptest.NewRecorder()
	IndexerStats(&mockStatsDB{}).ServeHTTP(rec, statsReq())

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("null poll: want 503, got %d", rec.Code)
	}
	var resp IndexerStatsResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Status != "stalled" {
		t.Errorf("want stalled, got %q", resp.Status)
	}
}

func TestIndexerStats_Healthy_Returns200(t *testing.T) {
	now := time.Now()
	ledger := int64(1000)
	total := int64(50000)
	last := int64(42)
	ms := int64(120)
	db := &mockStatsDB{lastLedger: &ledger, eventsTotal: &total, eventsLast: &last, pollMs: &ms, lastPollAt: &now}
	setChainTip(1005)

	rec := httptest.NewRecorder()
	IndexerStats(db).ServeHTTP(rec, statsReq())

	if rec.Code != http.StatusOK {
		t.Fatalf("healthy: want 200, got %d — body: %s", rec.Code, rec.Body.String())
	}
	var resp IndexerStatsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatal("decode:", err)
	}
	if resp.Status != "healthy" {
		t.Errorf("want healthy, got %q", resp.Status)
	}
	if resp.LastLedgerIndexed == nil || *resp.LastLedgerIndexed != 1000 {
		t.Errorf("last_ledger_indexed: got %v", resp.LastLedgerIndexed)
	}
	if resp.ChainTipLedger == nil || *resp.ChainTipLedger != 1005 {
		t.Errorf("chain_tip_ledger: got %v", resp.ChainTipLedger)
	}
	if resp.LagLedgers == nil || *resp.LagLedgers != 5 {
		t.Errorf("lag_ledgers: want 5, got %v", resp.LagLedgers)
	}
	if resp.EventsIndexedTotal == nil || *resp.EventsIndexedTotal != 50000 {
		t.Errorf("events_indexed_total: got %v", resp.EventsIndexedTotal)
	}
}

func TestIndexerStats_Lagging_Returns200(t *testing.T) {
	now := time.Now()
	ledger := int64(1000)
	setChainTip(1020) // 20 ahead — lagging (>10)

	rec := httptest.NewRecorder()
	IndexerStats(&mockStatsDB{lastLedger: &ledger, lastPollAt: &now}).ServeHTTP(rec, statsReq())

	if rec.Code != http.StatusOK {
		t.Fatalf("lagging: want 200, got %d", rec.Code)
	}
	var resp IndexerStatsResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Status != "lagging" {
		t.Errorf("want lagging, got %q", resp.Status)
	}
}

func TestIndexerStats_NilChainTip_LagIsNil(t *testing.T) {
	now := time.Now()
	ledger := int64(500)
	resetChainTipCache() // STELLAR_RPC_URL unset -> nil tip

	rec := httptest.NewRecorder()
	IndexerStats(&mockStatsDB{lastLedger: &ledger, lastPollAt: &now}).ServeHTTP(rec, statsReq())

	if rec.Code != http.StatusOK {
		t.Fatalf("nil tip: want 200, got %d", rec.Code)
	}
	var resp IndexerStatsResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.ChainTipLedger != nil {
		t.Errorf("chain_tip_ledger: want nil, got %v", resp.ChainTipLedger)
	}
	if resp.LagLedgers != nil {
		t.Errorf("lag_ledgers: want nil when tip unknown, got %v", resp.LagLedgers)
	}
}

func TestIndexerStats_LastPollAt_RFC3339(t *testing.T) {
	ts := time.Date(2025, 1, 15, 12, 0, 0, 0, time.UTC)
	resetChainTipCache()

	rec := httptest.NewRecorder()
	IndexerStats(&mockStatsDB{lastPollAt: &ts}).ServeHTTP(rec, statsReq())

	var resp IndexerStatsResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.LastPollAt == nil {
		t.Fatal("last_poll_at should not be nil")
	}
	if *resp.LastPollAt != "2025-01-15T12:00:00Z" {
		t.Errorf("last_poll_at: got %q, want RFC3339 UTC", *resp.LastPollAt)
	}
}

func TestIndexerStats_DBError_Returns503(t *testing.T) {
	db := &mockStatsDB{scanErr: fmt.Errorf("connection reset")}
	resetChainTipCache()

	rec := httptest.NewRecorder()
	IndexerStats(db).ServeHTTP(rec, statsReq())

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("db error: want 503, got %d", rec.Code)
	}
}

func TestMetricsHandler_ExposesAllThreeGauges(t *testing.T) {
	rec := httptest.NewRecorder()
	MetricsHandler(nil, nil).ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	body := rec.Body.String()
	for _, metric := range []string{
		"trident_api_indexer_lag_ledgers",
		"trident_api_indexer_last_poll_timestamp_seconds",
		"trident_api_indexer_events_indexed",
	} {
		if !strings.Contains(body, metric) {
			t.Errorf("metric %q not found in /metrics output", metric)
		}
	}
}

// TestContractsStats_NoParams_Returns200 verifies default parameters work
func TestContractsStats_NoParams_Returns200(t *testing.T) {
	t.Skip("requires database and redis integration")
}

// TestContractsStats_InvalidLimit_Returns400 validates limit bounds
func TestContractsStats_InvalidLimit_Returns400(t *testing.T) {
	t.Skip("requires database and redis integration")
}

// TestContractsStats_CacheHit_Returns200 verifies Redis caching
func TestContractsStats_CacheHit_Returns200(t *testing.T) {
	t.Skip("requires database and redis integration")
}

// TestContractsStats_RequiresAuth validates auth middleware
func TestContractsStats_RequiresAuth(t *testing.T) {
	t.Skip("requires database and redis integration")
}

// TestContractStatsRollup_MatchesLiveAggregation seeds soroban_events for a
// unique contract, refreshes contract_stats_rollup, and asserts the
// rollup-backed query returns the same event_count/last_seen_ledger as the
// live aggregation it replaces for the default (unfiltered) query (issue
// #257). Opt-in like the Rust indexer's DB tests: skipped unless
// TEST_DATABASE_URL is set, since the `go` CI job does not run a Postgres
// service.
func TestContractStatsRollup_MatchesLiveAggregation(t *testing.T) {
	dbURL := os.Getenv("TEST_DATABASE_URL")
	if dbURL == "" {
		t.Skip("TEST_DATABASE_URL not set")
	}

	ctx := context.Background()
	pool, err := pgxpool.New(ctx, dbURL)
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	defer pool.Close()

	contractID := fmt.Sprintf("CROLLUPTEST_%d", time.Now().UnixNano())
	const network = "testnet"

	t.Cleanup(func() {
		_, _ = pool.Exec(ctx, "DELETE FROM soroban_events WHERE contract_id = $1", contractID)
		_, _ = pool.Exec(ctx, "DELETE FROM contract_stats_rollup WHERE contract_id = $1", contractID)
	})

	seedEvent := `
		INSERT INTO soroban_events
			(contract_id, ledger_sequence, ledger_timestamp, transaction_hash,
			 event_index, event_type, network, topics, data)
		VALUES ($1, $2, $3, $4, 0, 'contract', $5, '[]', '{}')
	`
	for i, seq := range []int64{100, 101, 102} {
		ts := time.Unix(1_700_000_000+seq, 0).UTC()
		if _, err := pool.Exec(ctx, seedEvent, contractID, seq, ts, fmt.Sprintf("tx%d", i), network); err != nil {
			t.Fatalf("seed event: %v", err)
		}
	}

	if err := RefreshContractStatsRollup(ctx, pool); err != nil {
		t.Fatalf("refresh rollup: %v", err)
	}

	params := &validation.QueryStatsParams{Network: network, Limit: 100}

	rollupStats, populated, err := queryContractStatsFromRollup(ctx, pool, params)
	if err != nil {
		t.Fatalf("rollup query: %v", err)
	}
	if !populated {
		t.Fatalf("rollup should be populated for network %q after refresh", network)
	}

	liveStats, err := queryContractStats(ctx, pool, params)
	if err != nil {
		t.Fatalf("live query: %v", err)
	}

	var rollupRow, liveRow *ContractStats
	for _, cs := range rollupStats {
		if cs.ContractID == contractID {
			rollupRow = cs
		}
	}
	for _, cs := range liveStats {
		if cs.ContractID == contractID {
			liveRow = cs
		}
	}

	if rollupRow == nil || liveRow == nil {
		t.Fatalf("seeded contract missing from results: rollup=%v live=%v", rollupRow, liveRow)
	}
	if rollupRow.EventCount != liveRow.EventCount {
		t.Errorf("event_count mismatch: rollup=%d live=%d", rollupRow.EventCount, liveRow.EventCount)
	}
	if rollupRow.LastSeenLedger != liveRow.LastSeenLedger {
		t.Errorf("last_seen_ledger mismatch: rollup=%d live=%d", rollupRow.LastSeenLedger, liveRow.LastSeenLedger)
	}
	if rollupRow.EventCount != 3 {
		t.Errorf("expected 3 seeded events, got event_count=%d", rollupRow.EventCount)
	}
}

func TestLoadIndexerThresholds_Defaults(t *testing.T) {
	th := loadIndexerThresholds()
	if th.Stall != 60*time.Second {
		t.Errorf("stall: want 60s, got %v", th.Stall)
	}
	if th.LagLedgers != 11 {
		t.Errorf("lag ledgers: want 11, got %d", th.LagLedgers)
	}
	if th.AvgLedgerCloseSeconds != 5.5 {
		t.Errorf("close seconds: want 5.5, got %v", th.AvgLedgerCloseSeconds)
	}
}

func TestLoadIndexerThresholds_DerivedFromCeiling(t *testing.T) {
	t.Setenv("POLL_INTERVAL_CEILING_MS", "600000")
	th := loadIndexerThresholds()
	if th.Stall != 3000*time.Second {
		t.Errorf("stall: want 3000s (5 x ceiling), got %v", th.Stall)
	}
	if th.LagLedgers != 120 {
		t.Errorf("lag ledgers: want 120 (ceil(600/5.5)+10), got %d", th.LagLedgers)
	}
}

func TestLoadIndexerThresholds_ExplicitOverrides(t *testing.T) {
	t.Setenv("POLL_INTERVAL_CEILING_MS", "600000")
	t.Setenv("INDEXER_STALL_THRESHOLD_SECONDS", "120")
	t.Setenv("INDEXER_LAG_THRESHOLD_LEDGERS", "50")
	t.Setenv("LEDGER_CLOSE_SECONDS", "6")
	th := loadIndexerThresholds()
	if th.Stall != 120*time.Second || th.LagLedgers != 50 || th.AvgLedgerCloseSeconds != 6 {
		t.Errorf("overrides not applied: %+v", th)
	}
}

func TestIndexerStats_CaughtUpAtCeilingInterval_IsHealthy(t *testing.T) {
	t.Setenv("POLL_INTERVAL_CEILING_MS", "600000")
	pollAt := time.Now().Add(-9 * time.Minute)
	ledger := int64(1000)
	setChainTip(1100) // ~100 ledgers behind, normal for a 10 minute poll

	rec := httptest.NewRecorder()
	IndexerStats(&mockStatsDB{lastLedger: &ledger, lastPollAt: &pollAt}).ServeHTTP(rec, statsReq())

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}
	var resp IndexerStatsResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Status != "healthy" {
		t.Errorf("want healthy, got %q", resp.Status)
	}
}

func TestIndexerStats_LagSecondsFromLedgerTimestamp(t *testing.T) {
	now := time.Now()
	closedAt := now.Add(-42 * time.Second)
	ledger := int64(1000)
	setChainTip(1003)

	rec := httptest.NewRecorder()
	IndexerStats(&mockStatsDB{lastLedger: &ledger, lastPollAt: &now, lastLedgerT: &closedAt}).ServeHTTP(rec, statsReq())

	var resp IndexerStatsResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.LagSecondsEstimated == nil || *resp.LagSecondsEstimated < 41 || *resp.LagSecondsEstimated > 44 {
		t.Errorf("lag_seconds_estimated: want ~42 from ledger timestamp, got %v", resp.LagSecondsEstimated)
	}
}

func TestIndexerStats_LagSecondsFallsBackToConfiguredCloseTime(t *testing.T) {
	now := time.Now()
	ledger := int64(1000)
	setChainTip(1004)

	rec := httptest.NewRecorder()
	IndexerStats(&mockStatsDB{lastLedger: &ledger, lastPollAt: &now}).ServeHTTP(rec, statsReq())

	var resp IndexerStatsResponse
	_ = json.NewDecoder(rec.Body).Decode(&resp)
	if resp.LagSecondsEstimated == nil || *resp.LagSecondsEstimated != 22 {
		t.Errorf("lag_seconds_estimated: want 4 x 5.5 = 22, got %v", resp.LagSecondsEstimated)
	}
}
