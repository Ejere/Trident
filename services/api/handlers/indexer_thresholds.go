package handlers

import (
	"math"
	"os"
	"strconv"
	"time"
)

const (
	defaultPollCeilingMs      = 5000
	defaultLedgerCloseSeconds = 5.5
	minStallThreshold         = 60 * time.Second
	minLaggingLedgers         = 10
	laggingMarginLedgers      = 10
	stallCeilingMultiple      = 5
)

// IndexerThresholds are the cut-offs GET /v1/stats/indexer uses to classify
// the indexer as stalled or lagging. Defaults are derived from the indexer's
// POLL_INTERVAL_CEILING_MS so an indexer that is caught up and polling at its
// ceiling (or backing off after RPC degradation) reads as healthy.
type IndexerThresholds struct {
	Stall                 time.Duration
	LagLedgers            int64
	AvgLedgerCloseSeconds float64
}

// loadIndexerThresholds reads the env overrides and derives the defaults:
//
//	INDEXER_STALL_THRESHOLD_SECONDS  default max(60, 5 x ceiling) - the
//	    indexer multiplies its interval by 4 when the RPC is degraded, plus
//	    one ceiling of margin.
//	INDEXER_LAG_THRESHOLD_LEDGERS    default max(10, ceiling/close + 10) - a
//	    caught-up indexer polling at its ceiling is legitimately up to one
//	    ceiling interval behind the tip.
//	LEDGER_CLOSE_SECONDS             default 5.5 - observed mainnet average.
//	POLL_INTERVAL_CEILING_MS         shared with the indexer, default 5000.
func loadIndexerThresholds() IndexerThresholds {
	closeSeconds := envFloat("LEDGER_CLOSE_SECONDS", defaultLedgerCloseSeconds)
	ceiling := time.Duration(envPositiveInt("POLL_INTERVAL_CEILING_MS", defaultPollCeilingMs)) * time.Millisecond

	stall := ceiling * stallCeilingMultiple
	if stall < minStallThreshold {
		stall = minStallThreshold
	}
	if secs := envPositiveInt("INDEXER_STALL_THRESHOLD_SECONDS", 0); secs > 0 {
		stall = time.Duration(secs) * time.Second
	}

	lag := int64(math.Ceil(ceiling.Seconds()/closeSeconds)) + laggingMarginLedgers
	if lag < minLaggingLedgers {
		lag = minLaggingLedgers
	}
	if n := envPositiveInt("INDEXER_LAG_THRESHOLD_LEDGERS", 0); n > 0 {
		lag = int64(n)
	}

	return IndexerThresholds{Stall: stall, LagLedgers: lag, AvgLedgerCloseSeconds: closeSeconds}
}

func envPositiveInt(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil && n > 0 {
		return n
	}
	return def
}

func envFloat(key string, def float64) float64 {
	if f, err := strconv.ParseFloat(os.Getenv(key), 64); err == nil && f > 0 {
		return f
	}
	return def
}
