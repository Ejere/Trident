package handlers_test

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Depo-dev/trident/services/api/handlers"
	"github.com/Depo-dev/trident/services/api/internal/httputil"
	"github.com/Depo-dev/trident/services/api/middleware"
)

type countingRPC struct {
	calls atomic.Int32
	body  string
}

func (c *countingRPC) Call(_ context.Context, _ string, _ any, result any) error {
	c.calls.Add(1)
	return json.Unmarshal([]byte(c.body), result)
}

func okSimBody(t *testing.T) string {
	return `{"results":[{"xdr":"` + scValU64B64(t, 7) + `"}]}`
}

func callAs(t *testing.T, h http.HandlerFunc, network, keyID string) *httptest.ResponseRecorder {
	t.Helper()
	req := newCallRequest(t, validCallContractID, map[string]any{"function": "balance", "args": []string{}})
	ctx := req.Context()
	if network != "" {
		ctx = middleware.WithNetwork(ctx, network)
	}
	if keyID != "" {
		ctx = middleware.WithAPIKeyID(ctx, keyID)
	}
	rec := httptest.NewRecorder()
	h(rec, req.WithContext(ctx))
	return rec
}

func TestCallContract_NetworkMismatch_Returns403(t *testing.T) {
	mainnet := &countingRPC{body: okSimBody(t)}
	h := handlers.CallContractWith(handlers.ContractCallConfig{
		Callers: map[string]handlers.SorobanRPCCaller{"mainnet": mainnet},
	})

	rec := callAs(t, h, "testnet", "key-1")
	if rec.Code != http.StatusForbidden {
		t.Fatalf("want 403, got %d body=%s", rec.Code, rec.Body.String())
	}
	var body httputil.ErrorResponse
	_ = json.Unmarshal(rec.Body.Bytes(), &body)
	if body.Error.Code != httputil.FORBIDDEN {
		t.Errorf("code = %q, want FORBIDDEN", body.Error.Code)
	}
	if mainnet.calls.Load() != 0 {
		t.Fatal("mismatched key must not reach the mainnet RPC")
	}
}

func TestCallContract_RoutesByKeyNetwork(t *testing.T) {
	testnet := &countingRPC{body: okSimBody(t)}
	mainnet := &countingRPC{body: okSimBody(t)}
	h := handlers.CallContractWith(handlers.ContractCallConfig{
		Callers: map[string]handlers.SorobanRPCCaller{"testnet": testnet, "mainnet": mainnet},
	})

	if rec := callAs(t, h, "mainnet", "k"); rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rec.Code)
	}
	if mainnet.calls.Load() != 1 || testnet.calls.Load() != 0 {
		t.Fatalf("routing wrong: mainnet=%d testnet=%d", mainnet.calls.Load(), testnet.calls.Load())
	}
}

func TestCallContract_CachesIdenticalCalls(t *testing.T) {
	rpc := &countingRPC{body: okSimBody(t)}
	h := handlers.CallContractWith(handlers.ContractCallConfig{
		Callers:  map[string]handlers.SorobanRPCCaller{"testnet": rpc},
		CacheTTL: time.Minute,
	})

	for i := 0; i < 3; i++ {
		if rec := callAs(t, h, "testnet", "k"); rec.Code != http.StatusOK {
			t.Fatalf("call %d: want 200, got %d", i, rec.Code)
		}
	}
	if rpc.calls.Load() != 1 {
		t.Fatalf("upstream simulations = %d, want 1 (cached)", rpc.calls.Load())
	}
}

func TestCallContract_SimulationBudget_Returns429_CacheHitsFree(t *testing.T) {
	rpc := &countingRPC{body: okSimBody(t)}
	var budget atomic.Int32
	budget.Store(1)
	h := handlers.CallContractWith(handlers.ContractCallConfig{
		Callers:  map[string]handlers.SorobanRPCCaller{"testnet": rpc},
		CacheTTL: time.Minute,
		SimulationAllowed: func(context.Context, string) (bool, error) {
			return budget.Add(-1) >= 0, nil
		},
	})

	if rec := callAs(t, h, "testnet", "k"); rec.Code != http.StatusOK {
		t.Fatalf("first: want 200, got %d", rec.Code)
	}
	// Identical call is served from cache without spending budget.
	if rec := callAs(t, h, "testnet", "k"); rec.Code != http.StatusOK {
		t.Fatalf("cached: want 200, got %d", rec.Code)
	}

	req := newCallRequest(t, validCallContractID, map[string]any{"function": "other", "args": []string{}})
	rec := httptest.NewRecorder()
	h(rec, req.WithContext(middleware.WithAPIKeyID(middleware.WithNetwork(req.Context(), "testnet"), "k")))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over budget: want 429, got %d", rec.Code)
	}
	if rpc.calls.Load() != 1 {
		t.Fatalf("upstream calls = %d, want 1", rpc.calls.Load())
	}
}
