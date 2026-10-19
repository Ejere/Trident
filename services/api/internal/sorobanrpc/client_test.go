package sorobanrpc

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
)

func rpcServer(t *testing.T, status int, body string, hits *atomic.Int32) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if hits != nil {
			hits.Add(1)
		}
		w.WriteHeader(status)
		_, _ = w.Write([]byte(body))
	}))
	t.Cleanup(srv.Close)
	return srv
}

const okBody = `{"jsonrpc":"2.0","id":1,"result":{"ok":true}}`

func TestClient_NonOKStatusIsError(t *testing.T) {
	srv := rpcServer(t, http.StatusServiceUnavailable, okBody, nil)
	var out map[string]any
	err := NewClient(srv.URL).Call(context.Background(), "m", nil, &out)
	var se *StatusError
	if !errors.As(err, &se) || se.Code != http.StatusServiceUnavailable {
		t.Fatalf("want StatusError 503, got %v", err)
	}
}

func TestClient_OversizedResponseRejected(t *testing.T) {
	big := `{"result":"` + strings.Repeat("a", MaxResponseBytes+10) + `"}`
	srv := rpcServer(t, http.StatusOK, big, nil)
	var out any
	err := NewClient(srv.URL).Call(context.Background(), "m", nil, &out)
	if err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("want too-large error, got %v", err)
	}
}

func TestFailover_FallsOverToSecondaryOnServerError(t *testing.T) {
	var primaryHits, secondaryHits atomic.Int32
	primary := rpcServer(t, http.StatusBadGateway, "boom", &primaryHits)
	secondary := rpcServer(t, http.StatusOK, okBody, &secondaryHits)

	c := NewFailoverClient([]string{primary.URL, secondary.URL})
	var out map[string]bool
	if err := c.Call(context.Background(), "m", nil, &out); err != nil {
		t.Fatalf("expected failover success, got %v", err)
	}
	if !out["ok"] || secondaryHits.Load() != 1 {
		t.Fatalf("secondary not used: out=%v hits=%d", out, secondaryHits.Load())
	}

	// Primary is now cooling down: the next call goes straight to secondary.
	if err := c.Call(context.Background(), "m", nil, &out); err != nil {
		t.Fatal(err)
	}
	if primaryHits.Load() != 1 {
		t.Fatalf("unhealthy primary retried during cooldown: hits=%d", primaryHits.Load())
	}
}

func TestFailover_RPCErrorDoesNotFailOver(t *testing.T) {
	var secondaryHits atomic.Int32
	primary := rpcServer(t, http.StatusOK, `{"error":{"code":-32602,"message":"bad params"}}`, nil)
	secondary := rpcServer(t, http.StatusOK, okBody, &secondaryHits)

	c := NewFailoverClient([]string{primary.URL, secondary.URL})
	if err := c.Call(context.Background(), "m", nil, nil); err == nil {
		t.Fatal("want rpc error")
	}
	if secondaryHits.Load() != 0 {
		t.Fatal("application-level rpc error must not trigger failover")
	}
}

func TestFailover_AllEndpointsDownReturnsError(t *testing.T) {
	a := rpcServer(t, http.StatusInternalServerError, "x", nil)
	b := rpcServer(t, http.StatusBadGateway, "x", nil)
	c := NewFailoverClient([]string{a.URL, b.URL})
	if err := c.Call(context.Background(), "m", nil, nil); err == nil {
		t.Fatal("want error when every endpoint fails")
	}
}
