package middleware

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// blockingHandler holds each request open until release is closed, signalling
// on entered once the handler is running.
func blockingHandler(entered chan struct{}, release chan struct{}) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		entered <- struct{}{}
		<-release
		w.WriteHeader(http.StatusOK)
	})
}

func streamReq(key string) *http.Request {
	r := httptest.NewRequest(http.MethodGet, "/v1/events/stream", nil)
	if key != "" {
		r.Header.Set("X-API-Key", key)
	}
	return r
}

func openStream(t *testing.T, h http.Handler, key string, entered chan struct{}) chan *httptest.ResponseRecorder {
	t.Helper()
	done := make(chan *httptest.ResponseRecorder, 1)
	go func() {
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, streamReq(key))
		done <- rec
	}()
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("stream handler never started")
	}
	return done
}

func newTestLimiter(perProcess int) StreamLimitConfig {
	return StreamLimitConfig{
		Store:         NewMemoryStreamStore(),
		TierLimits:    map[string]int{"free": 2, "pro": 5},
		MaxPerProcess: perProcess,
	}
}

func TestStreamLimit_FreeKeyOverLimitGets429_OthersUnaffected(t *testing.T) {
	entered, release := make(chan struct{}, 10), make(chan struct{})
	h := NewStreamLimiter(newTestLimiter(0))(blockingHandler(entered, release))

	d1 := openStream(t, h, "greedy", entered)
	d2 := openStream(t, h, "greedy", entered)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, streamReq("greedy"))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("third stream: want 429, got %d", rec.Code)
	}

	d3 := openStream(t, h, "polite", entered)

	close(release)
	for _, d := range []chan *httptest.ResponseRecorder{d1, d2, d3} {
		<-d
	}
}

func TestStreamLimit_SlotFreedWhenStreamEnds(t *testing.T) {
	entered, release := make(chan struct{}, 10), make(chan struct{})
	h := NewStreamLimiter(newTestLimiter(0))(blockingHandler(entered, release))

	d1 := openStream(t, h, "k", entered)
	d2 := openStream(t, h, "k", entered)
	close(release)
	<-d1
	<-d2

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, streamReq("k"))
	if rec.Code != http.StatusOK {
		t.Fatalf("after streams closed: want 200, got %d", rec.Code)
	}
}

func TestStreamLimit_PerProcessCap(t *testing.T) {
	entered, release := make(chan struct{}, 10), make(chan struct{})
	h := NewStreamLimiter(newTestLimiter(2))(blockingHandler(entered, release))

	d1 := openStream(t, h, "a", entered)
	d2 := openStream(t, h, "b", entered)

	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, streamReq("c"))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("over process cap: want 429, got %d", rec.Code)
	}
	close(release)
	<-d1
	<-d2
}

func TestStreamLimit_ProTierGetsHigherLimit(t *testing.T) {
	entered, release := make(chan struct{}, 10), make(chan struct{})
	cfg := newTestLimiter(0)
	cfg.DB = &mockTierDB{tier: "pro"}
	h := NewStreamLimiter(cfg)(blockingHandler(entered, release))

	var ds []chan *httptest.ResponseRecorder
	for i := 0; i < 5; i++ {
		ds = append(ds, openStream(t, h, "pro-key", entered))
	}
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, streamReq("pro-key"))
	if rec.Code != http.StatusTooManyRequests {
		t.Fatalf("sixth pro stream: want 429, got %d", rec.Code)
	}
	close(release)
	for _, d := range ds {
		<-d
	}
}

func TestMemoryStreamStore_ExpiredLeaseFreesSlot(t *testing.T) {
	s := NewMemoryStreamStore()
	ctx := httptest.NewRequest(http.MethodGet, "/", nil).Context()
	ok, _ := s.Acquire(ctx, "k", "a", 1, time.Millisecond)
	if !ok {
		t.Fatal("first acquire should succeed")
	}
	time.Sleep(5 * time.Millisecond)
	ok, _ = s.Acquire(ctx, "k", "b", 1, time.Minute)
	if !ok {
		t.Fatal("acquire after lease expiry should succeed")
	}
}
