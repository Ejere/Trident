package main

import (
	"context"
	"database/sql"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type fakeOutboxStore struct {
	mu       sync.Mutex
	items    []*webhookOutboxItem
	subs     map[string]webhookSubscription
	claimed  map[int64]bool
	done     []int64
	resched  map[int64]time.Time
	attempts map[int64]int
	recorded []string
}

func newFakeOutboxStore(subs ...webhookSubscription) *fakeOutboxStore {
	f := &fakeOutboxStore{
		subs:     map[string]webhookSubscription{},
		claimed:  map[int64]bool{},
		resched:  map[int64]time.Time{},
		attempts: map[int64]int{},
	}
	for _, s := range subs {
		f.subs[s.ID] = s
	}
	return f
}

func (f *fakeOutboxStore) add(item *webhookOutboxItem) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.items = append(f.items, item)
}

func (f *fakeOutboxStore) Claim(_ context.Context, _ time.Duration) (*webhookOutboxItem, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, it := range f.items {
		if f.claimed[it.ID] {
			continue
		}
		if next, ok := f.resched[it.ID]; ok && next.After(time.Now()) {
			continue
		}
		f.claimed[it.ID] = true
		return it, nil
	}
	return nil, nil
}

func (f *fakeOutboxStore) Subscription(_ context.Context, id string) (webhookSubscription, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	s, ok := f.subs[id]
	if !ok {
		return s, sql.ErrNoRows
	}
	return s, nil
}

func (f *fakeOutboxStore) Done(_ context.Context, id int64) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.done = append(f.done, id)
	return nil
}

func (f *fakeOutboxStore) Reschedule(_ context.Context, id int64, failed int, next time.Time) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.resched[id] = next
	f.attempts[id] = failed
	f.claimed[id] = false
	return nil
}

func (f *fakeOutboxStore) RecordAttempt(_ context.Context, _ *webhookOutboxItem, _ int, status string, _ webhookDeliveryResult) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.recorded = append(f.recorded, status)
	return nil
}

func (f *fakeOutboxStore) isDone(id int64) bool {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, d := range f.done {
		if d == id {
			return true
		}
	}
	return false
}

func testRetryConfig() webhookRetryConfig {
	return webhookRetryConfig{
		Base:      10 * time.Second,
		MaxDelay:  time.Hour,
		Window:    24 * time.Hour,
		Workers:   4,
		jitterRng: func() float64 { return 0.5 },
	}
}

func TestWebhookRetryDelayIsCappedExponential(t *testing.T) {
	cfg := testRetryConfig()
	want := []time.Duration{10 * time.Second, 20 * time.Second, 40 * time.Second, 80 * time.Second}
	for i, w := range want {
		if got := cfg.retryDelay(i+1, 0.5); got != w {
			t.Errorf("retryDelay(%d) = %v, want %v", i+1, got, w)
		}
	}
	for _, n := range []int{10, 40, 1000} {
		if got := cfg.retryDelay(n, 0.5); got != time.Hour {
			t.Errorf("retryDelay(%d) = %v, want cap of 1h", n, got)
		}
	}
}

func TestWebhookRetryDelayJitterBounds(t *testing.T) {
	cfg := testRetryConfig()
	lo, hi := cfg.retryDelay(5, 0), cfg.retryDelay(5, 0.999999)
	base := 160 * time.Second
	if lo != time.Duration(float64(base)*0.8) {
		t.Errorf("min jitter delay = %v, want 80%% of %v", lo, base)
	}
	if hi < time.Duration(float64(base)*1.19) || hi > time.Duration(float64(base)*1.2) {
		t.Errorf("max jitter delay = %v, want just under 120%% of %v", hi, base)
	}
}

func TestWebhookRetryWindowSpansRoughlyOneDay(t *testing.T) {
	cfg := testRetryConfig()
	queued := time.Unix(0, 0)
	now := queued
	attempts := 0
	for {
		attempts++
		next, ok := cfg.nextAttempt(queued, now, attempts, 0.5)
		if !ok {
			break
		}
		now = next
	}
	if now.Sub(queued) < 22*time.Hour || now.Sub(queued) > 24*time.Hour {
		t.Fatalf("last retry at %v after queueing, want between 22h and 24h", now.Sub(queued))
	}
	if attempts < 20 {
		t.Fatalf("only %d attempts across the window, expected many more than the old 5", attempts)
	}
}

func TestLoadWebhookRetryConfigEnv(t *testing.T) {
	def := loadWebhookRetryConfig()
	if def.Base != 10*time.Second || def.MaxDelay != time.Hour || def.Window != 24*time.Hour || def.Workers != 16 {
		t.Fatalf("unexpected defaults: %+v", def)
	}
	t.Setenv("WEBHOOK_RETRY_BASE_SECONDS", "2")
	t.Setenv("WEBHOOK_RETRY_MAX_BACKOFF_SECONDS", "300")
	t.Setenv("WEBHOOK_RETRY_WINDOW_HOURS", "6")
	t.Setenv("WEBHOOK_DELIVERY_WORKERS", "3")
	got := loadWebhookRetryConfig()
	if got.Base != 2*time.Second || got.MaxDelay != 5*time.Minute || got.Window != 6*time.Hour || got.Workers != 3 {
		t.Fatalf("env not applied: %+v", got)
	}
	t.Setenv("WEBHOOK_RETRY_WINDOW_HOURS", "bogus")
	if loadWebhookRetryConfig().Window != 24*time.Hour {
		t.Fatal("invalid value should fall back to default")
	}
}

func TestProcessOutboxItemSuccessCompletes(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer srv.Close()
	store := newFakeOutboxStore(webhookSubscription{ID: "s1", Secret: "x", TargetURL: srv.URL})
	item := &webhookOutboxItem{ID: 1, SubscriptionID: "s1", EventID: "e1", CreatedAt: time.Now()}

	processWebhookOutboxItem(context.Background(), store, testRetryConfig(), item)

	if !store.isDone(1) || len(store.recorded) != 1 || store.recorded[0] != "success" {
		t.Fatalf("done=%v recorded=%v", store.done, store.recorded)
	}
}

func TestProcessOutboxItemFailureReschedulesWithBackoff(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	store := newFakeOutboxStore(webhookSubscription{ID: "s1", Secret: "x", TargetURL: srv.URL})
	item := &webhookOutboxItem{ID: 1, SubscriptionID: "s1", EventID: "e1", Attempt: 2, CreatedAt: time.Now()}

	before := time.Now()
	processWebhookOutboxItem(context.Background(), store, testRetryConfig(), item)

	if calls.Load() != 1 || store.isDone(1) {
		t.Fatalf("calls=%d done=%v", calls.Load(), store.done)
	}
	if store.attempts[1] != 3 {
		t.Errorf("failed attempts persisted = %d, want 3 (resumes counting after restart)", store.attempts[1])
	}
	delay := store.resched[1].Sub(before)
	if delay < 39*time.Second || delay > 41*time.Second {
		t.Errorf("next attempt in %v, want ~40s (10s * 2^2)", delay)
	}
	if len(store.recorded) != 1 || store.recorded[0] != "failed" {
		t.Errorf("recorded = %v, want [failed]", store.recorded)
	}
}

func TestProcessOutboxItemDeadLettersAfterWindow(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusBadGateway) }))
	defer srv.Close()
	store := newFakeOutboxStore(webhookSubscription{ID: "s1", Secret: "x", TargetURL: srv.URL})
	item := &webhookOutboxItem{ID: 1, SubscriptionID: "s1", EventID: "e1", Attempt: 30, CreatedAt: time.Now().Add(-25 * time.Hour)}

	processWebhookOutboxItem(context.Background(), store, testRetryConfig(), item)

	if !store.isDone(1) {
		t.Fatal("dead-lettered row must leave the outbox")
	}
	if len(store.recorded) != 1 || store.recorded[0] != "dead_lettered" {
		t.Fatalf("recorded = %v, want [dead_lettered]", store.recorded)
	}
}

func TestProcessOutboxItemDropsDeletedSubscription(t *testing.T) {
	store := newFakeOutboxStore()
	processWebhookOutboxItem(context.Background(), store, testRetryConfig(), &webhookOutboxItem{ID: 7, SubscriptionID: "gone", CreatedAt: time.Now()})
	if !store.isDone(7) || len(store.recorded) != 0 {
		t.Fatalf("done=%v recorded=%v", store.done, store.recorded)
	}
}

func TestProcessOutboxItemPausedSubscriptionIsNotDelivered(t *testing.T) {
	var calls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { calls.Add(1) }))
	defer srv.Close()
	paused := time.Now()
	store := newFakeOutboxStore(webhookSubscription{ID: "s1", TargetURL: srv.URL, PausedAt: &paused})
	processWebhookOutboxItem(context.Background(), store, testRetryConfig(), &webhookOutboxItem{ID: 1, SubscriptionID: "s1", CreatedAt: time.Now()})

	if calls.Load() != 0 || store.isDone(1) || store.attempts[1] != 0 {
		t.Fatalf("calls=%d done=%v attempts=%d", calls.Load(), store.done, store.attempts[1])
	}
	if store.resched[1].IsZero() {
		t.Fatal("paused row should be rescheduled, not dropped")
	}
}

// A subscriber returning 500 slowly must not delay a healthy subscriber:
// each delivery runs independently in the worker pool (issue #747).
func TestFailingEndpointDoesNotBlockHealthySubscriber(t *testing.T) {
	release := make(chan struct{})
	slow := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		<-release
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer slow.Close()
	defer close(release)
	healthy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	defer healthy.Close()

	store := newFakeOutboxStore(
		webhookSubscription{ID: "bad", Secret: "x", TargetURL: slow.URL},
		webhookSubscription{ID: "good", Secret: "x", TargetURL: healthy.URL},
	)
	store.add(&webhookOutboxItem{ID: 1, SubscriptionID: "bad", EventID: "e1", CreatedAt: time.Now()})
	store.add(&webhookOutboxItem{ID: 2, SubscriptionID: "good", EventID: "e1", CreatedAt: time.Now()})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	cfg := testRetryConfig()
	wake := make(chan struct{}, cfg.Workers)
	for i := 0; i < 2; i++ {
		go runWebhookDeliveryWorker(ctx, store, cfg, wake)
	}

	deadline := time.Now().Add(2 * time.Second)
	for !store.isDone(2) {
		if time.Now().After(deadline) {
			t.Fatal("healthy subscriber was not delivered while the failing endpoint was still in flight")
		}
		time.Sleep(5 * time.Millisecond)
	}
	if store.isDone(1) {
		t.Fatal("test precondition: failing endpoint should still be in flight")
	}
}
