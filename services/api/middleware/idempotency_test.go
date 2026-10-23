package middleware_test

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Depo-dev/trident/services/api/internal/httputil"
	"github.com/Depo-dev/trident/services/api/middleware"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

// countingCreateHandler simulates a POST create endpoint: it "creates" a
// resource with an incrementing id and returns it as JSON, so a test can
// tell whether the handler actually ran again or was replayed.
func countingCreateHandler(t *testing.T) (http.HandlerFunc, *int32) {
	t.Helper()
	var calls int32
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := atomic.AddInt32(&calls, 1)
		body, _ := io.ReadAll(r.Body)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(map[string]any{
			"id":            n,
			"received_body": string(body),
		})
	}), &calls
}

func newIdempotencyTestRedis(t *testing.T) *redis.Client {
	t.Helper()
	server := miniredis.RunT(t)
	client := redis.NewClient(&redis.Options{Addr: server.Addr()})
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// TestIdempotency_DuplicateKeySameBodyReplaysOriginalResponse is the core
// acceptance criterion: a retry with the same key + identical body must not
// re-execute the handler, and must return byte-for-byte the same response.
func TestIdempotency_DuplicateKeySameBodyReplaysOriginalResponse(t *testing.T) {
	rdb := newIdempotencyTestRedis(t)
	handler, calls := countingCreateHandler(t)
	h := middleware.Idempotency(rdb, time.Minute)(handler)

	body := []byte(`{"label":"my key"}`)

	doRequest := func() *httptest.ResponseRecorder {
		req := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewReader(body))
		req.Header.Set("Idempotency-Key", "retry-key-1")
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		return rec
	}

	first := doRequest()
	second := doRequest()

	if atomic.LoadInt32(calls) != 1 {
		t.Fatalf("handler executed %d times, want exactly 1", atomic.LoadInt32(calls))
	}
	if first.Code != http.StatusCreated || second.Code != http.StatusCreated {
		t.Fatalf("status codes = %d, %d, want both 201", first.Code, second.Code)
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("replayed body differs from original:\nfirst:  %s\nsecond: %s", first.Body.String(), second.Body.String())
	}
	if second.Header().Get("Idempotent-Replayed") != "true" {
		t.Fatal("replayed response should carry Idempotent-Replayed: true")
	}
	if first.Header().Get("Idempotent-Replayed") == "true" {
		t.Fatal("the original (non-replayed) response should not carry Idempotent-Replayed")
	}
}

// TestIdempotency_SameKeyDifferentBodyIsConflict asserts a key reused with a
// materially different request is rejected, not silently executed or
// replayed with the wrong response.
func TestIdempotency_SameKeyDifferentBodyIsConflict(t *testing.T) {
	rdb := newIdempotencyTestRedis(t)
	handler, calls := countingCreateHandler(t)
	h := middleware.Idempotency(rdb, time.Minute)(handler)

	req1 := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewReader([]byte(`{"label":"a"}`)))
	req1.Header.Set("Idempotency-Key", "reused-key")
	rec1 := httptest.NewRecorder()
	h.ServeHTTP(rec1, req1)

	req2 := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewReader([]byte(`{"label":"b"}`)))
	req2.Header.Set("Idempotency-Key", "reused-key")
	rec2 := httptest.NewRecorder()
	h.ServeHTTP(rec2, req2)

	if atomic.LoadInt32(calls) != 1 {
		t.Fatalf("handler executed %d times, want exactly 1 (second request must be rejected before execution)", atomic.LoadInt32(calls))
	}
	if rec2.Code != http.StatusConflict {
		t.Fatalf("status = %d, want %d", rec2.Code, http.StatusConflict)
	}
	var body httputil.ErrorResponse
	if err := json.Unmarshal(rec2.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode error body: %v", err)
	}
	if body.Error.Code != httputil.CONFLICT {
		t.Fatalf("error code = %q, want %q", body.Error.Code, httputil.CONFLICT)
	}
}

// TestIdempotency_NoHeaderExecutesEveryTime asserts the header is opt-in:
// omitting it must never engage replay/conflict logic.
func TestIdempotency_NoHeaderExecutesEveryTime(t *testing.T) {
	rdb := newIdempotencyTestRedis(t)
	handler, calls := countingCreateHandler(t)
	h := middleware.Idempotency(rdb, time.Minute)(handler)

	for i := 0; i < 3; i++ {
		req := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewReader([]byte(`{}`)))
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("attempt %d: status = %d, want 201", i, rec.Code)
		}
	}
	if atomic.LoadInt32(calls) != 3 {
		t.Fatalf("handler executed %d times, want 3 (no idempotency key = no dedup)", atomic.LoadInt32(calls))
	}
}

// TestIdempotency_DifferentKeysExecuteIndependently asserts two distinct
// keys never collide with each other.
func TestIdempotency_DifferentKeysExecuteIndependently(t *testing.T) {
	rdb := newIdempotencyTestRedis(t)
	handler, calls := countingCreateHandler(t)
	h := middleware.Idempotency(rdb, time.Minute)(handler)

	for _, key := range []string{"key-a", "key-b"} {
		req := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewReader([]byte(`{}`)))
		req.Header.Set("Idempotency-Key", key)
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusCreated {
			t.Fatalf("key %q: status = %d, want 201", key, rec.Code)
		}
	}
	if atomic.LoadInt32(calls) != 2 {
		t.Fatalf("handler executed %d times, want 2 (distinct keys must not dedup against each other)", atomic.LoadInt32(calls))
	}
}

// TestIdempotency_NilRedisPassesThrough matches the fail-open posture the
// rest of this service takes when Redis is unavailable: Idempotency must
// not block create requests just because Redis is down.
func TestIdempotency_NilRedisPassesThrough(t *testing.T) {
	handler, calls := countingCreateHandler(t)
	h := middleware.Idempotency(nil, time.Minute)(handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Idempotency-Key", "some-key")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201", rec.Code)
	}
	if atomic.LoadInt32(calls) != 1 {
		t.Fatalf("handler executed %d times, want 1", atomic.LoadInt32(calls))
	}
}

// TestIdempotency_OversizedKeyIsRejected guards against an unbounded
// client-supplied header being used as a Redis key.
func TestIdempotency_OversizedKeyIsRejected(t *testing.T) {
	rdb := newIdempotencyTestRedis(t)
	handler, calls := countingCreateHandler(t)
	h := middleware.Idempotency(rdb, time.Minute)(handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Idempotency-Key", string(make([]byte, 512)))
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", rec.Code)
	}
	if atomic.LoadInt32(calls) != 0 {
		t.Fatal("handler must not execute for a rejected key")
	}
}

// TestIdempotency_RequestBodyStillReachesTheHandler guards against the body
// being consumed by the fingerprinting step and never reaching the handler.
func TestIdempotency_RequestBodyStillReachesTheHandler(t *testing.T) {
	rdb := newIdempotencyTestRedis(t)
	handler, _ := countingCreateHandler(t)
	h := middleware.Idempotency(rdb, time.Minute)(handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/api-keys", bytes.NewReader([]byte(`{"label":"preserved"}`)))
	req.Header.Set("Idempotency-Key", "body-check")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if !bytes.Contains(rec.Body.Bytes(), []byte("preserved")) {
		t.Fatalf("handler did not see the original body: %s", rec.Body.String())
	}
}

// TestIdempotency_RedisValueIsEncryptedAtRest is the regression test for
// issue #572: two of the routes this middleware wraps (create api-key,
// create webhook) return a live credential in their 201 body, and that body
// used to be stored in Redis as plaintext JSON for the full TTL — readable
// by anything with Redis visibility (a replica, a backup, MONITOR, a
// compromise). The raw value stored under the idempotency key must contain
// neither the credential nor any recognisable JSON structure.
func TestIdempotency_RedisValueIsEncryptedAtRest(t *testing.T) {
	rdb := newIdempotencyTestRedis(t)

	const secret = "whsec_abcdef0123456789abcdef0123456789"
	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_, _ = w.Write([]byte(`{"secret":"` + secret + `"}`))
	})
	h := middleware.Idempotency(rdb, time.Minute)(handler)

	req := httptest.NewRequest(http.MethodPost, "/v1/webhooks", bytes.NewReader([]byte(`{"contractId":"C1","targetUrl":"https://example.test/hook"}`)))
	req.Header.Set("Idempotency-Key", "encrypt-check")
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusCreated {
		t.Fatalf("status = %d, want 201: %s", rec.Code, rec.Body.String())
	}
	if !bytes.Contains(rec.Body.Bytes(), []byte(secret)) {
		t.Fatalf("client response missing the secret: %s", rec.Body.String())
	}

	keys, err := rdb.Keys(t.Context(), "idempotency:*").Result()
	if err != nil {
		t.Fatalf("scanning redis keys: %v", err)
	}
	if len(keys) == 0 {
		t.Fatal("no idempotency record was written to redis")
	}

	for _, key := range keys {
		raw, err := rdb.Get(t.Context(), key).Result()
		if err != nil {
			t.Fatalf("reading redis key %q: %v", key, err)
		}
		if bytes.Contains([]byte(raw), []byte(secret)) {
			t.Fatalf("redis key %q stores the secret in plaintext: %q", key, raw)
		}
		if json.Valid([]byte(raw)) {
			t.Fatalf("redis key %q stores valid, presumably unencrypted JSON: %q", key, raw)
		}
	}
}

// TestValidateIdempotencyEncryptionConfig covers the production fail-fast
// gate: without a pinned IDEMPOTENCY_ENCRYPTION_KEY, every replica encrypts
// idempotency records under its own random key, so a retry landing on a
// different replica can't decrypt the first attempt's record and mints a
// second resource — this must refuse to start in production instead.
func TestValidateIdempotencyEncryptionConfig(t *testing.T) {
	validKey := "ab0123456789ab0123456789ab0123456789ab0123456789ab0123456789ab"

	cases := []struct {
		name    string
		appEnv  string
		key     string
		wantErr bool
	}{
		{name: "non-production, no key", appEnv: "development", key: "", wantErr: false},
		{name: "non-production, malformed key", appEnv: "development", key: "not-hex", wantErr: false},
		{name: "production, no key", appEnv: "production", key: "", wantErr: true},
		{name: "production, malformed key", appEnv: "production", key: "too-short", wantErr: true},
		{name: "production, valid key", appEnv: "production", key: validKey, wantErr: false},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("APP_ENV", tc.appEnv)
			t.Setenv("IDEMPOTENCY_ENCRYPTION_KEY", tc.key)

			err := middleware.ValidateIdempotencyEncryptionConfig()
			if tc.wantErr && err == nil {
				t.Fatal("expected an error, got nil")
			}
			if !tc.wantErr && err != nil {
				t.Fatalf("expected no error, got %v", err)
			}
		})
	}
}
