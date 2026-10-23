package middleware

import (
	"bytes"
	"context"
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"time"

	"github.com/Depo-dev/trident/services/api/internal/httputil"
	"github.com/redis/go-redis/v9"
)

// DefaultIdempotencyTTL bounds how long a replayed response stays available
// (issue #225). 24h comfortably covers a client retrying after a network
// partition, a redeploy, or an overnight retry queue, without keeping every
// create call's response in Redis forever.
const DefaultIdempotencyTTL = 24 * time.Hour

// maxIdempotencyBodyBytes bounds the request body read into memory to
// compute a fingerprint. The routes this wraps (create api-key, create
// webhook) accept small JSON bodies — BodySizeLimit already caps every body
// well under this — so this is a defensive ceiling, not an expected limit.
const maxIdempotencyBodyBytes = 1 << 20 // 1 MiB

// maxIdempotencyKeyLen bounds the client-supplied Idempotency-Key itself, so
// a pathologically large header cannot be used as an oversized Redis key.
// 255 matches the common convention (Stripe, among others) for this header.
const maxIdempotencyKeyLen = 255

// idempotencyRecord is what gets persisted to Redis under the client's key:
// enough to both detect a conflicting reuse (Fingerprint) and replay the
// original response byte-for-byte (Status/Header/Body).
type idempotencyRecord struct {
	Fingerprint string      `json:"fingerprint"`
	Status      int         `json:"status"`
	Header      http.Header `json:"header"`
	Body        []byte      `json:"body"`
	// InFlight marks a reservation written before the handler runs, so a
	// concurrent request carrying the same key can be told to retry instead
	// of executing the handler a second time.
	InFlight bool `json:"in_flight,omitempty"`
}

// idempotencyEncryptionKey is the AES-256 key used to encrypt idempotency
// records at rest in Redis (issue #572). Two of the routes this middleware
// wraps — create api-key, create webhook — return a live credential
// (trident_... key, whsec_... signing secret) in their 201 body, and that
// body used to sit in Redis in plaintext for the full 24h TTL: readable by
// anything with Redis visibility (a replica, a backup, MONITOR, a
// compromise), not just the original caller.
//
// IDEMPOTENCY_ENCRYPTION_KEY (64 hex chars = 32 bytes) lets an operator pin
// this across restarts and across replicas. That is not optional with more
// than one replica: left unset, each process generates its own random key
// at startup, so a retried create that lands on a different replica than the
// first attempt (or the same replica after a restart) cannot decrypt the
// first attempt's record, falls through to "cache entry unreadable;
// re-executing", and mints a second credential — the exact double-create
// #225 exists to prevent. ValidateIdempotencyEncryptionConfig enforces that
// this is set in production; left unset outside production, a random key is
// generated and a record written by a previous process simply fails to
// decrypt and is treated as a miss.
var idempotencyEncryptionKey = loadOrGenerateIdempotencyKey()

// idempotencyPreviousEncryptionKey, when set via
// IDEMPOTENCY_ENCRYPTION_KEY_PREVIOUS, is tried to decrypt a record that
// idempotencyEncryptionKey can't — so rotating IDEMPOTENCY_ENCRYPTION_KEY
// (move the old value here, set a freshly generated one as the new value)
// does not strand records written under the old key for the rest of their
// TTL. New records are always written under the current key only.
var idempotencyPreviousEncryptionKey = loadIdempotencyKeyFromEnv("IDEMPOTENCY_ENCRYPTION_KEY_PREVIOUS")

func loadOrGenerateIdempotencyKey() []byte {
	if key := loadIdempotencyKeyFromEnv("IDEMPOTENCY_ENCRYPTION_KEY"); key != nil {
		return key
	}
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		// crypto/rand failing is not something this service can recover
		// from safely anywhere else either; panicking at startup is
		// preferable to silently persisting credentials in plaintext.
		panic("idempotency: failed to generate encryption key: " + err.Error())
	}
	return key
}

// loadIdempotencyKeyFromEnv decodes a 32-byte AES-256 key from the named
// env var, or returns nil (with a warning for a present-but-malformed value)
// when it is unset or invalid.
func loadIdempotencyKeyFromEnv(envVar string) []byte {
	raw := os.Getenv(envVar)
	if raw == "" {
		return nil
	}
	key, err := decodeIdempotencyKey(raw)
	if err != nil {
		slog.Warn(envVar+" is not 64 hex characters (32 bytes); ignoring", "err", err)
		return nil
	}
	return key
}

func decodeIdempotencyKey(raw string) ([]byte, error) {
	key, err := hex.DecodeString(raw)
	if err != nil {
		return nil, err
	}
	if len(key) != 32 {
		return nil, fmt.Errorf("key must decode to 32 bytes, got %d", len(key))
	}
	return key, nil
}

// ValidateIdempotencyEncryptionConfig fails startup in production when
// IDEMPOTENCY_ENCRYPTION_KEY is unset or malformed. Call once at process
// startup (after initLogger, alongside ValidateAllowedOrigins) and exit on a
// non-nil error — see idempotencyEncryptionKey's doc comment for why an
// unset key is unsafe with more than one replica.
func ValidateIdempotencyEncryptionConfig() error {
	if os.Getenv("APP_ENV") != "production" {
		return nil
	}
	raw := os.Getenv("IDEMPOTENCY_ENCRYPTION_KEY")
	if raw == "" {
		return errors.New("IDEMPOTENCY_ENCRYPTION_KEY must be set in production: without it, each replica encrypts idempotency records under its own random key and cannot read another replica's (or its own post-restart) records, which can double-create a resource on retry")
	}
	if _, err := decodeIdempotencyKey(raw); err != nil {
		return fmt.Errorf("IDEMPOTENCY_ENCRYPTION_KEY is invalid (%w); it must be 64 hex characters (32 bytes)", err)
	}
	return nil
}

// encryptIdempotencyPayload seals plaintext with AES-256-GCM under
// idempotencyEncryptionKey, so what lands in Redis is ciphertext.
func encryptIdempotencyPayload(plaintext []byte) ([]byte, error) {
	block, err := aes.NewCipher(idempotencyEncryptionKey)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := io.ReadFull(rand.Reader, nonce); err != nil {
		return nil, err
	}
	return gcm.Seal(nonce, nonce, plaintext, nil), nil
}

// decryptIdempotencyPayload reverses encryptIdempotencyPayload, trying
// idempotencyPreviousEncryptionKey (when configured) if the current key
// fails — the overlap window that makes IDEMPOTENCY_ENCRYPTION_KEY rotatable
// without losing in-flight records. An error from both (wrong key and no/
// stale previous key configured, or corruption) is treated by callers as a
// cache miss, not a hard failure.
func decryptIdempotencyPayload(ciphertext []byte) ([]byte, error) {
	plaintext, err := decryptIdempotencyPayloadWithKey(ciphertext, idempotencyEncryptionKey)
	if err == nil {
		return plaintext, nil
	}
	if idempotencyPreviousEncryptionKey != nil {
		if plaintext, prevErr := decryptIdempotencyPayloadWithKey(ciphertext, idempotencyPreviousEncryptionKey); prevErr == nil {
			return plaintext, nil
		}
	}
	return nil, err
}

func decryptIdempotencyPayloadWithKey(ciphertext, key []byte) ([]byte, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	if len(ciphertext) < gcm.NonceSize() {
		return nil, errors.New("idempotency: ciphertext too short")
	}
	nonce, sealed := ciphertext[:gcm.NonceSize()], ciphertext[gcm.NonceSize():]
	return gcm.Open(nil, nonce, sealed, nil)
}

// marshalIdempotencyRecord JSON-encodes rec and seals it with AES-256-GCM
// before it is ever written to Redis (issue #572), so a Redis dump, replica,
// or MONITOR session sees only ciphertext — never the plaintext body two of
// this middleware's routes carry (a freshly minted API key or webhook
// signing secret).
func marshalIdempotencyRecord(rec idempotencyRecord) ([]byte, error) {
	plaintext, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	return encryptIdempotencyPayload(plaintext)
}

// unmarshalIdempotencyRecord reverses marshalIdempotencyRecord. Any failure —
// wrong/rotated key, corruption, or (pre-#572) a plaintext JSON record left
// over from before this change — is returned as an error, and every caller
// treats that as a cache miss rather than surfacing it to the client.
func unmarshalIdempotencyRecord(data []byte) (idempotencyRecord, error) {
	var rec idempotencyRecord
	plaintext, err := decryptIdempotencyPayload(data)
	if err != nil {
		return rec, err
	}
	if err := json.Unmarshal(plaintext, &rec); err != nil {
		return rec, err
	}
	return rec, nil
}

// inFlightTTL bounds how long a reservation survives if the process handling
// it dies mid-request. Short enough that a crash doesn't lock a key out for
// the full 24h record TTL, long enough to cover a slow create.
const inFlightTTL = 60 * time.Second

// idempotencyRedisKey scopes the client-supplied key to the authenticated
// caller. Without the tenant component, the key namespace is entirely
// client-chosen and shared: two tenants picking the same Idempotency-Key
// (a UUID collision is unlikely, but "order-1" is not) with the same request
// body would have the second replay the first's response — which for
// POST /v1/webhooks contains that tenant's plaintext signing secret, and for
// POST /v1/api-keys the plaintext API key.
//
// The id is hashed rather than interpolated so a key id can never be read
// back out of a Redis key name, and so an exotic id cannot break the
// delimiter structure.
func idempotencyRedisKey(ctx context.Context, clientKey string) string {
	tenant := APIKeyIDFromContext(ctx)
	if tenant == "" {
		// Admin-key auth carries no per-key id. Since issue #616, legacy
		// env-var auth has its own tenant value (LegacyEnvKeyID) rather than
		// landing here — kept distinct so two independently-configured
		// legacy deployments sharing the same env-var hash don't collide
		// with an admin-key caller's idempotency keys.
		tenant = "unscoped"
	}
	sum := sha256.Sum256([]byte(tenant + "\x00" + clientKey))
	return "idempotency:" + hex.EncodeToString(sum[:])
}

// Idempotency returns middleware that honours an `Idempotency-Key` header on
// an unsafe POST endpoint (issue #225): a retry carrying the same key and an
// identical request body replays the first attempt's response instead of
// re-executing the handler — so a client retrying after a network blip (but
// whose first request actually succeeded) cannot double-create a resource.
// The same key reused with a *different* body is rejected as a conflict
// rather than either silently executing twice or replaying the wrong
// response.
//
// A request with no Idempotency-Key header passes straight through —
// support is opt-in, matching the issue's "Accept an Idempotency-Key
// header", not a requirement that every POST supply one. It is also a
// no-op when rdb is nil (Redis unavailable), the same fail-open posture
// AuditMiddleware and the auth Redis cache take elsewhere in this service:
// idempotency protection is best-effort, and refusing every create request
// because Redis is down would be a worse outage than the problem it guards
// against.
//
// ttl <= 0 uses DefaultIdempotencyTTL.
func Idempotency(rdb *redis.Client, ttl time.Duration) func(http.Handler) http.Handler {
	if ttl <= 0 {
		ttl = DefaultIdempotencyTTL
	}
	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			key := r.Header.Get("Idempotency-Key")
			if key == "" || rdb == nil {
				next.ServeHTTP(w, r)
				return
			}
			if len(key) > maxIdempotencyKeyLen {
				httputil.WriteErrorCtx(r.Context(), w, http.StatusBadRequest, httputil.INVALID_ARGUMENT,
					"Idempotency-Key exceeds maximum length")
				return
			}

			// The body is needed twice — once here to fingerprint it, once
			// by the real handler — so it is read fully up front and
			// replaced with a fresh reader over the same bytes.
			body, err := io.ReadAll(io.LimitReader(r.Body, maxIdempotencyBodyBytes+1))
			_ = r.Body.Close()
			if err != nil {
				httputil.WriteErrorCtx(r.Context(), w, http.StatusBadRequest, httputil.INVALID_ARGUMENT,
					"failed to read request body")
				return
			}
			if len(body) > maxIdempotencyBodyBytes {
				httputil.WriteErrorCtx(r.Context(), w, http.StatusRequestEntityTooLarge, httputil.PAYLOAD_TOO_LARGE,
					"request body too large")
				return
			}
			r.Body = io.NopCloser(bytes.NewReader(body))

			fingerprint := fingerprintIdempotentRequest(r.Method, r.URL.Path, body)
			redisKey := idempotencyRedisKey(r.Context(), key)

			if cached, err := rdb.Get(r.Context(), redisKey).Result(); err == nil {
				if rec, decErr := unmarshalIdempotencyRecord([]byte(cached)); decErr == nil {
					if rec.InFlight {
						// Another request holding this key is still executing.
						// Replaying nothing and re-executing would double-create,
						// which is the whole failure this middleware exists to
						// prevent, so the retry is told to come back instead.
						if rec.Fingerprint != fingerprint {
							httputil.WriteErrorCtx(r.Context(), w, http.StatusConflict, httputil.CONFLICT,
								"Idempotency-Key was already used with a different request")
							return
						}
						w.Header().Set("Retry-After", "1")
						httputil.WriteErrorCtx(r.Context(), w, http.StatusConflict, httputil.CONFLICT,
							"a request with this Idempotency-Key is still in progress; retry shortly")
						return
					}
					if rec.Fingerprint != fingerprint {
						httputil.WriteErrorCtx(r.Context(), w, http.StatusConflict, httputil.CONFLICT,
							"Idempotency-Key was already used with a different request")
						return
					}
					replayIdempotentResponse(w, rec)
					return
				}
				// A malformed or undecryptable cache entry is treated as a
				// miss: re-executing costs a query, and — when
				// IDEMPOTENCY_ENCRYPTION_KEY is unset — this is also the
				// expected outcome for any record written by a previous
				// process (see loadOrGenerateIdempotencyKey).
				slog.WarnContext(r.Context(), "idempotency: cache entry unreadable; re-executing", "key", key)
			} else if err != redis.Nil {
				slog.WarnContext(r.Context(), "idempotency: redis get failed; proceeding without replay", "err", err)
			}

			// Reserve the key before executing. Without this, N concurrent
			// requests carrying the same key all miss the Get above and all
			// run the handler — N resources created, which is exactly the
			// double-create #225 exists to prevent. SET NX makes exactly one
			// of them the winner; the losers take the InFlight branch above.
			// A failed reservation is not fatal: Redis being down should
			// degrade to today's behaviour, not reject the write.
			reserved := true
			if data, err := marshalIdempotencyRecord(idempotencyRecord{Fingerprint: fingerprint, InFlight: true}); err == nil {
				ok, err := rdb.SetNX(r.Context(), redisKey, data, inFlightTTL).Result()
				if err != nil {
					slog.WarnContext(r.Context(), "idempotency: redis reservation failed; proceeding unreserved", "err", err)
					reserved = false
				} else if !ok {
					// Lost the race between the Get and here. The winner owns
					// the key; tell this caller to retry rather than execute.
					w.Header().Set("Retry-After", "1")
					httputil.WriteErrorCtx(r.Context(), w, http.StatusConflict, httputil.CONFLICT,
						"a request with this Idempotency-Key is still in progress; retry shortly")
					return
				}
			}

			capture := newCaptureWriter()
			next.ServeHTTP(capture, r)
			capture.flushTo(w)

			// Never persist a 5xx. Caching one would pin a transient failure
			// for the full TTL: the client's retry — the exact scenario this
			// feature serves — would replay the 500 instead of succeeding.
			// Releasing the reservation lets that retry through.
			if capture.statusCode >= 500 {
				if reserved {
					if err := rdb.Del(r.Context(), redisKey).Err(); err != nil {
						slog.WarnContext(r.Context(), "idempotency: releasing reservation after 5xx failed", "err", err)
					}
				}
				return
			}

			rec := idempotencyRecord{
				Fingerprint: fingerprint,
				Status:      capture.statusCode,
				Header:      capture.header,
				Body:        capture.body.Bytes(),
			}
			if data, err := marshalIdempotencyRecord(rec); err == nil {
				if err := rdb.Set(r.Context(), redisKey, data, ttl).Err(); err != nil {
					slog.WarnContext(r.Context(), "idempotency: redis set failed; a retry will re-execute", "err", err)
				}
			} else {
				slog.WarnContext(r.Context(), "idempotency: encrypting record failed; a retry will re-execute", "err", err)
			}
		})
	}
}

// fingerprintIdempotentRequest identifies "the same request" for conflict
// detection: method, path, and body must all match a stored record for a
// key reuse to be treated as a legitimate retry rather than a conflicting
// second use of the same key.
func fingerprintIdempotentRequest(method, path string, body []byte) string {
	h := sha256.New()
	h.Write([]byte(method))
	h.Write([]byte{0})
	h.Write([]byte(path))
	h.Write([]byte{0})
	h.Write(body)
	return hex.EncodeToString(h.Sum(nil))
}

// replayIdempotentResponse writes a previously-captured response byte-for-
// byte, so a retried request cannot tell it didn't re-execute the handler.
func replayIdempotentResponse(w http.ResponseWriter, rec idempotencyRecord) {
	for k, vv := range rec.Header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set("Idempotent-Replayed", "true")
	w.WriteHeader(rec.Status)
	_, _ = w.Write(rec.Body)
}

// captureWriter buffers a handler's full response (headers, status, body) in
// memory instead of streaming it, so nothing is sent to the real client
// until the response is known — and can be persisted for replay — in full.
// Safe here because both routes Idempotency wraps return a single small
// JSON object, never a stream.
type captureWriter struct {
	header      http.Header
	statusCode  int
	body        bytes.Buffer
	wroteHeader bool
}

func newCaptureWriter() *captureWriter {
	return &captureWriter{header: make(http.Header), statusCode: http.StatusOK}
}

func (c *captureWriter) Header() http.Header { return c.header }

func (c *captureWriter) WriteHeader(code int) {
	if !c.wroteHeader {
		c.statusCode = code
		c.wroteHeader = true
	}
}

func (c *captureWriter) Write(b []byte) (int, error) {
	if !c.wroteHeader {
		c.WriteHeader(http.StatusOK)
	}
	return c.body.Write(b)
}

// flushTo writes the captured response to the real ResponseWriter, once the
// underlying handler has finished.
func (c *captureWriter) flushTo(w http.ResponseWriter) {
	for k, vv := range c.header {
		for _, v := range vv {
			w.Header().Add(k, v)
		}
	}
	w.WriteHeader(c.statusCode)
	_, _ = w.Write(c.body.Bytes())
}
