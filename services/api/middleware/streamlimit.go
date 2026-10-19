package middleware

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sync"
	"sync/atomic"
	"time"

	"github.com/Depo-dev/trident/services/api/internal/httputil"
	"github.com/Depo-dev/trident/services/api/internal/metrics"
	"github.com/redis/go-redis/v9"
)

// streamLease is how long a stream slot is held without a refresh. Open
// streams refresh at a third of this, so a replica that dies without releasing
// frees its slots within one lease.
const streamLease = 90 * time.Second

// StreamStore tracks the open streams of an identity. Implementations must be
// safe for concurrent use; the Redis implementation is shared across replicas.
type StreamStore interface {
	// Acquire claims a slot for id unless limit slots are already held.
	Acquire(ctx context.Context, key, id string, limit int, lease time.Duration) (bool, error)
	Refresh(ctx context.Context, key, id string, lease time.Duration) error
	Release(ctx context.Context, key, id string) error
}

// StreamLimitConfig configures NewStreamLimiter.
type StreamLimitConfig struct {
	Store StreamStore
	// TierLimits maps tier -> max concurrent streams per key. Defaults come
	// from STREAM_LIMIT_<TIER> when nil.
	TierLimits map[string]int
	// MaxPerProcess caps open streams on this process across all keys
	// (0 = no cap).
	MaxPerProcess int
	Cache         *TierCache
	DB            TierDB
}

func defaultStreamLimits() map[string]int {
	pro := envInt("STREAM_LIMIT_PRO", 20)
	return map[string]int{
		"free":     envInt("STREAM_LIMIT_FREE", 2),
		"pro":      pro,
		"standard": pro,
		"internal": envInt("STREAM_LIMIT_INTERNAL", 200),
	}
}

// NewStreamLimiter returns middleware limiting concurrent long-lived streams
// (WebSocket, GraphQL subscriptions, SSE) per API key by tier, and optionally
// per process. It fails open if the store errors. Requests without an API key
// header (the GraphQL socket authenticates in-band) are limited per client IP
// at the free-tier limit.
func NewStreamLimiter(cfg StreamLimitConfig) func(http.Handler) http.Handler {
	if cfg.TierLimits == nil {
		cfg.TierLimits = defaultStreamLimits()
	}
	if cfg.Cache == nil {
		cfg.Cache = NewTierCache()
	}
	var open atomic.Int64

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if cfg.MaxPerProcess > 0 {
				if open.Add(1) > int64(cfg.MaxPerProcess) {
					open.Add(-1)
					rejectStream(w, r, "server stream capacity reached; retry shortly")
					return
				}
				defer open.Add(-1)
			}
			if cfg.Store == nil {
				next.ServeHTTP(w, r)
				return
			}

			identity, limit := streamIdentity(r, cfg)
			storeKey := "streams:" + identity
			id := newStreamID()

			ok, err := cfg.Store.Acquire(r.Context(), storeKey, id, limit, streamLease)
			if err != nil {
				slog.Warn("stream limit check failed; failing open", "err", err)
				next.ServeHTTP(w, r)
				return
			}
			if !ok {
				rejectStream(w, r, fmt.Sprintf("concurrent stream limit reached (%d)", limit))
				return
			}

			stop := make(chan struct{})
			go func() {
				t := time.NewTicker(streamLease / 3)
				defer t.Stop()
				for {
					select {
					case <-stop:
						return
					case <-t.C:
						if err := cfg.Store.Refresh(context.Background(), storeKey, id, streamLease); err != nil {
							slog.Warn("stream lease refresh failed", "err", err)
						}
					}
				}
			}()
			defer func() {
				close(stop)
				relCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
				defer cancel()
				if err := cfg.Store.Release(relCtx, storeKey, id); err != nil {
					slog.Warn("stream slot release failed", "err", err)
				}
			}()

			next.ServeHTTP(w, r)
		})
	}
}

func rejectStream(w http.ResponseWriter, r *http.Request, msg string) {
	rlRejected.Add(1)
	metrics.RateLimitRejectionsTotal.WithLabelValues("streams").Inc()
	w.Header().Set("Retry-After", "5")
	httputil.WriteErrorCtx(r.Context(), w, http.StatusTooManyRequests, httputil.RATE_LIMITED, msg)
}

func streamIdentity(r *http.Request, cfg StreamLimitConfig) (string, int) {
	free := cfg.TierLimits["free"]
	key := r.Header.Get("X-API-Key")
	if key == "" {
		host, _, err := net.SplitHostPort(r.RemoteAddr)
		if err != nil {
			host = r.RemoteAddr
		}
		return "ip:" + host, free
	}
	tier := cfg.Cache.resolve(r.Context(), key, cfg.DB)
	limit, ok := cfg.TierLimits[tier]
	if !ok {
		limit = free
	}
	return hashKey(key), limit
}

func newStreamID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return hex.EncodeToString(b)
}

// ---------------------------------------------------------------------------
// In-memory store (single process / tests)
// ---------------------------------------------------------------------------

type memoryStreamStore struct {
	mu   sync.Mutex
	held map[string]map[string]time.Time
}

// NewMemoryStreamStore returns a per-process StreamStore.
func NewMemoryStreamStore() StreamStore {
	return &memoryStreamStore{held: map[string]map[string]time.Time{}}
}

func (m *memoryStreamStore) Acquire(_ context.Context, key, id string, limit int, lease time.Duration) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	now := time.Now()
	slots := m.held[key]
	if slots == nil {
		slots = map[string]time.Time{}
		m.held[key] = slots
	}
	for k, exp := range slots {
		if now.After(exp) {
			delete(slots, k)
		}
	}
	if len(slots) >= limit {
		return false, nil
	}
	slots[id] = now.Add(lease)
	return true, nil
}

func (m *memoryStreamStore) Refresh(_ context.Context, key, id string, lease time.Duration) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slots := m.held[key]; slots != nil {
		if _, ok := slots[id]; ok {
			slots[id] = time.Now().Add(lease)
		}
	}
	return nil
}

func (m *memoryStreamStore) Release(_ context.Context, key, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if slots := m.held[key]; slots != nil {
		delete(slots, id)
		if len(slots) == 0 {
			delete(m.held, key)
		}
	}
	return nil
}

// ---------------------------------------------------------------------------
// Redis store (shared across replicas)
// ---------------------------------------------------------------------------

const streamAcquireLua = `
redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, ARGV[1])
if redis.call('ZCARD', KEYS[1]) >= tonumber(ARGV[3]) then return 0 end
redis.call('ZADD', KEYS[1], ARGV[1] + ARGV[2], ARGV[4])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`

const streamRefreshLua = `
redis.call('ZADD', KEYS[1], 'XX', ARGV[1] + ARGV[2], ARGV[3])
redis.call('PEXPIRE', KEYS[1], ARGV[2])
return 1
`

type redisStreamStore struct{ rc *redis.Client }

// NewRedisStreamStore returns a StreamStore whose counts hold across replicas.
// Each slot is a sorted-set member scored by its lease expiry.
func NewRedisStreamStore(rc *redis.Client) StreamStore { return &redisStreamStore{rc: rc} }

func (s *redisStreamStore) Acquire(ctx context.Context, key, id string, limit int, lease time.Duration) (bool, error) {
	n, err := s.rc.Eval(ctx, streamAcquireLua, []string{key},
		time.Now().UnixMilli(), lease.Milliseconds(), limit, id).Int64()
	if err != nil {
		return false, fmt.Errorf("redis stream acquire: %w", err)
	}
	return n == 1, nil
}

func (s *redisStreamStore) Refresh(ctx context.Context, key, id string, lease time.Duration) error {
	return s.rc.Eval(ctx, streamRefreshLua, []string{key},
		time.Now().UnixMilli(), lease.Milliseconds(), id).Err()
}

func (s *redisStreamStore) Release(ctx context.Context, key, id string) error {
	return s.rc.ZRem(ctx, key, id).Err()
}
