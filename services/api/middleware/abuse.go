package middleware

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/Depo-dev/trident/services/api/internal/httputil"
	"github.com/Depo-dev/trident/services/api/internal/metrics"
	"github.com/redis/go-redis/v9"
)

// ---------------------------------------------------------------------------
// Rejection metrics, split by reason (issue #318). Follows the same
// package-level atomic-counter pattern as rlAllowed/rlRejected in
// ratelimit.go, exposed to callers (handlers.MetricsHandler) via
// AbuseMetrics, the same way RateLimitMetrics() is.
// ---------------------------------------------------------------------------

var (
	perIPAllowed, perIPRejected   atomic.Int64
	globalAllowed, globalRejected atomic.Int64
)

// AbuseMetrics returns rejection counters for the per-IP limiter and the
// global concurrency cap, since startup. Per-key counters are already
// exposed by RateLimitMetrics().
func AbuseMetrics() (perIPAllowedN, perIPRejectedN, globalAllowedN, globalRejectedN int64) {
	return perIPAllowed.Load(), perIPRejected.Load(), globalAllowed.Load(), globalRejected.Load()
}

// ---------------------------------------------------------------------------
// Per-IP rate limit (pre-auth)
// ---------------------------------------------------------------------------

// PerIPRateLimitConfig configures PerIPRateLimit.
type PerIPRateLimitConfig struct {
	Redis *redis.Client
	// RPS and Window define the sliding-window limit applied per client IP.
	RPS    int
	Window time.Duration
	// TrustProxyHeaders, when true, resolves the client IP from
	// X-Forwarded-For / Fly-Client-IP instead of r.RemoteAddr, subject to
	// TrustedProxyCIDRs below. See the trust-assumption comment on
	// trustedClientIP before enabling this in an environment where
	// untrusted clients can reach the API directly.
	TrustProxyHeaders bool
	// TrustedProxyCIDRs lists the CIDRs of proxies allowed to be walked past
	// when resolving X-Forwarded-For (issue #779). r.RemoteAddr (the actual
	// TCP peer) must itself be inside one of these CIDRs, or the header is
	// ignored entirely and r.RemoteAddr is used — this stops a client that
	// connects directly (bypassing the proxy) from forging XFF. When empty
	// and TrustProxyHeaders is true, every hop is trusted (legacy
	// last-hop-only behaviour); set this whenever the real proxy's egress
	// range is known.
	TrustedProxyCIDRs []*net.IPNet
	// SliderFn overrides the Redis sliding window. Set in tests. Defaults to
	// the same redisSlider helper TieredRateLimit uses.
	SliderFn func(ctx context.Context, key string, limit, windowMs int64) (allowed bool, count int64, err error)
}

// trustedClientIP resolves the client IP to rate-limit on.
//
// Trust assumption (issue #779, superseding #318's single-last-hop
// heuristic): a request may pass through zero or more trusted proxies
// before reaching this process. r.RemoteAddr is always the immediate TCP
// peer, which may itself be a proxy. When cfg.TrustProxyHeaders is true:
//
//   - Fly-Client-IP, when present, is used directly — Fly's edge proxy sets
//     this to the real client IP and it cannot be set by the client itself
//     (Fly strips/overwrites it at the edge), so no CIDR check is needed.
//   - Otherwise X-Forwarded-For is walked from the RIGHT (the hop nearest
//     this process, e.g. nginx's `proxy_add_x_forwarded_for`, which
//     *appends*) toward the left, skipping each hop that is itself inside
//     cfg.TrustedProxyCIDRs, and the first hop NOT in that set is the
//     client. If r.RemoteAddr is not itself inside a trusted CIDR (i.e. the
//     immediate peer is not a known proxy), XFF is ignored outright — a
//     client connecting directly cannot forge a trusted-looking chain.
//   - If TrustedProxyCIDRs is empty, only the last XFF hop is trusted (the
//     original #318 behaviour), for deployments that haven't configured
//     explicit CIDRs yet.
//
// When TrustProxyHeaders is false (the default), r.RemoteAddr — which the
// Go runtime itself sets from the TCP peer and cannot be spoofed by a
// header — is used, which is safe in any deployment but attributes all
// traffic arriving through a proxy to the proxy's own IP.
func trustedClientIP(r *http.Request, trustProxyHeaders bool, trustedCIDRs []*net.IPNet) string {
	remoteHost, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		remoteHost = r.RemoteAddr
	}

	if !trustProxyHeaders {
		return remoteHost
	}

	if flyIP := strings.TrimSpace(r.Header.Get("Fly-Client-IP")); flyIP != "" {
		return flyIP
	}

	xff := r.Header.Get("X-Forwarded-For")
	if xff == "" {
		return remoteHost
	}

	if len(trustedCIDRs) == 0 {
		// Legacy behaviour: trust only the last hop unconditionally.
		parts := strings.Split(xff, ",")
		last := strings.TrimSpace(parts[len(parts)-1])
		if last != "" {
			return last
		}
		return remoteHost
	}

	// The immediate peer itself must be a known proxy, or a direct,
	// non-proxied client could simply set XFF to whatever it wants.
	if remoteIP := net.ParseIP(remoteHost); remoteIP == nil || !ipInCIDRs(remoteIP, trustedCIDRs) {
		return remoteHost
	}

	hops := strings.Split(xff, ",")
	for i := len(hops) - 1; i >= 0; i-- {
		hop := strings.TrimSpace(hops[i])
		if hop == "" {
			continue
		}
		hopIP := net.ParseIP(hop)
		if hopIP == nil {
			// Malformed hop; stop walking rather than trust a non-IP value.
			break
		}
		if !ipInCIDRs(hopIP, trustedCIDRs) {
			return hop
		}
	}
	// Every hop (and the peer) was inside the trusted set — no client IP
	// could be isolated, so fall back to the nearest hop rather than the
	// proxy's own address disappearing into an "unknown" bucket.
	return remoteHost
}

func ipInCIDRs(ip net.IP, cidrs []*net.IPNet) bool {
	for _, c := range cidrs {
		if c.Contains(ip) {
			return true
		}
	}
	return false
}

// parseTrustedProxyCIDRs parses a comma-separated list of CIDRs (e.g.
// "10.0.0.0/8,172.16.0.0/12"). Invalid entries are skipped with a warning
// rather than failing startup, since a typo here should degrade to the safe
// "ignore XFF" behavior, not crash the process.
func parseTrustedProxyCIDRs(raw string) []*net.IPNet {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	var out []*net.IPNet
	for _, part := range strings.Split(raw, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		_, cidr, err := net.ParseCIDR(part)
		if err != nil {
			slog.Warn("TRUSTED_PROXY_CIDRS: skipping invalid CIDR", "value", part, "err", err)
			continue
		}
		out = append(out, cidr)
	}
	return out
}

// publicPathPrefixes lists the path prefixes PerIPRateLimit applies to. It
// intentionally does NOT cover /internal. /metrics is not served on the public
// listener at all — it lives on the internal METRICS_PORT server only (see
// docs/threat-model.md).
var publicPathPrefixes = []string{
	"/v1/",
	"/ws",
	"/graphql",
}

func isPublicPath(path string) bool {
	for _, p := range publicPathPrefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

// PerIPRateLimit enforces a coarse, pre-auth sliding-window rate limit keyed
// on the resolved client IP (issue #318). It runs BEFORE middleware.NewDBAuth
// in the chain (see main.go) so a single source flooding the auth path itself
// — before any per-key limit from TieredRateLimit ever engages — is
// contained. Only applies to public paths (isPublicPath); operational
// endpoints like /internal/status are unaffected.
//
// Fail-open on Redis errors, matching TieredRateLimit's policy: availability
// over strict enforcement during a Redis outage.
func PerIPRateLimit(cfg PerIPRateLimitConfig) func(http.Handler) http.Handler {
	slide := cfg.SliderFn
	if slide == nil {
		if cfg.Redis != nil {
			slide = redisSlider(cfg.Redis)
		} else {
			slide = func(_ context.Context, _ string, _, _ int64) (bool, int64, error) {
				return true, 0, nil
			}
		}
	}
	rps := cfg.RPS
	if rps <= 0 {
		rps = 1
	}
	window := cfg.Window
	if window <= 0 {
		window = time.Second
	}

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			if !isPublicPath(r.URL.Path) {
				next.ServeHTTP(w, r)
				return
			}

			ip := trustedClientIP(r, cfg.TrustProxyHeaders, cfg.TrustedProxyCIDRs)
			redisKey := fmt.Sprintf("ratelimit:ip:%s", ip)
			windowMs := int64(window / time.Millisecond)

			allowed, _, err := slide(r.Context(), redisKey, int64(rps), windowMs)
			if err != nil {
				slog.Warn("per-IP rate limit check failed; failing open", "err", err)
				perIPAllowed.Add(1)
				next.ServeHTTP(w, r)
				return
			}

			if !allowed {
				perIPRejected.Add(1)
				metrics.RateLimitRejectionsTotal.WithLabelValues("per_ip").Inc()
				retryAfter := int64(window.Seconds())
				if retryAfter < 1 {
					retryAfter = 1
				}
				w.Header().Set("Retry-After", strconv.FormatInt(retryAfter, 10))
				httputil.WriteErrorCtx(r.Context(), w, http.StatusTooManyRequests, httputil.RATE_LIMITED, "too many requests from this IP")
				return
			}

			perIPAllowed.Add(1)
			next.ServeHTTP(w, r)
		})
	}
}

// NewPerIPRateLimitFromEnv constructs PerIPRateLimit from env vars, following
// the TieredRateLimit / NewTimeoutFromEnv pattern:
//
//   - PER_IP_RATE_LIMIT_RPS (default 20)
//   - PER_IP_RATE_LIMIT_WINDOW_MS (default 1000)
//   - TRUSTED_PROXY_ENABLED ("true" to resolve the client IP from
//     Fly-Client-IP / X-Forwarded-For instead of the raw TCP peer — see the
//     trust-assumption comment on trustedClientIP)
//   - TRUSTED_PROXY_CIDRS (issue #779): comma-separated CIDRs of the proxies
//     allowed to be walked past in X-Forwarded-For. Required to get a real
//     per-client bucket behind a chain of proxies (e.g. Helm's
//     LoadBalancer -> nginx); left unset, only the single last XFF hop is
//     trusted, which is still correct behind exactly one proxy hop.
func NewPerIPRateLimitFromEnv(redisClient *redis.Client) func(http.Handler) http.Handler {
	rps := envInt("PER_IP_RATE_LIMIT_RPS", 20)
	windowMs := envInt("PER_IP_RATE_LIMIT_WINDOW_MS", 1000)
	trustProxy := os.Getenv("TRUSTED_PROXY_ENABLED") == "true"
	trustedCIDRs := parseTrustedProxyCIDRs(os.Getenv("TRUSTED_PROXY_CIDRS"))

	return PerIPRateLimit(PerIPRateLimitConfig{
		Redis:             redisClient,
		RPS:               rps,
		Window:            time.Duration(windowMs) * time.Millisecond,
		TrustProxyHeaders: trustProxy,
		TrustedProxyCIDRs: trustedCIDRs,
	})
}

// ---------------------------------------------------------------------------
// Global concurrency / burst cap
// ---------------------------------------------------------------------------

var inFlight atomic.Int64

// InFlightRequests returns the current number of requests being served by
// GlobalConcurrencyLimit's handler, for observability.
func InFlightRequests() int64 { return inFlight.Load() }

// GlobalConcurrencyLimit sheds load once more than maxInFlight requests are
// concurrently in-flight, returning 503 with Retry-After rather than letting
// the process degrade under an attack or traffic spike (issue #318). It is
// deliberately simple — a single atomic counter, no queueing — and is meant
// to run as early/outermost in the middleware chain as possible, before any
// other work (auth lookups, rate-limit Redis calls, etc.) happens for a
// request that is going to be shed anyway.
func GlobalConcurrencyLimit(maxInFlight int) func(http.Handler) http.Handler {
	if maxInFlight <= 0 {
		maxInFlight = 1
	}
	limit := int64(maxInFlight)

	return func(next http.Handler) http.Handler {
		return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			n := inFlight.Add(1)
			defer inFlight.Add(-1)

			if n > limit {
				globalRejected.Add(1)
				metrics.RateLimitRejectionsTotal.WithLabelValues("global_concurrency").Inc()
				w.Header().Set("Retry-After", "1")
				httputil.WriteErrorCtx(r.Context(), w, http.StatusServiceUnavailable, httputil.UNAVAILABLE, "server is shedding load; try again shortly")
				return
			}

			globalAllowed.Add(1)
			next.ServeHTTP(w, r)
		})
	}
}

// NewGlobalConcurrencyLimitFromEnv reads MAX_IN_FLIGHT_REQUESTS (default
// 500) and returns configured GlobalConcurrencyLimit middleware.
func NewGlobalConcurrencyLimitFromEnv() func(http.Handler) http.Handler {
	return GlobalConcurrencyLimit(envInt("MAX_IN_FLIGHT_REQUESTS", 500))
}
