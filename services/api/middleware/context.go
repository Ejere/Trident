package middleware

import "context"

type contextKey string

const (
	contextKeyAPIKeyID contextKey = "api_key_id"
	contextKeyNetwork  contextKey = "network"
)

// APIKeyIDFromContext returns the authenticated API key UUID, or empty string
// when the request was authenticated via the legacy env-var path.
func APIKeyIDFromContext(ctx context.Context) string {
	v, _ := ctx.Value(contextKeyAPIKeyID).(string)
	return v
}

// WithAPIKeyID attaches an authenticated API key id to ctx for
// APIKeyIDFromContext to retrieve downstream. Set by NewDBAuth on successful
// DB-backed authentication; exported so tests can simulate an authenticated
// request without going through the full auth middleware.
func WithAPIKeyID(ctx context.Context, id string) context.Context {
	return context.WithValue(ctx, contextKeyAPIKeyID, id)
}

// NetworkFromContext returns the network associated with the authenticated API
// key. Defaults to "testnet" when the request is unauthenticated or the key
// has no network set.
func NetworkFromContext(ctx context.Context) string {
	if v, ok := ctx.Value(contextKeyNetwork).(string); ok && v != "" {
		return v
	}
	return "testnet"
}

// WithNetwork attaches an authenticated key's network to ctx for
// NetworkFromContext. Set by NewDBAuth; exported so tests can simulate it.
func WithNetwork(ctx context.Context, network string) context.Context {
	return context.WithValue(ctx, contextKeyNetwork, network)
}
