package httputil

import (
	"context"
	"encoding/json"
	"net/http"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ErrorCode represents the machine-readable error type
type ErrorCode string

const (
	NOT_FOUND    ErrorCode = "NOT_FOUND"
	UNAUTHORIZED ErrorCode = "UNAUTHORIZED"
	RATE_LIMITED ErrorCode = "RATE_LIMITED"
	// QUOTA_EXCEEDED is returned when a key exhausts its daily or monthly
	// request quota (distinct from the per-second RATE_LIMITED).
	QUOTA_EXCEEDED   ErrorCode = "QUOTA_EXCEEDED"
	INVALID_ARGUMENT ErrorCode = "INVALID_ARGUMENT"
	UNAVAILABLE      ErrorCode = "UNAVAILABLE"
	INTERNAL         ErrorCode = "INTERNAL"
	// PAYLOAD_TOO_LARGE is returned when a request body exceeds the
	// configured http.MaxBytesReader limit (issue #317).
	PAYLOAD_TOO_LARGE ErrorCode = "PAYLOAD_TOO_LARGE"
	// FORBIDDEN is returned when abuse-protection middleware rejects a
	// request outright (e.g. the global concurrency cap shedding load,
	// issue #318) rather than for a normal auth failure.
	FORBIDDEN ErrorCode = "FORBIDDEN"
)

// ErrorDetail is the nested error object required by the OpenAPI ErrorResponse
// schema and parsed by the client SDKs.
type ErrorDetail struct {
	Code      ErrorCode `json:"code"`
	Message   string    `json:"message"`
	RequestID string    `json:"request_id,omitempty"`
}

// ErrorResponse is the standardized JSON error body: {"error":{"code","message"}}.
type ErrorResponse struct {
	Error ErrorDetail `json:"error"`
}

// WriteError writes a standardized JSON error response matching the OpenAPI
// ErrorResponse schema ({"error":{"code","message"}}). It carries no request
// id; prefer WriteErrorCtx from a request-scoped handler so the error envelope
// includes error.request_id.
func WriteError(w http.ResponseWriter, statusCode int, code ErrorCode, message string) {
	writeError(w, statusCode, code, message, "")
}

// WriteErrorCtx writes a standardized JSON error response and populates
// error.request_id from the request id attached to ctx by the RequestID
// middleware, so a client failure can be correlated to server logs and traces.
func WriteErrorCtx(ctx context.Context, w http.ResponseWriter, statusCode int, code ErrorCode, message string) {
	writeError(w, statusCode, code, message, RequestIDFromContext(ctx))
}

func writeError(w http.ResponseWriter, statusCode int, code ErrorCode, message, requestID string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	_ = json.NewEncoder(w).Encode(ErrorResponse{
		Error: ErrorDetail{Code: code, Message: message, RequestID: requestID},
	})
}

// GRPCToHTTP maps a gRPC error to HTTP status code and error code
func GRPCToHTTP(err error) (int, ErrorCode) {
	if err == nil {
		return http.StatusOK, ""
	}

	st, ok := status.FromError(err)
	if !ok {
		return http.StatusInternalServerError, INTERNAL
	}

	switch st.Code() {
	case codes.NotFound:
		return http.StatusNotFound, NOT_FOUND
	case codes.InvalidArgument:
		return http.StatusBadRequest, INVALID_ARGUMENT
	case codes.Unauthenticated:
		return http.StatusUnauthorized, UNAUTHORIZED
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests, RATE_LIMITED
	case codes.DeadlineExceeded:
		// The backend did not answer within the call deadline: a gateway
		// timeout, not an internal fault — clients may retry (issue #227).
		return http.StatusGatewayTimeout, UNAVAILABLE
	case codes.Unavailable:
		// The backend is unreachable or refusing connections: a transient
		// upstream outage, not an internal fault — clients may retry.
		return http.StatusServiceUnavailable, UNAVAILABLE
	default:
		return http.StatusInternalServerError, INTERNAL
	}
}
