package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/Depo-dev/trident/services/api/gen"
	"github.com/Depo-dev/trident/services/api/internal/contracttest"
	"github.com/jackc/pgx/v5"
	"github.com/redis/go-redis/v9"
	"google.golang.org/grpc"
)

// fakeEventsClient implements EventsLister for Ready() tests (package
// handlers, not handlers_test — kept local rather than reusing
// events_test.go's MockEventsClient, which lives in the separate
// handlers_test package and isn't visible here).
type fakeEventsClient struct {
	listEvents func(context.Context, *gen.ListEventsRequest) (*gen.ListEventsResponse, error)
}

func (f *fakeEventsClient) ListEvents(ctx context.Context, in *gen.ListEventsRequest, _ ...grpc.CallOption) (*gen.ListEventsResponse, error) {
	return f.listEvents(ctx, in)
}

// healthMockDB implements DBPool for Ready() tests. When pingErr is set,
// Ping fails and QueryRow is never expected to matter (checkPostgres
// returns before calling it in production code paths that check the ping
// error first — this double doesn't need a real Row in that case).
type healthMockDB struct {
	pingErr    error
	lastLedger *int64
	scanErr    error
}

func (m *healthMockDB) Ping(_ context.Context) error { return m.pingErr }
func (m *healthMockDB) QueryRow(_ context.Context, _ string, _ ...any) pgx.Row {
	return &healthMockRow{m: m}
}
func (m *healthMockDB) Query(_ context.Context, _ string, _ ...any) (pgx.Rows, error) {
	return nil, nil
}

type healthMockRow struct{ m *healthMockDB }

func (r *healthMockRow) Scan(dest ...any) error {
	if r.m.scanErr != nil {
		return r.m.scanErr
	}
	*dest[0].(**int64) = r.m.lastLedger
	return nil
}

// healthyRedis and unhealthyRedis satisfy RedisPinger.
type fakeRedisPinger struct{ err error }

func (f fakeRedisPinger) Ping(ctx context.Context) *redis.StatusCmd {
	return redis.NewStatusResult("PONG", f.err)
}

func healthReadyReq(path string) *http.Request {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	req.URL.Scheme = "http"
	req.URL.Host = "localhost:3000"
	req.Host = "localhost:3000"
	return req
}

// TestHealth_AlwaysReturns200 verifies GET /v1/health is a cheap liveness
// check: no dependencies wired at all, always 200 (issue #243).
func TestHealth_AlwaysReturns200(t *testing.T) {
	req := healthReadyReq("/v1/health")
	rr := httptest.NewRecorder()
	Health().ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", rr.Code)
	}
	var body LivenessResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "ok" {
		t.Errorf("status: want ok, got %q", body.Status)
	}

	doc := contracttest.LoadSpec(t)
	router := contracttest.NewRouter(t, doc)
	contracttest.ValidateResponse(t, router, req, rr.Code, rr.Header(), rr.Body.Bytes())
}

// TestReady_AllHealthy_Returns200 verifies GET /v1/ready reports 200 with
// status ok when Postgres, Redis, and gRPC all succeed (issue #243).
func TestReady_AllHealthy_Returns200(t *testing.T) {
	ledger := int64(42)
	db := &healthMockDB{lastLedger: &ledger}
	rdb := fakeRedisPinger{}
	grpcClient := &fakeEventsClient{
		listEvents: func(_ context.Context, _ *gen.ListEventsRequest) (*gen.ListEventsResponse, error) {
			return &gen.ListEventsResponse{}, nil
		},
	}

	req := healthReadyReq("/v1/ready")
	rr := httptest.NewRecorder()
	Ready(db, rdb, grpcClient).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body.String())
	}
	var body ReadyResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "ok" {
		t.Errorf("status: want ok, got %q", body.Status)
	}
	if body.Checks.Postgres != "ok" || body.Checks.Redis != "ok" || body.Checks.GRPCAPI != "ok" {
		t.Errorf("want all checks ok, got %+v", body.Checks)
	}

	doc := contracttest.LoadSpec(t)
	router := contracttest.NewRouter(t, doc)
	contracttest.ValidateResponse(t, router, req, rr.Code, rr.Header(), rr.Body.Bytes())
}

// TestReady_PostgresDown_Returns503 verifies a Postgres ping failure alone
// degrades the whole readiness check to 503 (issue #243).
func TestReady_PostgresDown_Returns503(t *testing.T) {
	db := &healthMockDB{pingErr: errors.New("connection refused")}
	rdb := fakeRedisPinger{}
	grpcClient := &fakeEventsClient{
		listEvents: func(_ context.Context, _ *gen.ListEventsRequest) (*gen.ListEventsResponse, error) {
			return &gen.ListEventsResponse{}, nil
		},
	}

	req := healthReadyReq("/v1/ready")
	rr := httptest.NewRecorder()
	Ready(db, rdb, grpcClient).ServeHTTP(rr, req)

	assertDegraded(t, rr, "postgres")

	doc := contracttest.LoadSpec(t)
	router := contracttest.NewRouter(t, doc)
	contracttest.ValidateResponse(t, router, req, rr.Code, rr.Header(), rr.Body.Bytes())
}

// TestReady_RedisDown_StaysReady verifies a Redis failure is reported as
// degraded but does not fail readiness: Redis-backed middlewares fail open,
// so a Redis blip must not pull every pod out of the Service (issue #758).
func TestReady_RedisDown_StaysReady(t *testing.T) {
	ledger := int64(1)
	db := &healthMockDB{lastLedger: &ledger}
	rdb := fakeRedisPinger{err: errors.New("dial tcp 10.0.3.7:6379: connection refused")}
	grpcClient := &fakeEventsClient{
		listEvents: func(_ context.Context, _ *gen.ListEventsRequest) (*gen.ListEventsResponse, error) {
			return &gen.ListEventsResponse{}, nil
		},
	}

	req := healthReadyReq("/v1/ready")
	rr := httptest.NewRecorder()
	Ready(db, rdb, grpcClient).ServeHTTP(rr, req)

	if rr.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rr.Code, rr.Body.String())
	}
	var body ReadyResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "degraded" {
		t.Errorf("status: want degraded, got %q", body.Status)
	}
	if body.Checks.Redis != "degraded" || body.Checks.Postgres != "ok" || body.Checks.GRPCAPI != "ok" {
		t.Errorf("unexpected checks: %+v", body.Checks)
	}

	doc := contracttest.LoadSpec(t)
	router := contracttest.NewRouter(t, doc)
	contracttest.ValidateResponse(t, router, req, rr.Code, rr.Header(), rr.Body.Bytes())
}

// TestReady_ErrorsNotLeaked verifies hostnames, ports and driver messages
// from failing dependencies never appear in the public body (issue #758).
func TestReady_ErrorsNotLeaked(t *testing.T) {
	db := &healthMockDB{pingErr: errors.New("dial tcp pgbouncer.internal:6432: connection refused")}
	rdb := fakeRedisPinger{err: errors.New("dial tcp redis.internal:6379: i/o timeout")}
	grpcClient := &fakeEventsClient{
		listEvents: func(_ context.Context, _ *gen.ListEventsRequest) (*gen.ListEventsResponse, error) {
			return nil, errors.New("rpc error: dial tcp indexer.internal:50051: connect: refused")
		},
	}

	rr := httptest.NewRecorder()
	Ready(db, rdb, grpcClient).ServeHTTP(rr, healthReadyReq("/v1/ready"))

	body := rr.Body.String()
	for _, leak := range []string{"internal", "dial tcp", "6432", "6379", "50051", "refused", "timeout", "rpc error"} {
		if strings.Contains(body, leak) {
			t.Errorf("body leaks %q: %s", leak, body)
		}
	}
}

// TestReady_GRPCDown_Returns503 verifies a gRPC backend failure alone
// degrades the whole readiness check to 503 (issue #243).
func TestReady_GRPCDown_Returns503(t *testing.T) {
	ledger := int64(1)
	db := &healthMockDB{lastLedger: &ledger}
	rdb := fakeRedisPinger{}
	grpcClient := &fakeEventsClient{
		listEvents: func(_ context.Context, _ *gen.ListEventsRequest) (*gen.ListEventsResponse, error) {
			return nil, errors.New("backend unreachable")
		},
	}

	req := healthReadyReq("/v1/ready")
	rr := httptest.NewRecorder()
	Ready(db, rdb, grpcClient).ServeHTTP(rr, req)

	assertDegraded(t, rr, "grpc_api")

	doc := contracttest.LoadSpec(t)
	router := contracttest.NewRouter(t, doc)
	contracttest.ValidateResponse(t, router, req, rr.Code, rr.Header(), rr.Body.Bytes())
}

// TestReady_NilDependencies_Returns503 verifies unconfigured dependencies
// (nil db/redis/grpc, e.g. at cold start before DATABASE_URL connects) are
// treated as failures, not silently skipped (issue #243).
func TestReady_NilDependencies_Returns503(t *testing.T) {
	req := healthReadyReq("/v1/ready")
	rr := httptest.NewRecorder()
	Ready(nil, nil, nil).ServeHTTP(rr, req)

	assertDegraded(t, rr, "postgres")

	var body ReadyResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Checks.Redis == "ok" || body.Checks.GRPCAPI == "ok" {
		t.Errorf("want redis and grpc_api also reported as failing, got %+v", body.Checks)
	}
}

func assertDegraded(t *testing.T, rr *httptest.ResponseRecorder, failingCheck string) {
	t.Helper()
	if rr.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d: %s", rr.Code, rr.Body.String())
	}
	var body ReadyResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode body: %v", err)
	}
	if body.Status != "degraded" {
		t.Errorf("status: want degraded, got %q", body.Status)
	}
	var got string
	switch failingCheck {
	case "postgres":
		got = body.Checks.Postgres
	case "redis":
		got = body.Checks.Redis
	case "grpc_api":
		got = body.Checks.GRPCAPI
	}
	if got == "ok" || got == "" {
		t.Errorf("checks.%s: want a failure reason, got %q", failingCheck, got)
	}
}
