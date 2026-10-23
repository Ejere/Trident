package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"sort"
	"strings"
	"testing"

	"github.com/Depo-dev/trident/services/api/middleware"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

const validSchemaContractID = "CA7QYNF7SOWQ3GLR2BGMZEHXAVIRZA4KVWLTJJFC7MGXUA74P7UJVSGZ"

type mockSchemaDB struct {
	codeHash          string
	tokenEventTypes   []string
	observedTopics    []string
	sampleDataByEvent map[string][]byte
	execCalls         int
	// tokenEventsNetworkArgs records the network argument passed to every
	// "FROM token_events" query, so a test can assert the handler queries
	// the caller's actual network rather than always "testnet" (issue #595
	// follow-up: token_events.network was never bound on insert, so the
	// mainnet view silently had nothing to return even though the query
	// here was always correctly scoped).
	tokenEventsNetworkArgs []string
}

func (m *mockSchemaDB) QueryRow(_ context.Context, sql string, args ...any) pgx.Row {
	switch {
	case strings.Contains(sql, "FROM contract_verification"):
		if m.codeHash == "" {
			return mockSchemaRow{err: pgx.ErrNoRows}
		}
		return mockSchemaRow{values: []any{m.codeHash}}
	case strings.Contains(sql, "SELECT data"):
		eventName, _ := args[2].(string)
		payload, ok := m.sampleDataByEvent[eventName]
		if !ok {
			return mockSchemaRow{err: pgx.ErrNoRows}
		}
		return mockSchemaRow{values: []any{payload}}
	default:
		return mockSchemaRow{err: errors.New("unexpected QueryRow SQL")}
	}
}

func (m *mockSchemaDB) Query(_ context.Context, sql string, args ...any) (pgx.Rows, error) {
	switch {
	case strings.Contains(sql, "FROM token_events"):
		if len(args) > 1 {
			if network, ok := args[1].(string); ok {
				m.tokenEventsNetworkArgs = append(m.tokenEventsNetworkArgs, network)
			}
		}
		rows := make([][]any, 0, len(m.tokenEventTypes))
		for _, eventName := range m.tokenEventTypes {
			rows = append(rows, []any{eventName})
		}
		return &mockSchemaRows{rows: rows}, nil
	case strings.Contains(sql, "FROM soroban_events"):
		rows := make([][]any, 0, len(m.observedTopics))
		for _, eventName := range m.observedTopics {
			rows = append(rows, []any{eventName})
		}
		return &mockSchemaRows{rows: rows}, nil
	default:
		return nil, errors.New("unexpected Query SQL")
	}
}

func (m *mockSchemaDB) Exec(_ context.Context, _ string, _ ...any) (pgconn.CommandTag, error) {
	m.execCalls++
	return pgconn.CommandTag{}, nil
}

type mockSchemaRow struct {
	values []any
	err    error
}

func (r mockSchemaRow) Scan(dest ...any) error {
	if r.err != nil {
		return r.err
	}
	if len(dest) != len(r.values) {
		return errors.New("unexpected Scan arity")
	}
	for i, value := range r.values {
		switch target := dest[i].(type) {
		case *string:
			*target = value.(string)
		case *[]byte:
			*target = append((*target)[:0], value.([]byte)...)
		default:
			return errors.New("unsupported scan target")
		}
	}
	return nil
}

type mockSchemaRows struct {
	rows   [][]any
	idx    int
	closed bool
	err    error
}

func (r *mockSchemaRows) Close()                        { r.closed = true }
func (r *mockSchemaRows) Err() error                    { return r.err }
func (r *mockSchemaRows) CommandTag() pgconn.CommandTag { return pgconn.CommandTag{} }
func (r *mockSchemaRows) FieldDescriptions() []pgconn.FieldDescription {
	return nil
}
func (r *mockSchemaRows) Next() bool {
	if r.idx >= len(r.rows) {
		r.closed = true
		return false
	}
	r.idx++
	return true
}
func (r *mockSchemaRows) Scan(dest ...any) error {
	if r.idx == 0 || r.idx > len(r.rows) {
		return errors.New("Scan called before Next")
	}
	row := r.rows[r.idx-1]
	if len(dest) != len(row) {
		return errors.New("unexpected Scan arity")
	}
	for i, value := range row {
		switch target := dest[i].(type) {
		case *string:
			*target = value.(string)
		default:
			return errors.New("unsupported scan target")
		}
	}
	return nil
}
func (r *mockSchemaRows) Values() ([]any, error) { return nil, errors.New("not implemented") }
func (r *mockSchemaRows) RawValues() [][]byte    { return nil }
func (r *mockSchemaRows) Conn() *pgx.Conn        { return nil }

func TestContractEventSchemas_InvalidContractID_Returns400(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/contracts/not-a-contract/events/schema", nil)
	req.SetPathValue("id", "not-a-contract")
	rec := httptest.NewRecorder()

	ContractEventSchemas(&mockSchemaDB{}).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest {
		t.Fatalf("want 400, got %d", rec.Code)
	}
}

func TestContractEventSchemas_NoDB_Returns503(t *testing.T) {
	req := httptest.NewRequest(http.MethodGet, "/v1/contracts/"+validSchemaContractID+"/events/schema", nil)
	req.SetPathValue("id", validSchemaContractID)
	rec := httptest.NewRecorder()

	ContractEventSchemas(nil).ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("want 503, got %d", rec.Code)
	}
}

func TestContractEventSchemas_ReturnsObservedSchemasAndPersistsRegistry(t *testing.T) {
	db := &mockSchemaDB{
		codeHash:        "wasm-hash-123",
		tokenEventTypes: []string{"approve", "transfer"},
		observedTopics:  []string{"approve", "custom_event", "set_authorized", "transfer"},
		sampleDataByEvent: map[string][]byte{
			"custom_event": []byte(`{"enabled":true,"message":"hello"}`),
		},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/contracts/"+validSchemaContractID+"/events/schema", nil)
	req.SetPathValue("id", validSchemaContractID)
	rec := httptest.NewRecorder()

	ContractEventSchemas(db).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp ContractEventSchemaResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.CodeHash != "wasm-hash-123" {
		t.Fatalf("code_hash: got %q", resp.CodeHash)
	}
	if resp.ContractID != validSchemaContractID {
		t.Fatalf("contract_id: got %q", resp.ContractID)
	}
	if db.execCalls != 5 {
		t.Fatalf("expected 5 persistence execs (1 delete + 4 upserts), got %d", db.execCalls)
	}

	got := make(map[string][]ContractEventFieldSchema, len(resp.Events))
	for _, event := range resp.Events {
		got[event.EventName] = event.Fields
	}

	if len(got) != 4 {
		t.Fatalf("expected 4 event schemas, got %d", len(got))
	}

	assertFieldNames := func(eventName string, want []string) {
		t.Helper()
		fields := got[eventName]
		names := make([]string, 0, len(fields))
		for _, field := range fields {
			names = append(names, field.Name)
		}
		sort.Strings(names)
		sort.Strings(want)
		if strings.Join(names, ",") != strings.Join(want, ",") {
			t.Fatalf("%s fields: got %v want %v", eventName, names, want)
		}
	}

	assertFieldNames("approve", []string{"amount", "expiration_ledger", "from", "spender"})
	assertFieldNames("transfer", []string{"amount", "from", "to"})
	assertFieldNames("set_authorized", []string{"admin", "authorize", "id"})
	assertFieldNames("custom_event", []string{"enabled", "message"})
}

// TestContractEventSchemas_MainnetKey_QueriesMainnetTokenEvents guards
// against the regression this fixes: token_events.network was never bound
// on insert, so every row defaulted to 'testnet' and a mainnet request here
// would scope its token_events query to "mainnet" and get nothing back. The
// handler's query scoping was always correct — this asserts it stays that
// way and returns the token event types for the requested network.
func TestContractEventSchemas_MainnetKey_QueriesMainnetTokenEvents(t *testing.T) {
	db := &mockSchemaDB{
		codeHash:        "wasm-hash-456",
		tokenEventTypes: []string{"mint", "transfer"},
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/contracts/"+validSchemaContractID+"/events/schema", nil)
	req.SetPathValue("id", validSchemaContractID)
	req = req.WithContext(middleware.WithNetwork(req.Context(), "mainnet"))
	rec := httptest.NewRecorder()

	ContractEventSchemas(db).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", rec.Code, rec.Body.String())
	}

	var resp ContractEventSchemaResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.Network != "mainnet" {
		t.Fatalf("network: got %q, want mainnet", resp.Network)
	}

	for _, network := range db.tokenEventsNetworkArgs {
		if network != "mainnet" {
			t.Fatalf("token_events query scoped to %q, want mainnet", network)
		}
	}
	if len(db.tokenEventsNetworkArgs) == 0 {
		t.Fatal("expected the token_events query to run with a network argument")
	}

	gotTypes := make(map[string]bool)
	for _, event := range resp.Events {
		gotTypes[event.EventName] = true
	}
	if !gotTypes["mint"] || !gotTypes["transfer"] {
		t.Fatalf("expected mint and transfer token event types for mainnet, got %v", resp.Events)
	}
}
