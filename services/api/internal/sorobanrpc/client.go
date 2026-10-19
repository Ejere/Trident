// Package sorobanrpc provides a minimal JSON-RPC 2.0 client for the Soroban
// RPC endpoint (STELLAR_RPC_URL). It is transport-only: callers supply the
// method name, params, and a destination for the decoded result.
package sorobanrpc

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"time"
)

// DefaultTimeout bounds a single RPC call. simulateTransaction runs the
// contract in a sandboxed host and can take noticeably longer than a simple
// query like getLatestLedger (stats.go uses 2s for that), so this client
// defaults to a more generous budget. Callers apply it via
// context.WithTimeout before calling Call.
const DefaultTimeout = 10 * time.Second

// Client calls a single Soroban RPC endpoint.
type Client struct {
	URL        string
	HTTPClient *http.Client
}

// MaxResponseBytes bounds a decoded RPC response body.
const MaxResponseBytes = 4 << 20

// sharedHTTPClient is pooled across all clients: bounded dial and header
// timeouts, keep-alive connection reuse, and an overall request timeout.
var sharedHTTPClient = &http.Client{
	Timeout: DefaultTimeout,
	Transport: &http.Transport{
		DialContext:           (&net.Dialer{Timeout: 3 * time.Second, KeepAlive: 30 * time.Second}).DialContext,
		TLSHandshakeTimeout:   3 * time.Second,
		ResponseHeaderTimeout: DefaultTimeout,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       90 * time.Second,
	},
}

// NewClient builds a Client targeting url using the shared pooled HTTP client.
func NewClient(url string) *Client {
	return &Client{URL: url, HTTPClient: sharedHTTPClient}
}

// StatusError is returned when the endpoint answers with a non-2xx status.
type StatusError struct{ Code int }

func (e *StatusError) Error() string {
	return fmt.Sprintf("soroban rpc: unexpected HTTP status %d", e.Code)
}

type jsonRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      int    `json:"id"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type jsonRPCError struct {
	Code    int    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (e *jsonRPCError) Error() string {
	return fmt.Sprintf("soroban rpc error %d: %s", e.Code, e.Message)
}

type jsonRPCResponse struct {
	Result json.RawMessage `json:"result"`
	Error  *jsonRPCError   `json:"error"`
}

// Call issues a JSON-RPC 2.0 POST for method with params, decoding the
// "result" field into result. A JSON-RPC error envelope is surfaced as a Go
// error (of concrete type *jsonRPCError, unwrap with errors.As if the
// code/message need to be inspected).
func (c *Client) Call(ctx context.Context, method string, params any, result any) error {
	if c.URL == "" {
		return fmt.Errorf("soroban rpc: no endpoint configured")
	}

	reqBody, err := json.Marshal(jsonRPCRequest{
		JSONRPC: "2.0",
		ID:      1,
		Method:  method,
		Params:  params,
	})
	if err != nil {
		return fmt.Errorf("soroban rpc: encode request: %w", err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, c.URL, bytes.NewReader(reqBody))
	if err != nil {
		return fmt.Errorf("soroban rpc: build request: %w", err)
	}
	httpReq.Header.Set("Content-Type", "application/json")

	client := c.HTTPClient
	if client == nil {
		client = http.DefaultClient
	}

	resp, err := client.Do(httpReq)
	if err != nil {
		return fmt.Errorf("soroban rpc: call failed: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return &StatusError{Code: resp.StatusCode}
	}

	raw, err := io.ReadAll(io.LimitReader(resp.Body, MaxResponseBytes+1))
	if err != nil {
		return fmt.Errorf("soroban rpc: read response: %w", err)
	}
	if len(raw) > MaxResponseBytes {
		return fmt.Errorf("soroban rpc: response too large (> %d bytes)", MaxResponseBytes)
	}

	var rpcResp jsonRPCResponse
	if err := json.Unmarshal(raw, &rpcResp); err != nil {
		return fmt.Errorf("soroban rpc: decode response: %w", err)
	}
	if rpcResp.Error != nil {
		return rpcResp.Error
	}
	if result != nil && len(rpcResp.Result) > 0 {
		if err := json.Unmarshal(rpcResp.Result, result); err != nil {
			return fmt.Errorf("soroban rpc: decode result: %w", err)
		}
	}
	return nil
}

// endpointCooldown is how long an endpoint that failed with a transport or
// 5xx/429 error is deprioritised behind healthy ones.
const endpointCooldown = 30 * time.Second

// FailoverClient calls a prioritised list of Soroban RPC endpoints. A
// transport error, 5xx or 429 moves on to the next endpoint and puts the failed
// one on a short cooldown; JSON-RPC application errors are returned as-is.
type FailoverClient struct {
	endpoints []*Client
	mu        sync.Mutex
	downUntil []time.Time
}

// NewFailoverClient builds a FailoverClient over urls in priority order.
func NewFailoverClient(urls []string) *FailoverClient {
	f := &FailoverClient{downUntil: make([]time.Time, len(urls))}
	for _, u := range urls {
		f.endpoints = append(f.endpoints, NewClient(u))
	}
	return f
}

// order returns endpoint indexes: healthy ones first in priority order, then
// those on cooldown (still tried last rather than giving up).
func (f *FailoverClient) order() []int {
	now := time.Now()
	f.mu.Lock()
	defer f.mu.Unlock()
	var healthy, cooling []int
	for i := range f.endpoints {
		if now.Before(f.downUntil[i]) {
			cooling = append(cooling, i)
		} else {
			healthy = append(healthy, i)
		}
	}
	return append(healthy, cooling...)
}

func (f *FailoverClient) markDown(i int) {
	f.mu.Lock()
	f.downUntil[i] = time.Now().Add(endpointCooldown)
	f.mu.Unlock()
}

func retryable(err error) bool {
	var se *StatusError
	if errors.As(err, &se) {
		return se.Code >= 500 || se.Code == http.StatusTooManyRequests
	}
	var re *jsonRPCError
	if errors.As(err, &re) {
		return false
	}
	return true
}

// Call implements the same contract as Client.Call across all endpoints.
func (f *FailoverClient) Call(ctx context.Context, method string, params any, result any) error {
	var lastErr error
	for _, i := range f.order() {
		err := f.endpoints[i].Call(ctx, method, params, result)
		if err == nil {
			return nil
		}
		if !retryable(err) || ctx.Err() != nil {
			return err
		}
		f.markDown(i)
		lastErr = err
	}
	if lastErr == nil {
		return fmt.Errorf("soroban rpc: no endpoint configured")
	}
	return fmt.Errorf("soroban rpc: all endpoints failed: %w", lastErr)
}
