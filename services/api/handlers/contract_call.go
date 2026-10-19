package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"time"

	"github.com/Depo-dev/trident/services/api/internal/httputil"
	"github.com/Depo-dev/trident/services/api/middleware"
	"github.com/Depo-dev/trident/services/api/validation"
	"github.com/stellar/go/strkey"
	"github.com/stellar/go/txnbuild"
	"github.com/stellar/go/xdr"
)

// contractCallMaxArgs bounds the args array so a caller cannot force an
// unbounded-size simulate request.
const contractCallMaxArgs = 32

// contractCallTimeout bounds the whole request, including the RPC round
// trip. simulateTransaction is slower than a plain query (stats.go's
// getLatestLedger uses 2s), so this leaves headroom above
// sorobanrpc.DefaultTimeout for JSON (de)serialization and validation.
const contractCallTimeout = 12 * time.Second

// dummySourceAccount is a well-known, unfunded placeholder account (strkey
// encoding of the all-zero ed25519 public key) used as the transaction
// source for every simulated call. simulateTransaction only requires a
// well-formed source account address, not a funded or even real one, since
// simulation never touches the ledger's account balance or signature
// checks — see the "why this is read-only" note on CallContract below.
const dummySourceAccount = "GAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAWHF"

// contractCallRequest is the POST /v1/contracts/{id}/call body.
//
// Args are base64-encoded XDR ScVal strings rather than native JSON values.
// This is the simplest safe wire format: it avoids building a full
// native-JSON -> ScVal type-mapping layer (which would need a contract spec
// to know the target type of each argument, and no such spec-fetch system
// exists in this codebase — see decodeScValJSON's doc comment). Callers that
// already have an SDK can produce ScVal XDR directly.
type contractCallRequest struct {
	Function string   `json:"function"`
	Args     []string `json:"args"`
}

// ContractCallResponse is the response envelope for POST /v1/contracts/{id}/call.
type ContractCallResponse struct {
	Success bool   `json:"success"`
	Result  any    `json:"result,omitempty"`
	RawXDR  string `json:"raw_xdr,omitempty"`
	Error   string `json:"error,omitempty"`
}

// SorobanRPCCaller is the subset of *sorobanrpc.Client used by CallContract,
// so tests can substitute a fake without requiring a live RPC endpoint.
type SorobanRPCCaller interface {
	Call(ctx context.Context, method string, params any, result any) error
}

// CallContract handles POST /v1/contracts/{id}/call: a read-only contract
// invocation proxied through Soroban RPC's simulateTransaction (issue #264).
//
// Why this is safely read-only (the crux of the endpoint, read before
// changing anything below): the handler only ever constructs a single
// xdr.InvokeHostFunctionOp — function name + args against the target
// contract, nothing else (no payment, no create-account, no other operation
// type) — wraps it in a transaction envelope, and calls RPC
// "simulateTransaction". It never calls "sendTransaction" or any other
// method that submits to the network. Soroban RPC's simulateTransaction runs
// the invocation against a sandboxed snapshot of ledger state and returns a
// result without any network broadcast, so nothing this handler builds can
// ever commit a state change on-chain, regardless of the function name or
// args supplied by the caller. A denylist of "mutating-sounding" function
// names would not add any real safety (naming is not a security boundary)
// and is deliberately not implemented. If simulateTransaction's response
// includes footprint/state-diff information, it is surfaced only as
// informational metadata never as something this handler acts on.
func CallContract(rpc SorobanRPCCaller) http.HandlerFunc {
	return CallContractWith(ContractCallConfig{Default: rpc})
}

// ContractCallConfig configures CallContractWith.
type ContractCallConfig struct {
	// Callers routes by the authenticated key's network. A key whose network
	// has no caller is rejected with 403.
	Callers map[string]SorobanRPCCaller
	// Default is used regardless of network when Callers is empty (legacy
	// single-endpoint behaviour).
	Default SorobanRPCCaller
	// CacheTTL caches successful simulations of identical calls (0 = off).
	CacheTTL time.Duration
	// SimulationAllowed spends one unit of the key's simulation budget; nil
	// means unlimited. Errors fail open.
	SimulationAllowed func(ctx context.Context, apiKeyID string) (bool, error)
}

// CallContractWith is CallContract with per-network routing, a simulation
// budget and short-TTL caching. See CallContract for the read-only rationale.
func CallContractWith(cfg ContractCallConfig) http.HandlerFunc {
	cache := newCallCache(cfg.CacheTTL)
	return func(w http.ResponseWriter, r *http.Request) {
		ctx := r.Context()

		contractID := r.PathValue("id")
		if verr := validation.ValidateRequiredContractID("id", contractID); verr != nil {
			httputil.WriteErrorCtx(ctx, w, http.StatusBadRequest, httputil.INVALID_ARGUMENT, verr.Message)
			return
		}

		var req contractCallRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			if middleware.IsBodyTooLarge(err) {
				middleware.WriteBodyTooLarge(w, r)
				return
			}
			httputil.WriteErrorCtx(ctx, w, http.StatusBadRequest, httputil.INVALID_ARGUMENT, "request body must be valid JSON")
			return
		}

		req.Function = strings.TrimSpace(req.Function)
		if req.Function == "" {
			httputil.WriteErrorCtx(ctx, w, http.StatusBadRequest, httputil.INVALID_ARGUMENT, "function is required")
			return
		}
		if len(req.Args) > contractCallMaxArgs {
			httputil.WriteErrorCtx(ctx, w, http.StatusBadRequest, httputil.INVALID_ARGUMENT,
				fmt.Sprintf("maximum %d args per call", contractCallMaxArgs))
			return
		}

		args := make(xdr.ScVec, 0, len(req.Args))
		for i, encoded := range req.Args {
			var scv xdr.ScVal
			if err := xdr.SafeUnmarshalBase64(encoded, &scv); err != nil {
				httputil.WriteErrorCtx(ctx, w, http.StatusBadRequest, httputil.INVALID_ARGUMENT,
					fmt.Sprintf("args[%d] is not a valid base64-encoded XDR ScVal", i))
				return
			}
			args = append(args, scv)
		}

		network := middleware.NetworkFromContext(ctx)
		var rpc SorobanRPCCaller
		if len(cfg.Callers) > 0 {
			caller, ok := cfg.Callers[network]
			if !ok {
				httputil.WriteErrorCtx(ctx, w, http.StatusForbidden, httputil.FORBIDDEN,
					fmt.Sprintf("API key network %q is not served by this API", network))
				return
			}
			rpc = caller
		} else {
			rpc = cfg.Default
		}
		if rpc == nil {
			httputil.WriteErrorCtx(ctx, w, http.StatusServiceUnavailable, httputil.UNAVAILABLE, "Soroban RPC is not configured")
			return
		}

		cacheKey := network + "|" + contractID + "|" + req.Function + "|" + strings.Join(req.Args, ",")
		if cached, ok := cache.get(cacheKey); ok {
			writeJSON(w, http.StatusOK, cached)
			return
		}

		if cfg.SimulationAllowed != nil {
			allowed, err := cfg.SimulationAllowed(ctx, middleware.APIKeyIDFromContext(ctx))
			if err != nil {
				slog.Warn("contract call: simulation budget check failed; failing open", "err", err)
			} else if !allowed {
				w.Header().Set("Retry-After", "60")
				httputil.WriteErrorCtx(ctx, w, http.StatusTooManyRequests, httputil.RATE_LIMITED, "contract simulation budget exceeded")
				return
			}
		}

		envelopeXDR, err := buildSimulateEnvelope(contractID, req.Function, args)
		if err != nil {
			httputil.WriteErrorCtx(ctx, w, http.StatusBadRequest, httputil.INVALID_ARGUMENT, "unable to build invocation: "+err.Error())
			return
		}

		callCtx, cancel := context.WithTimeout(ctx, contractCallTimeout)
		defer cancel()

		var simResp simulateTransactionResponse
		if err := rpc.Call(callCtx, "simulateTransaction", simulateTransactionParams{Transaction: envelopeXDR}, &simResp); err != nil {
			slog.Warn("contract call: simulateTransaction failed", "contract_id", contractID, "err", err)
			httputil.WriteErrorCtx(ctx, w, http.StatusBadGateway, httputil.UNAVAILABLE, "Soroban RPC simulation failed")
			return
		}

		if simResp.Error != "" {
			// A contract trap / simulation error is a client-facing 200-shaped
			// rejection of the call itself, not a transport failure — surface it
			// without leaking raw RPC internals beyond the message field.
			writeJSON(w, http.StatusOK, ContractCallResponse{Success: false, Error: simResp.Error})
			return
		}
		if len(simResp.Results) == 0 {
			httputil.WriteErrorCtx(ctx, w, http.StatusBadGateway, httputil.UNAVAILABLE, "Soroban RPC returned no result")
			return
		}

		rawXDR := simResp.Results[0].XDR
		var scv xdr.ScVal
		if err := xdr.SafeUnmarshalBase64(rawXDR, &scv); err != nil {
			slog.Warn("contract call: decode result XDR failed", "contract_id", contractID, "err", err)
			writeJSON(w, http.StatusOK, ContractCallResponse{Success: true, RawXDR: rawXDR})
			return
		}

		resp := ContractCallResponse{
			Success: true,
			Result:  decodeScValJSON(scv),
			RawXDR:  rawXDR,
		}
		cache.set(cacheKey, resp)
		writeJSON(w, http.StatusOK, resp)
	}
}

// buildSimulateEnvelope constructs a single-operation InvokeHostFunction
// transaction (sequence 0, dummy source account) and returns its base64 XDR,
// ready to hand to simulateTransaction. See CallContract's doc comment for
// why this construction is safely read-only.
func buildSimulateEnvelope(contractID, function string, args xdr.ScVec) (string, error) {
	contractAddr, err := contractIDToScAddress(contractID)
	if err != nil {
		return "", err
	}

	op := &txnbuild.InvokeHostFunction{
		HostFunction: xdr.HostFunction{
			Type: xdr.HostFunctionTypeHostFunctionTypeInvokeContract,
			InvokeContract: &xdr.InvokeContractArgs{
				ContractAddress: contractAddr,
				FunctionName:    xdr.ScSymbol(function),
				Args:            args,
			},
		},
		SourceAccount: dummySourceAccount,
	}

	tx, err := txnbuild.NewTransaction(txnbuild.TransactionParams{
		SourceAccount:        &txnbuild.SimpleAccount{AccountID: dummySourceAccount, Sequence: 0},
		IncrementSequenceNum: false,
		Operations:           []txnbuild.Operation{op},
		BaseFee:              txnbuild.MinBaseFee,
		Preconditions:        txnbuild.Preconditions{TimeBounds: txnbuild.NewInfiniteTimeout()},
	})
	if err != nil {
		return "", fmt.Errorf("build transaction: %w", err)
	}

	return tx.Base64()
}

func contractIDToScAddress(contractID string) (xdr.ScAddress, error) {
	raw, err := strkey.Decode(strkey.VersionByteContract, contractID)
	if err != nil {
		return xdr.ScAddress{}, fmt.Errorf("invalid contract id: %w", err)
	}
	var cid xdr.ContractId
	copy(cid[:], raw)
	return xdr.ScAddress{
		Type:       xdr.ScAddressTypeScAddressTypeContract,
		ContractId: &cid,
	}, nil
}

// simulateTransactionParams is the request body for RPC simulateTransaction.
type simulateTransactionParams struct {
	Transaction string `json:"transaction"`
}

// simulateTransactionResponse covers the fields of the simulateTransaction
// result this handler needs; unused fields (footprint, cost, latestLedger,
// events, etc.) are intentionally omitted.
type simulateTransactionResponse struct {
	Error   string `json:"error,omitempty"`
	Results []struct {
		XDR string `json:"xdr"`
	} `json:"results"`
}
