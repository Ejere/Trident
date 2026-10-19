package main

import (
	"os"
	"strings"

	"github.com/Depo-dev/trident/services/api/handlers"
	"github.com/Depo-dev/trident/services/api/internal/sorobanrpc"
)

func splitURLs(raw string) []string {
	var out []string
	for _, u := range strings.Split(raw, ",") {
		if u = strings.TrimSpace(u); u != "" {
			out = append(out, u)
		}
	}
	return out
}

// sorobanCallersFrom builds one failover RPC caller per network. The
// STELLAR_RPC_URLS (or STELLAR_RPC_URL) endpoints serve the API's configured
// NETWORK (default testnet); STELLAR_RPC_URLS_TESTNET / _MAINNET (or the
// singular _URL_ forms) add or override a specific network. A key whose
// network has no caller is rejected by the contract-call handler.
func sorobanCallersFrom(getenv func(string) string) map[string]handlers.SorobanRPCCaller {
	callers := map[string]handlers.SorobanRPCCaller{}

	network := strings.ToLower(strings.TrimSpace(getenv("NETWORK")))
	if network == "" {
		network = "testnet"
	}
	if urls := splitURLs(getenv("STELLAR_RPC_URLS")); len(urls) > 0 {
		callers[network] = sorobanrpc.NewFailoverClient(urls)
	} else if urls := splitURLs(getenv("STELLAR_RPC_URL")); len(urls) > 0 {
		callers[network] = sorobanrpc.NewFailoverClient(urls)
	}

	for _, n := range []string{"testnet", "mainnet"} {
		suffix := strings.ToUpper(n)
		urls := splitURLs(getenv("STELLAR_RPC_URLS_" + suffix))
		if len(urls) == 0 {
			urls = splitURLs(getenv("STELLAR_RPC_URL_" + suffix))
		}
		if len(urls) > 0 {
			callers[n] = sorobanrpc.NewFailoverClient(urls)
		}
	}
	return callers
}

func sorobanCallersFromEnv() map[string]handlers.SorobanRPCCaller {
	return sorobanCallersFrom(os.Getenv)
}
