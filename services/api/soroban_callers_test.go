package main

import "testing"

func envMap(m map[string]string) func(string) string { return func(k string) string { return m[k] } }

func TestSorobanCallers_ConfiguredNetworkOnly(t *testing.T) {
	c := sorobanCallersFrom(envMap(map[string]string{"NETWORK": "mainnet", "STELLAR_RPC_URL": "http://a"}))
	if _, ok := c["mainnet"]; !ok || len(c) != 1 {
		t.Fatalf("want only mainnet caller, got %v", c)
	}
}

func TestSorobanCallers_DefaultsToTestnet(t *testing.T) {
	c := sorobanCallersFrom(envMap(map[string]string{"STELLAR_RPC_URLS": "http://a, http://b"}))
	if _, ok := c["testnet"]; !ok {
		t.Fatalf("want testnet caller, got %v", c)
	}
}

func TestSorobanCallers_PerNetworkOverrides(t *testing.T) {
	c := sorobanCallersFrom(envMap(map[string]string{
		"STELLAR_RPC_URL_TESTNET":  "http://t",
		"STELLAR_RPC_URLS_MAINNET": "http://m1,http://m2",
	}))
	if len(c) != 2 || c["testnet"] == nil || c["mainnet"] == nil {
		t.Fatalf("want both networks, got %v", c)
	}
}

func TestSorobanCallers_NoneConfigured(t *testing.T) {
	if c := sorobanCallersFrom(envMap(nil)); len(c) != 0 {
		t.Fatalf("want none, got %v", c)
	}
}
