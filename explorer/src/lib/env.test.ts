import { describe, expect, it, vi } from "vitest";
import { createExplorerConfig } from "./env";

describe("createExplorerConfig", () => {
  const fullEnv = {
    TRIDENT_TESTNET_API_URL: "https://testnet.example",
    TRIDENT_MAINNET_API_URL: "https://mainnet.example",
    EXPLORER_TESTNET_API_KEY: "testnet-key",
    EXPLORER_MAINNET_API_KEY: "mainnet-key",
  };

  it("selects the testnet URL and key for network=testnet", () => {
    const config = createExplorerConfig(fullEnv, false);
    expect(config.apiBaseUrl("testnet")).toBe("https://testnet.example");
    expect(config.apiKeyFor("testnet")).toBe("testnet-key");
  });

  it("selects the mainnet URL and key for network=mainnet", () => {
    const config = createExplorerConfig(fullEnv, false);
    expect(config.apiBaseUrl("mainnet")).toBe("https://mainnet.example");
    expect(config.apiKeyFor("mainnet")).toBe("mainnet-key");
  });

  it("never mixes up keys between networks", () => {
    const config = createExplorerConfig(fullEnv, false);
    expect(config.apiKeyFor("testnet")).not.toBe(config.apiKeyFor("mainnet"));
  });

  it("falls back to the documented default base URLs when URLs are unset", () => {
    const config = createExplorerConfig(
      { EXPLORER_TESTNET_API_KEY: "a", EXPLORER_MAINNET_API_KEY: "b" },
      false,
    );
    expect(config.apiBaseUrl("testnet")).toBe("https://api.testnet.trident.dev");
    expect(config.apiBaseUrl("mainnet")).toBe("https://api.mainnet.trident.dev");
  });

  it("throws in production when the mainnet key is missing", () => {
    expect(() =>
      createExplorerConfig({ EXPLORER_TESTNET_API_KEY: "a" }, true),
    ).toThrow(/EXPLORER_MAINNET_API_KEY/);
  });

  it("throws in production when the testnet key is missing", () => {
    expect(() =>
      createExplorerConfig({ EXPLORER_MAINNET_API_KEY: "b" }, true),
    ).toThrow(/EXPLORER_TESTNET_API_KEY/);
  });

  it("warns instead of throwing outside production", () => {
    const warn = vi.spyOn(console, "warn").mockImplementation(() => {});
    expect(() => createExplorerConfig({}, false)).not.toThrow();
    expect(warn).toHaveBeenCalled();
    warn.mockRestore();
  });
});
