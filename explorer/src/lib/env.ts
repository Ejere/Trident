import type { Network } from "./types";

export interface ExplorerEnv {
  TRIDENT_TESTNET_API_URL?: string;
  TRIDENT_MAINNET_API_URL?: string;
  EXPLORER_TESTNET_API_KEY?: string;
  EXPLORER_MAINNET_API_KEY?: string;
}

export interface ExplorerConfig {
  apiBaseUrl(network: Network): string;
  apiKeyFor(network: Network): string;
}

const DEFAULT_TESTNET_URL = "https://api.testnet.trident.dev";
const DEFAULT_MAINNET_URL = "https://api.mainnet.trident.dev";

/**
 * Build the explorer's network-scoped config from raw env vars.
 *
 * Trident API keys are bound to one network server-side (api_keys.network),
 * so a single shared key silently returns the wrong network's data (or
 * none). Each network gets its own key here instead, validated up front:
 * a missing key fails loudly at startup in production rather than quietly
 * returning empty results.
 */
export function createExplorerConfig(env: ExplorerEnv, isProd: boolean): ExplorerConfig {
  const testnetUrl = env.TRIDENT_TESTNET_API_URL || DEFAULT_TESTNET_URL;
  const mainnetUrl = env.TRIDENT_MAINNET_API_URL || DEFAULT_MAINNET_URL;
  const testnetKey = env.EXPLORER_TESTNET_API_KEY ?? "";
  const mainnetKey = env.EXPLORER_MAINNET_API_KEY ?? "";

  const missing: string[] = [];
  if (!testnetKey) missing.push("EXPLORER_TESTNET_API_KEY");
  if (!mainnetKey) missing.push("EXPLORER_MAINNET_API_KEY");

  if (missing.length > 0) {
    const message = `Missing required environment variable(s): ${missing.join(", ")}. Trident API keys are network-scoped — set both EXPLORER_TESTNET_API_KEY and EXPLORER_MAINNET_API_KEY (see .env.example).`;
    if (isProd) throw new Error(message);
    console.warn(`[explorer] ${message} Requests to that network will be unauthenticated.`);
  }

  return {
    apiBaseUrl(network: Network): string {
      return network === "mainnet" ? mainnetUrl : testnetUrl;
    },
    apiKeyFor(network: Network): string {
      return network === "mainnet" ? mainnetKey : testnetKey;
    },
  };
}

let cached: ExplorerConfig | null = null;

/** Lazily-built, memoized config — reads real env vars on first use. */
export function getExplorerConfig(): ExplorerConfig {
  if (!cached) {
    cached = createExplorerConfig(
      import.meta.env as unknown as ExplorerEnv,
      Boolean(import.meta.env.PROD),
    );
  }
  return cached;
}
