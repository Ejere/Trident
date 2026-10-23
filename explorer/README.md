# Trident Explorer

Public event explorer for Soroban contracts on Stellar. Read-only, no API key required for end users.

## Stack

- [Astro](https://astro.build) (SSR, `@astrojs/node` standalone adapter)
- Tailwind CSS
- TypeScript

## Routes

| Path | Description |
|------|-------------|
| `/` | Landing page — search + live recent events ticker |
| `/contract/:address` | All events for a contract, server-rendered with pagination + filters. Distinct error/empty states, plus a live SSE feed (status pill + auto-reconnect) |
| `/contract/:address/event/:id` | Single event detail, shareable, og:tags |
| `/api/events.json` | Events API with classified result states (see below) |
| `/api/events/stream` | Server-side SSE proxy for the live feed (keeps the network-scoped API key and the `Last-Event-ID` handshake private) |
| `/api/recent-events.json` | Recent events feed for the homepage ticker |

## Setup

```bash
cp .env.example .env
# edit .env with your API URLs and internal key
npm install
npm run dev        # http://localhost:4321
npm run build      # production build
npm run preview    # preview production build
npm run lint       # type-check with astro check
```

## Environment variables

| Variable | Required | Description |
|----------|----------|-------------|
| `TRIDENT_TESTNET_API_URL` | Yes | Base URL for the testnet Trident REST API |
| `TRIDENT_MAINNET_API_URL` | Yes | Base URL for the mainnet Trident REST API |
| `EXPLORER_TESTNET_API_KEY` | Yes | Internal API key for the testnet Trident API (free tier, created at deploy time) |
| `EXPLORER_MAINNET_API_KEY` | Yes | Internal API key for the mainnet Trident API (free tier, created at deploy time) |
| `TRIDENT_TESTNET_SOROBAN_RPC_URL` | No | Soroban RPC used to probe contracts on-chain (testnet default: `https://soroban-testnet.stellar.org`) |
| `TRIDENT_MAINNET_SOROBAN_RPC_URL` | No | Soroban RPC used to probe contracts on-chain (mainnet default: `https://mainnet.sorobanrpc.com`) |

Trident API keys are scoped to a single network (`api_keys.network`), so the explorer needs one key per network — a testnet key cannot read mainnet data. `EXPLORER_TESTNET_API_KEY` and `EXPLORER_MAINNET_API_KEY` are used server-side only and are never sent to the browser; both are required and the server fails to start in production if either is missing.

## Result states

The explorer distinguishes failures deliberately instead of showing blank pages or raw errors:

- **Loading**: the homepage ticker shows a content skeleton while the recent-events feed loads, never an empty wait.
- **No events yet**: the contract is quiet (and being watched live), so nothing is missed.
- **Not indexed yet**: the contract is emitting on-chain events but Trident hasn't indexed them — shown only after a best-effort Soroban RPC probe confirms the contract is live.
- **Invalid contract**: the searched address fails the Stellar strkey format + checksum check, answered locally in milliseconds.
- **Not found**: the event or contract isn't in the index (e.g. rotated out of retention).
- **Indexer unavailable**: any upstream failure maps to a human-readable reason (`network`, `rate_limited`, `unauthorized`, `timeout`, `down`) with a retry path.
- **Live feed status**: the contract page shows a persistent connection pill (connecting / live / reconnecting / off) and auto-resumes the SSE stream via `Last-Event-ID`, so no events are skipped during a drop.

## Rate limiting

- The explorer uses its internal, network-scoped API keys at the free tier (60 req/min).
- IP-based rate limiting (30 req/min per IP) must be configured at the CDN/edge layer:
  - **Vercel**: Edge Config rate limiting rule
  - **Cloudflare**: Rate limiting rule on the `explorer.*` hostname

## Deployment

The Astro SSR app runs as a Node.js standalone server. A `Dockerfile` is not included — use Vercel, Railway, or Fly.io with `npm run build && node dist/server/entry.mjs`.
