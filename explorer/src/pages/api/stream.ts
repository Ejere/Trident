import type { APIRoute } from 'astro';
import { getExplorerConfig } from '../../lib/env';
import type { Network } from '../../lib/types';

// EventSource cannot send custom headers, so the browser connects here and
// this endpoint attaches the API key while piping the upstream SSE body
// through untouched (same pattern as /api/events.json, but streaming).
const CONTRACT_ID_RE = /^C[A-Z2-7]{55}$/;

export const GET: APIRoute = async ({ url, request }) => {
  const rawNetwork = url.searchParams.get('network');
  const network: Network = rawNetwork === 'mainnet' ? 'mainnet' : 'testnet';
  const contractId = url.searchParams.get('contractId') ?? '';
  const topic0 = url.searchParams.get('topic0') ?? '';

  if (!CONTRACT_ID_RE.test(contractId)) {
    return new Response(JSON.stringify({ error: 'invalid contractId' }), {
      status: 400,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  const config = getExplorerConfig();
  const upstream = new URL(`${config.apiBaseUrl(network)}/v1/events/stream`);
  upstream.searchParams.set('contract_id', contractId);
  if (topic0) upstream.searchParams.set('topic_0', topic0);

  const headers: Record<string, string> = { Accept: 'text/event-stream' };
  const apiKey = config.apiKeyFor(network);
  if (apiKey) headers['X-API-Key'] = apiKey;
  // Preserve resume semantics: the browser re-sends Last-Event-ID on
  // reconnect and the upstream uses it to continue from that stream entry.
  const lastEventId = request.headers.get('last-event-id');
  if (lastEventId) headers['Last-Event-ID'] = lastEventId;

  let res: Response;
  try {
    res = await fetch(upstream.toString(), {
      headers,
      signal: request.signal,
    });
  } catch {
    return new Response(JSON.stringify({ error: 'upstream unreachable' }), {
      status: 502,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  if (!res.ok || !res.body) {
    return new Response(JSON.stringify({ error: `upstream ${res.status}` }), {
      status: res.status === 429 ? 429 : 502,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  return new Response(res.body, {
    status: 200,
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-store',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  });
};
