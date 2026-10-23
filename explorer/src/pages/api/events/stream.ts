import type { APIRoute } from 'astro';
import { streamHeaders, streamUrl } from '../../../lib/api';
import { isValidContractId } from '../../../lib/contracts';
import type { Network } from '../../../lib/types';

/**
 * Server-Sent Events proxy.
 *
 * The browser opens an EventSource against this route; it forwards to the
 * Trident `/v1/events/stream` endpoint so the `X-API-Key`, the `Last-Event-ID`
 * resume header, and any SSE `id:` frames all stay on the server hop. The
 * browser's EventSource reconnects by itself and re-sends `Last-Event-ID`,
 * which this route forwards so no events are skipped after a drop.
 */
export const GET: APIRoute = async ({ url, request }) => {
  const rawNetwork = url.searchParams.get('network');
  const network: Network = rawNetwork === 'mainnet' ? 'mainnet' : 'testnet';
  const contractId = url.searchParams.get('contractId') ?? '';
  const topic0 = url.searchParams.get('topic0') ?? '';

  if (!contractId || !isValidContractId(contractId)) {
    return new Response('invalid contract id', { status: 400 });
  }

  const lastEventId = request.headers.get('last-event-id') ?? undefined;

  let upstream: Response;
  try {
    upstream = await fetch(streamUrl(network, contractId, topic0), {
      headers: streamHeaders(network, lastEventId),
    });
  } catch {
    return new Response(
      JSON.stringify({
        status: 'api_unreachable',
        reason: 'network',
        message: 'Could not reach the event stream. Retrying automatically.',
      }),
      { status: 502, headers: { 'Content-Type': 'application/json' } },
    );
  }

  // A refused or unavailable upstream (missing key, indexer down) is not a
  // valid stream. The browser's retries will surface a visible reconnecting
  // status; when the stream recovers, the reconnects succeed automatically.
  if (!upstream.ok || !upstream.body) {
    return new Response(upstream.body, {
      status: upstream.status,
      headers: { 'Content-Type': 'application/json' },
    });
  }

  return new Response(upstream.body, {
    status: 200,
    headers: {
      'Content-Type': 'text/event-stream',
      'Cache-Control': 'no-cache, no-store',
      Connection: 'keep-alive',
      'X-Accel-Buffering': 'no',
    },
  });
};