-- Migration 0038: bind token_events.network, backfill existing rows (issue #595 follow-up)
--
-- insert_token_events_batch never included `network` in its column list, so
-- every row silently fell back to the column's DEFAULT 'testnet' regardless
-- of the indexer's actual network — every mainnet token event was mislabeled
-- testnet. Issue #595 fixed the same bug for soroban_events and
-- contract_invocation_metrics but missed this table; the application-level
-- fix lands in the same change as this migration.
--
-- Backfill existing rows from their parent soroban_events row: token_events
-- is a projection of soroban_events keyed by event_id = soroban_events.id (the
-- DB-level FK was dropped in migration 0017 because soroban_events is
-- partitioned, but the logical relationship still holds), so the parent row
-- always carries the correct network.
UPDATE token_events te
SET network = se.network
FROM soroban_events se
WHERE se.id = te.event_id
  AND te.network IS DISTINCT FROM se.network;

-- Drop the default on every table where #595 and this migration confirm the
-- application always binds `network` explicitly. An unbound insert should
-- fail loudly (NOT NULL violation) instead of silently mislabeling the row.
ALTER TABLE token_events ALTER COLUMN network DROP DEFAULT;
ALTER TABLE soroban_events ALTER COLUMN network DROP DEFAULT;
ALTER TABLE contract_invocation_metrics ALTER COLUMN network DROP DEFAULT;
ALTER TABLE webhook_subscriptions ALTER COLUMN network DROP DEFAULT;
