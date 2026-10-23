ALTER TABLE token_events ALTER COLUMN network SET DEFAULT 'testnet';
ALTER TABLE soroban_events ALTER COLUMN network SET DEFAULT 'testnet';
ALTER TABLE contract_invocation_metrics ALTER COLUMN network SET DEFAULT 'testnet';
ALTER TABLE webhook_subscriptions ALTER COLUMN network SET DEFAULT 'testnet';

-- The backfill in the up migration is not reversible (the pre-backfill
-- values are gone), so the down migration only restores the defaults.
