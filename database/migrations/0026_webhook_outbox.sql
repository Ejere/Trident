-- webhook_outbox: durable per-subscription delivery queue (issues #747, #748).
--
-- The stream worker inserts one row per matching subscription and acks the
-- Redis Stream entry immediately; a pool of delivery workers claims due rows
-- independently, so one failing endpoint cannot hold up the others. Retry
-- state (attempt, next_attempt_at) lives here, so retries survive restarts.
-- A row is deleted once delivered or dead-lettered; history stays in
-- webhook_deliveries.
CREATE TABLE IF NOT EXISTS webhook_outbox (
    id              BIGSERIAL   PRIMARY KEY,
    subscription_id UUID        NOT NULL REFERENCES webhook_subscriptions(id) ON DELETE CASCADE,
    event_id        TEXT        NOT NULL,
    event           JSONB       NOT NULL,
    -- failed attempts made so far
    attempt         INT         NOT NULL DEFAULT 0,
    next_attempt_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- lease taken by the worker delivering this row; an expired lease means
    -- the worker died and the row is claimable again.
    locked_until    TIMESTAMPTZ,
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    -- a redelivered stream entry must not enqueue a second delivery
    CONSTRAINT uq_webhook_outbox_subscription_event UNIQUE (subscription_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_webhook_outbox_due ON webhook_outbox (next_attempt_at);
