package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"log/slog"
	"math/rand/v2"
	"os"
	"strconv"
	"time"

	"github.com/Depo-dev/trident/services/api/handlers"
)

const (
	webhookDefaultWorkers    = 16
	webhookDefaultRetryBase  = 10 * time.Second
	webhookDefaultRetryMax   = time.Hour
	webhookDefaultRetryWin   = 24 * time.Hour
	webhookLease             = 30 * time.Second
	webhookPollInterval      = time.Second
	webhookPausedRecheck     = time.Minute
	webhookJitterFraction    = 0.2
	webhookMaxBackoffExp     = 30
	webhookMinRetryDelay     = time.Second
	webhookEnqueueRetryDelay = time.Second
)

// webhookRetryConfig controls the retry schedule: capped exponential backoff
// with jitter, for as long as RetryWindow after the delivery was first queued.
type webhookRetryConfig struct {
	Base      time.Duration
	MaxDelay  time.Duration
	Window    time.Duration
	Workers   int
	jitterRng func() float64
}

// loadWebhookRetryConfig reads WEBHOOK_RETRY_BASE_SECONDS (10),
// WEBHOOK_RETRY_MAX_BACKOFF_SECONDS (3600), WEBHOOK_RETRY_WINDOW_HOURS (24)
// and WEBHOOK_DELIVERY_WORKERS (16).
func loadWebhookRetryConfig() webhookRetryConfig {
	return webhookRetryConfig{
		Base:      time.Duration(webhookEnvInt("WEBHOOK_RETRY_BASE_SECONDS", int(webhookDefaultRetryBase/time.Second))) * time.Second,
		MaxDelay:  time.Duration(webhookEnvInt("WEBHOOK_RETRY_MAX_BACKOFF_SECONDS", int(webhookDefaultRetryMax/time.Second))) * time.Second,
		Window:    time.Duration(webhookEnvInt("WEBHOOK_RETRY_WINDOW_HOURS", int(webhookDefaultRetryWin/time.Hour))) * time.Hour,
		Workers:   webhookEnvInt("WEBHOOK_DELIVERY_WORKERS", webhookDefaultWorkers),
		jitterRng: rand.Float64,
	}
}

func webhookEnvInt(key string, def int) int {
	if n, err := strconv.Atoi(os.Getenv(key)); err == nil && n > 0 {
		return n
	}
	return def
}

// retryDelay is the wait after the failedAttempts-th failure:
// min(MaxDelay, Base * 2^(failedAttempts-1)), spread by +-20% so subscribers
// recovering from the same outage are not hit in lockstep. jitter is in [0,1).
func (c webhookRetryConfig) retryDelay(failedAttempts int, jitter float64) time.Duration {
	exp := failedAttempts - 1
	if exp < 0 {
		exp = 0
	}
	if exp > webhookMaxBackoffExp {
		exp = webhookMaxBackoffExp
	}
	delay := c.Base << uint(exp)
	if delay > c.MaxDelay || delay <= 0 {
		delay = c.MaxDelay
	}
	spread := 1 - webhookJitterFraction + 2*webhookJitterFraction*jitter
	delay = time.Duration(float64(delay) * spread)
	if delay < webhookMinRetryDelay {
		delay = webhookMinRetryDelay
	}
	return delay
}

// nextAttempt returns when to retry after the failedAttempts-th failure, or
// false once that time falls outside the retry window that opened when the
// delivery was queued (the delivery is then dead-lettered).
func (c webhookRetryConfig) nextAttempt(queuedAt, now time.Time, failedAttempts int, jitter float64) (time.Time, bool) {
	next := now.Add(c.retryDelay(failedAttempts, jitter))
	if next.After(queuedAt.Add(c.Window)) {
		return time.Time{}, false
	}
	return next, true
}

type webhookOutboxItem struct {
	ID             int64
	SubscriptionID string
	EventID        string
	Event          webhookEvent
	Attempt        int
	CreatedAt      time.Time
}

// webhookOutboxStore is the persistence the delivery workers need.
type webhookOutboxStore interface {
	// Claim leases the next due row, or returns nil when none is due.
	Claim(ctx context.Context, lease time.Duration) (*webhookOutboxItem, error)
	// Subscription returns sql.ErrNoRows when the subscription no longer exists.
	Subscription(ctx context.Context, id string) (webhookSubscription, error)
	Done(ctx context.Context, id int64) error
	Reschedule(ctx context.Context, id int64, failedAttempts int, next time.Time) error
	RecordAttempt(ctx context.Context, item *webhookOutboxItem, attempt int, status string, result webhookDeliveryResult) error
}

type sqlWebhookOutbox struct{ db *sql.DB }

func (s sqlWebhookOutbox) Claim(ctx context.Context, lease time.Duration) (*webhookOutboxItem, error) {
	var (
		item webhookOutboxItem
		raw  []byte
	)
	err := s.db.QueryRowContext(ctx, `
		UPDATE webhook_outbox
		   SET locked_until = NOW() + make_interval(secs => $1)
		 WHERE id = (
		       SELECT id FROM webhook_outbox
		        WHERE next_attempt_at <= NOW()
		          AND (locked_until IS NULL OR locked_until <= NOW())
		        ORDER BY next_attempt_at, id
		        LIMIT 1
		        FOR UPDATE SKIP LOCKED)
		RETURNING id, subscription_id, event_id, event, attempt, created_at
	`, lease.Seconds()).Scan(&item.ID, &item.SubscriptionID, &item.EventID, &raw, &item.Attempt, &item.CreatedAt)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, &item.Event); err != nil {
		return nil, err
	}
	return &item, nil
}

func (s sqlWebhookOutbox) Subscription(ctx context.Context, id string) (webhookSubscription, error) {
	var (
		sub      webhookSubscription
		topic0   sql.NullString
		pausedAt sql.NullTime
	)
	err := s.db.QueryRowContext(ctx, `
		SELECT id, api_key_id, contract_id, topic0, target_url, secret, created_at, paused_at, network
		FROM webhook_subscriptions WHERE id = $1
	`, id).Scan(&sub.ID, &sub.APIKeyID, &sub.ContractID, &topic0, &sub.TargetURL, &sub.Secret, &sub.CreatedAt, &pausedAt, &sub.Network)
	if err != nil {
		return sub, err
	}
	if topic0.Valid {
		sub.Topic0 = &topic0.String
	}
	if pausedAt.Valid {
		sub.PausedAt = &pausedAt.Time
	}
	return sub, nil
}

func (s sqlWebhookOutbox) Done(ctx context.Context, id int64) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM webhook_outbox WHERE id = $1`, id)
	return err
}

func (s sqlWebhookOutbox) Reschedule(ctx context.Context, id int64, failedAttempts int, next time.Time) error {
	_, err := s.db.ExecContext(ctx, `
		UPDATE webhook_outbox SET attempt = $2, next_attempt_at = $3, locked_until = NULL WHERE id = $1
	`, id, failedAttempts, next)
	return err
}

func (s sqlWebhookOutbox) RecordAttempt(ctx context.Context, item *webhookOutboxItem, attempt int, status string, result webhookDeliveryResult) error {
	return recordWebhookDelivery(ctx, s.db, item.SubscriptionID, item.EventID, attempt, status, result)
}

// enqueueWebhookEvent queues one delivery per matching subscription. It is
// idempotent per (subscription, event), so a redelivered stream entry is safe.
// It returns the number of newly queued deliveries.
func enqueueWebhookEvent(ctx context.Context, db *sql.DB, event webhookEvent) (int64, error) {
	if db == nil {
		return 0, nil
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return 0, err
	}
	res, err := db.ExecContext(ctx, `
		INSERT INTO webhook_outbox (subscription_id, event_id, event)
		SELECT id, $4, $5::jsonb
		  FROM webhook_subscriptions
		 WHERE contract_id = $1
		   AND paused_at IS NULL
		   AND (topic0 IS NULL OR topic0 = $2)
		   AND network = $3
		ON CONFLICT (subscription_id, event_id) DO NOTHING
	`, event.ContractID, event.Topic0, event.Network, event.ID, string(payload))
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// processWebhookOutboxItem makes one delivery attempt for a claimed row and
// persists the outcome: delete on success or dead-letter, reschedule on a
// retryable failure.
func processWebhookOutboxItem(ctx context.Context, store webhookOutboxStore, cfg webhookRetryConfig, item *webhookOutboxItem) {
	sub, err := store.Subscription(ctx, item.SubscriptionID)
	if errors.Is(err, sql.ErrNoRows) {
		if err := store.Done(ctx, item.ID); err != nil {
			slog.Warn("webhook outbox: drop orphan failed", "outbox_id", item.ID, "err", err)
		}
		return
	}
	if err != nil {
		slog.Warn("webhook outbox: load subscription failed", "outbox_id", item.ID, "err", err)
		reschedule(ctx, store, item, item.Attempt, time.Now().Add(webhookLease))
		return
	}
	if sub.PausedAt != nil {
		reschedule(ctx, store, item, item.Attempt, time.Now().Add(webhookPausedRecheck))
		return
	}

	attempt := item.Attempt + 1
	start := time.Now()
	result := performWebhookDelivery(ctx, sub, item.Event)
	durationMs := time.Since(start).Milliseconds()

	if result.Success {
		record(ctx, store, item, attempt, "success", result)
		handlers.RecordWebhookDelivery(true, false, durationMs)
		if err := store.Done(ctx, item.ID); err != nil {
			slog.Warn("webhook outbox: complete failed", "outbox_id", item.ID, "err", err)
		}
		return
	}

	next, retry := cfg.nextAttempt(item.CreatedAt, time.Now(), attempt, cfg.jitterRng())
	if !retry {
		record(ctx, store, item, attempt, "dead_lettered", result)
		handlers.RecordWebhookDelivery(false, true, durationMs)
		slog.Warn("webhook delivery dead-lettered after retry window",
			"subscription_id", sub.ID, "event_id", item.EventID, "attempts", attempt, "window", cfg.Window)
		if err := store.Done(ctx, item.ID); err != nil {
			slog.Warn("webhook outbox: complete failed", "outbox_id", item.ID, "err", err)
		}
		return
	}
	record(ctx, store, item, attempt, "failed", result)
	handlers.RecordWebhookDelivery(false, false, durationMs)
	reschedule(ctx, store, item, attempt, next)
}

func record(ctx context.Context, store webhookOutboxStore, item *webhookOutboxItem, attempt int, status string, result webhookDeliveryResult) {
	if err := store.RecordAttempt(ctx, item, attempt, status, result); err != nil {
		slog.Warn("failed to record webhook delivery", "err", err)
	}
}

func reschedule(ctx context.Context, store webhookOutboxStore, item *webhookOutboxItem, failedAttempts int, next time.Time) {
	if err := store.Reschedule(ctx, item.ID, failedAttempts, next); err != nil {
		slog.Warn("webhook outbox: reschedule failed", "outbox_id", item.ID, "err", err)
	}
}

// runWebhookDeliveryWorker claims and delivers due rows until ctx is done.
// wake is signalled when new rows are queued so fresh events do not wait for
// the next poll tick.
func runWebhookDeliveryWorker(ctx context.Context, store webhookOutboxStore, cfg webhookRetryConfig, wake <-chan struct{}) {
	ticker := time.NewTicker(webhookPollInterval)
	defer ticker.Stop()
	for ctx.Err() == nil {
		item, err := store.Claim(ctx, webhookLease)
		if err != nil {
			if ctx.Err() == nil {
				slog.Error("webhook outbox: claim failed", "err", err)
			}
		} else if item != nil {
			processWebhookOutboxItem(ctx, store, cfg, item)
			continue
		}
		select {
		case <-ctx.Done():
		case <-ticker.C:
		case <-wake:
		}
	}
}
