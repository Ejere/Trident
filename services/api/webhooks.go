package main

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"crypto/subtle"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Depo-dev/trident/services/api/handlers"
	"github.com/Depo-dev/trident/services/api/internal/httputil"
	"github.com/Depo-dev/trident/services/api/middleware"
	"github.com/Depo-dev/trident/services/api/validation"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/redis/go-redis/v9"
)

type webhookSubscription struct {
	ID         string     `json:"id"`
	APIKeyID   string     `json:"apiKeyId,omitempty"`
	ContractID string     `json:"contractId"`
	Topic0     *string    `json:"topic0,omitempty"`
	TargetURL  string     `json:"targetUrl"`
	Secret     string     `json:"secret,omitempty"`
	CreatedAt  time.Time  `json:"createdAt"`
	PausedAt   *time.Time `json:"pausedAt,omitempty"`
	Network    string     `json:"network"`
}

type webhookEvent struct {
	ID              string         `json:"id"`
	ContractID      string         `json:"contractId"`
	LedgerSequence  int64          `json:"ledgerSequence"`
	Topic0          string         `json:"topic0"`
	Data            map[string]any `json:"data"`
	TransactionHash string         `json:"txHash"`
	Network         string         `json:"network"`
}

type webhookPayload struct {
	ID          string       `json:"id"`
	WebhookID   string       `json:"webhook_id"`
	Event       webhookEvent `json:"event"`
	Timestamp   int64        `json:"timestamp"` // Unix seconds; also used in signature
	DeliveredAt string       `json:"delivered_at"`
}

type webhookDelivery struct {
	ID             int64     `json:"id"`
	SubscriptionID string    `json:"subscriptionId"`
	EventID        string    `json:"eventId"`
	Attempt        int       `json:"attempt"`
	Attempts       int       `json:"attempts"`
	Status         string    `json:"status"`
	StatusCode     *int      `json:"statusCode,omitempty"`
	ResponseBody   string    `json:"responseBody,omitempty"`
	DeliveredAt    time.Time `json:"deliveredAt"`
	Success        bool      `json:"success"`
}

func resolveAPIKeyID(ctx context.Context, db *sql.DB, r *http.Request) (string, error) {
	if db == nil {
		return "", nil
	}
	if header := strings.TrimSpace(r.Header.Get("X-API-Key")); header != "" {
		var id string
		if err := db.QueryRowContext(ctx, `SELECT id FROM api_keys WHERE id = $1`, header).Scan(&id); err == nil {
			return id, nil
		}
	}
	var id string
	if err := db.QueryRowContext(ctx, `INSERT INTO api_keys DEFAULT VALUES RETURNING id`).Scan(&id); err != nil {
		return "", err
	}
	return id, nil
}

type webhookDeliveryResult struct {
	Success      bool
	StatusCode   int
	ResponseBody string
	Err          error
}

// signWebhookPayload signs "${timestamp}.${body}" with the subscription secret
// using HMAC-SHA256. The timestamp (Unix seconds) is also sent as the
// X-Trident-Timestamp header so receivers can verify replay attacks:
//
//	mac := hmac.New(sha256.New, []byte(secret))
//	mac.Write([]byte(fmt.Sprintf("%d.%s", timestamp, body)))
//	expected := "sha256=" + hex.EncodeToString(mac.Sum(nil))
func signWebhookPayload(timestamp int64, body string, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = fmt.Fprintf(mac, "%d.%s", timestamp, body)
	return hex.EncodeToString(mac.Sum(nil))
}

// verifyWebhookSignature checks the HMAC-SHA256 signature over
// "${timestamp}.${body}" against the X-Trident-Signature header value.
func verifyWebhookSignature(timestamp int64, body string, signature string, secret string) bool {
	if !strings.HasPrefix(signature, "sha256=") {
		return false
	}
	expected := "sha256=" + signWebhookPayload(timestamp, body, secret)
	return subtle.ConstantTimeCompare([]byte(signature), []byte(expected)) == 1
}

func newDB() (*sql.DB, error) {
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		return nil, errors.New("DATABASE_URL is not set")
	}
	db, err := sql.Open("pgx", dsn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func startWebhookWorker(ctx context.Context, db *sql.DB, redisClient *redis.Client) {
	if db == nil || redisClient == nil {
		return
	}
	streamKey := os.Getenv("REDIS_STREAM_KEY")
	if streamKey == "" {
		streamKey = "trident:events"
	}
	groupName := os.Getenv("WEBHOOK_CONSUMER_GROUP")
	if groupName == "" {
		groupName = "trident-webhooks"
	}
	consumerName := os.Getenv("WEBHOOK_CONSUMER_NAME")
	if consumerName == "" {
		consumerName = "webhook-worker"
	}

	cfg := loadWebhookRetryConfig()
	wake := make(chan struct{}, cfg.Workers)
	store := sqlWebhookOutbox{db: db}
	for i := 0; i < cfg.Workers; i++ {
		go runWebhookDeliveryWorker(ctx, store, cfg, wake)
	}

	ack := func(id string) {
		if _, err := redisClient.XAck(ctx, streamKey, groupName, id).Result(); err != nil {
			slog.Warn("failed to ack message", "err", err)
		}
	}

	go func() {
		for {
			entries, err := redisClient.XReadGroup(ctx, &redis.XReadGroupArgs{
				Group:    groupName,
				Consumer: consumerName,
				Streams:  []string{streamKey, ">"},
				Count:    10,
				Block:    2 * time.Second,
				NoAck:    false,
			}).Result()
			if err != nil {
				if errors.Is(err, redis.Nil) {
					continue
				}
				if ctx.Err() != nil || errors.Is(err, context.Canceled) {
					return
				}
				slog.Error("webhook worker read failed", "err", err)
				time.Sleep(time.Second)
				continue
			}
			for _, stream := range entries {
				for _, message := range stream.Messages {
					raw, ok := message.Values["event"]
					if !ok {
						raw, ok = message.Values["payload"]
					}
					if !ok {
						slog.Warn("webhook worker received empty payload", "id", message.ID)
						ack(message.ID)
						continue
					}
					event, err := parseWebhookEvent(raw)
					if err != nil {
						slog.Error("failed to parse webhook event", "err", err)
						ack(message.ID)
						continue
					}
					queued, err := enqueueWithRetry(ctx, db, event)
					if err != nil {
						return
					}
					ack(message.ID)
					for i := int64(0); i < queued; i++ {
						select {
						case wake <- struct{}{}:
						default:
						}
					}
				}
			}
		}
	}()
}

// enqueueWithRetry persists the delivery intents for an event, retrying until
// it succeeds so the stream entry is never acked before it is durable. It
// only returns an error when ctx is done.
func enqueueWithRetry(ctx context.Context, db *sql.DB, event webhookEvent) (int64, error) {
	for {
		queued, err := enqueueWebhookEvent(ctx, db, event)
		if err == nil {
			return queued, nil
		}
		slog.Error("failed to enqueue webhook deliveries", "event_id", event.ID, "err", err)
		select {
		case <-ctx.Done():
			return 0, ctx.Err()
		case <-time.After(webhookEnqueueRetryDelay):
		}
	}
}

func parseWebhookEvent(raw any) (webhookEvent, error) {
	switch value := raw.(type) {
	case string:
		var event webhookEvent
		if err := json.Unmarshal([]byte(value), &event); err != nil {
			return webhookEvent{}, err
		}
		return event, nil
	case []byte:
		var event webhookEvent
		if err := json.Unmarshal(value, &event); err != nil {
			return webhookEvent{}, err
		}
		return event, nil
	case map[string]any:
		payload, err := json.Marshal(value)
		if err != nil {
			return webhookEvent{}, err
		}
		var event webhookEvent
		if err := json.Unmarshal(payload, &event); err != nil {
			return webhookEvent{}, err
		}
		return event, nil
	default:
		return webhookEvent{}, fmt.Errorf("unsupported event payload type %T", raw)
	}
}

func performWebhookDelivery(ctx context.Context, sub webhookSubscription, event webhookEvent) webhookDeliveryResult {
	now := time.Now().Unix()
	payload, err := buildWebhookPayload(sub.ID, event, now)
	if err != nil {
		return webhookDeliveryResult{Err: err}
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, sub.TargetURL, bytes.NewReader(payload))
	if err != nil {
		return webhookDeliveryResult{Err: err}
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("X-Trident-Timestamp", strconv.FormatInt(now, 10))
	req.Header.Set("X-Trident-Signature", "sha256="+signWebhookPayload(now, string(payload), sub.Secret))

	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return webhookDeliveryResult{Err: err}
	}
	defer func() { _ = resp.Body.Close() }()

	bodyBytes, _ := io.ReadAll(io.LimitReader(resp.Body, 500))
	responseBody := strings.TrimSpace(string(bodyBytes))
	if resp.StatusCode >= http.StatusOK && resp.StatusCode < http.StatusMultipleChoices {
		return webhookDeliveryResult{Success: true, StatusCode: resp.StatusCode, ResponseBody: responseBody}
	}
	return webhookDeliveryResult{Success: false, StatusCode: resp.StatusCode, ResponseBody: responseBody, Err: fmt.Errorf("webhook returned status %d", resp.StatusCode)}
}

func buildWebhookPayload(subscriptionID string, event webhookEvent, timestamp int64) ([]byte, error) {
	payload := webhookPayload{
		ID:          fmt.Sprintf("wh_%d", time.Now().UnixNano()),
		WebhookID:   subscriptionID,
		Event:       event,
		Timestamp:   timestamp,
		DeliveredAt: time.Now().UTC().Format(time.RFC3339),
	}
	return json.Marshal(payload)
}

func recordWebhookDelivery(ctx context.Context, db *sql.DB, subscriptionID string, eventID string, attempt int, status string, result webhookDeliveryResult) error {
	if db == nil {
		return nil
	}
	var statusCode *int
	if result.StatusCode != 0 {
		statusCode = &result.StatusCode
	}
	_, err := db.ExecContext(ctx, `
		INSERT INTO webhook_deliveries (subscription_id, event_id, attempt, attempts, status, status_code, response_body, success)
		VALUES ($1, $2, $3, $3, $4, $5, $6, $7)
	`, subscriptionID, eventID, attempt, status, statusCode, truncateString(result.ResponseBody, 500), result.Success)
	return err
}

func truncateString(input string, max int) string {
	if len(input) <= max {
		return input
	}
	return input[:max]
}

func listWebhooksHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if db == nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		apiKeyID, err := resolveAPIKeyID(r.Context(), db, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		rows, err := db.QueryContext(r.Context(), `
			SELECT id, api_key_id, contract_id, topic0, target_url, secret, created_at, paused_at, network
			FROM webhook_subscriptions
			WHERE api_key_id = $1
			ORDER BY created_at DESC
		`, apiKeyID)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() { _ = rows.Close() }()

		var subscriptions []webhookSubscription
		for rows.Next() {
			var sub webhookSubscription
			var topic0 sql.NullString
			var pausedAt sql.NullTime
			if err := rows.Scan(&sub.ID, &sub.APIKeyID, &sub.ContractID, &topic0, &sub.TargetURL, &sub.Secret, &sub.CreatedAt, &pausedAt, &sub.Network); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if topic0.Valid {
				sub.Topic0 = &topic0.String
			}
			if pausedAt.Valid {
				sub.PausedAt = &pausedAt.Time
			}
			subscriptions = append(subscriptions, sub)
		}
		writeJSON(w, http.StatusOK, subscriptions)
	}
}

func createWebhookHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if db == nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		var req struct {
			ContractID string  `json:"contractId"`
			Topic0     *string `json:"topic0"`
			TargetURL  string  `json:"targetUrl"`
			Network    string  `json:"network"`
		}
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			if middleware.IsBodyTooLarge(err) {
				middleware.WriteBodyTooLarge(w, r)
				return
			}
			http.Error(w, "invalid request body", http.StatusBadRequest)
			return
		}
		if req.TargetURL == "" || req.ContractID == "" {
			http.Error(w, "contractId and targetUrl are required", http.StatusBadRequest)
			return
		}
		if req.Network == "" {
			req.Network = "testnet"
		}
		secret := generateWebhookSecret()
		apiKeyID, err := resolveAPIKeyID(r.Context(), db, r)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		var topic0 sql.NullString
		if req.Topic0 != nil {
			topic0 = sql.NullString{String: *req.Topic0, Valid: true}
		}
		var id string
		err = db.QueryRowContext(r.Context(), `
			INSERT INTO webhook_subscriptions (api_key_id, contract_id, topic0, target_url, secret, network)
			VALUES ($1, $2, $3, $4, $5, $6)
			RETURNING id
		`, apiKeyID, req.ContractID, topic0, req.TargetURL, secret, req.Network).Scan(&id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusCreated, map[string]any{"id": id, "secret": secret, "targetUrl": req.TargetURL, "contractId": req.ContractID, "network": req.Network})
	}
}

func deleteWebhookHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if verr := validation.ValidateUUID("id", id); verr != nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusBadRequest, httputil.INVALID_ARGUMENT, verr.Message)
			return
		}
		if db == nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusServiceUnavailable, httputil.UNAVAILABLE, "database unavailable")
			return
		}
		result, err := db.ExecContext(r.Context(), `DELETE FROM webhook_subscriptions WHERE id = $1`, id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		affected, _ := result.RowsAffected()
		if affected == 0 {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}
}

func pauseWebhookHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if verr := validation.ValidateUUID("id", id); verr != nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusBadRequest, httputil.INVALID_ARGUMENT, verr.Message)
			return
		}
		if db == nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusServiceUnavailable, httputil.UNAVAILABLE, "database unavailable")
			return
		}
		if _, err := db.ExecContext(r.Context(), `UPDATE webhook_subscriptions SET paused_at = NOW() WHERE id = $1`, id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "paused"})
	}
}

func resumeWebhookHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if verr := validation.ValidateUUID("id", id); verr != nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusBadRequest, httputil.INVALID_ARGUMENT, verr.Message)
			return
		}
		if db == nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusServiceUnavailable, httputil.UNAVAILABLE, "database unavailable")
			return
		}
		if _, err := db.ExecContext(r.Context(), `UPDATE webhook_subscriptions SET paused_at = NULL WHERE id = $1`, id); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		writeJSON(w, http.StatusOK, map[string]string{"status": "resumed"})
	}
}

func deliveriesWebhookHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if verr := validation.ValidateUUID("id", id); verr != nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusBadRequest, httputil.INVALID_ARGUMENT, verr.Message)
			return
		}
		if db == nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusServiceUnavailable, httputil.UNAVAILABLE, "database unavailable")
			return
		}
		rows, err := db.QueryContext(r.Context(), `
			SELECT id, subscription_id, event_id, attempt, attempts, status, status_code, response_body, delivered_at, success
			FROM webhook_deliveries
			WHERE subscription_id = $1
			ORDER BY delivered_at DESC
			LIMIT 100
		`, id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() { _ = rows.Close() }()

		var deliveries []webhookDelivery
		for rows.Next() {
			var delivery webhookDelivery
			var statusCode sql.NullInt64
			if err := rows.Scan(&delivery.ID, &delivery.SubscriptionID, &delivery.EventID, &delivery.Attempt, &delivery.Attempts, &delivery.Status, &statusCode, &delivery.ResponseBody, &delivery.DeliveredAt, &delivery.Success); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if statusCode.Valid {
				code := int(statusCode.Int64)
				delivery.StatusCode = &code
			}
			deliveries = append(deliveries, delivery)
		}
		writeJSON(w, http.StatusOK, deliveries)
	}
}

// deadLettersWebhookHandler handles GET /v1/webhooks/{id}/dead-letters.
// Returns all dead-lettered deliveries for operator inspection.
func deadLettersWebhookHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		id := r.PathValue("id")
		if id == "" {
			http.Error(w, "missing webhook id", http.StatusBadRequest)
			return
		}
		if db == nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}
		rows, err := db.QueryContext(r.Context(), `
			SELECT id, subscription_id, event_id, attempt, attempts, status, status_code, response_body, delivered_at, success
			FROM webhook_deliveries
			WHERE subscription_id = $1 AND status = 'dead_lettered'
			ORDER BY delivered_at DESC
			LIMIT 200
		`, id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		defer func() { _ = rows.Close() }()

		var deliveries []webhookDelivery
		for rows.Next() {
			var delivery webhookDelivery
			var statusCode sql.NullInt64
			if err := rows.Scan(&delivery.ID, &delivery.SubscriptionID, &delivery.EventID, &delivery.Attempt, &delivery.Attempts, &delivery.Status, &statusCode, &delivery.ResponseBody, &delivery.DeliveredAt, &delivery.Success); err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			if statusCode.Valid {
				code := int(statusCode.Int64)
				delivery.StatusCode = &code
			}
			deliveries = append(deliveries, delivery)
		}
		if deliveries == nil {
			deliveries = []webhookDelivery{}
		}
		writeJSON(w, http.StatusOK, deliveries)
	}
}

// replayDeadLetterHandler handles POST /v1/webhooks/{id}/dead-letters/{deliveryId}/replay.
// Re-attempts delivery of a single dead-lettered event and returns the result.
func replayDeadLetterHandler(db *sql.DB) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		subID := r.PathValue("id")
		deliveryIDStr := r.PathValue("deliveryId")
		if subID == "" || deliveryIDStr == "" {
			http.Error(w, "missing webhook id or delivery id", http.StatusBadRequest)
			return
		}
		if db == nil {
			http.Error(w, "database unavailable", http.StatusServiceUnavailable)
			return
		}

		// Load the dead-lettered delivery.
		var eventID string
		var prevAttempts int
		err := db.QueryRowContext(r.Context(), `
			SELECT event_id, attempts FROM webhook_deliveries
			WHERE id = $1 AND subscription_id = $2 AND status = 'dead_lettered'
		`, deliveryIDStr, subID).Scan(&eventID, &prevAttempts)
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		// Load the subscription.
		var sub webhookSubscription
		var topic0 sql.NullString
		var pausedAt sql.NullTime
		err = db.QueryRowContext(r.Context(), `
			SELECT id, api_key_id, contract_id, topic0, target_url, secret, created_at, paused_at, network
			FROM webhook_subscriptions WHERE id = $1
		`, subID).Scan(&sub.ID, &sub.APIKeyID, &sub.ContractID, &topic0, &sub.TargetURL, &sub.Secret, &sub.CreatedAt, &pausedAt, &sub.Network)
		if errors.Is(err, sql.ErrNoRows) {
			http.NotFound(w, r)
			return
		}
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		if topic0.Valid {
			sub.Topic0 = &topic0.String
		}

		// Load the original event.
		var event webhookEvent
		err = db.QueryRowContext(r.Context(), `
			SELECT id, contract_id, ledger_sequence, topic0, data::text, transaction_hash, network
			FROM soroban_events WHERE id = $1
		`, eventID).Scan(&event.ID, &event.ContractID, &event.LedgerSequence, &event.Topic0, &event.TransactionHash, &event.TransactionHash, &event.Network)
		if err != nil {
			// If event can't be loaded, synthesise a minimal one for replay.
			event.ID = eventID
		}

		start := time.Now()
		result := performWebhookDelivery(r.Context(), sub, event)
		handlers.RecordWebhookDelivery(result.Success, false, time.Since(start).Milliseconds())
		status := "failed"
		if result.Success {
			status = "success"
		}
		replayAttempt := prevAttempts + 1
		if err := recordWebhookDelivery(r.Context(), db, subID, eventID, replayAttempt, status, result); err != nil {
			slog.Warn("failed to record replay delivery", "err", err)
		}

		writeJSON(w, http.StatusOK, map[string]any{
			"success":       result.Success,
			"status":        status,
			"attempt":       replayAttempt,
			"status_code":   result.StatusCode,
			"response_body": truncateString(result.ResponseBody, 500),
		})
	}
}

func generateWebhookSecret() string {
	return fmt.Sprintf("whsec_%d", time.Now().UnixNano())
}

func writeJSON(w http.ResponseWriter, status int, payload any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

var deliverWebhook = func(ctx context.Context, sub webhookSubscription, event webhookEvent) error {
	result := performWebhookDelivery(ctx, sub, event)
	if result.Success {
		return nil
	}
	return result.Err
}
