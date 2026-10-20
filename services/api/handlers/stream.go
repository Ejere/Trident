package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/Depo-dev/trident/services/api/internal/httputil"
	"github.com/Depo-dev/trident/services/api/validation"
	"github.com/redis/go-redis/v9"
)

const (
	eventStreamKey = "trident:events"
	streamReadWait = time.Second

	// sseWriteDeadline bounds a single SSE write. A stalled client (full TCP
	// send buffer) must not block this connection's goroutine forever
	// (issue #224); the deadline turns a stuck socket into a write error so
	// the handler returns and cleans up instead of leaking.
	sseWriteDeadline = 10 * time.Second
)

type streamRedisClient interface {
	XRead(ctx context.Context, a *redis.XReadArgs) *redis.XStreamSliceCmd
	XRevRangeN(ctx context.Context, key, start, stop string, count int64) *redis.XMessageSliceCmd
}

// eventStreamGapEvent is the documented SSE event sent when a requested
// Last-Event-ID is older than the stream retention window.
const eventStreamGapEvent = "event: gap\ndata: {\"message\":\"requested Last-Event-ID is outside the retention window; resuming from oldest available\"}\n\n"

// Stream returns an SSE handler that forwards new Redis Stream events for one
// contract. The handler owns the blocking read loop, so request cancellation
// stops all streaming work without a detached goroutine.
//
// It honours the standard SSE Last-Event-ID header (issue #235):
//   - On first connect: tail the stream from the latest id.
//   - On reconnect with Last-Event-ID: resume from that id + 1.
//   - If the requested id is older than the retention window, emit a `gap` event
//     and replay every entry still retained.
//   - Every SSE event includes an `id:` field so the browser automatically sends
//     Last-Event-ID on reconnect.
func Stream(rdb streamRedisClient) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		if verr := validation.RejectUnknownParams(q, "contractId", "topic0"); verr != nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusBadRequest, httputil.INVALID_ARGUMENT, verr.Message)
			return
		}

		// The stream filters server-side on this id, so a malformed one would
		// otherwise silently match nothing for the life of the connection.
		contractID := q.Get("contractId")
		if verr := validation.ValidateRequiredContractID("contractId", contractID); verr != nil {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusBadRequest, httputil.INVALID_ARGUMENT, verr.Message)
			return
		}

		flusher, ok := w.(http.Flusher)
		if !ok {
			httputil.WriteErrorCtx(r.Context(), w, http.StatusInternalServerError, httputil.INTERNAL, "streaming is not supported")
			return
		}

		// Honour Last-Event-ID for resumption (issue #235).
		// If the header is present and non-empty, try to resume from that point.
		lastID := ""
		gap := false
		if lastEventID := r.Header.Get("Last-Event-ID"); lastEventID != "" {
			// Verify the requested id exists in the stream. If not, flag a gap and
			// replay everything still retained. XREAD returns entries strictly after
			// the id, so 0-0 includes the oldest one.
			msgs, lookupErr := rdb.XRevRangeN(r.Context(), eventStreamKey, lastEventID, lastEventID, 1).Result()
			if lookupErr != nil || len(msgs) == 0 {
				gap = true
				lastID = "0-0"
			} else {
				lastID = lastEventID
			}
		}

		if lastID == "" {
			var err error
			lastID, err = latestStreamID(r.Context(), rdb)
			if err != nil {
				if r.Context().Err() != nil {
					return
				}
				slog.Error("sse: failed to read Redis Stream tail", "err", err)
				httputil.WriteErrorCtx(r.Context(), w, http.StatusServiceUnavailable, httputil.UNAVAILABLE, "event stream is unavailable")
				return
			}
		}

		h := w.Header()
		h.Set("Content-Type", "text/event-stream")
		h.Set("Cache-Control", "no-cache")
		h.Set("X-Accel-Buffering", "no")
		h.Set("Connection", "keep-alive")
		rc := http.NewResponseController(w)
		w.WriteHeader(http.StatusOK)

		if gap {
			if _, writeErr := fmt.Fprint(w, eventStreamGapEvent); writeErr != nil {
				slog.Warn("sse: write failed, disconnecting slow consumer", "contractId", contractID, "err", writeErr)
				return
			}
		}
		flusher.Flush()

		topic0 := q.Get("topic0")

		for {
			streams, readErr := rdb.XRead(r.Context(), &redis.XReadArgs{
				Streams: []string{eventStreamKey, lastID},
				Count:   100,
				Block:   streamReadWait,
			}).Result()

			if readErr != nil {
				if r.Context().Err() != nil {
					return
				}
				if errors.Is(readErr, redis.Nil) {
					continue
				}

				slog.Warn("sse: Redis XREAD failed", "err", readErr)
				select {
				case <-r.Context().Done():
					return
				case <-time.After(time.Second):
					continue
				}
			}

			for _, stream := range streams {
				for _, msg := range stream.Messages {
					lastID = msg.ID

					if redisString(msg.Values["contract_id"]) != contractID {
						continue
					}
					if topic0 != "" && !matchesTopic0(msg.Values, topic0) {
						continue
					}

					payload, marshalErr := json.Marshal(msg.Values)
					if marshalErr != nil {
						slog.Warn("sse: failed to marshal stream event", "id", msg.ID, "err", marshalErr)
						continue
					}

					if err := rc.SetWriteDeadline(time.Now().Add(sseWriteDeadline)); err != nil &&
						!errors.Is(err, http.ErrNotSupported) {
						slog.Warn("sse: failed to set write deadline", "err", err)
					}
					// Emit SSE id: field so browsers auto-send Last-Event-ID on reconnect.
					if _, writeErr := fmt.Fprintf(w, "id: %s\ndata: %s\n\n", msg.ID, payload); writeErr != nil {
						metricSSESlowConsumerDisconnects.Add(1)
						slog.Warn("sse: write failed, disconnecting slow consumer", "contractId", contractID, "err", writeErr)
						return
					}
					flusher.Flush()
				}
			}
		}
	}
}

func latestStreamID(ctx context.Context, rdb streamRedisClient) (string, error) {
	messages, err := rdb.XRevRangeN(ctx, eventStreamKey, "+", "-", 1).Result()
	if err != nil {
		return "", err
	}
	if len(messages) == 0 {
		return "0-0", nil
	}
	return messages[0].ID, nil
}

func matchesTopic0(values map[string]any, want string) bool {
	var topics []string
	if err := json.Unmarshal([]byte(redisString(values["topics"])), &topics); err != nil {
		// Malformed topics cannot safely satisfy a server-side filter, so skip.
		return false
	}
	return len(topics) > 0 && topics[0] == want
}

func redisString(value any) string {
	switch value := value.(type) {
	case string:
		return value
	case []byte:
		return string(value)
	default:
		return ""
	}
}
