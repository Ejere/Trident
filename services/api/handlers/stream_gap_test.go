package handlers_test

import (
	"bufio"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/Depo-dev/trident/services/api/handlers"
	"github.com/alicebob/miniredis/v2"
	"github.com/redis/go-redis/v9"
)

type sseFrame struct {
	event string
	id    string
	data  string
}

// readSSEFrame parses one blank-line-terminated frame the way an EventSource
// does: "\n" ends a field line and an empty line dispatches the event.
func readSSEFrame(t *testing.T, r *bufio.Reader) sseFrame {
	t.Helper()
	var f sseFrame
	for {
		line, err := r.ReadString('\n')
		if err != nil {
			t.Fatalf("read SSE frame: %v (partial %+v)", err, f)
		}
		line = strings.TrimRight(line, "\r\n")
		if line == "" {
			return f
		}
		name, value, _ := strings.Cut(line, ":")
		value = strings.TrimPrefix(value, " ")
		switch name {
		case "event":
			f.event = value
		case "id":
			f.id = value
		case "data":
			f.data = value
		}
	}
}

func TestStream_GapEventThenOldestRetainedEvents(t *testing.T) {
	const contractID = "CAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"

	mr := miniredis.RunT(t)
	rdb := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	defer func() { _ = rdb.Close() }()

	ctx := context.Background()
	var ids []string
	for i := 1; i <= 5; i++ {
		id, err := rdb.XAdd(ctx, &redis.XAddArgs{
			Stream: "trident:events",
			ID:     fmt.Sprintf("%d-0", i),
			Values: map[string]any{"contract_id": contractID, "topics": `["t"]`, "n": i},
		}).Result()
		if err != nil {
			t.Fatalf("xadd: %v", err)
		}
		ids = append(ids, id)
	}
	if err := rdb.XTrimMaxLen(ctx, "trident:events", 3).Err(); err != nil {
		t.Fatalf("xtrim: %v", err)
	}

	srv := httptest.NewServer(handlers.Stream(rdb))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/?contractId="+contractID, nil)
	req.Header.Set("Last-Event-ID", ids[0]) // trimmed away
	reqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	resp, err := http.DefaultClient.Do(req.WithContext(reqCtx))
	if err != nil {
		t.Fatalf("request: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if got := resp.Header.Get("Content-Type"); !strings.HasPrefix(got, "text/event-stream") {
		t.Fatalf("Content-Type = %q, want text/event-stream", got)
	}

	br := bufio.NewReader(resp.Body)
	gap := readSSEFrame(t, br)
	if gap.event != "gap" || !strings.Contains(gap.data, "retention window") {
		t.Fatalf("first frame = %+v, want gap event", gap)
	}

	for _, want := range ids[2:] {
		f := readSSEFrame(t, br)
		if f.event != "" || f.id != want {
			t.Fatalf("frame = %+v, want message with id %s", f, want)
		}
	}
}
