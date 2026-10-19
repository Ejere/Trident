package handlers

import (
	"sync"
	"time"
)

const callCacheMaxEntries = 1000

type callCacheEntry struct {
	resp ContractCallResponse
	exp  time.Time
}

// callCache is a small TTL cache for successful read-only simulations. A zero
// ttl disables it.
type callCache struct {
	ttl     time.Duration
	mu      sync.Mutex
	entries map[string]callCacheEntry
}

func newCallCache(ttl time.Duration) *callCache {
	return &callCache{ttl: ttl, entries: map[string]callCacheEntry{}}
}

func (c *callCache) get(key string) (ContractCallResponse, bool) {
	if c.ttl <= 0 {
		return ContractCallResponse{}, false
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	e, ok := c.entries[key]
	if !ok || time.Now().After(e.exp) {
		delete(c.entries, key)
		return ContractCallResponse{}, false
	}
	return e.resp, true
}

func (c *callCache) set(key string, resp ContractCallResponse) {
	if c.ttl <= 0 {
		return
	}
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.entries) >= callCacheMaxEntries {
		for k, e := range c.entries {
			if now.After(e.exp) {
				delete(c.entries, k)
			}
		}
		if len(c.entries) >= callCacheMaxEntries {
			return
		}
	}
	c.entries[key] = callCacheEntry{resp: resp, exp: now.Add(c.ttl)}
}
