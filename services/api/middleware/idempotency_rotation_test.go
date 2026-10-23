package middleware

import (
	"strings"
	"testing"
)

func TestDecodeIdempotencyKey(t *testing.T) {
	if _, err := decodeIdempotencyKey("not-hex"); err == nil {
		t.Fatal("expected error for non-hex input")
	}
	if _, err := decodeIdempotencyKey("ab"); err == nil {
		t.Fatal("expected error for a key shorter than 32 bytes")
	}
	key, err := decodeIdempotencyKey(strings.Repeat("ab", 32))
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(key) != 32 {
		t.Fatalf("key length = %d, want 32", len(key))
	}
}

// TestDecryptIdempotencyPayload_FallsBackToPreviousKey exercises
// IDEMPOTENCY_ENCRYPTION_KEY_PREVIOUS's whole purpose: a record encrypted
// under a key that then gets rotated out must still decrypt once that key
// moves to "previous", and must stop decrypting (a safe cache miss, not a
// crash) once the previous key is dropped too.
func TestDecryptIdempotencyPayload_FallsBackToPreviousKey(t *testing.T) {
	savedCurrent, savedPrevious := idempotencyEncryptionKey, idempotencyPreviousEncryptionKey
	t.Cleanup(func() {
		idempotencyEncryptionKey = savedCurrent
		idempotencyPreviousEncryptionKey = savedPrevious
	})

	rotatedOutKey := bytesOfPattern(0)
	newKey := bytesOfPattern(1)

	// Write a record under what is about to become the "previous" key.
	idempotencyEncryptionKey = rotatedOutKey
	idempotencyPreviousEncryptionKey = nil
	sealed, err := encryptIdempotencyPayload([]byte(`{"hello":"world"}`))
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}

	// Rotate: a fresh key becomes current, the old key becomes previous.
	idempotencyEncryptionKey = newKey
	idempotencyPreviousEncryptionKey = rotatedOutKey

	plaintext, err := decryptIdempotencyPayload(sealed)
	if err != nil {
		t.Fatalf("decrypt during the rotation overlap window: %v", err)
	}
	if string(plaintext) != `{"hello":"world"}` {
		t.Fatalf("plaintext = %q", plaintext)
	}

	// Once the previous key is no longer configured, the same ciphertext
	// must fail closed rather than panic or return garbage.
	idempotencyPreviousEncryptionKey = nil
	if _, err := decryptIdempotencyPayload(sealed); err == nil {
		t.Fatal("expected decrypt to fail once the previous key is no longer configured")
	}
}

func bytesOfPattern(offset byte) []byte {
	key := make([]byte, 32)
	for i := range key {
		key[i] = byte(i) + offset
	}
	return key
}
