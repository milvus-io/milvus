package message

import (
	"crypto/sha256"
	"encoding/hex"
)

const idempotencyKeyFingerprintBytes = 16

// IdempotencyKeyOf returns the idempotency key carried by the message, or "" when
// the message is not an idempotent write.
//
// The key lives in the `_ik` property rather than in a header field, so this one
// accessor serves every message type and every message stage (broadcast, mutable,
// immutable). Callers that only honor the key on specific message types must gate
// on the type themselves: any message may technically carry the property.
func IdempotencyKeyOf(msg BasicMessage) string {
	if msg == nil {
		return ""
	}
	key, _ := msg.Properties().Get(messageIdempotencyKey)
	return key
}

// IdempotencyKeyFingerprint returns a stable, non-plaintext identifier suitable
// for correlating idempotency-key events in logs. The original key must never be
// logged: it is client-controlled and may contain sensitive data.
func IdempotencyKeyFingerprint(key string) string {
	sum := sha256.Sum256([]byte(key))
	return hex.EncodeToString(sum[:idempotencyKeyFingerprintBytes])
}
