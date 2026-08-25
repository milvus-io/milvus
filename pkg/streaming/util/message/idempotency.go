package message

import (
	"crypto/sha256"
	"encoding/hex"
	"strconv"
	"strings"
)

const idempotencyKeyFingerprintBytes = 16

// IdempotencyScopeDomain names the kind of object a client key deduplicates within.
//
// The domain is carried explicitly rather than inferred from the scope id, even
// though collection ids and database ids come from one rootcoord allocator today
// and therefore never collide. Inferring it would make the broadcaster depend on
// an allocator property it does not own, and would lock every future scope into
// having an int64 identity at all. The failure mode of getting that wrong is two
// unrelated operations sharing one dedup entry, i.e. one of them silently
// swallowed, and the encoding is persisted in the WAL and in etcd, so it cannot
// be revised cheaply after release.
type IdempotencyScopeDomain int

const (
	IdempotencyScopeCluster    IdempotencyScopeDomain = 1
	IdempotencyScopeDatabase   IdempotencyScopeDomain = 2
	IdempotencyScopeCollection IdempotencyScopeDomain = 3
)

// IdempotencyKey is a client-supplied idempotency key together with the scope it
// deduplicates within, encoded as `<domain>:<scopeID>:<clientKey>`.
//
// The scope is an IDENTITY, never a name: a collection id rather than a
// collection name. That is what makes a rename transparent to a retry, and what
// makes a drop-and-recreate under the same name a different operation. Callers
// choose it explicitly through one of the New*ScopedIdempotencyKey constructors;
// there is no constructor that takes a bare string, so a caller cannot end up
// with an unscoped key by omission. Choosing cluster scope is a decision that
// reads as one.
//
// The encoding is injective without any framing tricks: the domain and the scope
// id are decimal digits, so the first two colons delimit them and the client key
// is the unbounded tail. A crafted client key cannot impersonate another scope no
// matter what it contains.
//
// The empty value means "not an idempotent write". Every constructor returns it
// for an empty client key, so an absent key can never encode to a non-empty scope
// -- which would otherwise make every keyless broadcast of one message type
// deduplicate against every other.
type IdempotencyKey string

// NewClusterScopedIdempotencyKey scopes the key to the whole cluster: the client
// key must be unique across every object, and the operation is deduplicated
// wherever it happens.
func NewClusterScopedIdempotencyKey(clientKey string) IdempotencyKey {
	return newIdempotencyKey(IdempotencyScopeCluster, 0, clientKey)
}

// NewDatabaseScopedIdempotencyKey scopes the key to one database, so the same
// client key stays a distinct operation against another database.
func NewDatabaseScopedIdempotencyKey(dbID int64, clientKey string) IdempotencyKey {
	return newIdempotencyKey(IdempotencyScopeDatabase, dbID, clientKey)
}

// NewCollectionScopedIdempotencyKey scopes the key to one collection, so the same
// client key stays a distinct operation against another collection.
func NewCollectionScopedIdempotencyKey(collectionID int64, clientKey string) IdempotencyKey {
	return newIdempotencyKey(IdempotencyScopeCollection, collectionID, clientKey)
}

// newIdempotencyKey encodes the scope onto the client key. The scope id is unused
// for cluster scope and encoded as 0 there.
func newIdempotencyKey(domain IdempotencyScopeDomain, scopeID int64, clientKey string) IdempotencyKey {
	if clientKey == "" {
		return ""
	}
	var b strings.Builder
	b.WriteString(strconv.Itoa(int(domain)))
	b.WriteByte(':')
	b.WriteString(strconv.FormatInt(scopeID, 10))
	b.WriteByte(':')
	b.WriteString(clientKey)
	return IdempotencyKey(b.String())
}

// ClientKey returns the client-supplied portion, i.e. the key without the scope
// this package encoded onto it. Bounds and fingerprints must be taken over this
// rather than over the whole value: the client controls only this part, and a
// bound applied to the encoded form would reject a key the request entrypoint
// already accepted.
//
// A value this package did not produce has no recoverable client portion, so it
// is returned whole. That keeps a length bound conservative on a malformed key
// instead of letting it through unmeasured.
func (k IdempotencyKey) ClientKey() string {
	parts := strings.SplitN(string(k), ":", 3)
	if len(parts) < 3 {
		return string(k)
	}
	return parts[2]
}

// IdempotencyKeyOf returns the idempotency key carried by the message, or "" when
// the message is not an idempotent write.
//
// The key lives in the `_ik` property rather than in a header field, so this one
// accessor serves every message type and every message stage (broadcast, mutable,
// immutable). Callers that only honor the key on specific message types must gate
// on the type themselves: any message may technically carry the property.
func IdempotencyKeyOf(msg BasicMessage) IdempotencyKey {
	if msg == nil {
		return ""
	}
	key, _ := msg.Properties().Get(messageIdempotencyKey)
	return IdempotencyKey(key)
}

// IdempotencyKeyFingerprint returns a stable identifier suitable for correlating
// idempotency-key events in logs. The original key must never be logged: it is
// client-controlled and may contain sensitive data.
//
// This obfuscates the key, it does not protect it: an unkeyed digest of a
// low-entropy client key (a run id, a short batch name) is recoverable by anyone
// who can guess candidates. Use it to correlate log lines, never as a security
// control or an authorization token.
func IdempotencyKeyFingerprint(clientKey string) string {
	sum := sha256.Sum256([]byte(clientKey))
	return hex.EncodeToString(sum[:idempotencyKeyFingerprintBytes])
}
