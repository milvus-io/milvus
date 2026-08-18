package broadcaster

import (
	"strconv"
	"strings"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// idempotencyScope derives the dedup identity of a broadcast from the broadcast
// itself: a client-supplied key only deduplicates within
// (messageType, sorted unique resource keys, clientKey).
//
// Both qualifiers are load-bearing:
//   - messageType, because CreateIndex and DropIndex on one collection carry
//     identical resource keys. Without it the same client key on those two
//     operations collides and the second one is silently swallowed.
//   - resource keys, because they name the objects the operation acts on, so the
//     same key reused against another collection stays a distinct operation.
//
// Returns "" when the client key is empty, i.e. the broadcast is not idempotent.
//
// Encoding: every field is emitted length-prefixed as "<byteLen>:<bytes>", so the
// framing is carried by the lengths and never by the field content. The client key
// is attacker-controlled; length prefixing is what makes the encoding injective, so
// no crafted key can embed a separator and impersonate another scope.
//
// ResourceKey.Shared is deliberately left out. The set of resource keys may hold
// the same (domain, key) twice with different Shared flags, and uniqueSortResourceKeys
// does not order by Shared — including the flag would make the encoding depend on an
// unspecified sort order. Dropping it keeps the encoding deterministic, at the cost of
// merging scopes that differ only in lock mode. The concrete case is the cluster key:
// NewSharedClusterResourceKey and NewExclusiveClusterResourceKey are both
// (ResourceDomainCluster, ""), so a broadcast started through WithResourceKeys, which
// auto-appends the shared cluster key, encodes the same as one started through
// WithSecondaryClusterResourceKey. That is harmless today — neither carries an
// idempotency key, and the lock mode is fixed per message type — but a future caller
// that gives those two paths the same client key would see them share one scope.
func idempotencyScope(msgType message.MessageType, resourceKeys []message.ResourceKey, clientKey string) string {
	if clientKey == "" {
		return ""
	}
	keys := uniqueSortResourceKeys(resourceKeys)

	var b strings.Builder
	writeLengthPrefixed(&b, strconv.Itoa(int(msgType)))
	writeLengthPrefixed(&b, strconv.Itoa(len(keys)))
	for _, key := range keys {
		writeLengthPrefixed(&b, strconv.Itoa(int(key.Domain)))
		writeLengthPrefixed(&b, key.Key)
	}
	writeLengthPrefixed(&b, clientKey)
	return b.String()
}

// idempotencyScopeOfMessage derives the scope from the message's own broadcast header.
// Only valid once the header carries the resource keys of the broadcast, which is true
// for every message that reached a broadcast task (OverwriteBroadcastHeader stamps them
// before the task is created) and therefore for every recovered message.
//
// The empty-key case returns before touching the header on purpose: BroadcastHeader()
// is uncached, decoding a proto and rebuilding a Set on every call, and most broadcasts
// carry no idempotency key at all.
func idempotencyScopeOfMessage(msg message.BroadcastMutableMessage) string {
	clientKey := message.IdempotencyKeyOf(msg)
	if clientKey == "" {
		return ""
	}
	return idempotencyScope(msg.MessageType(), msg.BroadcastHeader().ResourceKeys.Collect(), clientKey)
}

func writeLengthPrefixed(b *strings.Builder, s string) {
	b.WriteString(strconv.Itoa(len(s)))
	b.WriteByte(':')
	b.WriteString(s)
}

// validateIdempotencyKeyLength rejects an oversized client key at the broadcaster.
// This is the only place the bound is enforced: no entry path validates the key
// earlier, and the broadcaster is the single point every one of them passes through,
// as well as where the key is retained for the whole idempotency window.
//
// The bound is inclusive and fails closed: a limit of 0 or less rejects every
// non-empty key, i.e. the cluster accepts no idempotency keys at all. A broadcast that
// carries no key is not idempotent and is never rejected here.
func validateIdempotencyKeyLength(key string) error {
	limit := paramtable.Get().StreamingCfg.IdempotencyMaxKeyLength.GetAsInt()
	if len(key) > limit {
		return merr.WrapErrParameterInvalidMsg("idempotency key length %d exceeds limit %d", len(key), limit)
	}
	return nil
}

// idempotencyIndex maps an idempotency scope (see idempotencyScope) to the
// broadcastID that first used it.
//
// Its lifetime is tied to broadcastTaskManager.tasks: an entry appears when a task
// is created and disappears when the task leaves the map (tombstone GC). The
// idempotency window a client observes is therefore exactly the tombstone
// retention window — see streaming.walBroadcaster.tombstone.{maxLifetime,maxCount}.
//
// Not internally locked: every caller already holds broadcastTaskManager.mu.
type idempotencyIndex struct {
	scopeToBroadcastID map[string]uint64
}

func newIdempotencyIndex() *idempotencyIndex {
	return &idempotencyIndex{scopeToBroadcastID: make(map[string]uint64)}
}

// Add records the scope unless it is empty or already present. Keeping the FIRST
// broadcastID is the whole point: a duplicate must resolve to the original
// broadcast, not to whichever retry raced in last.
func (i *idempotencyIndex) Add(scope string, broadcastID uint64) {
	if scope == "" {
		return
	}
	if _, ok := i.scopeToBroadcastID[scope]; ok {
		return
	}
	i.scopeToBroadcastID[scope] = broadcastID
}

// Get returns the broadcastID that owns the scope.
func (i *idempotencyIndex) Get(scope string) (uint64, bool) {
	if scope == "" {
		return 0, false
	}
	id, ok := i.scopeToBroadcastID[scope]
	return id, ok
}

// Remove drops the entry only when it is owned by the given broadcastID, so a
// task that lost the Add race cannot evict the owner's entry on its own GC.
func (i *idempotencyIndex) Remove(scope string, broadcastID uint64) {
	if scope == "" {
		return
	}
	if id, ok := i.scopeToBroadcastID[scope]; ok && id == broadcastID {
		delete(i.scopeToBroadcastID, scope)
	}
}
