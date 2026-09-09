package broadcaster

import (
	"strconv"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// idempotencyScope derives the dedup identity of a broadcast: a client key only
// deduplicates within (messageType, the scope the key was built with).
//
// messageType is load-bearing and is added here rather than by the caller, because
// CreateIndex and DropIndex on one collection carry the same scope otherwise, and
// the second one would be silently swallowed. It is a property of the broadcast, so
// the broadcaster owns it; the scope is a property of the operation, so the caller
// owns it (see message.IdempotencyKey).
//
// Returns "" when the key is zero, i.e. the broadcast is not idempotent.
//
// The encoding needs no framing: messageType is decimal digits, so the first
// separator delimits it and the key is the unbounded tail -- and the key is itself
// injectively encoded. This string is a map key only. It embeds the raw client key,
// so it must never be logged; log message.IdempotencyKeyFingerprint of the client
// portion instead.
func idempotencyScope(msgType message.MessageType, key message.IdempotencyKey) string {
	if key == "" {
		return ""
	}
	return strconv.Itoa(int(msgType)) + "/" + string(key)
}

// idempotencyScopeOfMessage derives the scope from the message alone.
//
// Everything the scope is built from travels in the message itself -- the type, and
// the `_ik` property the caller scoped -- so this is the single derivation, used by
// the lookup, by task creation and by recovery alike. It deliberately does not read
// the broadcast header: an earlier revision derived the scope from the broadcast's
// resource keys, which meant the lookup (which runs before the header is stamped)
// and task creation (which runs after) had to derive it from two different places
// and agree.
func idempotencyScopeOfMessage(msg message.BroadcastMutableMessage) string {
	return idempotencyScope(msg.MessageType(), message.IdempotencyKeyOf(msg))
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
