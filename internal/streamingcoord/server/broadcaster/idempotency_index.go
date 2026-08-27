package broadcaster

import (
	"strconv"

	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
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

// validateIdempotencyKeyLength rejects an oversized client key at the broadcaster.
//
// This is the backstop, not the first check. Both doors a client key can enter
// through -- the REST middleware and the propagation interceptor -- bound it via
// interceptor.ValidateIdempotencyKey, which they must, because by the time the key
// reaches here it has already been copied onto every coordinator RPC of the
// request. What this check still owns is the broadcaster's own admission: it is
// where the key is retained for the whole idempotency window, and it is the one
// point every entry path passes through, including a future caller that mints a key
// without going through a proxy door.
//
// The bound is taken over the CLIENT portion, not the encoded key: the scope prefix
// is added by this process, and measuring it would reject a key the door already
// accepted.
//
// The bound is inclusive and fails closed: a limit of 0 or less rejects every
// non-empty key, i.e. the cluster accepts no idempotency keys at all. A broadcast that
// carries no key is not idempotent and is never rejected here.
// The limit is passed in rather than read here: the only caller applies this
// inside the manager lock, and a refreshable-config read does not belong there.
func validateIdempotencyKeyLength(key message.IdempotencyKey, limit int) error {
	clientKey := key.ClientKey()
	if clientKey == "" {
		// The empty key is short-circuited rather than compared, because the only
		// caller applies this to every broadcast, keyed or not, and a negative limit
		// would otherwise reject the whole cluster's DDL: the parameter is
		// refreshable and unbounded below, and "-1 means unlimited" is a reasonable
		// thing for an operator to assume. interceptor.ValidateIdempotencyKey admits
		// the absent key the same way.
		return nil
	}
	if len(clientKey) > limit {
		return merr.WrapErrParameterInvalidMsg("idempotency key length %d exceeds limit %d", len(clientKey), limit)
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
