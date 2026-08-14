package broadcaster

// idempotencyIndex maps an idempotency key to the broadcastID that first used it.
//
// Its lifetime is tied to broadcastTaskManager.tasks: an entry appears when a task
// is created and disappears when the task leaves the map (tombstone GC). The
// idempotency window a client observes is therefore exactly the tombstone
// retention window — see streaming.walBroadcaster.tombstone.{maxLifetime,maxCount}.
//
// Not internally locked: every caller already holds broadcastTaskManager.mu.
type idempotencyIndex struct {
	keyToBroadcastID map[string]uint64
}

func newIdempotencyIndex() *idempotencyIndex {
	return &idempotencyIndex{keyToBroadcastID: make(map[string]uint64)}
}

// Add records the key unless it is empty or already present. Keeping the FIRST
// broadcastID is the whole point: a duplicate must resolve to the original
// broadcast, not to whichever retry raced in last.
func (i *idempotencyIndex) Add(key string, broadcastID uint64) {
	if key == "" {
		return
	}
	if _, ok := i.keyToBroadcastID[key]; ok {
		return
	}
	i.keyToBroadcastID[key] = broadcastID
}

// Get returns the broadcastID that owns the key.
func (i *idempotencyIndex) Get(key string) (uint64, bool) {
	if key == "" {
		return 0, false
	}
	id, ok := i.keyToBroadcastID[key]
	return id, ok
}

// Remove drops the entry only when it is owned by the given broadcastID, so a
// task that lost the Add race cannot evict the owner's entry on its own GC.
func (i *idempotencyIndex) Remove(key string, broadcastID uint64) {
	if key == "" {
		return
	}
	if id, ok := i.keyToBroadcastID[key]; ok && id == broadcastID {
		delete(i.keyToBroadcastID, key)
	}
}
