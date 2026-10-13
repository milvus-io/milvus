package partialupdate

import (
	"context"
	"sort"
	"sync"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
)

// A vchannel belongs to one collection. Include the PK type so that Int64 and
// VarChar values cannot alias, and use the actual PK rather than a hash bucket.
type pkLockKey struct {
	vchannel    string
	kind        primaryKeyKind
	int64Value  int64
	stringValue string
}

// Unknown write sets need a conservative admission barrier, independent of the
// shard lifecycle lock. Only CAS commits acquire PK locks. Ordinary writes
// register once per batch, and wait only for an overlapping CAS reservation.
type writeGuards struct {
	admission  *lock.KeyLock[string]
	pks        *lock.KeyLock[pkLockKey]
	registryMu sync.Mutex
	registries map[string]*writeRegistry
}

// refs is protected by writeGuards.registryMu; the other fields by mu. A
// registry is retained by every acquiring or admitted writer, and removed when
// its last writer releases it, so dropped vchannels leave no permanent state.
type writeRegistry struct {
	refs     int
	mu       sync.Mutex
	ordinary map[*ordinaryWriteBatch]struct{}
	cas      map[pkLockKey]*casReservation
}

type ordinaryWriteBatch struct {
	// Extraction and the frozen transaction snapshot produce immutable PKs.
	// Retaining their slice headers avoids copying or sorting ordinary batches.
	pks  primaryKeys
	done chan struct{}
}

type casReservation struct {
	done chan struct{}
}

func newWriteGuards() *writeGuards {
	return &writeGuards{
		admission:  lock.NewKeyLock[string](),
		pks:        lock.NewKeyLock[pkLockKey](),
		registries: make(map[string]*writeRegistry),
	}
}

func (g *writeGuards) acquire(ctx context.Context, vchannel string, pks primaryKeys, exclusive, unknown bool) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if unknown {
		g.admission.Lock(vchannel)
		if err := ctx.Err(); err != nil {
			g.admission.Unlock(vchannel)
			return nil, err
		}
		return func() { g.admission.Unlock(vchannel) }, nil
	}
	if pks.Len() == 0 {
		return func() {}, nil
	}
	g.admission.RLock(vchannel)
	registry := g.refRegistry(vchannel)
	var release func()
	var err error
	if exclusive {
		release, err = g.acquireCAS(ctx, vchannel, registry, pks)
	} else {
		release, err = registry.acquireOrdinary(ctx, vchannel, pks)
	}
	if err != nil {
		g.unrefRegistry(vchannel, registry)
		g.admission.RUnlock(vchannel)
		return nil, err
	}
	return func() {
		release()
		g.unrefRegistry(vchannel, registry)
		g.admission.RUnlock(vchannel)
	}, nil
}

func (g *writeGuards) refRegistry(vchannel string) *writeRegistry {
	g.registryMu.Lock()
	defer g.registryMu.Unlock()
	registry := g.registries[vchannel]
	if registry == nil {
		registry = &writeRegistry{
			ordinary: make(map[*ordinaryWriteBatch]struct{}),
			cas:      make(map[pkLockKey]*casReservation),
		}
		g.registries[vchannel] = registry
	}
	registry.refs++
	return registry
}

func (g *writeGuards) unrefRegistry(vchannel string, registry *writeRegistry) {
	g.registryMu.Lock()
	defer g.registryMu.Unlock()
	registry.refs--
	if registry.refs == 0 {
		delete(g.registries, vchannel)
	}
}

func (r *writeRegistry) acquireOrdinary(ctx context.Context, vchannel string, pks primaryKeys) (func(), error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		r.mu.Lock()
		// With no active CAS, registration does not inspect individual PKs.
		var blocked *casReservation
		if len(r.cas) != 0 {
			for _, value := range pks.int64Values {
				if blocked = r.cas[pkLockKey{vchannel: vchannel, kind: primaryKeyKindInt64, int64Value: value}]; blocked != nil {
					break
				}
			}
			if blocked == nil {
				for _, value := range pks.stringValues {
					if blocked = r.cas[pkLockKey{vchannel: vchannel, kind: primaryKeyKindString, stringValue: value}]; blocked != nil {
						break
					}
				}
			}
		}
		if blocked != nil {
			r.mu.Unlock()
			if err := waitForWriteGuard(ctx, blocked.done); err != nil {
				return nil, err
			}
			continue
		}
		batch := &ordinaryWriteBatch{pks: pks, done: make(chan struct{})}
		r.ordinary[batch] = struct{}{}
		r.mu.Unlock()
		return func() {
			r.mu.Lock()
			delete(r.ordinary, batch)
			close(batch.done)
			r.mu.Unlock()
		}, nil
	}
}

func (g *writeGuards) acquireCAS(ctx context.Context, vchannel string, registry *writeRegistry, pks primaryKeys) (func(), error) {
	keys := make([]pkLockKey, 0, pks.Len())
	for _, value := range pks.int64Values {
		keys = append(keys, pkLockKey{vchannel: vchannel, kind: primaryKeyKindInt64, int64Value: value})
	}
	for _, value := range pks.stringValues {
		keys = append(keys, pkLockKey{vchannel: vchannel, kind: primaryKeyKindString, stringValue: value})
	}
	// CAS writers acquire a unique, sorted set; duplicate PKs are common in
	// Delete/Insert transactions and would otherwise deadlock on a CAS lock.
	sort.Slice(keys, func(a, b int) bool {
		if keys[a].kind != keys[b].kind {
			return keys[a].kind < keys[b].kind
		}
		if keys[a].kind == primaryKeyKindInt64 {
			return keys[a].int64Value < keys[b].int64Value
		}
		return keys[a].stringValue < keys[b].stringValue
	})
	unique := keys[:0]
	for _, key := range keys {
		if len(unique) == 0 || unique[len(unique)-1] != key {
			unique = append(unique, key)
		}
	}
	locked := 0
	unlockPKs := func() {
		for idx := locked - 1; idx >= 0; idx-- {
			g.pks.Unlock(unique[idx])
		}
	}
	for _, key := range unique {
		g.pks.Lock(key)
		locked++
		if err := ctx.Err(); err != nil {
			unlockPKs()
			return nil, err
		}
	}
	reservation := &casReservation{done: make(chan struct{})}
	registry.mu.Lock()
	for _, key := range unique {
		registry.cas[key] = reservation
	}
	// Registration and snapshot are atomic with ordinary admission: an
	// overlapping batch is either in this snapshot, or waits for reservation.done.
	ordinary := make([]*ordinaryWriteBatch, 0, len(registry.ordinary))
	for batch := range registry.ordinary {
		ordinary = append(ordinary, batch)
	}
	registry.mu.Unlock()
	release := func() {
		registry.mu.Lock()
		for _, key := range unique {
			delete(registry.cas, key)
		}
		close(reservation.done)
		registry.mu.Unlock()
		unlockPKs()
	}
	// Scanning in-flight ordinary batches happens outside the short registry
	// mutex, so large ordinary write sets do not block registration of other PKs.
	keySet := make(map[pkLockKey]struct{}, len(unique))
	for _, key := range unique {
		keySet[key] = struct{}{}
	}
	for _, batch := range ordinary {
		if ordinaryBatchOverlaps(vchannel, batch.pks, keySet) {
			if err := waitForWriteGuard(ctx, batch.done); err != nil {
				release()
				return nil, err
			}
		}
	}
	if err := ctx.Err(); err != nil {
		release()
		return nil, err
	}
	return release, nil
}

func ordinaryBatchOverlaps(vchannel string, pks primaryKeys, keys map[pkLockKey]struct{}) bool {
	for _, value := range pks.int64Values {
		if _, ok := keys[pkLockKey{vchannel: vchannel, kind: primaryKeyKindInt64, int64Value: value}]; ok {
			return true
		}
	}
	for _, value := range pks.stringValues {
		if _, ok := keys[pkLockKey{vchannel: vchannel, kind: primaryKeyKindString, stringValue: value}]; ok {
			return true
		}
	}
	return false
}

func waitForWriteGuard(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
		return ctx.Err()
	case <-ctx.Done():
		return ctx.Err()
	}
}

type ordinaryWriteSet struct {
	pks                 primaryKeys
	fenceCollectionID   int64
	droppedCollectionID int64
}

type ordinaryWriteSetContextKey struct{}

// Acquire runs after a CommitTxn has frozen and drained its body, but before
// its final timestamp is allocated. The TimeTick interceptor retains this guard
// until PK/fence publication, transaction completion, and acknowledgment finish.
func (i *appendInterceptor) Acquire(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
	if msg.MessageType() == message.MessageTypeCommitTxn {
		txnContext := msg.TxnContext()
		if txnContext == nil {
			return ctx, nil, status.NewUnrecoverableError("partial update commit transaction context is missing")
		}
		state := i.state.getTxn(txnContext.TxnID)
		// A recovered transaction may have an unobserved body prefix. Its known
		// suffix cannot safely substitute for the complete write set.
		if state == nil || !state.observedBegin {
			release, err := i.state.writeGuards.acquire(ctx, msg.VChannel(), primaryKeys{}, false, true)
			return ctx, release, err
		}
		localCAS := message.HasPartialUpdateCAS(msg) && msg.ReplicateHeader() == nil
		release, err := i.state.writeGuards.acquire(ctx, msg.VChannel(), state.pks, localCAS, state.fenceCollection != 0)
		return ctx, release, err
	}
	// Transaction bodies only stage writes. Their CommitTxn acquires the union
	// of all durable body PKs, so a CAS waiter never blocks its own body.
	if msg.TxnContext() != nil {
		return ctx, func() {}, nil
	}
	if message.HasPartialUpdateCAS(msg) {
		return ctx, nil, status.NewUnrecoverableError("partial update CAS message must be transactional")
	}
	set, err := i.extractOrdinaryWriteSet(ctx, msg)
	if err != nil {
		return ctx, nil, err
	}
	ctx = context.WithValue(ctx, ordinaryWriteSetContextKey{}, set)
	release, err := i.state.writeGuards.acquire(ctx, msg.VChannel(), set.pks, false, set.fenceCollectionID != 0)
	return ctx, release, err
}

func (i *appendInterceptor) extractOrdinaryWriteSet(ctx context.Context, msg message.MutableMessage) (ordinaryWriteSet, error) {
	var set ordinaryWriteSet
	pks, ok, err := extractPKsWithContext(ctx, msg)
	if err != nil {
		return set, err
	}
	if ok {
		set.pks = pks
	} else if collectionID, ok := extractCollectionFenceID(msg); ok {
		if collectionID == 0 {
			return set, status.NewUnrecoverableError("partial update collection fence has empty collection id")
		}
		set.fenceCollectionID = collectionID
	} else if collectionID, ok := extractDropCollectionID(msg); ok {
		if collectionID == 0 {
			return set, status.NewUnrecoverableError("partial update dropped collection has empty collection id")
		}
		set.droppedCollectionID = collectionID
	} else if msg.MessageType() == message.MessageTypeInsert {
		set.pks, set.fenceCollectionID, err = extractPKsFromOrdinaryInsertWithContext(ctx, msg, i.pkDescriptorGetter)
	}
	return set, err
}
