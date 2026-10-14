package partialupdate

import (
	"context"
	"sort"

	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility/primarykey"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/lock"
)

// A vchannel belongs to one collection. Include the PK type so that Int64 and
// VarChar values cannot alias, and use the actual PK rather than a hash bucket.
type pkLockKey struct {
	vchannel    string
	kind        primarykey.Kind
	int64Value  int64
	stringValue string
}

// globalRowLockManager coordinates row writes across all WALs in this process.
// WAL close and collection drop never reset it; active entries are removed when
// their last acquiring or admitted writer releases them. Proof state stays per WAL.
var globalRowLockManager = newRowLockManager()

// Complete writes take shared PK locks, and local CAS commits take exclusive
// PK locks, before their visibility timestamp is allocated. Versioned writes
// require exact PKs; legacy schema-less inserts retain their compatibility path.
type rowLockManager struct {
	pks *lock.KeyLock[pkLockKey]
}

func newRowLockManager() *rowLockManager {
	return &rowLockManager{
		pks: lock.NewKeyLock[pkLockKey](),
	}
}

func (g *rowLockManager) acquire(ctx context.Context, vchannel string, pks primarykey.Keys, exclusive bool) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if pks.Len() == 0 {
		return func() {}, nil
	}
	keys := make([]pkLockKey, 0, pks.Len())
	for _, value := range pks.Int64Values {
		keys = append(keys, pkLockKey{vchannel: vchannel, kind: primarykey.KindInt64, int64Value: value})
	}
	for _, value := range pks.StringValues {
		keys = append(keys, pkLockKey{vchannel: vchannel, kind: primarykey.KindString, stringValue: value})
	}
	// Both modes use the same sorted, unique set. A duplicate exclusive lock
	// self-deadlocks; a duplicate shared lock can also deadlock if a writer queues
	// between the two RLocks, because RWMutex gives waiting writers priority.
	sort.Slice(keys, func(a, b int) bool {
		if keys[a].kind != keys[b].kind {
			return keys[a].kind < keys[b].kind
		}
		if keys[a].kind == primarykey.KindInt64 {
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
	release := func() {
		for idx := locked - 1; idx >= 0; idx-- {
			if exclusive {
				g.pks.Unlock(unique[idx])
			} else {
				g.pks.RUnlock(unique[idx])
			}
		}
	}
	for _, key := range unique {
		if exclusive {
			g.pks.Lock(key)
		} else {
			g.pks.RLock(key)
		}
		locked++
		// KeyLock waits are not cancellable. Once a wait ends, cancellation
		// releases every acquired PK lock.
		if err := ctx.Err(); err != nil {
			release()
			return nil, err
		}
	}
	return release, nil
}

type ordinaryWriteSet struct {
	pks                 primarykey.Keys
	fenceCollectionID   int64
	droppedCollectionID int64
}

type ordinaryWriteSetContextKey struct{}

// Acquire runs after a CommitTxn has frozen and drained its body, but before
// its final timestamp is allocated. The TimeTick interceptor retains row locks
// until PK/fence publication, transaction completion, and acknowledgment finish.
func (i *appendInterceptor) Acquire(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
	if msg.MessageType() == message.MessageTypeCommitTxn {
		txnContext := msg.TxnContext()
		if txnContext == nil {
			return ctx, nil, status.NewUnrecoverableError("partial update commit transaction context is missing")
		}
		state, err := i.state.completeTxnWriteSet(msg, txnContext.TxnID)
		if err != nil {
			return ctx, nil, err
		}
		localCAS := message.HasPartialUpdateCAS(msg) && msg.ReplicateHeader() == nil
		release, err := i.state.rowLocks.acquire(ctx, msg.VChannel(), state.pks, localCAS)
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
	release, err := i.state.rowLocks.acquire(ctx, msg.VChannel(), set.pks, false)
	return ctx, release, err
}

func (i *appendInterceptor) extractOrdinaryWriteSet(ctx context.Context, msg message.BasicMessage) (ordinaryWriteSet, error) {
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
