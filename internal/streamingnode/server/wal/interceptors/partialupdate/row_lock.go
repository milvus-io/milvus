package partialupdate

import (
	"context"
	"math/bits"
	"sync"

	"github.com/cespare/xxhash/v2"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// Each active vchannel uses a fixed table (96 KiB with 24-byte RWMutexes).
// Different PKs may share a stripe: this adds contention but never omits a lock.
const rowLockStripeCount = 4096

// globalRowLockManager coordinates row writes across all WALs in this process.
// WAL close and collection drop never reset active tables. Proof stays per WAL.
var globalRowLockManager = newRowLockManager()

type rowLockTable struct {
	stripes [rowLockStripeCount]sync.RWMutex
	refs    int // protected by rowLockManager.mu; includes holders and waiters
}

// Ordinary writes take shared stripes and local CAS commits take exclusive
// stripes before their final timestamp is allocated. The short manager mutex
// only protects table references; no stripe wait or WAL I/O holds it.
type rowLockManager struct {
	mu       sync.Mutex
	channels map[string]*rowLockTable
	tables   sync.Pool
}

func newRowLockManager() *rowLockManager {
	return &rowLockManager{
		channels: make(map[string]*rowLockTable),
		tables:   sync.Pool{New: func() any { return new(rowLockTable) }},
	}
}

func (g *rowLockManager) pin(vchannel string) *rowLockTable {
	g.mu.Lock()
	defer g.mu.Unlock()
	table := g.channels[vchannel]
	if table == nil {
		table = g.tables.Get().(*rowLockTable)
		g.channels[vchannel] = table
	}
	table.refs++
	return table
}

func (g *rowLockManager) unpin(vchannel string, table *rowLockTable) {
	g.mu.Lock()
	table.refs--
	if table.refs == 0 {
		delete(g.channels, vchannel)
		// All holders have unlocked and all waiters have finished. Reuse the
		// table without retaining an unbounded cache of inactive vchannels.
		g.tables.Put(table)
	}
	g.mu.Unlock()
}

// Int64 and VarChar PKs use different hash functions. Both are independent of
// the murmur3/CRC32 PK-to-vchannel routing hashes, so the PKs of one vchannel
// spread over all stripes. Collisions, including across types, are allowed:
// they only serialize unrelated writes, and the PK version index still compares
// exact values.
func int64RowLockStripe(value int64) uint16 {
	hash := uint64(value) + 0x9e3779b97f4a7c15
	hash = (hash ^ (hash >> 30)) * 0xbf58476d1ce4e5b9
	hash = (hash ^ (hash >> 27)) * 0x94d049bb133111eb
	return uint16((hash ^ (hash >> 31)) & (rowLockStripeCount - 1))
}

func stringRowLockStripe(value string) uint16 {
	return uint16(xxhash.Sum64String(value) & (rowLockStripeCount - 1))
}

func (g *rowLockManager) acquire(ctx context.Context, vchannel string, pks primaryKeys, exclusive bool) (func(), error) {
	if pks.Len() == 0 {
		return func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Avoid allocating the batch bitmap for the common single-row write.
	if pks.Len() == 1 {
		var stripe uint16
		if len(pks.int64Values) == 1 {
			stripe = int64RowLockStripe(pks.int64Values[0])
		} else {
			stripe = stringRowLockStripe(pks.stringValues[0])
		}
		table := g.pin(vchannel)
		mutex := &table.stripes[stripe]
		if exclusive {
			mutex.Lock()
		} else {
			mutex.RLock()
		}
		release := func() {
			if exclusive {
				mutex.Unlock()
			} else {
				mutex.RUnlock()
			}
			g.unpin(vchannel, table)
		}
		if err := ctx.Err(); err != nil {
			release()
			return nil, err
		}
		return release, nil
	}
	// A fixed bitmap bounds both memory and the number of locks regardless of
	// batch size. Enumerating set bits sorts and deduplicates stripe numbers,
	// not PKs: hash collisions must never acquire a stripe twice. Even a second
	// RLock can self-deadlock when a writer queues between the two RLocks.
	var stripes [rowLockStripeCount / 64]uint64
	for _, value := range pks.int64Values {
		stripe := int64RowLockStripe(value)
		stripes[stripe/64] |= uint64(1) << (stripe % 64)
	}
	for _, value := range pks.stringValues {
		stripe := stringRowLockStripe(value)
		stripes[stripe/64] |= uint64(1) << (stripe % 64)
	}
	// Pin before the first wait. A newer WAL for the same vchannel must share
	// this table until every holder and waiter has released its reference.
	table := g.pin(vchannel)
	locked := 0
	release := func() {
		remaining := locked
		for word, mask := range stripes {
			for mask != 0 && remaining > 0 {
				stripe := word*64 + bits.TrailingZeros64(mask)
				if exclusive {
					table.stripes[stripe].Unlock()
				} else {
					table.stripes[stripe].RUnlock()
				}
				remaining--
				mask &= mask - 1
			}
		}
		g.unpin(vchannel, table)
	}
	for word, mask := range stripes {
		for mask != 0 {
			stripe := word*64 + bits.TrailingZeros64(mask)
			if exclusive {
				table.stripes[stripe].Lock()
			} else {
				table.stripes[stripe].RLock()
			}
			locked++
			// RWMutex waits are not cancellable. Once a wait ends, cancellation
			// releases the acquired prefix and the table reference.
			if err := ctx.Err(); err != nil {
				release()
				return nil, err
			}
			mask &= mask - 1
		}
	}
	return release, nil
}

type ordinaryWriteSet struct {
	pks                 primaryKeys
	fenceCollectionID   int64
	droppedCollectionID int64
}

type ordinaryWriteSetContextKey struct{}

// Acquire runs in the lock interceptor under the shard lifecycle lock, before
// TimeTick allocates the write timestamp. The lock interceptor keeps the row
// locks until the downstream append returns: after PK/fence publication,
// transaction completion, acknowledgement and MVCC update. Replicated messages
// are serialized by the replicate interceptor and never reach Acquire.
func (i *appendInterceptor) Acquire(ctx context.Context, msg message.MutableMessage) (context.Context, func(), error) {
	switch msg.MessageType() {
	case message.MessageTypeCommitTxn:
		txnContext := msg.TxnContext()
		if txnContext == nil {
			return ctx, nil, status.NewUnrecoverableError("partial update commit transaction context is missing")
		}
		pks, err := i.state.admitTxnCommit(msg, txnContext.TxnID)
		if err != nil {
			return ctx, nil, err
		}
		localCAS := message.HasPartialUpdateCAS(msg) && msg.ReplicateHeader() == nil
		release, err := i.state.rowLocks.acquire(ctx, msg.VChannel(), pks, localCAS)
		return ctx, release, err
	case message.MessageTypeBeginTxn, message.MessageTypeRollbackTxn, message.MessageTypeTimeTick:
		// These messages carry no row writes.
		return ctx, func() {}, nil
	}
	// Transaction bodies only stage writes. Their CommitTxn locks the union of
	// all body PKs, so a CAS waiter never blocks its own body.
	if txnContext := msg.TxnContext(); txnContext != nil {
		release, err := i.state.admitTxnBody(txnContext.TxnID)
		return ctx, release, err
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
