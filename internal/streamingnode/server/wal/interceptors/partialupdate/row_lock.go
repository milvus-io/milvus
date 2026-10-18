package partialupdate

import (
	"cmp"
	"context"
	"slices"
	"sync"
	"sync/atomic"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
)

// globalRowLockManager coordinates row writes across all WALs in this process.
// WAL close and collection drop never reset active tables. Proof stays per WAL.
var globalRowLockManager = newRowLockManager()

// rowLockScanChunk is how many PKs of an ordinary write a CAS compares before
// it checks again whether the write is still in flight.
const rowLockScanChunk = 4096

// rowLockManager keeps exact PK row locks for each active vchannel. Each write
// takes its place in line under the table's keys lock and then waits only for
// earlier writes that share a PK with it: an ordinary write waits for earlier
// CAS commits, and a local CAS waits for earlier CAS commits and earlier
// ordinary writes. Ordinary writes never wait for each other. Every wait
// happens before the waiter's write timestamp is allocated, and a write
// releases only after its append and publication have returned, so a later
// holder of a PK always receives a later timestamp than the earlier holder,
// and a CAS validates only after earlier same-PK writes have published.
//
// Taking a place in line:
//   - an ordinary write, under keys.RLock, looks up the latest CAS of each of
//     its PKs and registers itself in the list of ordinary writes;
//   - a CAS, under keys.Lock, becomes the latest CAS of each of its PKs,
//     remembering the CAS commits it replaces, and snapshots the ordinary list.
//
// keys.Lock excludes every other critical section, so each CAS section is
// ordered against the section of every other write. Of an overlapping pair
// that includes a CAS, the later one therefore sees the earlier one: a CAS
// finds an earlier ordinary write in its snapshot, and every write finds an
// earlier CAS as the latest CAS of the common PK or behind it in that PK's
// chain. Two ordinary writes need no order. A CAS releases, or gives up its
// place, only after the CAS commits it replaced have done so; waiting for the
// latest CAS of a PK therefore waits for all earlier CAS commits of that PK.
//
// Every wait points to an earlier write, so waits never form a cycle, and
// writes are served in the order they took their place, without retries. A
// write waits directly only for earlier writes it overlaps, but such a write
// may itself wait for a still earlier write it overlaps, so waits chain
// through real overlaps and can delay a write behind one it shares no PK with.
// A waiting write keeps its place on all of its PKs, so heavy multi-PK CAS
// traffic on a small hot set forms long chains; the design doc describes the
// resulting throughput ceiling, which is lower than with lock disciplines that
// let waiters be overtaken. Every wait also ends when its context is canceled.
// In production that context is the WAL append context, which the WAL detaches
// from the client: client cancellation does not reach a wait, which ends when
// the earlier writes it waits for release or when the WAL closes. The short
// manager mutex only protects table references; no row-lock wait or WAL I/O
// holds any mutex here.
type rowLockManager struct {
	mu       sync.Mutex
	channels map[string]*rowLockTable
}

// rowLockTable holds the row locks of one active vchannel. Each map exists only
// while it has entries.
type rowLockTable struct {
	// refs is protected by rowLockManager.mu. It counts holders, waiters and
	// canceled CAS commits that still keep their place in line.
	refs int

	// keys guards the latest CAS of each PK. Int64 and VarChar PKs use separate
	// maps, so equal-looking values of different types never alias.
	keys         sync.RWMutex
	lastCASInt64 map[int64]*casRowLock
	lastCASStr   map[string]*casRowLock

	// ordMu guards the list of registered ordinary writes still in flight.
	// Lock order: keys before ordMu.
	ordMu    sync.Mutex
	ordinary *ordinaryRowLock
}

// ordinaryRowLock is a registered ordinary write. A CAS reads pks from another
// goroutine, possibly after the write has released, so the caller must never
// modify the slices passed to acquire.
type ordinaryRowLock struct {
	pks primaryKeys
	// registered is written under rowLockTable.ordMu. CAS scans also read it
	// without the mutex to skip a write that has released.
	registered atomic.Bool
	// The fields below are protected by rowLockTable.ordMu.
	prev, next *ordinaryRowLock
	// done is created only when a CAS has to wait for this write.
	done chan struct{}
}

// casRowLock is one local CAS commit with its sorted, deduplicated PKs. done is
// closed when it releases or, if canceled, gives up its place in line.
type casRowLock struct {
	pks  primaryKeys
	done chan struct{}
}

// casSet is the set of CAS commits that a write waits for.
type casSet map[*casRowLock]struct{}

func (s *casSet) add(lock *casRowLock) {
	if lock == nil {
		return
	}
	if *s == nil {
		*s = make(casSet)
	}
	(*s)[lock] = struct{}{}
}

func newRowLockManager() *rowLockManager {
	return &rowLockManager{channels: make(map[string]*rowLockTable)}
}

func (g *rowLockManager) pin(vchannel string) *rowLockTable {
	g.mu.Lock()
	defer g.mu.Unlock()
	table := g.channels[vchannel]
	if table == nil {
		table = new(rowLockTable)
		g.channels[vchannel] = table
	}
	table.refs++
	return table
}

func (g *rowLockManager) unpin(vchannel string, table *rowLockTable) {
	g.mu.Lock()
	table.refs--
	if table.refs == 0 {
		// Every write has released or given up its place, so the table holds
		// no CAS and no ordinary write.
		delete(g.channels, vchannel)
	}
	g.mu.Unlock()
}

// acquire takes shared row locks for an ordinary write or exclusive row locks
// for a local CAS commit on the exact PKs. The manager may read pks from other
// goroutines even after release, so the caller must never modify the slices
// after passing them.
func (g *rowLockManager) acquire(ctx context.Context, vchannel string, pks primaryKeys, exclusive bool) (func(), error) {
	if pks.Len() == 0 {
		return func() {}, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	// Pin before the first wait. A newer WAL for the same vchannel must share
	// this table until every holder and waiter has released its reference.
	table := g.pin(vchannel)
	if exclusive {
		return g.acquireCAS(ctx, vchannel, table, newCASRowLock(pks))
	}
	return g.acquireOrdinary(ctx, vchannel, table, &ordinaryRowLock{pks: pks})
}

func (g *rowLockManager) acquireOrdinary(ctx context.Context, vchannel string, table *rowLockTable, lock *ordinaryRowLock) (func(), error) {
	var earlier casSet
	table.keys.RLock()
	latestCAS(table.lastCASInt64, lock.pks.int64Values, &earlier)
	latestCAS(table.lastCASStr, lock.pks.stringValues, &earlier)
	table.registerOrdinary(lock)
	table.keys.RUnlock()
	release := func() {
		table.ordMu.Lock()
		if !lock.registered.Load() {
			table.ordMu.Unlock()
			// A second release would unlink other writes from the list and
			// unpin a table that is still in use, silently losing exclusion.
			panic("partial update row lock released twice")
		}
		table.unregisterOrdinaryLocked(lock)
		if lock.done != nil {
			close(lock.done)
		}
		table.ordMu.Unlock()
		g.unpin(vchannel, table)
	}
	// Every CAS that takes its place from now on waits for this write. A
	// canceled write leaves at once: only CAS commits wait for an ordinary
	// write, and each of them waits itself for every earlier write it overlaps,
	// so none relies on the CAS commits this write would have waited for.
	for cas := range earlier {
		if err := waitRowLock(ctx, cas.done); err != nil {
			release()
			return nil, err
		}
	}
	return release, nil
}

func (g *rowLockManager) acquireCAS(ctx context.Context, vchannel string, table *rowLockTable, lock *casRowLock) (func(), error) {
	var earlier casSet
	table.keys.Lock()
	table.lastCASInt64 = enqueueCAS(table.lastCASInt64, lock.pks.int64Values, lock, &earlier)
	table.lastCASStr = enqueueCAS(table.lastCASStr, lock.pks.stringValues, lock, &earlier)
	// No ordinary write can register while keys is locked, so the snapshot
	// holds exactly the ordinary writes registered before this CAS.
	var written []*ordinaryRowLock
	table.ordMu.Lock()
	for write := table.ordinary; write != nil; write = write.next {
		written = append(written, write)
	}
	table.ordMu.Unlock()
	table.keys.Unlock()
	release := func() {
		table.keys.Lock()
		table.lastCASInt64 = deleteOwn(table.lastCASInt64, lock.pks.int64Values, lock)
		table.lastCASStr = deleteOwn(table.lastCASStr, lock.pks.stringValues, lock)
		table.keys.Unlock()
		close(lock.done) // panics on a second release
		g.unpin(vchannel, table)
	}
	for cas := range earlier {
		if err := waitRowLock(ctx, cas.done); err != nil {
			// Later writes that found this CAS wait only for it, not for the
			// CAS commits it replaced, so it keeps its place in line until
			// those have released. They always do, so the goroutine ends.
			go func() {
				for cas := range earlier {
					<-cas.done
				}
				release()
			}()
			return nil, err
		}
	}
	// Compare exact PKs outside the mutexes. The snapshotted writes registered
	// before this CAS, so none of them ever waits for it. A canceled CAS
	// releases at once here: a later CAS waits itself for every snapshotted
	// write that is still in flight, and an ordinary write never needs to.
	for _, write := range written {
		if !lock.overlapsInFlight(write) {
			continue
		}
		if done := table.ordinaryDone(write); done != nil {
			if err := waitRowLock(ctx, done); err != nil {
				release()
				return nil, err
			}
		}
	}
	return release, nil
}

// waitRowLock waits until done is closed or ctx is canceled. A request canceled
// during the wait fails even if done was closed at the same time.
func waitRowLock(ctx context.Context, done <-chan struct{}) error {
	select {
	case <-done:
	case <-ctx.Done():
	}
	return ctx.Err()
}

func newCASRowLock(pks primaryKeys) *casRowLock {
	// Duplicates are common, for example in an upsert with Delete and Insert of
	// the same PK, and a CAS must not find itself as an earlier CAS. Sort
	// copies so the caller's write set stays untouched.
	lock := &casRowLock{done: make(chan struct{})}
	if len(pks.int64Values) > 0 {
		lock.pks.int64Values = slices.Clone(pks.int64Values)
		slices.Sort(lock.pks.int64Values)
		lock.pks.int64Values = slices.Compact(lock.pks.int64Values)
	}
	if len(pks.stringValues) > 0 {
		lock.pks.stringValues = slices.Clone(pks.stringValues)
		slices.Sort(lock.pks.stringValues)
		lock.pks.stringValues = slices.Compact(lock.pks.stringValues)
	}
	return lock
}

// overlapsInFlight reports whether write is still in flight and contains a PK
// of this CAS. It stops scanning soon after a large write releases, because a
// released write never needs a wait.
func (l *casRowLock) overlapsInFlight(write *ordinaryRowLock) bool {
	return overlapsSorted(l.pks.int64Values, write.pks.int64Values, &write.registered) ||
		overlapsSorted(l.pks.stringValues, write.pks.stringValues, &write.registered)
}

// overlapsSorted reports whether pks contains a value of sorted. It checks
// registered before every rowLockScanChunk PKs and reports false once the
// write has released.
func overlapsSorted[K cmp.Ordered](sorted, pks []K, registered *atomic.Bool) bool {
	if len(sorted) == 0 {
		return false
	}
	for start := 0; start < len(pks); start += rowLockScanChunk {
		if !registered.Load() {
			return false
		}
		for _, pk := range pks[start:min(start+rowLockScanChunk, len(pks))] {
			if _, found := slices.BinarySearch(sorted, pk); found {
				return true
			}
		}
	}
	return false
}

// latestCAS adds the latest CAS of each of pks to earlier. Without any CAS in
// the table it costs O(1) regardless of the batch size. The caller holds keys.
func latestCAS[K comparable](last map[K]*casRowLock, pks []K, earlier *casSet) {
	if len(last) == 0 {
		return
	}
	for _, pk := range pks {
		earlier.add(last[pk])
	}
}

// enqueueCAS makes lock the latest CAS of each of its distinct pks and adds the
// CAS commits it replaces to earlier. The caller holds keys.Lock.
func enqueueCAS[K comparable](last map[K]*casRowLock, pks []K, lock *casRowLock, earlier *casSet) map[K]*casRowLock {
	if len(pks) > 0 && last == nil {
		last = make(map[K]*casRowLock, len(pks))
	}
	for _, pk := range pks {
		earlier.add(last[pk])
		last[pk] = lock
	}
	return last
}

// deleteOwn deletes the entries of pks that still point to lock; a later CAS
// that replaced lock keeps its entry. Go maps do not shrink, so it drops a map
// once it is empty. While other CAS commits stay in line, the map keeps the
// buckets of the largest recent CAS. The caller holds keys.Lock.
func deleteOwn[K comparable](last map[K]*casRowLock, pks []K, lock *casRowLock) map[K]*casRowLock {
	for _, pk := range pks {
		if last[pk] == lock {
			delete(last, pk)
		}
	}
	if len(last) == 0 {
		return nil
	}
	return last
}

func (t *rowLockTable) registerOrdinary(lock *ordinaryRowLock) {
	t.ordMu.Lock()
	defer t.ordMu.Unlock()
	lock.registered.Store(true)
	lock.next = t.ordinary
	if t.ordinary != nil {
		t.ordinary.prev = lock
	}
	t.ordinary = lock
}

func (t *rowLockTable) unregisterOrdinaryLocked(lock *ordinaryRowLock) {
	if lock.prev != nil {
		lock.prev.next = lock.next
	} else {
		t.ordinary = lock.next
	}
	if lock.next != nil {
		lock.next.prev = lock.prev
	}
	lock.prev, lock.next = nil, nil
	lock.registered.Store(false)
}

// ordinaryDone returns a channel closed when a registered ordinary write
// releases, or nil if it has already released. Every caller waiting for the
// same write gets the same channel.
func (t *rowLockTable) ordinaryDone(lock *ordinaryRowLock) <-chan struct{} {
	t.ordMu.Lock()
	defer t.ordMu.Unlock()
	if !lock.registered.Load() {
		return nil
	}
	if lock.done == nil {
		lock.done = make(chan struct{})
	}
	return lock.done
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
		// Admission freezes the write set and rejects a commit that arrives
		// while a body is still in flight.
		pks, err := i.state.admitTxnCommit(txnContext.TxnID)
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
	// all body PKs, so a CAS waiter never blocks its own body. A body arriving
	// after its CommitTxn is rejected.
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
	pks, ok, err := extractPKsFromDeleteWithContext(ctx, msg)
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
		set.pks, err = extractPKsFromOrdinaryInsertWithContext(ctx, msg, i.pkDescriptorGetter)
	}
	return set, err
}
