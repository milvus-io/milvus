package idempotency

import (
	"context"
	"sync"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/util/tsoutil"
)

type IdempotencyKey string

type BeginDecision int

const (
	BeginDecisionOwner BeginDecision = iota
	BeginDecisionWait
	BeginDecisionDuplicate
)

type WindowConfig struct {
	Enabled      bool
	WindowTTL    time.Duration
	MinEntries   int
	MaxBytes     int
	MaxKeyLength int
	Now          func() time.Time
}

type Window struct {
	mu sync.Mutex

	entries  map[IdempotencyKey]*streamingpb.WindowEntry
	inflight map[IdempotencyKey]*PendingEntry

	commitOrder          []IdempotencyKey
	evictedWatermarkTT   uint64
	snapshotCheckpointTT uint64
	// ttlEvictBoundTT is the highest TTL eviction bound this window has ever been
	// asked to apply (commitTT of "now" minus the TTL). Entries older than it are
	// TTL-expired even when the minEntries floor still keeps them in memory; see
	// servableLocked.
	ttlEvictBoundTT uint64

	minEntries int
	// maxBytes caps the total serialized size of retained entries (each entry
	// carries the per-row PKs of its insert), overriding both the minEntries floor
	// and the TTL horizon: entry COUNT alone cannot bound dedup-metadata memory.
	// 0 disables the cap.
	maxBytes  int
	bytes     int
	windowTTL time.Duration
	now       func() time.Time
}

type PendingEntry struct {
	Key       IdempotencyKey
	State     EntryState
	StartedAt time.Time
	// done is closed exactly once by the owner (Complete/Fail). It carries no
	// value: the result is published into result before done is closed, so any
	// number of waiters can read it after observing the close. A buffered
	// single-value channel would only deliver to the first reader and hand the
	// rest the zero value, breaking concurrent duplicate requests.
	done   chan struct{}
	result PendingResult
}

type EntryState int

const (
	EntryStateAdding EntryState = iota
)

type BeginResult struct {
	Decision BeginDecision
	Pending  *PendingEntry
	Entry    *streamingpb.WindowEntry
	Err      error
}

type CommitResult struct {
	CommitTimeTick         uint64
	MessageID              *commonpb.MessageID
	LastConfirmedMessageID *commonpb.MessageID
	IdempotentResult       *messagespb.IdempotentInsertResult
}

type PendingResult struct {
	Entry *streamingpb.WindowEntry
	Err   error
	// OwnerResolved reports that this result was published by the owner
	// (Complete or Fail), which necessarily happens after the owner's Build
	// consumed its txn insert-result buffer — so a same-txnID waiter may safely
	// reclaim the (vchannel, txnID) buffer. It stays false when Wait exited on
	// the waiter's own context: the owner may not have reached Build yet, and
	// removing the buffer then would destroy the owner's un-built results.
	OwnerResolved bool
}

func NewWindow(config WindowConfig) *Window {
	now := config.Now
	if now == nil {
		now = time.Now
	}
	return &Window{
		entries:    make(map[IdempotencyKey]*streamingpb.WindowEntry),
		inflight:   make(map[IdempotencyKey]*PendingEntry),
		minEntries: config.MinEntries,
		maxBytes:   config.MaxBytes,
		windowTTL:  config.WindowTTL,
		now:        now,
	}
}

func NewWindowFromSnapshot(config WindowConfig, snapshot *streamingpb.WindowSnapshot) *Window {
	window := NewWindow(config)
	if snapshot == nil {
		return window
	}
	window.snapshotCheckpointTT = snapshot.GetSnapshotCheckpointTimetick()
	window.evictedWatermarkTT = snapshot.GetEvictedWatermarkTimetick()
	// The recovery-side store deliberately retains past-TTL entries (minEntries
	// floor), so a restored window may hold entries that must no longer answer
	// duplicates. Seed the TTL visibility bound from the snapshot checkpoint
	// timetick — the latest clock the snapshot vouches for — so those entries
	// are unservable immediately at WAL open instead of only after the first
	// TimeTick sweep. The bound is conservative (checkpoint TT <= now) and
	// evictLocked only ever advances it.
	if config.WindowTTL > 0 {
		window.ttlEvictBoundTT = evictBeforeCommitTT(window.snapshotCheckpointTT, config.WindowTTL)
	}
	for _, snapshotEntry := range snapshot.GetEntries() {
		if snapshotEntry == nil {
			continue
		}
		key := IdempotencyKey(snapshotEntry.GetKey())
		window.entries[key] = snapshotEntry
		window.bytes += proto.Size(snapshotEntry)
		window.commitOrder = append(window.commitOrder, key)
	}
	window.refreshEvictedWatermarkLocked()
	return window
}

func (w *Window) Begin(key IdempotencyKey, msg message.MutableMessage) BeginResult {
	w.mu.Lock()
	defer w.mu.Unlock()

	if entry, ok := w.entries[key]; ok {
		if w.servableLocked(entry) {
			observeWindowDuplicate(vchannelOf(msg))
			return BeginResult{Decision: BeginDecisionDuplicate, Entry: entry}
		}
		// TTL-expired but still held by the minEntries floor: serving it would make
		// the floor extend duplicate visibility forever on quiet shards. For entries
		// still retained in memory, duplicate visibility ends at the TTL bound. The
		// hard maxBytes cap below is a stricter capacity limit and may remove an
		// entry before TTL; that is a documented retention limitation, not an
		// extension of the TTL answer.
		w.dropEntryLocked(key)
	}

	if pending, ok := w.inflight[key]; ok {
		return BeginResult{Decision: BeginDecisionWait, Pending: pending}
	}

	pending := &PendingEntry{
		Key:       key,
		State:     EntryStateAdding,
		StartedAt: w.now(),
		done:      make(chan struct{}),
	}
	w.inflight[key] = pending
	observeWindowInflight(vchannelOf(msg), len(w.inflight))
	return BeginResult{Decision: BeginDecisionOwner, Pending: pending}
}

func (w *Window) Complete(pending *PendingEntry, result CommitResult, msg message.MutableMessage) (bool, int) {
	w.mu.Lock()
	defer w.mu.Unlock()

	current, ok := w.inflight[pending.Key]
	if !ok || current != pending {
		return false, 0
	}

	entry := &streamingpb.WindowEntry{
		Key:                    string(pending.Key),
		CommitTimetick:         result.CommitTimeTick,
		MessageId:              result.MessageID,
		LastConfirmedMessageId: result.LastConfirmedMessageID,
		IdempotentResult:       result.IdempotentResult,
	}
	delete(w.inflight, pending.Key)
	w.entries[pending.Key] = entry
	w.bytes += proto.Size(entry)
	w.insertCommitOrderLocked(pending.Key, entry.GetCommitTimetick())
	evicted := 0
	if w.windowTTL > 0 {
		evicted = w.evictLocked(evictBeforeCommitTT(entry.GetCommitTimetick(), w.windowTTL))
	} else if w.maxBytes > 0 {
		evicted = w.evictLocked(0)
	}
	w.refreshEvictedWatermarkLocked()
	observeWindowEviction(vchannelOf(msg), evicted)
	observeWindowEntries(vchannelOf(msg), len(w.entries))
	observeWindowInflight(vchannelOf(msg), len(w.inflight))

	pending.result = PendingResult{Entry: entry, OwnerResolved: true}
	close(pending.done)
	return true, evicted
}

func (w *Window) Fail(pending *PendingEntry, err error, msg message.MutableMessage) bool {
	w.mu.Lock()
	defer w.mu.Unlock()

	current, ok := w.inflight[pending.Key]
	if !ok || current != pending {
		return false
	}
	delete(w.inflight, pending.Key)
	observeWindowInflight(vchannelOf(msg), len(w.inflight))
	pending.result = PendingResult{Err: err, OwnerResolved: true}
	close(pending.done)
	return true
}

func (p *PendingEntry) Wait(ctx context.Context, msg message.MutableMessage) PendingResult {
	select {
	case <-p.done:
		// The close of p.done happens-after the owner published p.result, so
		// every waiter observes the same committed result here.
		if p.result.Err == nil {
			observeWindowDuplicate(vchannelOf(msg))
		}
		return p.result
	case <-ctx.Done():
		return PendingResult{Err: ctx.Err()}
	}
}

// Evict takes the vchannel name directly (not a message) so the idle-vchannel
// TTL sweep — which has no message for the window it is sweeping — still
// reports its evictions and refreshes the entry gauge.
func (w *Window) Evict(evictBeforeTT uint64, vchannel string) int {
	w.mu.Lock()
	defer w.mu.Unlock()
	evicted := w.evictLocked(evictBeforeTT)
	w.refreshEvictedWatermarkLocked()
	observeWindowEviction(vchannel, evicted)
	observeWindowEntries(vchannel, len(w.entries))
	return evicted
}

// evictBeforeCommitTT derives the TTL eviction bound from the just-committed
// entry's timetick instead of the local wall clock, so the live window and the
// clock-free recovery-side window (evictBeforeTimetick in the recovery package)
// retain the same key set under NTP skew. The physical guard keeps a timetick
// younger than the TTL from underflowing into an evict-everything bound.
func evictBeforeCommitTT(commitTT uint64, ttl time.Duration) uint64 {
	if ttl <= 0 {
		return 0
	}
	physical, logical := tsoutil.ParseHybridTs(commitTT)
	msecs := ttl.Milliseconds()
	if physical <= msecs {
		return 0
	}
	return tsoutil.ComposeTS(physical-msecs, logical)
}

// insertCommitOrderLocked keeps commitOrder sorted by commit timetick.
// Completion order is NOT commit-timetick order: this interceptor is outermost,
// the timetick is assigned by the inner timetick interceptor, and concurrent
// appends on one vchannel may complete out of order. Eviction and the evicted
// watermark both read commitOrder head as "oldest", and the recovery-side
// window sorts its entries, so the live window must keep the same invariant.
// Entries arrive near-sorted, so the tail walk is O(1) amortized.
func (w *Window) insertCommitOrderLocked(key IdempotencyKey, commitTT uint64) {
	i := len(w.commitOrder)
	for i > 0 {
		prev, ok := w.entries[w.commitOrder[i-1]]
		if ok && prev.GetCommitTimetick() <= commitTT {
			break
		}
		if !ok {
			// Stale key without an entry cannot be compared; keep walking past it.
			i--
			continue
		}
		i--
	}
	w.commitOrder = append(w.commitOrder, "")
	copy(w.commitOrder[i+1:], w.commitOrder[i:])
	w.commitOrder[i] = key
}

// servableLocked reports whether an entry may still answer a duplicate hit.
// For entries still retained in memory, the minEntries floor must not extend
// duplicate visibility beyond the TTL bound. Hard capacity caps are different:
// maxBytes may delete entries before TTL to bound memory, at which point there
// is no entry left to serve.
func (w *Window) servableLocked(entry *streamingpb.WindowEntry) bool {
	if w.windowTTL <= 0 || w.ttlEvictBoundTT == 0 {
		return true
	}
	return entry.GetCommitTimetick() >= w.ttlEvictBoundTT
}

// dropEntryLocked removes a retained-but-unservable entry together with its
// commitOrder slot. The slot must go too: Complete re-appends the key at its new
// commit timetick, and a leftover old slot would resolve to that new entry and
// stall eviction of everything queued behind it.
func (w *Window) dropEntryLocked(key IdempotencyKey) {
	if entry, ok := w.entries[key]; ok {
		w.bytes -= proto.Size(entry)
	}
	delete(w.entries, key)
	for i, ordered := range w.commitOrder {
		if ordered == key {
			w.commitOrder = append(w.commitOrder[:i], w.commitOrder[i+1:]...)
			break
		}
	}
	w.refreshEvictedWatermarkLocked()
}

func (w *Window) evictLocked(evictBeforeTT uint64) int {
	if evictBeforeTT > w.ttlEvictBoundTT {
		w.ttlEvictBoundTT = evictBeforeTT
	}
	evicted := 0
	for len(w.entries) > w.minEntries && len(w.commitOrder) > 0 {
		key := w.commitOrder[0]
		w.commitOrder = w.commitOrder[1:]
		entry, ok := w.entries[key]
		if !ok {
			continue
		}
		if entry.GetCommitTimetick() >= evictBeforeTT {
			w.commitOrder = append([]IdempotencyKey{key}, w.commitOrder...)
			break
		}
		w.bytes -= proto.Size(entry)
		delete(w.entries, key)
		evicted++
	}

	// The byte cap is a hard bound: it overrides the minEntries floor and may
	// shorten the effective dedup horizon below TTL, because a floor measured in
	// entries cannot promise anything about memory when each entry carries an
	// unbounded per-row PK list.
	for w.maxBytes > 0 && w.bytes > w.maxBytes && len(w.commitOrder) > 0 {
		key := w.commitOrder[0]
		w.commitOrder = w.commitOrder[1:]
		if entry, ok := w.entries[key]; ok {
			w.bytes -= proto.Size(entry)
			delete(w.entries, key)
			evicted++
		}
	}
	return evicted
}

func (w *Window) Len() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.entries)
}

func (w *Window) InflightLen() int {
	w.mu.Lock()
	defer w.mu.Unlock()
	return len(w.inflight)
}

func (w *Window) EvictedWatermarkTT() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.evictedWatermarkTT
}

func (w *Window) SnapshotCheckpointTT() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.snapshotCheckpointTT
}

func (w *Window) SetSnapshotCheckpointTT(tt uint64) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.snapshotCheckpointTT = tt
	w.refreshEvictedWatermarkLocked()
}

func (w *Window) refreshEvictedWatermarkLocked() {
	for len(w.commitOrder) > 0 {
		key := w.commitOrder[0]
		entry, ok := w.entries[key]
		if !ok {
			w.commitOrder = w.commitOrder[1:]
			continue
		}
		// The watermark is inclusive: it points to the oldest retained entry, not a strict evicted lower bound.
		w.evictedWatermarkTT = entry.GetCommitTimetick()
		return
	}
	w.evictedWatermarkTT = w.snapshotCheckpointTT
}
