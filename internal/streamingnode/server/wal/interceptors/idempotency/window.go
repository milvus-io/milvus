package idempotency

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
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
	MaxBytes     int
	MaxKeyLength int
	Now          func() time.Time
}

type Window struct {
	mu sync.Mutex

	entries  map[IdempotencyKey]*recovery.SummaryRecord
	inflight map[IdempotencyKey]*PendingEntry

	commitOrder []IdempotencyKey

	// maxBytes is the only retention bound. Nothing is evicted while the window
	// is under it; once it is reached, entries are replaced oldest-first by
	// commit order. There is deliberately no TTL: a horizon expressed in time is
	// invalidated by time passing, so after an outage the window would be empty
	// exactly when a resuming client needs it. A byte bound is invalidated only
	// by new data, which is the condition under which forgetting is safe.
	// 0 disables the cap.
	maxBytes int
	bytes    int
	now      func() time.Time
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
	Entry    *recovery.SummaryRecord
	Err      error
}

type CommitResult struct {
	CommitTimeTick         uint64
	MessageID              *commonpb.MessageID
	LastConfirmedMessageID *commonpb.MessageID
	IdempotentResult       *messagespb.IdempotentInsertResult
}

type PendingResult struct {
	Entry *recovery.SummaryRecord
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
		entries:  make(map[IdempotencyKey]*recovery.SummaryRecord),
		inflight: make(map[IdempotencyKey]*PendingEntry),
		maxBytes: config.MaxBytes,
		now:      now,
	}
}

func NewWindowFromSnapshot(config WindowConfig, snapshot *recovery.VChannelSummarySnapshot) *Window {
	window := NewWindow(config)
	if snapshot == nil {
		return window
	}
	// Everything the store handed over is immediately servable, however old it
	// is. That is the point of restoring it: an upstream resuming from its
	// breakpoint after an outage must be deduplicated, and age is exactly the
	// wrong reason to refuse.
	for _, record := range snapshot.Records {
		if record == nil || record.IdempotencyKey == "" {
			continue
		}
		key := IdempotencyKey(record.IdempotencyKey)
		window.entries[key] = record
		window.bytes += record.Size()
		window.commitOrder = append(window.commitOrder, key)
	}
	return window
}

func (w *Window) Begin(key IdempotencyKey, msg message.MutableMessage) BeginResult {
	w.mu.Lock()
	defer w.mu.Unlock()

	// Retention is the only thing that decides visibility: an entry still held is
	// still servable. Eviction removes entries; it never leaves an unservable one
	// behind.
	if entry, ok := w.entries[key]; ok {
		observeWindowDuplicate(vchannelOf(msg))
		return BeginResult{Decision: BeginDecisionDuplicate, Entry: entry}
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

	entry := &recovery.SummaryRecord{
		SourceMessageID:        result.MessageID,
		SourceTimeTick:         result.CommitTimeTick,
		LastConfirmedMessageID: result.LastConfirmedMessageID,
		IdempotencyKey:         string(pending.Key),
		InsertResult:           result.IdempotentResult,
	}
	delete(w.inflight, pending.Key)
	w.entries[pending.Key] = entry
	w.bytes += entry.Size()
	w.insertCommitOrderLocked(pending.Key, entry.SourceTimeTick)
	evicted := w.evictLocked()
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
		if ok && prev.SourceTimeTick <= commitTT {
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

// evictLocked enforces the byte cap by replacing the oldest entries. It is the
// only retention mechanism: entries are removed for capacity, never for age.
func (w *Window) evictLocked() int {
	evicted := 0
	for w.maxBytes > 0 && w.bytes > w.maxBytes && len(w.commitOrder) > 0 {
		key := w.commitOrder[0]
		w.commitOrder = w.commitOrder[1:]
		entry, ok := w.entries[key]
		if !ok {
			continue
		}
		w.bytes -= entry.Size()
		delete(w.entries, key)
		evicted++
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
