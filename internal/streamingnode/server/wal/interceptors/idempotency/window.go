package idempotency

import (
	"container/list"
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/idempotencyview"
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
	TTL          time.Duration
	MaxSize      int
	MinCount     int
	MaxKeyLength int
	Now          func() time.Time
}

// Window owns one VChannel's key scope and pending requests. Committed
// records share the PChannel manager's mutex, budget and LRU lifecycle.
type Window struct {
	mu        *sync.Mutex
	manager   *windowManager
	vchannel  string
	entries   map[IdempotencyKey]*cachedRecord
	inflight  map[IdempotencyKey]*PendingEntry
	order     list.List
	heapIndex int
	bytes     int
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
	Entry    *idempotencyview.Record
	Err      error
}

type CommitResult struct {
	CommitTimeTick         uint64
	MessageID              *commonpb.MessageID
	LastConfirmedMessageID *commonpb.MessageID
	IdempotentResult       *messagespb.IdempotentInsertResult
}

type PendingResult struct {
	Entry *idempotencyview.Record
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
	return newWindowManager(config).newWindow("")
}

func NewWindowFromSnapshot(config WindowConfig, snapshot *idempotencyview.Snapshot) *Window {
	if snapshot == nil {
		return NewWindow(config)
	}
	manager := newWindowManager(config)
	return manager.restore(map[string]*idempotencyview.Snapshot{snapshot.VChannel: snapshot})[snapshot.VChannel]
}

func (w *Window) Begin(key IdempotencyKey, msg message.MutableMessage) BeginResult {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.vchannel == "" {
		w.vchannel = vchannelOf(msg)
	}
	now := w.manager.config.Now()
	w.manager.expireLocked(now, 256)
	if entry, ok := w.entries[key]; ok {
		if !w.manager.expired(entry, now) {
			w.manager.touchLocked(entry, now)
			observeWindowDuplicate(vchannelOf(msg))
			return BeginResult{Decision: BeginDecisionDuplicate, Entry: entry.Record}
		}
		w.manager.removeLocked(entry)
	}

	if pending, ok := w.inflight[key]; ok {
		return BeginResult{Decision: BeginDecisionWait, Pending: pending}
	}

	pending := &PendingEntry{
		Key:       key,
		State:     EntryStateAdding,
		StartedAt: now,
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

	delete(w.inflight, pending.Key)
	entry, evicted := w.storeLocked(pending.Key, result)
	observeWindowEntries(vchannelOf(msg), len(w.entries))
	observeWindowInflight(vchannelOf(msg), len(w.inflight))

	pending.result = PendingResult{Entry: entry, OwnerResolved: true}
	close(pending.done)
	return true, evicted
}

// Record remembers a write that this window never arbitrated: a replicated
// write, which the replicate interceptor appends unconditionally. There is no
// inflight phase and no waiter to resolve. An entry already held for the key is
// replaced: the source cluster evicted the key and a client legally reused it,
// so the newer write is the one a retry must see. Returns the number of
// entries evicted by the byte cap.
func (w *Window) Record(key IdempotencyKey, result CommitResult, msg message.MutableMessage) int {
	w.mu.Lock()
	defer w.mu.Unlock()

	_, evicted := w.storeLocked(key, result)
	observeWindowEntries(vchannelOf(msg), len(w.entries))
	return evicted
}

// Forget drops the entry held for key, if any. It is used when a write for key
// landed but its result cannot be recorded: an entry still held for the key
// would then describe an older write, and would answer a retry with that
// write's primary keys. Without the entry the retry appends again. inflight is
// untouched: a replicated write never owns an inflight slot.
func (w *Window) Forget(key IdempotencyKey) {
	w.mu.Lock()
	defer w.mu.Unlock()

	if old := w.entries[key]; old != nil {
		w.manager.removeLocked(old)
	}
}

func (w *Window) storeLocked(key IdempotencyKey, result CommitResult) (*idempotencyview.Record, int) {
	entry := &idempotencyview.Record{
		SourceMessageID:        result.MessageID,
		SourceTimeTick:         result.CommitTimeTick,
		LastConfirmedMessageID: result.LastConfirmedMessageID,
		IdempotencyKey:         string(key),
		InsertResult:           result.IdempotentResult,
	}
	now := w.manager.config.Now()
	w.manager.addLocked(w, entry, now)
	return entry, w.manager.evictLocked(now)
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
