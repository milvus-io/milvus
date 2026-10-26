// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dedup

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cockroachdb/errors"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
)

// decider makes the write path decisions of one vchannel on top of its
// engine.
//
// The decider never writes the engine. A decided write is kept in an
// in-flight set from before its append until the observe of the WAL recovery
// applied it to the engine and reported that with Observe. The observe is the
// single writer of the engine, for the replay at WAL open and for the live
// messages alike.
//
// TODO: duplicate primary keys inside one insert are not detected.
// TODO: the observe does not apply the WAL to the engine yet, so a decided
// write stays in the in-flight set for the life of the index. The recovery
// change adds the observe.
type decider struct {
	eng      engine.RW
	degraded func() bool
	closed   atomic.Bool
	inflight inflightSet
	// waitUntil is the time tick before which the writes of this vchannel wait:
	// while observe has not reached it, Decide returns a *WaitError. See WaitUntil.
	waitUntil atomic.Uint64
	// stop is closed by Close. It ends the waits of WaitError.Wait.
	stop     chan struct{}
	stopOnce sync.Once

	// pendingMu guards pending and every pendingTxn in it. No code waits for an
	// in-flight write while it holds pendingMu.
	pendingMu sync.Mutex
	pending   map[int64]*pendingTxn
}

// New creates the Decider of a vchannel over its engine. It does not own
// the engine: the caller closes it, after Close.
func New(eng engine.RW, cfg Config) Decider {
	return &decider{
		eng:      eng,
		degraded: cfg.Degraded,
		stop:     make(chan struct{}),
		pending:  make(map[int64]*pendingTxn),
	}
}

// WaitUntil makes the writes of the vchannel wait until observe reaches tt.
// While observe has not reached it, Decide returns a *WaitError for every write
// that registers keys, before it registers or probes anything. The caller waits
// for it outside the WAL lock and decides again. A write that waits gets an
// exact decision afterwards, not a companion delete for every key.
//
// It covers the events that the in-flight set can not cover, because writes that
// took effect before them did not pass through this decider. A not-ready index
// (Config.Degraded) never waits: it decides degraded.
//
// tt must be the time tick of a persisted WAL message, or not above the observed
// time tick. Only persisted messages move the observed time tick: the applied
// writes and the persisted TimeTicks. The TimeTick after a persisted message is
// always persisted, but on an idle vchannel the TimeTicks are not, so a tt of no
// persisted message makes the writes wait until the next persisted message.
//
// TODO: call it when the index of a vchannel is enabled by CreateIndex, with the
// time tick of that message.
// TODO: call it when the WAL is promoted from a secondary to a primary, with the
// time tick of the role switch.
// TODO: call it when the engine recovers from a failure.
func (d *decider) WaitUntil(tt uint64) {
	for {
		cur := d.waitUntil.Load()
		if tt <= cur || d.waitUntil.CompareAndSwap(cur, tt) {
			return
		}
	}
}

// isDegraded reports whether a decision treats every key as present: the
// committed table set is not installed (Config.Degraded), or a WaitUntil raced
// with this decision after it passed the wait check.
func (d *decider) isDegraded() bool {
	if d.degraded != nil && d.degraded() {
		return true
	}
	return d.inflight.observedTimeTick() < d.waitUntil.Load()
}

// waitError returns a *WaitError when a write that registers keys must wait for
// observe, nil otherwise. A not-ready index decides degraded and never waits.
func (d *decider) waitError(vchannel string) error {
	if d.degraded != nil && d.degraded() {
		return nil
	}
	target := d.waitUntil.Load()
	if d.inflight.observedTimeTick() >= target {
		return nil
	}
	return &WaitError{TimeTick: target, d: d, vchannel: vchannel}
}

// Observe tells the decider that the observe of the WAL recovery applied
// every write up to tt to the engine. pks are the keys of the write at tt, none
// for a TimeTick. It removes the committed entries of pks from the in-flight
// set (rule A: the engine first, then the set), raises the observed time tick
// and removes the in-doubt entries that it passed. The pchannel
// calls it for every index when its driver wakes, so an idle vchannel sheds
// them too.
func (d *decider) Observe(tt uint64, pks []PK) {
	d.inflight.observe(tt, pks)
}

func (d *decider) Decide(ctx context.Context, req DecideRequest) (*WriteIntent, error) {
	if d.closed.Load() {
		return nil, errClosed(req.VChannel)
	}
	return d.decide(ctx, &WriteIntent{d: d, req: req})
}

// errClosed reports a Decide after Close.
func errClosed(vchannel string) error {
	return errors.Mark(errors.Wrapf(engine.ErrClosed, "primary key index of vchannel %s is closed", vchannel), pkroot.ErrUnavailable)
}

func (d *decider) decide(ctx context.Context, intent *WriteIntent) (*WriteIntent, error) {
	req := intent.req
	switch {
	case req.Kind == KindRollbackTxn:
		return intent, nil
	case req.Kind == KindCommitTxn:
		d.pendingMu.Lock()
		if txn, ok := d.pending[req.TxnID]; ok {
			// WAL contract this snapshot depends on (timetick_and_txn.md,
			// "Transaction Lifecycle", Commit): a producer appends the CommitTxn
			// only after it received the results of all bodies, and one producer
			// drives one transaction. So no body of the same transaction is
			// applied between this snapshot and Committed of this WriteIntent.
			// Committed ends the process when that rule is broken.
			intent.txn = txn.snapshot()
		}
		d.pendingMu.Unlock()
		if intent.txn == nil {
			return intent, nil
		}
		if err := d.waitError(req.VChannel); err != nil {
			return nil, err
		}
		// pendingMu must be released before the wait: a body of another
		// transaction takes pendingMu in Committed.
		if err := intent.acquire(ctx, intent.txn.allPKs()); err != nil {
			return nil, err
		}
		hits, err := d.existing(ctx, intent.write, intent.txn.companionCandidates())
		if err != nil {
			intent.abort()
			return nil, err
		}
		intent.decision.CompanionDeletePKs = hits
		return intent, nil
	case req.TxnID != 0:
		// A transaction body takes effect at the commit time tick, so it is decided at commit.
		return intent, nil
	}

	if err := d.waitError(req.VChannel); err != nil {
		return nil, err
	}
	if err := intent.acquire(ctx, req.PKs); err != nil {
		return nil, err
	}
	present, err := d.existing(ctx, intent.write, req.PKs)
	if err != nil {
		intent.abort()
		return nil, err
	}
	switch req.Kind {
	case KindInsert:
		intent.decision.CompanionDeletePKs = present
	case KindDelete:
		intent.decision.PresentPKs = present
	}
	return intent, nil
}

// existing returns the keys of pks that exist, in the order of pks. w is the
// caller's own write in the in-flight set, which has already waited for the
// writes ahead of it.
//
// Rule B: the in-flight set is read before the engine. Observe writes the engine
// before it removes an entry (rule A), so a key that left the set between the two
// reads is found in the engine.
func (d *decider) existing(ctx context.Context, w *inflightWrite, pks []PK) ([]PK, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	if d.isDegraded() {
		return slices.Clone(pks), nil
	}
	inSet := d.inflight.keysAhead(w)
	var probe []PK
	for _, pk := range pks {
		if _, ok := inSet[pk.lockKey()]; !ok {
			probe = append(probe, pk)
		}
	}
	if len(probe) == len(pks) && len(probe) > 0 {
		// The common case: no key is in the set, the engine answers alone.
		return d.probe(ctx, probe)
	}
	inEngine, err := d.probe(ctx, probe)
	if err != nil {
		return nil, err
	}
	var present []PK
	j := 0
	for _, pk := range pks {
		if _, ok := inSet[pk.lockKey()]; ok {
			present = append(present, pk)
			continue
		}
		if j < len(inEngine) && pk == inEngine[j] {
			present = append(present, pk)
			j++
		}
	}
	return present, nil
}

// probe returns the keys of pks that the engine holds, in the order of pks.
func (d *decider) probe(ctx context.Context, pks []PK) ([]PK, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	entries, err := lookup(ctx, d.eng, pks)
	if err != nil {
		return nil, err
	}
	var present []PK
	for i, entry := range entries {
		if entry != nil {
			present = append(present, pks[i])
		}
	}
	return present, nil
}

func (d *decider) DropEndedTxns(ended func(txnID int64) bool) {
	d.pendingMu.Lock()
	defer d.pendingMu.Unlock()
	for id := range d.pending {
		if ended(id) {
			delete(d.pending, id)
		}
	}
}

// dropTxn forgets the pending writes of a transaction that its CommitTxn or
// RollbackTxn ended.
func (d *decider) dropTxn(txnID int64) {
	d.pendingMu.Lock()
	defer d.pendingMu.Unlock()
	delete(d.pending, txnID)
}

func (d *decider) Close() {
	d.closed.Store(true)
	d.stopOnce.Do(func() { close(d.stop) })
}

// acquire enqueues the keys of this write in the in-flight set and waits for the
// writes ahead of it, which makes it the lock of those keys. It must run before the downstream append:
// observe may apply a write as soon as its append is acknowledged, which can be
// before the append returns, so an entry enqueued after the append could be
// left in the set forever.
func (i *WriteIntent) acquire(ctx context.Context, pks []PK) error {
	start := time.Now()
	w, ahead := i.d.inflight.enqueue(pks)
	if err := i.d.inflight.waitAhead(ctx, w, ahead); err != nil {
		return err
	}
	i.write = w
	i.lockWait = time.Since(start)
	return nil
}

// abort removes the in-flight entry of a decision that failed before anything
// was appended.
func (i *WriteIntent) abort() {
	i.d.inflight.abort(i.write)
	i.write = nil
}
