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

// Package pkindex is the primary key index of a streaming node: the index of
// each vchannel (Authority) and the write path decisions made with it
// (Decider): which keys of an insert need a companion delete, which keys of a
// delete exist, and how the writes of a transaction take effect at its commit.
package pkindex

import (
	"context"
	"slices"
	"sync"
	"sync/atomic"
	"time"
)

// WriteKind is the kind of write the caller is about to append.
type WriteKind int

const (
	KindInsert WriteKind = iota
	KindDelete
	KindCommitTxn
	KindRollbackTxn
)

// Request describes one write in primary key index terms.
type Request struct {
	Kind     WriteKind
	VChannel string
	PKs      []PK  // The keys of an insert or delete. Empty for commit and rollback.
	TxnID    int64 // 0 for an autocommit write
}

// Decision tells the caller what to append.
type Decision struct {
	CompanionDeletePKs []PK // keys of an insert that need a companion delete in the same transaction
	PresentPKs         []PK // keys of a delete that exist in the index, in request order. Nil when none exists.
}

// Applied is what the caller read back from the appended main message.
type Applied struct {
	// TimeTick is the time tick at which the write takes effect: the time tick of
	// the CommitTxn for a write that was upgraded to a transaction.
	TimeTick uint64
}

// Decider makes the write path decisions of one vchannel on top of its Authority.
//
// The decider never writes the engine. A decided write is kept in an in-flight
// set from before its append until observe applied it to the engine. Observe is
// the single writer of the engine, for the replay at WAL open and for the live
// messages alike.
//
// TODO: duplicate primary keys inside one insert are not detected.
// TODO: observe does not apply the WAL to the engine yet, so a decided write stays
// in the in-flight set for the life of the index. The recovery step adds observe.
type Decider struct {
	authority Authority
	inflight  inflightSet
	// degradedUntil is the time tick before which this vchannel is degraded: while
	// observe has not reached it, every key counts as present and the engine is
	// not probed. See DegradeUntil.
	degradedUntil atomic.Uint64

	// pendingMu guards pending and every pendingTxn in it. No code waits for an
	// in-flight write while it holds pendingMu.
	pendingMu sync.Mutex
	pending   map[int64]*pendingTxn
}

// NewDecider creates the Decider of a vchannel. It takes ownership of a.
func NewDecider(a Authority) *Decider {
	return &Decider{
		authority: a,
		pending:   make(map[int64]*pendingTxn),
	}
}

// DegradeUntil makes the vchannel degraded until observe reaches tt. A degraded
// decision treats every key as present: an insert gets a companion delete for
// each of its keys. That is always correct and costs only extra deletes. It
// never blocks a write.
//
// It covers the events that the in-flight set can not cover, because writes that
// took effect before them did not pass through this decider.
//
// TODO: call it when the index of a vchannel is enabled by CreateIndex, with the
// time tick of that message.
// TODO: call it when the WAL is promoted from a secondary to a primary, with the
// time tick of the role switch.
// TODO: call it when the engine recovers from a failure.
func (d *Decider) DegradeUntil(tt uint64) {
	for {
		cur := d.degradedUntil.Load()
		if tt <= cur || d.degradedUntil.CompareAndSwap(cur, tt) {
			return
		}
	}
}

func (d *Decider) degraded() bool {
	return d.inflight.observedTimeTick() < d.degradedUntil.Load()
}

// Decide makes the decision for req. For an autocommit write and for a CommitTxn
// it registers the affected keys in the in-flight set and waits for the earlier
// writes of the same keys, before it reads the index. The returned Intent holds
// that registration, which is the lock of those keys until Apply or Discard. The
// caller must call exactly one of Apply, Discard and DiscardUncertain, exactly
// once, after the downstream append. A panic between Decide and that call ends
// the process, like every panic on the append path.
func (d *Decider) Decide(ctx context.Context, req Request) (*Intent, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-d.authority.Ready():
	}

	intent := &Intent{d: d, req: req}
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
			// applied between this snapshot and this Intent's Apply. Apply ends
			// the process when that rule is broken.
			intent.txn = txn.snapshot()
		}
		d.pendingMu.Unlock()
		if intent.txn == nil {
			return intent, nil
		}
		// pendingMu must be released before the wait: a body of another
		// transaction takes pendingMu in Apply.
		if err := intent.register(ctx, intent.txn.allPKs()); err != nil {
			return nil, err
		}
		hits, err := d.present(ctx, intent.write, intent.txn.companionCandidates())
		if err != nil {
			intent.withdraw()
			return nil, err
		}
		intent.decision.CompanionDeletePKs = hits
		return intent, nil
	case req.TxnID != 0:
		// A transaction body takes effect at the commit time tick, so it is decided at commit.
		return intent, nil
	}

	if err := intent.register(ctx, req.PKs); err != nil {
		return nil, err
	}
	present, err := d.present(ctx, intent.write, req.PKs)
	if err != nil {
		intent.withdraw()
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

// present returns the keys of pks that exist, in the order of pks. w is the
// caller's own registration, which has already waited for the earlier writes.
//
// Rule B: the in-flight set is read before the engine. Observe writes the engine
// before it removes an entry (rule A), so a key that left the set between the two
// reads is found in the engine.
func (d *Decider) present(ctx context.Context, w *inflightWrite, pks []PK) ([]PK, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	if d.degraded() {
		return slices.Clone(pks), nil
	}
	inSet := d.inflight.presentBefore(w)
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
func (d *Decider) probe(ctx context.Context, pks []PK) ([]PK, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	entries, err := d.authority.MultiGet(ctx, pks)
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

// PendingTxnIDs returns the transactions that have pending writes.
func (d *Decider) PendingTxnIDs() []int64 {
	d.pendingMu.Lock()
	defer d.pendingMu.Unlock()
	ids := make([]int64, 0, len(d.pending))
	for id := range d.pending {
		ids = append(ids, id)
	}
	return ids
}

// DropTxn forgets the pending writes of a transaction that expired or was failed.
func (d *Decider) DropTxn(txnID int64) {
	d.pendingMu.Lock()
	defer d.pendingMu.Unlock()
	delete(d.pending, txnID)
}

// Close closes the underlying Authority.
func (d *Decider) Close() error {
	return d.authority.Close()
}

// register adds the keys of this write to the in-flight set and waits for the
// earlier writes of the same keys. It must run before the downstream append:
// observe may apply a write as soon as its append is acknowledged, which can be
// before the append returns, so a registration made after the append could be
// left in the set forever.
func (i *Intent) register(ctx context.Context, pks []PK) error {
	start := time.Now()
	w, earlier := i.d.inflight.register(pks)
	if err := i.d.inflight.wait(ctx, w, earlier); err != nil {
		return err
	}
	i.write = w
	i.lockWait = time.Since(start)
	return nil
}

// withdraw removes the registration of a decision that failed before anything
// was appended.
func (i *Intent) withdraw() {
	i.d.inflight.remove(i.write)
	i.write = nil
}
