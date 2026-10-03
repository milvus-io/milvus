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

// Package dedup makes the write path decisions of the primary key index: which
// keys of an insert need a companion delete, which keys of a delete exist, and
// how the writes of a transaction take effect at its commit.
package dedup

import (
	"context"
	"sync"
	"time"

	"github.com/milvus-io/milvus/internal/pkindex/authority"
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
	PKs      []authority.PK // The keys of an insert or delete. Empty for commit and rollback.
	TxnID    int64          // 0 for an autocommit write
}

// Decision tells the caller what to append.
type Decision struct {
	CompanionDeletePKs []authority.PK // keys of an insert that need a companion delete in the same transaction
	PresentPKs         []authority.PK // keys of a delete that exist in the index, in request order. Nil when none exists.
}

// Applied is what the caller read back from the appended main message.
type Applied struct {
	SegmentID int64 // the segment an insert was assigned to
	// TimeTick is the time tick at which the write takes effect. This version of
	// the decider does not read it: the decision order of one key is enforced by
	// the stripe locks, not by comparing time ticks. The field is kept for callers
	// and for a future version that may need it.
	TimeTick uint64
}

// Decider makes the write path decisions of one vchannel on top of its Authority.
//
// TODO: duplicate primary keys inside one insert are not detected.
// TODO: pick the stripe count from a throughput benchmark.
type Decider struct {
	authority authority.Authority
	stripes   *stripeLocks

	// pendingMu guards pending and every pendingTxn in it. Lock order: stripes
	// first, then pendingMu. No code may wait for a stripe while it holds pendingMu.
	pendingMu sync.Mutex
	pending   map[int64]*pendingTxn
}

// New creates the Decider of a vchannel. It takes ownership of a.
func New(a authority.Authority, stripeCount int) *Decider {
	return &Decider{
		authority: a,
		stripes:   newStripeLocks(stripeCount),
		pending:   make(map[int64]*pendingTxn),
	}
}

// Decide makes the decision for req. For an autocommit write and for a CommitTxn it
// acquires the stripes of the affected keys, which the returned Intent holds until
// Apply or Discard. The caller must call exactly one of them, exactly once. A panic
// between Decide and that call ends the process, like every panic on the append path.
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
		// mu must be released before acquire: Apply takes mu while it holds the
		// stripes, so holding both here would deadlock with a concurrent Apply.
		intent.acquire(intent.txn.allPKs())
		hits, err := d.present(ctx, intent.txn.companionCandidates())
		if err != nil {
			intent.guard.Release()
			return nil, err
		}
		intent.decision.CompanionDeletePKs = hits
		return intent, nil
	case req.TxnID != 0:
		// A transaction body takes effect at the commit time tick, so it is decided at commit.
		return intent, nil
	}

	intent.acquire(req.PKs)
	present, err := d.present(ctx, req.PKs)
	if err != nil {
		intent.guard.Release()
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

// present returns the keys of pks that exist in the index, in the order of pks.
func (d *Decider) present(ctx context.Context, pks []authority.PK) ([]authority.PK, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	entries, err := d.authority.MultiGet(ctx, pks)
	if err != nil {
		return nil, err
	}
	var present []authority.PK
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

func (i *Intent) acquire(pks []authority.PK) {
	start := time.Now()
	i.guard = i.d.stripes.acquire(pks)
	i.lockWait = time.Since(start)
}
