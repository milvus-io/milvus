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

package pkindex

import (
	"context"
	"fmt"
	"time"

	"github.com/cockroachdb/errors"

	pkerr "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Decide makes the dedup decision for req. It registers the writer with the
// watermark of the pchannel before the decision. The caller must call Apply
// or Discard on the returned Intent exactly once. Until then the Intent holds
// the watermark back and keeps the Index open.
//
// After the Index is closed, Decide returns an error that matches
// engine.ErrClosed, like a call on a closed engine.
func (idx *Index) Decide(ctx context.Context, req Request) (*Intent, error) {
	if !idx.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, errClosed("vchannel " + idx.vchannel)
	}
	token := idx.ch.wm.enter()
	intent, err := idx.decider.Decide(ctx, req)
	if err != nil {
		idx.ch.wm.end(token)
		idx.lifetime.Done()
		if errors.Is(err, pkerr.ErrCorrupted) {
			idx.logger.Error(ctx, "primary key index lookup found corrupted data, the WAL becomes unavailable", mlog.Err(err))
			idx.ch.fatal(err)
		}
		return nil, err
	}
	intent.release = func() {
		idx.ch.wm.end(token)
		idx.lifetime.Done()
	}
	return intent, nil
}

// PendingTxnIDs returns the transactions that have pending writes.
func (idx *Index) PendingTxnIDs() []int64 {
	return idx.decider.PendingTxnIDs()
}

// DropTxn forgets the pending writes of a transaction that expired or was failed.
func (idx *Index) DropTxn(txnID int64) {
	idx.decider.DropTxn(txnID)
}

// Intent is a dedup decision that the caller confirms with Apply or gives up
// with Discard, exactly once. It belongs to one append and must not be shared
// between goroutines.
type Intent struct {
	d        *decider
	req      Request
	decision Decision
	txn      *pendingTxn // snapshot of the pending writes, for a CommitTxn
	guard    *StripeGuard
	lockWait time.Duration
	resolved bool
	// release ends the writer in the watermark and lets the Index close.
	// It is nil for an Intent that does not come from an Index.
	release func()
}

// Decision returns what the caller should append.
func (i *Intent) Decision() Decision {
	return i.decision
}

// LockWait returns how long Decide waited for the stripes.
func (i *Intent) LockWait() time.Duration {
	return i.lockWait
}

// Apply confirms the decision after the whole group of messages was appended,
// and releases the writer. A second Apply or Discard panics.
func (i *Intent) Apply(ctx context.Context, r Applied) {
	i.resolve()
	i.apply(ctx, r)
	if i.release != nil {
		i.release()
	}
}

func (i *Intent) apply(ctx context.Context, r Applied) {
	defer i.guard.Release()

	switch {
	case i.req.Kind == KindRollbackTxn:
		i.d.DropTxn(i.req.TxnID)
	case i.req.Kind == KindCommitTxn:
		if i.txn == nil {
			return
		}
		batch := &entryBatch{}
		// Deletes first. An insert of the same transaction takes effect at the same
		// time tick and survives the delete.
		for _, pk := range i.txn.deletes {
			batch.delete(pk)
		}
		for _, put := range i.txn.puts {
			batch.put(put.pk, Entry{SegmentID: put.segmentID})
		}
		i.mustWrite(ctx, batch)

		i.d.pendingMu.Lock()
		live, ok := i.d.pending[i.req.TxnID]
		changed := ok && live.changedSince(i.txn)
		i.d.pendingMu.Unlock()
		if changed {
			// The index of these keys was decided without the late body, so it is
			// wrong from here on. The rule is in the WAL contract, see Decide.
			panic(fmt.Sprintf("a body of transaction %d on vchannel %s was applied after its commit decision, "+
				"a producer must wait for the result of every body before it appends the CommitTxn",
				i.req.TxnID, i.req.VChannel))
		}
		i.d.DropTxn(i.req.TxnID)
	case i.req.TxnID != 0:
		i.d.pendingMu.Lock()
		txn, ok := i.d.pending[i.req.TxnID]
		if !ok {
			txn = newPendingTxn()
			i.d.pending[i.req.TxnID] = txn
		}
		if i.req.Kind == KindInsert {
			txn.recordInsert(i.req.PKs, r.SegmentID)
		} else {
			txn.recordDelete(i.req.PKs)
		}
		i.d.pendingMu.Unlock()
	case i.req.Kind == KindInsert:
		batch := &entryBatch{}
		for _, pk := range i.req.PKs {
			batch.put(pk, Entry{SegmentID: r.SegmentID})
		}
		i.mustWrite(ctx, batch)
	case i.req.Kind == KindDelete:
		if len(i.decision.PresentPKs) == 0 {
			return
		}
		batch := &entryBatch{}
		for _, pk := range i.decision.PresentPKs {
			batch.delete(pk)
		}
		i.mustWrite(ctx, batch)
	}
}

// Discard gives the decision up after an append failure, including a redo, and
// releases the writer. Neither the index nor the pending writes change.
// Discard does not log the error, the caller reports it. A second Apply or
// Discard panics.
func (i *Intent) Discard(_ context.Context, _ error) {
	i.resolve()
	i.guard.Release()
	if i.release != nil {
		i.release()
	}
}

func (i *Intent) resolve() {
	if i.resolved {
		panic("primary key index intent is resolved more than once")
	}
	i.resolved = true
}

// mustWrite applies the index update of a write that the WAL already accepted. The
// caller's context may be canceled by then, and the update must still happen, so
// only its values are kept. An engine write is a memory operation that fails only
// on a bug or a closed engine. Continuing would leave the index stale for these
// keys and let duplicates through in silence, so the failure ends the process.
func (i *Intent) mustWrite(ctx context.Context, batch *entryBatch) {
	if err := i.d.store.write(context.WithoutCancel(ctx), batch); err != nil {
		panic(fmt.Sprintf("failed to apply an appended write to the primary key index of vchannel %s (%d keys): %v",
			i.req.VChannel, batch.len(), err))
	}
}
