package decider

import (
	"context"
	"time"

	"github.com/milvus-io/milvus/internal/pkindex/authority"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
)

// Intent is a decision that has not been confirmed yet. It belongs to one
// append and must not be shared between goroutines.
type Intent struct {
	d        *Decider
	req      Request
	decision Decision
	txn      *pendingTxn // snapshot of the pending writes, for a CommitTxn
	guard    *StripeGuard
	lockWait time.Duration
	resolved bool
}

// Decision returns what the caller should append.
func (i *Intent) Decision() Decision {
	return i.decision
}

// LockWait returns how long Decide waited for the stripes.
func (i *Intent) LockWait() time.Duration {
	return i.lockWait
}

// Apply confirms the decision after the whole group of messages was appended.
func (i *Intent) Apply(ctx context.Context, r Applied) {
	i.resolve()
	defer i.guard.Release()

	switch {
	case i.req.Kind == KindRollbackTxn:
		i.d.DropTxn(i.req.TxnID)
	case i.req.Kind == KindCommitTxn:
		if i.txn == nil {
			return
		}
		batch := i.d.authority.NewBatch()
		// Deletes first. An insert of the same transaction takes effect at the same
		// time tick and survives the delete.
		for _, pk := range i.txn.deletes {
			batch.Delete(pk)
		}
		for _, put := range i.txn.puts {
			batch.Put(put.pk, authority.Entry{SegmentID: put.segmentID})
		}
		i.write(ctx, batch)

		i.d.mu.Lock()
		if live, ok := i.d.pending[i.req.TxnID]; ok && live.changedSince(i.txn) {
			mlog.Error(ctx, "a transaction body was applied after its commit decision",
				mlog.FieldVChannel(i.req.VChannel),
				mlog.Int64("txnID", i.req.TxnID))
		}
		i.d.mu.Unlock()
		i.d.DropTxn(i.req.TxnID)
	case i.req.TxnID != 0:
		i.d.mu.Lock()
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
		i.d.mu.Unlock()
	case i.req.Kind == KindInsert:
		batch := i.d.authority.NewBatch()
		for _, pk := range i.req.PKs {
			batch.Put(pk, authority.Entry{SegmentID: r.SegmentID})
		}
		i.write(ctx, batch)
	case i.req.Kind == KindDelete:
		if i.decision.Skip {
			return
		}
		pks := i.req.PKs
		if i.decision.KeepPKs != nil {
			pks = i.decision.KeepPKs
		}
		batch := i.d.authority.NewBatch()
		for _, pk := range pks {
			batch.Delete(pk)
		}
		i.write(ctx, batch)
	}
}

// Discard gives the decision up after an append failure, including a redo.
// Neither the index nor the pending writes change. Discard does not log the
// error, the caller reports it.
func (i *Intent) Discard(_ context.Context, _ error) {
	i.resolve()
	i.guard.Release()
}

func (i *Intent) resolve() {
	if i.resolved {
		panic("primary key index intent is resolved more than once")
	}
	i.resolved = true
}

func (i *Intent) write(ctx context.Context, batch *authority.Batch) {
	if err := i.d.authority.Write(ctx, batch); err != nil {
		// TODO: the WAL write already succeeded, so the index is now stale for these keys.
		mlog.Error(ctx, "failed to apply an appended write to the primary key index",
			mlog.FieldVChannel(i.req.VChannel),
			mlog.Int("keys", batch.Len()),
			mlog.Err(err))
	}
}
