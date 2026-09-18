package decider

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
	CompanionDeletePKs []authority.PK // keys that need a companion delete in the same transaction
	KeepPKs            []authority.PK // Keys of a delete that still exist. Nil means do not rewrite.
	Skip               bool           // none of the keys of a delete exist
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

	// Lock order: stripes first, then mu. No code may wait for a stripe while it
	// holds mu.
	mu      sync.Mutex
	pending map[int64]*pendingTxn
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
// Apply or Discard. The caller must call exactly one of them, exactly once.
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
		d.mu.Lock()
		if txn, ok := d.pending[req.TxnID]; ok {
			// WAL invariant this snapshot depends on: a CommitTxn reaches this
			// interceptor only after the client received the results of all
			// bodies of that transaction, and one client drives one transaction.
			// So no body of the same transaction is applied between this
			// snapshot and this Intent's Apply.
			intent.txn = txn.snapshot()
		}
		d.mu.Unlock()
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
		switch {
		case len(present) == 0:
			intent.decision.Skip = true
		case len(present) < len(req.PKs):
			intent.decision.KeepPKs = present
		}
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

// ClaimTxnCleanup returns true exactly once for a transaction that has pending
// writes. The caller then registers DropTxn as a cleanup of the txn session.
// Call it after Apply of a body. The pending set of a transaction does not
// exist before the first body was applied.
func (d *Decider) ClaimTxnCleanup(txnID int64) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	txn, ok := d.pending[txnID]
	if !ok || txn.cleanupClaimed {
		return false
	}
	txn.cleanupClaimed = true
	return true
}

// DropTxn forgets the pending writes of a transaction that expired or was failed.
func (d *Decider) DropTxn(txnID int64) {
	d.mu.Lock()
	defer d.mu.Unlock()
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
