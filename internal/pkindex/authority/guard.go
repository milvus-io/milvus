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

package authority

import (
	"context"
	"fmt"
	"time"

	"github.com/milvus-io/milvus/internal/pkindex/codec"
)

// WriteGuard is a decision that the caller confirms with Apply or gives up
// with Discard, exactly once. It belongs to one append and must not be shared
// between goroutines. Until Apply or Discard it holds the lock stripes of its
// keys, counts as a writer in flight, and keeps the Authority open.
type WriteGuard struct {
	a         *Authority
	req       DecideRequest
	decision  Decision
	txn       *pendingTxn // snapshot of the pending writes, for a CommitTxn
	guard     *stripeGuard
	lockWait  time.Duration
	endWriter func() // from Config.OnWriterBegin, nil when the config has none
	resolved  bool
}

// Decision returns what the caller should append.
func (g *WriteGuard) Decision() Decision {
	return g.decision
}

// LockWait returns how long Decide waited for the stripes.
func (g *WriteGuard) LockWait() time.Duration {
	return g.lockWait
}

// Apply confirms the decision after the whole group of messages was appended,
// and ends the writer. A second Apply or Discard panics.
func (g *WriteGuard) Apply(ctx context.Context, r Applied) {
	g.resolve()
	g.apply(ctx, r)
	g.release()
}

func (g *WriteGuard) apply(ctx context.Context, r Applied) {
	defer g.guard.Release()

	switch {
	case g.req.Kind == KindRollbackTxn:
		g.a.DropTxn(g.req.TxnID)
	case g.req.Kind == KindCommitTxn:
		if g.txn == nil {
			return
		}
		b := &batch{}
		// Deletes first. An insert of the same transaction takes effect at the same
		// time tick and survives the delete.
		for _, pk := range g.txn.deletes {
			b.delete(pk)
		}
		for _, put := range g.txn.puts {
			b.put(put.pk, codec.PKEntry{SegmentID: put.segmentID})
		}
		g.mustWrite(ctx, b)

		g.a.pendingMu.Lock()
		live, ok := g.a.pending[g.req.TxnID]
		changed := ok && live.changedSince(g.txn)
		g.a.pendingMu.Unlock()
		if changed {
			// The index of these keys was decided without the late body, so it is
			// wrong from here on. The rule is in the WAL contract, see Decide.
			panic(fmt.Sprintf("a body of transaction %d on vchannel %s was applied after its commit decision, "+
				"a producer must wait for the result of every body before it appends the CommitTxn",
				g.req.TxnID, g.req.VChannel))
		}
		g.a.DropTxn(g.req.TxnID)
	case g.req.TxnID != 0:
		g.a.pendingMu.Lock()
		txn, ok := g.a.pending[g.req.TxnID]
		if !ok {
			txn = newPendingTxn()
			g.a.pending[g.req.TxnID] = txn
		}
		if g.req.Kind == KindInsert {
			txn.recordInsert(g.req.PKs, r.SegmentID)
		} else {
			txn.recordDelete(g.req.PKs)
		}
		g.a.pendingMu.Unlock()
	case g.req.Kind == KindInsert:
		b := &batch{}
		for _, pk := range g.req.PKs {
			b.put(pk, codec.PKEntry{SegmentID: r.SegmentID})
		}
		g.mustWrite(ctx, b)
	case g.req.Kind == KindDelete:
		if len(g.decision.PresentPKs) == 0 {
			return
		}
		b := &batch{}
		for _, pk := range g.decision.PresentPKs {
			b.delete(pk)
		}
		g.mustWrite(ctx, b)
	}
}

// Discard gives the decision up after an append failure, including a redo, and
// ends the writer. Neither the index nor the pending writes change. Discard
// does not log the error, the caller reports it. A second Apply or Discard
// panics.
func (g *WriteGuard) Discard(_ context.Context, _ error) {
	g.resolve()
	g.guard.Release()
	g.release()
}

func (g *WriteGuard) resolve() {
	if g.resolved {
		panic("primary key index write guard is resolved more than once")
	}
	g.resolved = true
}

// release ends the writer: it runs the end function of OnWriterBegin and lets
// the Authority close. Decide calls it on a failed decision, Apply and Discard
// at the end. The stripes are released before it.
func (g *WriteGuard) release() {
	g.guard.Release()
	if g.endWriter != nil {
		g.endWriter()
	}
	g.a.lifetime.Done()
}

// mustWrite applies the index update of a write that the WAL already accepted. The
// caller's context may be canceled by then, and the update must still happen, so
// only its values are kept. An engine write is a memory operation that fails only
// on a bug or a closed engine. Continuing would leave the index stale for these
// keys and let duplicates through in silence, so the failure ends the process.
func (g *WriteGuard) mustWrite(ctx context.Context, b *batch) {
	if err := write(context.WithoutCancel(ctx), g.a.eng, b); err != nil {
		panic(fmt.Sprintf("failed to apply an appended write to the primary key index of vchannel %s (%d keys): %v",
			g.req.VChannel, b.len(), err))
	}
}
