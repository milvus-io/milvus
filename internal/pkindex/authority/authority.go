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

// Package authority decides the writes of one vchannel in primary key index
// terms. An Authority turns a DecideRequest into a WriteGuard: the companion
// deletes of an insert, the present keys of a delete, and the pending bodies
// of a transaction. The WriteGuard holds the lock stripes of its keys until
// the caller confirms it with Apply or gives it up with Discard. The package
// knows the engine only as engine.RW, and it knows nothing about the WAL.
package authority

import (
	"context"
	"sync"
	"time"

	"github.com/cockroachdb/errors"

	pkroot "github.com/milvus-io/milvus/internal/pkindex"
	"github.com/milvus-io/milvus/internal/pkindex/engine"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Config configures an Authority.
type Config struct {
	// LockStripes is the number of lock stripes. Keys are hashed onto them.
	LockStripes int
	// OnWriterBegin runs at the start of every Decide that succeeds. The
	// function it returns runs once when the write ends, after Apply or
	// Discard. The owner uses it to count the writers in flight. Nil is allowed.
	OnWriterBegin func() (end func())
}

// Authority makes the write path decisions of one vchannel on top of its engine.
//
// TODO: duplicate primary keys inside one insert are not detected.
// TODO: pick the stripe count from a throughput benchmark.
type Authority struct {
	eng           engine.RW
	stripes       *stripeLocks
	onWriterBegin func() func()
	lifetime      *typeutil.Lifetime

	// pendingMu guards pending and every pendingTxn in it. Lock order: stripes
	// first, then pendingMu. No code may wait for a stripe while it holds pendingMu.
	pendingMu sync.Mutex
	pending   map[int64]*pendingTxn
}

// New creates the Authority of a vchannel over its engine. It does not own
// the engine: the caller closes it, after Close.
func New(eng engine.RW, cfg Config) *Authority {
	return &Authority{
		eng:           eng,
		stripes:       newStripeLocks(cfg.LockStripes),
		onWriterBegin: cfg.OnWriterBegin,
		lifetime:      typeutil.NewLifetime(),
		pending:       make(map[int64]*pendingTxn),
	}
}

// Decide makes the decision for req. For an autocommit write and for a CommitTxn it
// acquires the stripes of the affected keys, which the returned WriteGuard holds until
// Apply or Discard. The caller must call exactly one of them, exactly once. A panic
// between Decide and that call ends the process, like every panic on the append path.
//
// After Close, Decide returns an error that matches engine.ErrClosed, like a
// call on a closed engine.
func (a *Authority) Decide(ctx context.Context, req DecideRequest) (*WriteGuard, error) {
	if !a.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, errClosed(req.VChannel)
	}
	g := &WriteGuard{a: a, req: req}
	if a.onWriterBegin != nil {
		g.endWriter = a.onWriterBegin()
	}
	if err := a.decide(ctx, g); err != nil {
		g.release()
		return nil, err
	}
	return g, nil
}

func (a *Authority) decide(ctx context.Context, g *WriteGuard) error {
	req := g.req
	switch {
	case req.Kind == KindRollbackTxn:
		return nil
	case req.Kind == KindCommitTxn:
		a.pendingMu.Lock()
		if txn, ok := a.pending[req.TxnID]; ok {
			// WAL contract this snapshot depends on (timetick_and_txn.md,
			// "Transaction Lifecycle", Commit): a producer appends the CommitTxn
			// only after it received the results of all bodies, and one producer
			// drives one transaction. So no body of the same transaction is
			// applied between this snapshot and this guard's Apply. Apply ends
			// the process when that rule is broken.
			g.txn = txn.snapshot()
		}
		a.pendingMu.Unlock()
		if g.txn == nil {
			return nil
		}
		// pendingMu must be released before acquire: Apply takes pendingMu while
		// it holds the stripes, so holding both here would deadlock with a
		// concurrent Apply.
		g.acquire(g.txn.allPKs())
		hits, err := a.present(ctx, g.txn.companionCandidates())
		if err != nil {
			return err
		}
		g.decision.CompanionDeletePKs = hits
		return nil
	case req.TxnID != 0:
		// A transaction body takes effect at the commit time tick, so it is decided at commit.
		return nil
	}

	g.acquire(req.PKs)
	present, err := a.present(ctx, req.PKs)
	if err != nil {
		return err
	}
	switch req.Kind {
	case KindInsert:
		g.decision.CompanionDeletePKs = present
	case KindDelete:
		g.decision.PresentPKs = present
	}
	return nil
}

// present returns the keys of pks that exist in the index, in the order of pks.
func (a *Authority) present(ctx context.Context, pks []PK) ([]PK, error) {
	if len(pks) == 0 {
		return nil, nil
	}
	entries, err := lookup(ctx, a.eng, pks)
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
func (a *Authority) PendingTxnIDs() []int64 {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	ids := make([]int64, 0, len(a.pending))
	for id := range a.pending {
		ids = append(ids, id)
	}
	return ids
}

// DropTxn forgets the pending writes of a transaction that expired or was failed.
func (a *Authority) DropTxn(txnID int64) {
	a.pendingMu.Lock()
	defer a.pendingMu.Unlock()
	delete(a.pending, txnID)
}

// Close rejects every later Decide and waits for every open WriteGuard. It
// does not close the engine.
func (a *Authority) Close() {
	a.lifetime.SetState(typeutil.LifetimeStateStopped)
	a.lifetime.Wait()
}

// errClosed is the error of a Decide after Close. It matches engine.ErrClosed
// and is marked unavailable, like a call on a closed engine.
func errClosed(vchannel string) error {
	return errors.Mark(errors.Wrapf(engine.ErrClosed, "primary key index of vchannel %s is closed", vchannel), pkroot.ErrUnavailable)
}

// acquire locks the stripes of pks for the guard and records the wait.
func (g *WriteGuard) acquire(pks []PK) {
	start := time.Now()
	g.guard = g.a.stripes.acquire(pks)
	g.lockWait = time.Since(start)
}
