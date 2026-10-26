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

// Package dedup is the write path decision of the primary key index of one
// vchannel: which keys of an insert need a companion delete, which keys of a
// delete exist, and how the writes of a transaction take effect at its commit.
// It reads the engine of the vchannel and never writes it: the observe of the
// WAL recovery writes it.
package dedup

import (
	"context"
	"fmt"
	"time"
)

// Decider makes the write path decisions of one vchannel.
type Decider interface {
	// Decide makes the decision for req. For an autocommit write and for a
	// CommitTxn it registers the affected keys in the in-flight set and waits
	// for the earlier writes of the same keys, before it reads the index. The
	// returned WriteIntent holds that registration, which is the lock of those
	// keys until Committed or Aborted. The caller must call exactly one of
	// Committed, Aborted and InDoubt, exactly once, after the downstream
	// append. A panic between Decide and that call ends the process, like
	// every panic on the append path.
	//
	// Decide never waits for the index to be ready: a not-ready index decides
	// degraded. It fails with an error that wraps engine.ErrClosed after Close.
	// While the writes of the vchannel must wait for observe (WaitUntil), it
	// returns a *WaitError for an autocommit write and for a CommitTxn with
	// pending writes, before it registers or probes anything. It never blocks
	// on that wait itself: the caller may hold a lock that observe needs.
	Decide(ctx context.Context, req DecideRequest) (*WriteIntent, error)

	// DropEndedTxns forgets the pending writes of every transaction for which
	// ended returns true: one that expired or was failed, so that no CommitTxn
	// or RollbackTxn will end it. ended runs under a lock of the decider.
	DropEndedTxns(ended func(txnID int64) bool)

	// Close stops the decider: a later Decide fails. It does not wait for the
	// open intents, they end on their own, and it does not close the engine:
	// the caller does, and a probe on a closed engine fails with
	// engine.ErrClosed.
	Close()
}

// Config configures a Decider.
type Config struct {
	// Degraded reports whether the engine may miss records: the committed
	// table set of the vchannel is not installed yet. While it returns true,
	// a decision does not probe the engine and treats every key as present.
	// An insert then gets a companion delete for every key, and a delete
	// writes a tombstone for every key, so that a record that the install
	// brings back later does not come back to life. Nil means never degraded.
	// It never blocks a decision: a not-ready index is degraded, not closed.
	Degraded func() bool
}

// WaitError is returned by Decide when the write must wait until observe
// reaches TimeTick before it can be decided. Nothing is registered for the
// write. The caller releases the locks that the observe may need, calls Wait,
// and then calls Decide again.
type WaitError struct {
	TimeTick uint64

	d        *decider
	vchannel string
}

func (e *WaitError) Error() string {
	return fmt.Sprintf("primary key index of vchannel %s waits for observe to reach time tick %d", e.vchannel, e.TimeTick)
}

// Wait blocks until observe reaches TimeTick. It returns ctx.Err() when ctx
// ends first, and an error that wraps engine.ErrClosed when the decider is
// closed first.
func (e *WaitError) Wait(ctx context.Context) error {
	return e.d.inflight.waitObserved(ctx, e.TimeTick, e.d.stop, e.vchannel)
}

// WriteKind is the kind of write the caller is about to append.
type WriteKind int

const (
	KindInsert WriteKind = iota
	KindDelete
	KindCommitTxn
	KindRollbackTxn
)

// DecideRequest describes one write in primary key index terms.
type DecideRequest struct {
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

// WriteIntent is a decision that has not been confirmed yet. It belongs to one
// append and must not be shared between goroutines.
//
// Decision returns what the caller should append.
//
// LockWait returns how long Decide waited for the earlier writes of the same keys.
//
// Committed confirms the decision after the whole group of messages was
// appended. tt is the time tick at which the write takes effect: the time tick
// of the CommitTxn for a write that was upgraded to a transaction. It does not
// write the engine: observe applies the appended messages. For a registered
// write it fills in the time tick, which releases the lock of its keys; the
// keys stay present in the in-flight set until observe applies them.
//
// Aborted ends a decision whose append failed and is known not to be in the
// WAL, a redo for example. The registration is removed. Pending writes of a
// transaction are kept: the client may retry the commit.
//
// InDoubt ends a decision whose append failed but may be in the WAL at a time
// tick up to tt, a CommitTxn that may have been persisted for example. tt must
// not be 0. The keys stay present in the in-flight set until observe passes
// tt, and no longer block other writes.
type WriteIntent struct {
	d        *decider
	req      DecideRequest
	decision Decision
	txn      *pendingTxn    // snapshot of the pending writes, for a CommitTxn
	write    *inflightWrite // the registration in the in-flight set; nil when the write registers no key
	lockWait time.Duration
	resolved bool
}

// PK is one primary key, either an int64 or a varchar.
// All keys of one vchannel have the same type, so the encoding carries no type tag.
type PK struct {
	varchar bool
	i       int64
	s       string
}

func Int64PK(v int64) PK { return PK{i: v} }

func VarCharPK(v string) PK { return PK{varchar: true, s: v} }
