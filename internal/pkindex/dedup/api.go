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
	// Decide makes the decision for req. For an autocommit insert and for a
	// CommitTxn it registers the affected keys in the in-flight set and waits
	// for the earlier writes of the same keys, before it reads the index. The
	// returned WriteIntent holds that registration, which is the lock of those
	// keys until the caller reports the outcome of the downstream append with
	// exactly one of Appended, NotAppended and InDoubt, exactly once. A panic
	// between Decide and that call ends the process, like every panic on the
	// append path.
	//
	// An autocommit delete needs no decision, and Decide panics on one: its
	// content depends on nothing, and an earlier write of its keys is in the
	// in-flight set or the engine whether it is registered or not. Until
	// observe applies it, a later insert of its keys can find them present and
	// get a companion delete of a key that is gone, which deletes nothing.
	//
	// While the index is not ready (Config.Ready) or the writes of the
	// vchannel must wait for observe (WaitUntil), Decide returns a *WaitError
	// for an autocommit insert and for a CommitTxn with pending writes. Nothing
	// is registered for that write then. Decide never blocks on that wait
	// itself: the caller may hold a lock that the install or observe needs.
	// It fails with an error that wraps engine.ErrClosed after Close.
	Decide(ctx context.Context, req DecideRequest) (*WriteIntent, error)

	// DropEndedTxns forgets the pending writes of every transaction for which
	// ended returns true: one that expired or was failed, so that no CommitTxn
	// or RollbackTxn will end it. ended runs under a lock of the decider.
	DropEndedTxns(ended func(txnID int64) bool)

	// Observe tells the decider that observe applied every write up to tt to
	// the engine. pks are the keys of the write at tt, none for a TimeTick. It
	// removes the committed entries of pks from the in-flight set (rule A: the
	// engine first, then the set), raises the observed time tick and removes
	// the in-doubt entries that it passed.
	Observe(tt uint64, pks []PK)

	// Close stops the decider: a later Decide fails. It does not wait for the
	// open intents, they end on their own, and it does not close the engine:
	// the caller does, and a probe on a closed engine fails with
	// engine.ErrClosed.
	Close()
}

// Config configures a Decider.
type Config struct {
	// Ready is closed when the engine holds every record of the vchannel: its
	// committed table set is installed. Until then the writes that register
	// keys wait (WaitError). Nil means ready at once.
	Ready <-chan struct{}
}

// WaitError is returned by Decide when the write must wait before it can be
// decided: until the index is ready, and until observe reaches TimeTick.
// Nothing is registered for the write. The caller releases the locks that
// the install or observe may need, calls Wait, and then calls Decide again.
type WaitError struct {
	// TimeTick is the time tick that observe must reach. It is 0 when the
	// write waits only for the index to be ready.
	TimeTick uint64

	d *decider
}

func (e *WaitError) Error() string {
	if e.TimeTick == 0 {
		return fmt.Sprintf("primary key index of vchannel %s waits for its committed tables to be installed", e.d.vchannel)
	}
	return fmt.Sprintf("primary key index of vchannel %s waits for observe to reach time tick %d", e.d.vchannel, e.TimeTick)
}

// Wait blocks until the index is ready and observe reaches TimeTick. It
// returns ctx.Err() when ctx ends first, and an error that wraps
// engine.ErrClosed when the decider is closed first.
func (e *WaitError) Wait(ctx context.Context) error {
	if e.d.ready != nil {
		select {
		case <-e.d.ready:
		case <-ctx.Done():
			return ctx.Err()
		case <-e.d.stop:
			return errClosed(e.d.vchannel)
		}
	}
	return e.d.inflight.waitObserved(ctx, e.TimeTick, e.d.stop, e.d.vchannel)
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
	Kind  WriteKind
	PKs   []PK  // The keys of an insert or delete. Empty for commit and rollback.
	TxnID int64 // 0 for an autocommit write
}

// WriteIntent is a decision that has not been confirmed yet. It belongs to one
// append and must not be shared between goroutines.
//
// CompanionDeletePKs returns the keys of an insert that need a companion
// delete in the same transaction.
//
// LockWait returns how long Decide waited for the earlier writes of the same keys.
//
// Appended reports that the whole group of messages is in the WAL. tt is the
// time tick at which the write takes effect: the time tick of the CommitTxn
// for an insert appended as a transaction. It does not write the
// engine: observe applies the appended messages. For a registered write it
// fills in the time tick, which releases the lock of its keys; the keys stay
// present in the in-flight set until observe applies them. For a transaction
// body it records the keys as pending writes of the transaction, and for a
// RollbackTxn it forgets them.
//
// NotAppended reports that the append failed and is known not to be in the
// WAL, a redo for example. The registration is removed. Pending writes of a
// transaction are kept: the client may retry the commit.
//
// InDoubt reports that the append failed but may be in the WAL at a time
// tick up to tt, a CommitTxn that may have been persisted for example. tt must
// not be 0. The keys stay present in the in-flight set until observe passes
// tt, and no longer block other writes.
type WriteIntent struct {
	d                  *decider
	req                DecideRequest
	companionDeletePKs []PK
	txn                *pendingTxn    // snapshot of the pending writes, for a CommitTxn
	write              *inflightWrite // the registration in the in-flight set; nil when the write registers no key
	lockWait           time.Duration
	resolved           bool
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
