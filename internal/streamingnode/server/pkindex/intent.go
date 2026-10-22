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
)

// Intent is a decision that has not been confirmed yet. It belongs to one
// append and must not be shared between goroutines.
type Intent struct {
	d        *Decider
	req      Request
	decision Decision
	txn      *pendingTxn    // snapshot of the pending writes, for a CommitTxn
	write    *inflightWrite // the registration in the in-flight set; nil when the write registers no key
	lockWait time.Duration
	resolved bool
}

// Decision returns what the caller should append.
func (i *Intent) Decision() Decision {
	return i.decision
}

// LockWait returns how long Decide waited for the earlier writes of the same keys.
func (i *Intent) LockWait() time.Duration {
	return i.lockWait
}

// Apply confirms the decision after the whole group of messages was appended.
// It does not write the engine: observe applies the appended messages. For a
// registered write it fills in the time tick, which releases the lock of its
// keys; the keys stay present in the in-flight set until observe applies them.
func (i *Intent) Apply(_ context.Context, r Applied) {
	i.resolve()

	switch {
	case i.req.Kind == KindRollbackTxn:
		i.d.DropTxn(i.req.TxnID)
	case i.req.Kind == KindCommitTxn:
		if i.txn == nil {
			return
		}
		i.d.inflight.fill(i.write, r.TimeTick)

		i.d.pendingMu.Lock()
		live, ok := i.d.pending[i.req.TxnID]
		changed := ok && live.changedSince(i.txn)
		i.d.pendingMu.Unlock()
		if changed {
			// The companion deletes of this commit were decided without the late
			// body, so a duplicate may be in the WAL. The rule is in the WAL
			// contract, see Decide.
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
			txn.recordInsert(i.req.PKs)
		} else {
			txn.recordDelete(i.req.PKs)
		}
		i.d.pendingMu.Unlock()
	default:
		i.d.inflight.fill(i.write, r.TimeTick)
	}
}

// Discard ends a decision whose append failed and is known not to be in the WAL,
// a redo for example. The registration is removed. Pending writes of a
// transaction are kept: the client may retry the commit.
func (i *Intent) Discard(_ context.Context, _ error) {
	i.resolve()
	if i.write != nil {
		i.d.inflight.remove(i.write)
	}
}

// DiscardUncertain ends a decision whose append failed but may be in the WAL at
// a time tick up to tt, a CommitTxn that may have been persisted for example.
// tt must not be 0. The keys stay present in the in-flight set until observe
// passes tt, and no longer block other writes.
func (i *Intent) DiscardUncertain(_ context.Context, _ error, tt uint64) {
	i.resolve()
	if tt == 0 {
		panic("primary key index intent discarded as uncertain without a time tick")
	}
	if i.write != nil {
		i.d.inflight.markUncertain(i.write, tt)
	}
}

func (i *Intent) resolve() {
	if i.resolved {
		panic("primary key index intent is resolved more than once")
	}
	i.resolved = true
}
