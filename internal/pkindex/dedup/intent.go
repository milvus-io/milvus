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

package dedup

import (
	"fmt"
	"time"
)

func (i *WriteIntent) Decision() Decision {
	return i.decision
}

func (i *WriteIntent) LockWait() time.Duration {
	return i.lockWait
}

func (i *WriteIntent) Appended(tt uint64) {
	i.resolve()

	switch {
	case i.req.Kind == KindRollbackTxn:
		i.d.dropTxn(i.req.TxnID)
	case i.req.Kind == KindCommitTxn:
		if i.txn == nil {
			return
		}
		i.d.inflight.commit(i.write, tt)

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
				i.req.TxnID, i.d.vchannel))
		}
		i.d.dropTxn(i.req.TxnID)
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
		i.d.inflight.commit(i.write, tt)
	}
}

func (i *WriteIntent) NotAppended() {
	i.resolve()
	if i.write != nil {
		i.d.inflight.abort(i.write)
	}
}

func (i *WriteIntent) InDoubt(tt uint64) {
	i.resolve()
	if tt == 0 {
		panic("primary key index intent marked in doubt without a time tick")
	}
	if i.write != nil {
		i.d.inflight.markInDoubt(i.write, tt)
	}
}

// resolve ends the intent: it may be resolved once.
func (i *WriteIntent) resolve() {
	if i.resolved {
		panic("primary key index write intent is resolved more than once")
	}
	i.resolved = true
}
