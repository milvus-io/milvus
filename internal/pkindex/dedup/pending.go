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

import "maps"

// pendingTxn collects the primary keys written by the bodies of a transaction
// that has not been committed yet. The CommitTxn decides on them, because the
// bodies take effect at its time tick.
type pendingTxn struct {
	puts    map[PK]struct{} // keys inserted by a body
	deletes map[PK]struct{} // keys deleted by a body
}

func newPendingTxn() *pendingTxn {
	return &pendingTxn{
		puts:    make(map[PK]struct{}),
		deletes: make(map[PK]struct{}),
	}
}

func (p *pendingTxn) recordInsert(pks []PK) {
	for _, pk := range pks {
		p.puts[pk] = struct{}{}
	}
}

func (p *pendingTxn) recordDelete(pks []PK) {
	for _, pk := range pks {
		p.deletes[pk] = struct{}{}
	}
}

// snapshot returns a copy, so that it can be used outside pendingMu.
func (p *pendingTxn) snapshot() *pendingTxn {
	return &pendingTxn{puts: maps.Clone(p.puts), deletes: maps.Clone(p.deletes)}
}

// changedSince reports whether p holds a different number of puts or deletes
// than snapshot. It is used to detect a body applied after a commit decision
// was made from snapshot.
func (p *pendingTxn) changedSince(snapshot *pendingTxn) bool {
	return len(p.puts) != len(snapshot.puts) || len(p.deletes) != len(snapshot.deletes)
}

// allPKs returns every key touched by the transaction, for locking.
func (p *pendingTxn) allPKs() []PK {
	pks := make([]PK, 0, len(p.puts)+len(p.deletes))
	for pk := range p.puts {
		pks = append(pks, pk)
	}
	for pk := range p.deletes {
		if _, ok := p.puts[pk]; !ok {
			pks = append(pks, pk)
		}
	}
	return pks
}

// insertedPKs returns every key inserted by the transaction.
func (p *pendingTxn) insertedPKs() []PK {
	pks := make([]PK, 0, len(p.puts))
	for pk := range p.puts {
		pks = append(pks, pk)
	}
	return pks
}
