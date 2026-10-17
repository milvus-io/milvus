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

// Applied is what the caller read back from the appended main message.
type Applied struct {
	SegmentID int64 // the segment an insert was assigned to
	// TimeTick is the time tick at which the write takes effect. The Authority
	// does not read it: the decision order of one key is enforced by the stripe
	// locks, not by comparing time ticks. The field is kept for callers and for
	// a future version that may need it.
	TimeTick uint64
}
