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

package metacache

import (
	"fmt"
	"sync"
)

// SettleOutcome is how a sync reservation ends. The distinction that matters is
// whether the rows still exist somewhere a replay can find them.
type SettleOutcome int

const (
	// SettleCommitted means DataCoord accepted the batch, so the rows move from
	// syncing to flushed.
	SettleCommitted SettleOutcome = iota
	// SettleDiscarded means the segment no longer exists, so no metadata is left
	// for these rows to belong to. A segment id is never reissued, so the
	// segment cannot come back.
	SettleDiscarded
	// SettleFailed means the rows were neither persisted nor left in memory. The
	// caller MUST keep the checkpoint pin so a replay re-reads them.
	SettleFailed
)

func (o SettleOutcome) String() string {
	switch o {
	case SettleCommitted:
		return "committed"
	case SettleDiscarded:
		return "discarded"
	case SettleFailed:
		return "failed"
	default:
		return fmt.Sprintf("unknown(%d)", int(o))
	}
}

// SyncReservation owns one batch's row-accounting transition. Apply moves rows
// from buffered to syncing; Settle ends the reservation exactly once.
//
// Every transition here is a delta, and there is deliberately no absolute
// setter. The accounting this replaces was not observably wrong: wb.mut
// serialized its writers. It was fragile, because correctness rested on that
// serialization plus the fact that yieldBuffer discarded the whole buffer, and
// on every terminal path remembering to undo StartSyncing. The last of those
// did fail: no plain SyncTask path ever called AbortSyncing, so a failed sync
// leaked syncingRows and syncingTasks permanently.
type SyncReservation struct {
	segmentID int64
	rows      int64
	once      sync.Once
}

// NewSyncReservation creates a reservation for rows of one segment. The caller
// applies it inside the same metacache update that yields the payload, so the
// accounting and the payload move together.
func NewSyncReservation(segmentID int64, rows int64) *SyncReservation {
	return &SyncReservation{segmentID: segmentID, rows: rows}
}

// SegmentID is the segment this reservation belongs to.
func (r *SyncReservation) SegmentID() int64 { return r.segmentID }

// Rows is the batch size this reservation moves into syncing.
func (r *SyncReservation) Rows() int64 { return r.rows }

// Apply moves rows from buffered to syncing.
func (r *SyncReservation) Apply() SegmentAction {
	return func(info *SegmentInfo) {
		if r.rows > info.bufferRows {
			// An internal accounting bug, not drift to absorb. This assertion
			// is new; the removed growing-source path clamped its own computed
			// row count at zero instead, and the common path asserted nothing.
			panic(fmt.Sprintf(
				"sync reservation exceeds buffered rows: segment=%d reserve=%d buffered=%d",
				r.segmentID, r.rows, info.bufferRows))
		}
		info.bufferRows -= r.rows
		info.syncingRows += r.rows
		info.syncingTasks++
	}
}

// Settle ends the reservation. Calling it more than once is a no-op, so every
// terminal path may call it unconditionally without tracking whether an earlier
// path already did.
func (r *SyncReservation) Settle(outcome SettleOutcome) SegmentAction {
	return func(info *SegmentInfo) {
		r.once.Do(func() {
			info.syncingRows -= r.rows
			info.syncingTasks--
			if outcome == SettleCommitted {
				info.flushedRows += r.rows
			}
		})
	}
}
