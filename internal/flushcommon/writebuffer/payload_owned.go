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

package writebuffer

import (
	"context"
	"math"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// ownedFloor is one in-flight snapshot's replay origin. It is appended by
// Snapshot, removed by CommitFlush, and deliberately KEPT by AbortFlush: a
// terminal flush failure means the rows are gone from memory and WAL replay
// from this position is the only copy left, so the channel checkpoint must not
// advance past it.
type ownedFloor struct {
	snapshotID int64
	startPos   *msgpb.MsgPosition
}

// ownedPayload is the segmentPayload whose insert bytes live in this process:
// today's InsertBuffer plus the floor list of in-flight snapshots. The floor
// list is the ONLY data pin for owned segments — the legacy syncCheckpoint
// registry is gone.
type ownedPayload struct {
	insertBuffer *InsertBuffer

	// nextSnapshotID makes snapshot IDs unique per payload; 0 is reserved for
	// "no snapshot".
	nextSnapshotID int64
	// floors holds one entry per in-flight snapshot that carried rows, in
	// snapshot order (oldest first).
	floors []ownedFloor
}

var _ segmentPayload = (*ownedPayload)(nil)

func newOwnedPayload(insertBuffer *InsertBuffer) *ownedPayload {
	return &ownedPayload{insertBuffer: insertBuffer}
}

// Buffer copies one insert pack's rows into owned Go memory. It returns the
// resident bytes added, which the caller feeds into metric accounting.
func (p *ownedPayload) Buffer(data *InsertData, start, end *msgpb.MsgPosition) (int64, error) {
	return p.insertBuffer.Buffer(data, start, end), nil
}

// Snapshot moves the buffered rows out — ownership goes to the flush task —
// and records their start position as a floor that EarliestPosition keeps
// reporting until CommitFlush settles it. throughTs is unused for owned data:
// every buffered row is already at or before the seal by construction (the
// seal arrives in-band on the same flowgraph goroutine that buffers rows).
// Snapshot cannot fail for owned data; the error return exists for the ref
// implementation, whose source may be Unavailable/Pending.
func (p *ownedPayload) Snapshot(_ context.Context, _ uint64) (*flushInput, error) {
	ib := p.insertBuffer
	input := &flushInput{
		insert:    ib.Yield(),
		bm25:      ib.YieldStats(),
		schema:    ib.collSchema,
		timeRange: ib.GetTimeRange(),
		startPos:  ib.startPos,
		rows:      ib.rows,
		bytes:     ib.size,
	}
	p.nextSnapshotID++
	input.snapshotID = p.nextSnapshotID
	if input.startPos != nil {
		p.floors = append(p.floors, ownedFloor{snapshotID: input.snapshotID, startPos: input.startPos})
	}
	// Reset the accounting for the next round while keeping the same buffer
	// object (limits and schema carry over — they are properties of the
	// segment, not of one flush round).
	ib.BufferBase = BufferBase{
		rowLimit:      ib.rowLimit,
		sizeLimit:     ib.sizeLimit,
		TimestampFrom: math.MaxUint64,
		TimestampTo:   0,
	}
	return input, nil
}

// widenFloor lowers (or creates) the floor recorded for snapshotID so it also
// covers pos. Snapshot records the INSERT side's start position only; the same
// task also carries the segment's delta buffer, which the coordinator yields
// next to the snapshot and whose start may precede the insert start (an L0
// segment's task is even delta-only, so Snapshot recorded no floor at all).
// The floor must be the COMBINED task start — the WAL position replay must
// resume from to regenerate everything the task took — so the coordinator
// calls this right after yielding the delta, under the same wb.mut hold that
// took the snapshot. Settlement is unchanged: CommitFlush drops the widened
// floor, AbortFlush keeps it.
//
// Owned-only on purpose: deletes are dispatched exclusively to L0 segments,
// which are always owned, so a ref task never carries delta.
func (p *ownedPayload) widenFloor(snapshotID int64, pos *msgpb.MsgPosition) {
	if pos == nil || snapshotID == 0 {
		return
	}
	for i, floor := range p.floors {
		if floor.snapshotID == snapshotID {
			p.floors[i].startPos = getEarliestCheckpoint(floor.startPos, pos)
			return
		}
	}
	// Delta-only snapshot: no insert floor was recorded, create one. Appending
	// keeps snapshot order — this is the newest snapshot by construction.
	p.floors = append(p.floors, ownedFloor{snapshotID: snapshotID, startPos: pos})
}

// CommitFlush is the only settlement: it drops exactly the committed
// snapshot's floor, leaving floors of other in-flight snapshots pinned.
func (p *ownedPayload) CommitFlush(snapshotID int64) {
	for i, floor := range p.floors {
		if floor.snapshotID == snapshotID {
			p.floors = append(p.floors[:i], p.floors[i+1:]...)
			return
		}
	}
}

// AbortFlush keeps the floor on purpose: for owned data a terminal failure
// means the snapshotted rows exist nowhere but the WAL, so the checkpoint must
// keep pinning their replay origin. The debt itself (Sealed/Dropped metacache
// state, queue intent) is tracked by the coordinator, not here.
func (p *ownedPayload) AbortFlush(_ int64) {
	// Deliberately a no-op for owned payloads.
}

// EarliestPosition covers buffered rows AND in-flight floors — the replay
// origin of everything not yet CommitFlush'd.
func (p *ownedPayload) EarliestPosition() *msgpb.MsgPosition {
	positions := make([]*msgpb.MsgPosition, 0, len(p.floors)+1)
	positions = append(positions, p.insertBuffer.startPos)
	for _, floor := range p.floors {
		positions = append(positions, floor.startPos)
	}
	return getEarliestCheckpoint(positions...)
}

func (p *ownedPayload) UnflushedRows() int64 {
	return p.insertBuffer.rows
}

func (p *ownedPayload) UnflushedBytes() int64 {
	return p.insertBuffer.size
}

func (p *ownedPayload) MinTimestamp() typeutil.Timestamp {
	return p.insertBuffer.MinTimestamp()
}

func (p *ownedPayload) IsFull() bool {
	return p.insertBuffer.IsFull()
}
