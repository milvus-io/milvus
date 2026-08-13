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

	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/flushcommon/syncmgr"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// fakeBufferedRows makes an owned payload account for buffered rows without
// building real column data: the floor/checkpoint machinery only reads the
// statistics (rows, size, positions), never the payload bytes.
func fakeBufferedRows(p segmentPayload, startTs, endTs uint64, rows, size int64) {
	op := p.(*ownedPayload)
	op.insertBuffer.UpdateStatistics(rows, size,
		TimeRange{timestampMin: startTs, timestampMax: endTs},
		&msgpb.MsgPosition{Timestamp: startTs}, &msgpb.MsgPosition{Timestamp: endTs})
}

// buildInFlightOwnedEntry is the shared core of the in-flight owned-task
// builders: install a buffer filled by fill, build the sync task for it (which
// snapshots the payload and records its floor), and return the queue entry.
// What these tests observe through GetCheckpoint is the payload floor — the
// ONLY data pin left now that the syncCheckpoint registry is gone.
func (s *WriteBufferSuite) buildInFlightOwnedEntry(segmentID int64, checkpointTs uint64, fill func(buffer *segmentBuffer)) *writeBufferSyncEntry {
	segment := growingSegment(segmentID, storage.StorageV2)
	s.metacache.EXPECT().GetSegmentByID(segmentID).Return(segment, true).Maybe()
	s.metacache.EXPECT().UpdateSegments(mock.Anything, mock.Anything).Return().Maybe()

	buffer, err := newSegmentBuffer(segmentID, s.collSchema)
	s.Require().NoError(err)
	fill(buffer)

	s.wb.mut.Lock()
	s.wb.buffers[segmentID] = buffer
	s.wb.checkpoint = &msgpb.MsgPosition{Timestamp: checkpointTs}
	task, err := s.wb.getWriteBufferSyncTaskForTest(context.Background(), segment)
	var entry *writeBufferSyncEntry
	if task != nil {
		entry = s.wb.writeBufferSyncEntryLocked(task.(*syncmgr.SyncTask))
	}
	s.wb.mut.Unlock()
	s.Require().NoError(err)
	s.Require().NotNil(entry)
	s.Require().NotNil(entry.payload)
	s.Require().NotZero(entry.snapshotID)
	return entry
}

// buildInFlightOwnedTask buffers fake rows starting at startTs and builds the
// sync task for them.
func (s *WriteBufferSuite) buildInFlightOwnedTask(segmentID int64, startTs, checkpointTs uint64) *writeBufferSyncEntry {
	return s.buildInFlightOwnedEntry(segmentID, checkpointTs, func(buffer *segmentBuffer) {
		fakeBufferedRows(buffer.payload, startTs, startTs+20, 10, 64)
	})
}

// TestOwnedInFlightFloorPinsCheckpointUntilCommitFlush: rows moved out of the
// buffer by Snapshot must keep pinning the channel checkpoint at their replay
// origin until CommitFlush — the only settlement — releases the floor.
//
// Red-verified by mutation: removing the floor append from ownedPayload.Snapshot
// makes the first assertion observe the bare channel checkpoint (200).
func (s *WriteBufferSuite) TestOwnedInFlightFloorPinsCheckpointUntilCommitFlush() {
	segmentID := int64(1401)
	entry := s.buildInFlightOwnedTask(segmentID, 100, 200)

	s.EqualValues(100, s.wb.GetCheckpoint().GetTimestamp(),
		"in-flight snapshot floor must pin the checkpoint at the snapshot's start position")

	s.wb.mut.Lock()
	entry.payload.CommitFlush(entry.snapshotID)
	s.wb.mut.Unlock()

	s.EqualValues(200, s.wb.GetCheckpoint().GetTimestamp(),
		"CommitFlush must release the floor and let the checkpoint advance")
}

// TestOwnedTerminalFailureKeepsFloorPinned: a terminal sync failure abandons
// the task's payload — the rows are gone from memory, WAL replay is the only
// copy — so AbortFlush must KEEP the floor and the checkpoint must stay pinned.
//
// Red-verified by mutation: making ownedPayload.AbortFlush drop the floor (call
// CommitFlush) advances the checkpoint to 200 and fails this test.
func (s *WriteBufferSuite) TestOwnedTerminalFailureKeepsFloorPinned() {
	segmentID := int64(1402)
	entry := s.buildInFlightOwnedTask(segmentID, 100, 200)

	// Terminal failures escalate through errHandler (fatal by default); this
	// test asserts the checkpoint outcome, not the escalation.
	s.wb.errHandler = func(error) {}
	// Balance the payload-release accounting the abandoned task will subtract.
	s.wb.addBufferMetric(64)

	terminalErr := merr.WrapErrSegmentNotFound(segmentID)
	err := s.wb.finishWriteBufferSync(context.Background(), entry, entry.task, terminalErr)
	s.ErrorIs(err, merr.ErrSegmentNotFound)

	s.wb.mut.RLock()
	_, queued := s.wb.writeBufferSyncQueues[segmentID]
	s.wb.mut.RUnlock()
	s.False(queued, "terminal failure must settle the queue entry")

	s.EqualValues(100, s.wb.GetCheckpoint().GetTimestamp(),
		"terminal failure must keep the floor pinned: WAL replay is the only remaining copy")
}

// buildInFlightOwnedTaskWithDelta is buildInFlightOwnedTask plus buffered
// deletes at deltaStartTs; insertRows may be 0 for a delta-only (L0-shaped)
// task.
func (s *WriteBufferSuite) buildInFlightOwnedTaskWithDelta(segmentID int64, insertRows int64, insertStartTs, deltaStartTs, checkpointTs uint64) *writeBufferSyncEntry {
	return s.buildInFlightOwnedEntry(segmentID, checkpointTs, func(buffer *segmentBuffer) {
		if insertRows > 0 {
			fakeBufferedRows(buffer.payload, insertStartTs, insertStartTs+20, insertRows, 64)
		}
		buffer.deltaBuffer.Buffer(
			[]storage.PrimaryKey{storage.NewInt64PrimaryKey(1)},
			[]typeutil.Timestamp{deltaStartTs},
			&msgpb.MsgPosition{Timestamp: deltaStartTs},
			&msgpb.MsgPosition{Timestamp: deltaStartTs + 10})
	})
}

// TestOwnedCombinedFloorCoversDeltaStart: the floor recorded for an in-flight
// snapshot must be the COMBINED task start — when the task's delta start
// precedes its insert start, the checkpoint must stay pinned at the delta
// start until CommitFlush settles the snapshot.
//
// Red-verified by mutation: dropping the widenFloor call from
// getWriteBufferSyncTask leaves only the insert floor (100) and fails the 50
// assertion.
func (s *WriteBufferSuite) TestOwnedCombinedFloorCoversDeltaStart() {
	segmentID := int64(1404)
	entry := s.buildInFlightOwnedTaskWithDelta(segmentID, 10, 100, 50, 200)

	s.EqualValues(50, s.wb.GetCheckpoint().GetTimestamp(),
		"in-flight combined floor must pin the checkpoint at the delta start, which precedes the insert start")

	s.wb.mut.Lock()
	entry.payload.CommitFlush(entry.snapshotID)
	s.wb.mut.Unlock()

	s.EqualValues(200, s.wb.GetCheckpoint().GetTimestamp(),
		"CommitFlush must release the combined floor")
}

// TestOwnedDeltaOnlyTaskPinsCheckpoint: an L0-shaped task carries deletes only
// — Snapshot records no insert floor — so the coordinator must create the
// floor from the delta start, and AbortFlush (terminal failure) must keep it.
//
// Red-verified by the same widenFloor-removal mutation: without it a
// delta-only in-flight task pins nothing and GetCheckpoint reports 200.
func (s *WriteBufferSuite) TestOwnedDeltaOnlyTaskPinsCheckpoint() {
	segmentID := int64(1405)
	entry := s.buildInFlightOwnedTaskWithDelta(segmentID, 0, 0, 60, 200)

	s.EqualValues(60, s.wb.GetCheckpoint().GetTimestamp(),
		"delta-only in-flight task must pin the checkpoint at its delta start")

	// Terminal failure: the delta rows are gone from memory, WAL replay is the
	// only copy — AbortFlush must keep the floor.
	s.wb.mut.Lock()
	entry.payload.AbortFlush(entry.snapshotID)
	s.wb.mut.Unlock()
	s.EqualValues(60, s.wb.GetCheckpoint().GetTimestamp(),
		"AbortFlush must keep the delta floor pinned")

	s.wb.mut.Lock()
	entry.payload.CommitFlush(entry.snapshotID)
	s.wb.mut.Unlock()
	s.EqualValues(200, s.wb.GetCheckpoint().GetTimestamp())
}

// TestOwnedCommitFlushReleasesOnlyItsOwnFloor: with more than one snapshot in
// flight (reorder window > 1), CommitFlush must settle exactly its own
// snapshot's floor, in order or out of order.
//
// Red-verified by mutation: making CommitFlush clear ALL floors lets the first
// commit release the second snapshot's floor and fails the 150 assertion.
func (s *WriteBufferSuite) TestOwnedCommitFlushReleasesOnlyItsOwnFloor() {
	buffer, err := newSegmentBuffer(1403, s.collSchema)
	s.Require().NoError(err)
	p := buffer.payload

	fakeBufferedRows(p, 100, 120, 10, 64)
	first, err := p.Snapshot(context.Background(), 0)
	s.Require().NoError(err)
	s.Zero(p.UnflushedRows(), "snapshot must move the buffered rows out")

	fakeBufferedRows(p, 150, 170, 5, 32)
	second, err := p.Snapshot(context.Background(), 0)
	s.Require().NoError(err)
	s.NotEqual(first.snapshotID, second.snapshotID)

	s.EqualValues(100, p.EarliestPosition().GetTimestamp(),
		"both snapshots in flight: the earlier floor wins")

	p.CommitFlush(first.snapshotID)
	s.Require().NotNil(p.EarliestPosition())
	s.EqualValues(150, p.EarliestPosition().GetTimestamp(),
		"committing the first snapshot must release exactly its own floor")

	p.CommitFlush(second.snapshotID)
	s.Nil(p.EarliestPosition(), "all snapshots settled: nothing left to pin")

	// Out-of-order settlement: committing the newer snapshot first must leave
	// the older floor pinned.
	fakeBufferedRows(p, 200, 210, 3, 16)
	third, err := p.Snapshot(context.Background(), 0)
	s.Require().NoError(err)
	fakeBufferedRows(p, 250, 260, 3, 16)
	fourth, err := p.Snapshot(context.Background(), 0)
	s.Require().NoError(err)

	p.CommitFlush(fourth.snapshotID)
	s.EqualValues(200, p.EarliestPosition().GetTimestamp(),
		"committing the newer snapshot must not release the older floor")
	p.CommitFlush(third.snapshotID)
	s.Nil(p.EarliestPosition())
}
