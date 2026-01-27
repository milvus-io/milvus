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

package segments

import (
	"sync"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

// BatchInfo records the mapping between row offset and WAL position for a batch of inserted data.
// this is used to determine the correct checkpoint position when flushing a range of rows.
type BatchInfo struct {
	EndOffset int64

	Position *msgpb.MsgPosition

	// minTimestamp is the minimum timestamp in this batch (used for StaleBufferPolicy)
	MinTimestamp typeutil.Timestamp
}

// CheckpointTracker tracks the mapping between segment row offsets and WAL positions.
// it is used by GrowingFlushManager to determine the correct checkpoint when flushing
// a range of rows from a Growing Segment.
//
// design:
//   - each Insert operation records a BatchInfo with (endOffset, position)
//   - when flushing rows [0, syncedOffset], we find the last batch where EndOffset <= syncedOffset
//   - the Position of that batch becomes the checkpoint (meaning that WAL message is fully persisted)
//   - after flush completes, we clean up BatchInfo records that are no longer needed
type CheckpointTracker struct {
	mu sync.RWMutex

	// key: segmentID, value: list of BatchInfo sorted by EndOffset ascending
	segmentBatches map[int64][]*BatchInfo

	// key: segmentID, value: last flushed row offset
	flushedOffsets map[int64]int64
}

// NewCheckpointTracker creates a new CheckpointTracker instance.
func NewCheckpointTracker() *CheckpointTracker {
	return &CheckpointTracker{
		segmentBatches: make(map[int64][]*BatchInfo),
		flushedOffsets: make(map[int64]int64),
	}
}

// RecordBatch records the batch information after a successful Insert operation.
func (t *CheckpointTracker) RecordBatch(segID int64, endOffset int64, position *msgpb.MsgPosition) {
	if position == nil {
		return
	}

	t.mu.Lock()
	defer t.mu.Unlock()

	batch := &BatchInfo{
		EndOffset:    endOffset,
		Position:     position,
		MinTimestamp: position.GetTimestamp(),
	}

	t.segmentBatches[segID] = append(t.segmentBatches[segID], batch)
}

// GetCheckpoint returns the appropriate checkpoint position for a given synced offset.
// it finds the last batch where EndOffset <= syncedOffset, meaning all rows in that
// WAL message have been persisted.
//
// returns nil if no suitable checkpoint is found.
//
// example:
//
//	Batches: [{EndOffset:50, Pos:A}, {EndOffset:120, Pos:B}, {EndOffset:200, Pos:C}]
//	GetCheckpoint(150) -> returns Pos:B (because 120 <= 150, but 200 > 150)
//	GetCheckpoint(200) -> returns Pos:C (because 200 <= 200)
//	GetCheckpoint(40)  -> returns nil (no batch has EndOffset <= 40... wait, 50 > 40)
func (t *CheckpointTracker) GetCheckpoint(segID int64, syncedOffset int64) *msgpb.MsgPosition {
	t.mu.RLock()
	defer t.mu.RUnlock()

	batches := t.segmentBatches[segID]
	if len(batches) == 0 {
		return nil
	}

	var checkpoint *msgpb.MsgPosition
	for _, batch := range batches {
		if batch.EndOffset <= syncedOffset {
			checkpoint = batch.Position
		} else {
			break
		}
	}
	return checkpoint
}

// GetMinTimestamp returns the minimum timestamp of unflushed data for a segment.
// this is used by StaleBufferPolicy to determine if a segment has stale data.
//
// returns 0 if no unflushed data exists.
func (t *CheckpointTracker) GetMinTimestamp(segID int64) typeutil.Timestamp {
	t.mu.RLock()
	defer t.mu.RUnlock()

	flushedOffset := t.flushedOffsets[segID]
	batches := t.segmentBatches[segID]

	for _, batch := range batches {
		if batch.EndOffset > flushedOffset {
			return batch.MinTimestamp
		}
	}
	return 0
}

// GetFlushedOffset returns the last flushed row offset for a segment.
// returns 0 if the segment has never been flushed.
func (t *CheckpointTracker) GetFlushedOffset(segID int64) int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	return t.flushedOffsets[segID]
}

// UpdateFlushedOffset updates the flushed offset after a successful sync operation.
// it also cleans up BatchInfo records that are no longer needed (EndOffset <= offset).
//
// this should be called after the sync operation completes successfully.
func (t *CheckpointTracker) UpdateFlushedOffset(segID int64, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.flushedOffsets[segID] = offset

	// clean up batches that are fully flushed (EndOffset <= offset)
	// keep only batches where EndOffset > offset (still have unflushed data)
	batches := t.segmentBatches[segID]
	if len(batches) == 0 {
		return
	}

	keepIdx := 0
	for i, batch := range batches {
		if batch.EndOffset > offset {
			keepIdx = i
			break
		}
		keepIdx = i + 1
	}

	if keepIdx >= len(batches) {
		// all batches are flushed, clear the slice
		t.segmentBatches[segID] = nil
	} else if keepIdx > 0 {
		// keep batches from keepIdx onwards
		t.segmentBatches[segID] = batches[keepIdx:]
	}
}

// InitSegment initializes tracking for a recovered segment.
// this is used during recovery when loading a segment from binlog.
//
// parameters:
//   - segID: the segment ID
//   - flushedOffset: the row count that has been persisted to binlog
func (t *CheckpointTracker) InitSegment(segID int64, flushedOffset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	t.flushedOffsets[segID] = flushedOffset
}

// RemoveSegment removes all tracking data for a segment.
// this should be called when a segment is sealed or dropped.
func (t *CheckpointTracker) RemoveSegment(segID int64) {
	t.mu.Lock()
	defer t.mu.Unlock()

	delete(t.segmentBatches, segID)
	delete(t.flushedOffsets, segID)
}

// GetUnflushedRowCount returns the number of unflushed rows for a segment.
// Parameters:
//   - segID: the segment ID
//   - currentRowCount: the current total row count of the segment
//
// Returns the number of rows that haven't been flushed yet.
func (t *CheckpointTracker) GetUnflushedRowCount(segID int64, currentRowCount int64) int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	flushedOffset := t.flushedOffsets[segID]
	if currentRowCount <= flushedOffset {
		return 0
	}
	return currentRowCount - flushedOffset
}

// HasUnflushedData returns true if the segment has unflushed data.
func (t *CheckpointTracker) HasUnflushedData(segID int64, currentRowCount int64) bool {
	return t.GetUnflushedRowCount(segID, currentRowCount) > 0
}

// GetSegmentIDs returns all segment IDs being tracked.
func (t *CheckpointTracker) GetSegmentIDs() []int64 {
	t.mu.RLock()
	defer t.mu.RUnlock()

	// Collect unique segment IDs from both maps
	segmentSet := make(map[int64]struct{})
	for segID := range t.segmentBatches {
		segmentSet[segID] = struct{}{}
	}
	for segID := range t.flushedOffsets {
		segmentSet[segID] = struct{}{}
	}

	result := make([]int64, 0, len(segmentSet))
	for segID := range segmentSet {
		result = append(result, segID)
	}
	return result
}
