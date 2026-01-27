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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
)

func TestCheckpointTracker_RecordBatch(t *testing.T) {
	tracker := NewCheckpointTracker()

	segID := int64(1001)
	pos1 := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{1}, Timestamp: 100}
	pos2 := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{2}, Timestamp: 200}
	pos3 := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{3}, Timestamp: 300}

	// Record three batches
	tracker.RecordBatch(segID, 50, pos1)
	tracker.RecordBatch(segID, 120, pos2)
	tracker.RecordBatch(segID, 200, pos3)

	// Verify batches are recorded
	assert.Len(t, tracker.segmentBatches[segID], 3)
	assert.Equal(t, int64(50), tracker.segmentBatches[segID][0].EndOffset)
	assert.Equal(t, int64(120), tracker.segmentBatches[segID][1].EndOffset)
	assert.Equal(t, int64(200), tracker.segmentBatches[segID][2].EndOffset)
}

func TestCheckpointTracker_RecordBatch_NilPosition(t *testing.T) {
	tracker := NewCheckpointTracker()

	segID := int64(1001)

	// Recording nil position should be a no-op
	tracker.RecordBatch(segID, 50, nil)

	assert.Len(t, tracker.segmentBatches[segID], 0)
}

func TestCheckpointTracker_GetCheckpoint(t *testing.T) {
	tracker := NewCheckpointTracker()

	segID := int64(1001)
	posA := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{1}, Timestamp: 100}
	posB := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{2}, Timestamp: 200}
	posC := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{3}, Timestamp: 300}

	// Record batches: A(0-50), B(50-120), C(120-200)
	tracker.RecordBatch(segID, 50, posA)
	tracker.RecordBatch(segID, 120, posB)
	tracker.RecordBatch(segID, 200, posC)

	// Test cases
	testCases := []struct {
		name         string
		syncedOffset int64
		expectedPos  *msgpb.MsgPosition
	}{
		{
			name:         "offset less than first batch",
			syncedOffset: 40,
			expectedPos:  nil, // No batch has EndOffset <= 40
		},
		{
			name:         "offset equals first batch",
			syncedOffset: 50,
			expectedPos:  posA,
		},
		{
			name:         "offset between first and second batch",
			syncedOffset: 80,
			expectedPos:  posA, // Only batch A is fully covered
		},
		{
			name:         "offset equals second batch",
			syncedOffset: 120,
			expectedPos:  posB,
		},
		{
			name:         "offset between second and third batch",
			syncedOffset: 150,
			expectedPos:  posB, // Batches A and B are fully covered
		},
		{
			name:         "offset equals third batch",
			syncedOffset: 200,
			expectedPos:  posC,
		},
		{
			name:         "offset greater than all batches",
			syncedOffset: 250,
			expectedPos:  posC,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			checkpoint := tracker.GetCheckpoint(segID, tc.syncedOffset)
			if tc.expectedPos == nil {
				assert.Nil(t, checkpoint)
			} else {
				assert.Equal(t, tc.expectedPos.Timestamp, checkpoint.Timestamp)
			}
		})
	}
}

func TestCheckpointTracker_GetCheckpoint_EmptySegment(t *testing.T) {
	tracker := NewCheckpointTracker()

	// No batches recorded for this segment
	checkpoint := tracker.GetCheckpoint(9999, 100)
	assert.Nil(t, checkpoint)
}

func TestCheckpointTracker_GetMinTimestamp(t *testing.T) {
	tracker := NewCheckpointTracker()

	segID := int64(1001)
	posA := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{1}, Timestamp: 100}
	posB := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{2}, Timestamp: 200}
	posC := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{3}, Timestamp: 300}

	tracker.RecordBatch(segID, 50, posA)
	tracker.RecordBatch(segID, 120, posB)
	tracker.RecordBatch(segID, 200, posC)

	// Initially, flushedOffset is 0, so minTs should be from batch A
	minTs := tracker.GetMinTimestamp(segID)
	assert.Equal(t, uint64(100), minTs)

	// After flushing to offset 50, minTs should be from batch B
	tracker.UpdateFlushedOffset(segID, 50)
	minTs = tracker.GetMinTimestamp(segID)
	assert.Equal(t, uint64(200), minTs)

	// After flushing to offset 120, minTs should be from batch C
	tracker.UpdateFlushedOffset(segID, 120)
	minTs = tracker.GetMinTimestamp(segID)
	assert.Equal(t, uint64(300), minTs)

	// After flushing all, minTs should be 0
	tracker.UpdateFlushedOffset(segID, 200)
	minTs = tracker.GetMinTimestamp(segID)
	assert.Equal(t, uint64(0), minTs)
}

func TestCheckpointTracker_UpdateFlushedOffset(t *testing.T) {
	tracker := NewCheckpointTracker()

	segID := int64(1001)
	posA := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{1}, Timestamp: 100}
	posB := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{2}, Timestamp: 200}
	posC := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{3}, Timestamp: 300}

	tracker.RecordBatch(segID, 50, posA)
	tracker.RecordBatch(segID, 120, posB)
	tracker.RecordBatch(segID, 200, posC)

	// Verify initial state
	assert.Len(t, tracker.segmentBatches[segID], 3)
	assert.Equal(t, int64(0), tracker.GetFlushedOffset(segID))

	// Update to offset 50 - should remove batch A
	tracker.UpdateFlushedOffset(segID, 50)
	assert.Equal(t, int64(50), tracker.GetFlushedOffset(segID))
	assert.Len(t, tracker.segmentBatches[segID], 2)
	assert.Equal(t, int64(120), tracker.segmentBatches[segID][0].EndOffset)

	// Update to offset 150 - should remove batch B (120 <= 150)
	tracker.UpdateFlushedOffset(segID, 150)
	assert.Equal(t, int64(150), tracker.GetFlushedOffset(segID))
	assert.Len(t, tracker.segmentBatches[segID], 1)
	assert.Equal(t, int64(200), tracker.segmentBatches[segID][0].EndOffset)

	// Update to offset 200 - should remove all batches
	tracker.UpdateFlushedOffset(segID, 200)
	assert.Equal(t, int64(200), tracker.GetFlushedOffset(segID))
	assert.Nil(t, tracker.segmentBatches[segID])
}

func TestCheckpointTracker_InitSegment(t *testing.T) {
	tracker := NewCheckpointTracker()

	segID := int64(1001)

	// Initialize segment with flushed offset (recovery scenario)
	tracker.InitSegment(segID, 100)

	assert.Equal(t, int64(100), tracker.GetFlushedOffset(segID))
	assert.Len(t, tracker.segmentBatches[segID], 0) // No batches yet

	// New inserts should be recorded normally
	pos := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{1}, Timestamp: 500}
	tracker.RecordBatch(segID, 150, pos)

	assert.Len(t, tracker.segmentBatches[segID], 1)
}

func TestCheckpointTracker_RemoveSegment(t *testing.T) {
	tracker := NewCheckpointTracker()

	segID := int64(1001)
	pos := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{1}, Timestamp: 100}

	tracker.RecordBatch(segID, 50, pos)
	tracker.UpdateFlushedOffset(segID, 25)

	// Verify data exists
	assert.Equal(t, int64(25), tracker.GetFlushedOffset(segID))
	assert.Len(t, tracker.segmentBatches[segID], 1)

	// Remove segment
	tracker.RemoveSegment(segID)

	// Verify data is removed
	assert.Equal(t, int64(0), tracker.GetFlushedOffset(segID))
	assert.Len(t, tracker.segmentBatches[segID], 0)
}

func TestCheckpointTracker_GetUnflushedRowCount(t *testing.T) {
	tracker := NewCheckpointTracker()

	segID := int64(1001)

	// No flushed offset set
	assert.Equal(t, int64(100), tracker.GetUnflushedRowCount(segID, 100))

	// Set flushed offset
	tracker.UpdateFlushedOffset(segID, 50)
	assert.Equal(t, int64(50), tracker.GetUnflushedRowCount(segID, 100))

	// Current row count equals flushed offset
	assert.Equal(t, int64(0), tracker.GetUnflushedRowCount(segID, 50))

	// Current row count less than flushed offset (shouldn't happen, but handle it)
	assert.Equal(t, int64(0), tracker.GetUnflushedRowCount(segID, 30))
}

func TestCheckpointTracker_HasUnflushedData(t *testing.T) {
	tracker := NewCheckpointTracker()

	segID := int64(1001)

	assert.True(t, tracker.HasUnflushedData(segID, 100))

	tracker.UpdateFlushedOffset(segID, 100)
	assert.False(t, tracker.HasUnflushedData(segID, 100))
	assert.True(t, tracker.HasUnflushedData(segID, 150))
}

func TestCheckpointTracker_GetSegmentIDs(t *testing.T) {
	tracker := NewCheckpointTracker()

	pos := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{1}, Timestamp: 100}

	tracker.RecordBatch(1001, 50, pos)
	tracker.RecordBatch(1002, 50, pos)
	tracker.InitSegment(1003, 100) // Only has flushed offset, no batches

	segIDs := tracker.GetSegmentIDs()
	assert.Len(t, segIDs, 3)
	assert.Contains(t, segIDs, int64(1001))
	assert.Contains(t, segIDs, int64(1002))
	assert.Contains(t, segIDs, int64(1003))
}

func TestCheckpointTracker_MultipleSegments(t *testing.T) {
	tracker := NewCheckpointTracker()

	seg1 := int64(1001)
	seg2 := int64(1002)

	pos1 := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{1}, Timestamp: 100}
	pos2 := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{2}, Timestamp: 200}

	// Record batches for different segments
	tracker.RecordBatch(seg1, 50, pos1)
	tracker.RecordBatch(seg2, 100, pos2)

	// Verify they are tracked separately
	cp1 := tracker.GetCheckpoint(seg1, 50)
	cp2 := tracker.GetCheckpoint(seg2, 100)

	assert.Equal(t, uint64(100), cp1.Timestamp)
	assert.Equal(t, uint64(200), cp2.Timestamp)

	// Update one segment shouldn't affect the other
	tracker.UpdateFlushedOffset(seg1, 50)
	assert.Nil(t, tracker.segmentBatches[seg1])
	assert.Len(t, tracker.segmentBatches[seg2], 1)
}
