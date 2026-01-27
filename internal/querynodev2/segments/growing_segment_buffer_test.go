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
	"container/heap"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/tsoutil"
)

func init() {
	paramtable.Init()
}

func TestGrowingSegmentBuffer_Basic(t *testing.T) {
	// Create mock segment
	seg := NewMockSegment(t)
	seg.EXPECT().ID().Return(int64(1001))
	seg.EXPECT().MemSize().Return(int64(1024 * 1024)) // 1MB
	seg.EXPECT().RowNum().Return(int64(100))

	// Create buffer without checkpoint tracker
	buf := NewGrowingSegmentBuffer(seg, nil)

	assert.Equal(t, int64(1001), buf.SegmentID())
	assert.Equal(t, seg, buf.Segment())
	assert.Equal(t, int64(1024*1024), buf.MemorySize())
	assert.Equal(t, int64(100), buf.RowCount())
	assert.Equal(t, uint64(0), buf.MinTimestamp()) // nil tracker returns 0
	assert.Equal(t, int64(0), buf.FlushedOffset()) // nil tracker returns 0
	assert.Equal(t, int64(100), buf.UnflushedRowCount())
	assert.True(t, buf.HasUnflushedData())
}

func TestGrowingSegmentBuffer_WithTracker(t *testing.T) {
	tracker := NewCheckpointTracker()
	segID := int64(1001)

	// Record some batches
	pos1 := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{1}, Timestamp: 100}
	pos2 := &msgpb.MsgPosition{ChannelName: "ch1", MsgID: []byte{2}, Timestamp: 200}
	tracker.RecordBatch(segID, 50, pos1)
	tracker.RecordBatch(segID, 100, pos2)

	// Create mock segment
	seg := NewMockSegment(t)
	seg.EXPECT().ID().Return(segID).Maybe()
	seg.EXPECT().MemSize().Return(int64(1024 * 1024)).Maybe()
	seg.EXPECT().RowNum().Return(int64(100)).Maybe()

	buf := NewGrowingSegmentBuffer(seg, tracker)

	// Initially no flushed data
	assert.Equal(t, uint64(100), buf.MinTimestamp()) // timestamp from first batch
	assert.Equal(t, int64(0), buf.FlushedOffset())
	assert.Equal(t, int64(100), buf.UnflushedRowCount())
	assert.True(t, buf.HasUnflushedData())

	// After flushing to offset 50
	tracker.UpdateFlushedOffset(segID, 50)
	assert.Equal(t, uint64(200), buf.MinTimestamp()) // timestamp from second batch
	assert.Equal(t, int64(50), buf.FlushedOffset())
	assert.Equal(t, int64(50), buf.UnflushedRowCount())
	assert.True(t, buf.HasUnflushedData())

	// After flushing all
	tracker.UpdateFlushedOffset(segID, 100)
	assert.Equal(t, uint64(0), buf.MinTimestamp()) // no more unflushed data
	assert.Equal(t, int64(100), buf.FlushedOffset())
	assert.Equal(t, int64(0), buf.UnflushedRowCount())
	assert.False(t, buf.HasUnflushedData())
}

func TestGrowingSegmentBuffer_IsFull(t *testing.T) {
	// Get the configured threshold - uses FlushInsertBufferSize (same as IsFull method)
	threshold := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
	if threshold <= 0 {
		threshold = 16 * 1024 * 1024 // 16MB default
	}

	t.Run("not full", func(t *testing.T) {
		seg := NewMockSegment(t)
		seg.EXPECT().MemSize().Return(threshold - 1)
		buf := NewGrowingSegmentBuffer(seg, nil)
		assert.False(t, buf.IsFull())
	})

	t.Run("full", func(t *testing.T) {
		seg := NewMockSegment(t)
		seg.EXPECT().MemSize().Return(threshold)
		buf := NewGrowingSegmentBuffer(seg, nil)
		assert.True(t, buf.IsFull())
	})

	t.Run("over full", func(t *testing.T) {
		seg := NewMockSegment(t)
		seg.EXPECT().MemSize().Return(threshold + 1)
		buf := NewGrowingSegmentBuffer(seg, nil)
		assert.True(t, buf.IsFull())
	})
}

// ============================================================================
// Sync Policy Tests
// ============================================================================

func createMockBuffer(id int64, memSize int64, rowCount int64, tracker *CheckpointTracker) *GrowingSegmentBuffer {
	seg := &MockSegment{}
	seg.On("ID").Return(id)
	seg.On("MemSize").Return(memSize)
	seg.On("RowNum").Return(rowCount)
	return NewGrowingSegmentBuffer(seg, tracker)
}

func TestGetGrowingFullBufferPolicy(t *testing.T) {
	threshold := paramtable.Get().DataNodeCfg.FlushInsertBufferSize.GetAsInt64()
	if threshold <= 0 {
		threshold = 16 * 1024 * 1024
	}

	policy := GetGrowingFullBufferPolicy()
	assert.Equal(t, "buffer full", policy.Reason())

	buffers := []*GrowingSegmentBuffer{
		createMockBuffer(1, threshold-1, 100, nil), // not full
		createMockBuffer(2, threshold, 100, nil),   // full
		createMockBuffer(3, threshold+1, 100, nil), // full
	}

	selected := policy.SelectSegments(buffers, 0)
	assert.Len(t, selected, 2)
	assert.Contains(t, selected, int64(2))
	assert.Contains(t, selected, int64(3))
}

func TestGetGrowingRowCountPolicy(t *testing.T) {
	tracker := NewCheckpointTracker()

	// Create buffers with different unflushed row counts
	buffers := []*GrowingSegmentBuffer{
		createMockBuffer(1, 1024, 50, tracker),  // 50 unflushed
		createMockBuffer(2, 1024, 100, tracker), // 100 unflushed
		createMockBuffer(3, 1024, 150, tracker), // 150 unflushed
	}

	policy := GetGrowingRowCountPolicy(100)
	assert.Equal(t, "row count threshold", policy.Reason())

	selected := policy.SelectSegments(buffers, 0)
	assert.Len(t, selected, 2)
	assert.Contains(t, selected, int64(2)) // 100 >= 100
	assert.Contains(t, selected, int64(3)) // 150 >= 100
}

func TestGetGrowingStaleBufferPolicy(t *testing.T) {
	tracker := NewCheckpointTracker()
	staleDuration := 10 * time.Second

	// Record batches with different timestamps
	now := time.Now()
	oldTs := tsoutil.ComposeTSByTime(now.Add(-20*time.Second), 0) // 20s ago - stale
	recentTs := tsoutil.ComposeTSByTime(now.Add(-5*time.Second), 0) // 5s ago - not stale
	currentTs := tsoutil.ComposeTSByTime(now, 0)

	tracker.RecordBatch(1, 100, &msgpb.MsgPosition{Timestamp: oldTs})
	tracker.RecordBatch(2, 100, &msgpb.MsgPosition{Timestamp: recentTs})

	buffers := []*GrowingSegmentBuffer{
		createMockBuffer(1, 1024, 100, tracker), // stale
		createMockBuffer(2, 1024, 100, tracker), // not stale
		createMockBuffer(3, 1024, 100, tracker), // no data (minTs = 0)
	}

	policy := GetGrowingStaleBufferPolicy(staleDuration)
	assert.Equal(t, "buffer stale", policy.Reason())

	// Use current time as the timestamp for comparison
	selected := policy.SelectSegments(buffers, currentTs)
	// Only segment 1 should be stale (20s > 10s + jitter)
	// Segment 3 has no unflushed data (minTs = 0)
	assert.Contains(t, selected, int64(1))
	assert.NotContains(t, selected, int64(3)) // no unflushed data
}

func TestGetGrowingOldestBufferPolicy(t *testing.T) {
	tracker := NewCheckpointTracker()

	// Record batches with different timestamps
	tracker.RecordBatch(1, 100, &msgpb.MsgPosition{Timestamp: 100})
	tracker.RecordBatch(2, 100, &msgpb.MsgPosition{Timestamp: 200})
	tracker.RecordBatch(3, 100, &msgpb.MsgPosition{Timestamp: 300})
	tracker.RecordBatch(4, 100, &msgpb.MsgPosition{Timestamp: 400})

	buffers := []*GrowingSegmentBuffer{
		createMockBuffer(1, 1024, 100, tracker), // oldest (ts=100)
		createMockBuffer(2, 1024, 100, tracker), // second oldest (ts=200)
		createMockBuffer(3, 1024, 100, tracker), // third oldest (ts=300)
		createMockBuffer(4, 1024, 100, tracker), // newest (ts=400)
	}

	t.Run("select 2 oldest", func(t *testing.T) {
		policy := GetGrowingOldestBufferPolicy(2)
		assert.Equal(t, "oldest buffers", policy.Reason())

		selected := policy.SelectSegments(buffers, 0)
		assert.Len(t, selected, 2)
		assert.Contains(t, selected, int64(1))
		assert.Contains(t, selected, int64(2))
	})

	t.Run("select more than available", func(t *testing.T) {
		policy := GetGrowingOldestBufferPolicy(10)
		selected := policy.SelectSegments(buffers, 0)
		assert.Len(t, selected, 4) // returns all with unflushed data
	})

	t.Run("no unflushed data", func(t *testing.T) {
		// Flush all data
		tracker.UpdateFlushedOffset(1, 100)
		tracker.UpdateFlushedOffset(2, 100)
		tracker.UpdateFlushedOffset(3, 100)
		tracker.UpdateFlushedOffset(4, 100)

		policy := GetGrowingOldestBufferPolicy(2)
		selected := policy.SelectSegments(buffers, 0)
		assert.Len(t, selected, 0)
	})
}

func TestGetGrowingMemoryPressurePolicy(t *testing.T) {
	tracker := NewCheckpointTracker()

	// Create buffers with different memory sizes
	buffers := []*GrowingSegmentBuffer{
		createMockBuffer(1, 10*1024*1024, 100, tracker),  // 10MB
		createMockBuffer(2, 20*1024*1024, 100, tracker),  // 20MB
		createMockBuffer(3, 30*1024*1024, 100, tracker),  // 30MB
		createMockBuffer(4, 40*1024*1024, 100, tracker),  // 40MB
	}

	t.Run("below threshold", func(t *testing.T) {
		// Total: 100MB, threshold: 200MB -> no pressure
		policy := GetGrowingMemoryPressurePolicy(200 * 1024 * 1024)
		assert.Equal(t, "memory pressure", policy.Reason())

		selected := policy.SelectSegments(buffers, 0)
		assert.Len(t, selected, 0)
	})

	t.Run("above threshold", func(t *testing.T) {
		// Total: 100MB, threshold: 50MB -> pressure
		// Target free: 100MB - 25MB = 75MB
		policy := GetGrowingMemoryPressurePolicy(50 * 1024 * 1024)

		selected := policy.SelectSegments(buffers, 0)
		assert.True(t, len(selected) > 0)
		// Should select largest segments first (4, 3, ...)
		assert.Contains(t, selected, int64(4)) // 40MB - largest
	})
}

func TestGrowingSegmentHeap(t *testing.T) {
	tracker := NewCheckpointTracker()

	// Record batches with different timestamps
	tracker.RecordBatch(1, 100, &msgpb.MsgPosition{Timestamp: 300}) // newer
	tracker.RecordBatch(2, 100, &msgpb.MsgPosition{Timestamp: 100}) // oldest
	tracker.RecordBatch(3, 100, &msgpb.MsgPosition{Timestamp: 200}) // middle

	buf1 := createMockBuffer(1, 1024, 100, tracker)
	buf2 := createMockBuffer(2, 1024, 100, tracker)
	buf3 := createMockBuffer(3, 1024, 100, tracker)

	h := &growingSegmentHeap{}
	heap.Init(h)

	// Push all buffers using heap.Push to maintain heap property
	heap.Push(h, buf1)
	heap.Push(h, buf2)
	heap.Push(h, buf3)

	assert.Equal(t, 3, h.Len())

	// Pop should return newest first (max-heap by timestamp)
	popped := heap.Pop(h).(*GrowingSegmentBuffer)
	assert.Equal(t, int64(1), popped.SegmentID()) // ts=300, newest

	popped = heap.Pop(h).(*GrowingSegmentBuffer)
	assert.Equal(t, int64(3), popped.SegmentID()) // ts=200, middle

	popped = heap.Pop(h).(*GrowingSegmentBuffer)
	assert.Equal(t, int64(2), popped.SegmentID()) // ts=100, oldest
}

func TestGrowingSyncPolicyFunc(t *testing.T) {
	// Test that GrowingSyncPolicyFunc correctly implements the interface
	customPolicy := GrowingSyncPolicyFunc{
		fn: func(buffers []*GrowingSegmentBuffer, ts uint64) []int64 {
			return []int64{1, 2, 3}
		},
		reason: "custom reason",
	}

	var policy GrowingSyncPolicy = customPolicy
	assert.Equal(t, "custom reason", policy.Reason())
	assert.Equal(t, []int64{1, 2, 3}, policy.SelectSegments(nil, 0))
}
