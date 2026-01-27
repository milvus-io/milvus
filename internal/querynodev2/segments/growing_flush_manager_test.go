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
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestNewGrowingFlushManager(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
		},
	}

	tracker := NewCheckpointTracker()

	t.Run("default configuration", func(t *testing.T) {
		m := NewGrowingFlushManager(
			1,                   // collectionID
			"test-channel",      // channelName
			schema,              // schema
			nil,                 // collectionManager
			nil,                 // segmentManager
			nil,                 // broker
			nil,                 // chunkManager
			nil,                 // allocator
			tracker,             // checkpointTracker
		)

		assert.NotNil(t, m)
		assert.Equal(t, int64(1), m.collectionID)
		assert.Equal(t, "test-channel", m.channelName)
		assert.Equal(t, schema, m.schema)
		assert.Equal(t, tracker, m.checkpointTracker)
		assert.Equal(t, 100*time.Millisecond, m.flushInterval)
		assert.True(t, len(m.policies) > 0) // Default policies should be set
	})

	t.Run("custom configuration", func(t *testing.T) {
		customPolicy := GrowingSyncPolicyFunc{
			fn:     func(buffers []*GrowingSegmentBuffer, ts uint64) []int64 { return nil },
			reason: "custom",
		}

		m := NewGrowingFlushManager(
			1,
			"test-channel",
			schema,
			nil,
			nil,
			nil,
			nil,
			nil,
			tracker,
			WithFlushInterval(500*time.Millisecond),
			WithSyncPolicies(customPolicy),
		)

		assert.Equal(t, 500*time.Millisecond, m.flushInterval)
		assert.Len(t, m.policies, 1)
		assert.Equal(t, "custom", m.policies[0].Reason())
	})
}

func TestGrowingFlushManager_StartStop(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		},
	}

	tracker := NewCheckpointTracker()
	segMgr := NewMockSegmentManager(t)

	m := NewGrowingFlushManager(
		1,
		"test-channel",
		schema,
		nil,
		segMgr,
		nil,
		nil,
		nil,
		tracker,
		WithFlushInterval(10*time.Millisecond),
	)

	ctx := context.Background()

	// Start the manager
	m.Start(ctx)

	// Let it run briefly
	time.Sleep(50 * time.Millisecond)

	// Stop should complete without blocking
	done := make(chan struct{})
	go func() {
		m.Stop()
		close(done)
	}()

	select {
	case <-done:
		// Success - stopped cleanly
	case <-time.After(time.Second):
		t.Fatal("Stop() did not complete in time")
	}
}

func TestGrowingFlushManager_WrapAsBuffers(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
	}

	tracker := NewCheckpointTracker()
	// Record batches so segments have unflushed data (needed by HasUnflushedData check)
	pos := &msgpb.MsgPosition{Timestamp: 100}
	tracker.RecordBatch(1001, 50, pos)
	tracker.RecordBatch(1002, 50, pos)

	m := NewGrowingFlushManager(
		1,
		"test-channel",
		schema,
		nil,
		nil,
		nil,
		nil,
		nil,
		tracker,
	)

	// Create mock segments with all necessary expectations
	seg1 := NewMockSegment(t)
	seg1.EXPECT().ID().Return(int64(1001)).Maybe()
	seg1.EXPECT().RowNum().Return(int64(50)).Maybe()

	seg2 := NewMockSegment(t)
	seg2.EXPECT().ID().Return(int64(1002)).Maybe()
	seg2.EXPECT().RowNum().Return(int64(50)).Maybe()

	segments := []Segment{seg1, seg2}
	buffers := m.wrapAsBuffers(segments)

	assert.Len(t, buffers, 2)
	assert.Equal(t, int64(1001), buffers[0].SegmentID())
	assert.Equal(t, int64(1002), buffers[1].SegmentID())
}

func TestGrowingFlushManager_GetGrowingSegments(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
	}

	tracker := NewCheckpointTracker()
	segMgr := NewMockSegmentManager(t)

	m := NewGrowingFlushManager(
		1,
		"test-channel",
		schema,
		nil,
		segMgr,
		nil,
		nil,
		nil,
		tracker,
	)

	t.Run("no tracked segments", func(t *testing.T) {
		segments := m.getGrowingSegments()
		assert.Len(t, segments, 0)
	})

	t.Run("with tracked segments", func(t *testing.T) {
		// Record batches to track segments
		pos := &msgpb.MsgPosition{Timestamp: 100}
		tracker.RecordBatch(1001, 50, pos)
		tracker.RecordBatch(1002, 50, pos)

		// Setup mock expectations
		seg1 := NewMockSegment(t)
		seg2 := NewMockSegment(t)

		segMgr.EXPECT().GetGrowing(int64(1001)).Return(seg1).Once()
		segMgr.EXPECT().GetGrowing(int64(1002)).Return(seg2).Once()

		segments := m.getGrowingSegments()
		assert.Len(t, segments, 2)
	})

	t.Run("segment not found in manager", func(t *testing.T) {
		// Remove old tracker data and create fresh tracker
		tracker = NewCheckpointTracker()
		m.checkpointTracker = tracker

		pos := &msgpb.MsgPosition{Timestamp: 100}
		tracker.RecordBatch(9999, 50, pos)

		// Create a fresh mock for this test
		segMgr2 := NewMockSegmentManager(t)
		m.segmentManager = segMgr2

		// Segment doesn't exist
		segMgr2.EXPECT().GetGrowing(int64(9999)).Return(nil).Once()

		segments := m.getGrowingSegments()
		assert.Len(t, segments, 0)
	})
}

func TestGrowingFlushManager_FindSegment(t *testing.T) {
	paramtable.Init()

	m := &GrowingFlushManager{}

	seg1 := NewMockSegment(t)
	seg1.EXPECT().ID().Return(int64(1001))

	seg2 := NewMockSegment(t)
	seg2.EXPECT().ID().Return(int64(1002))

	segments := []Segment{seg1, seg2}

	t.Run("found", func(t *testing.T) {
		found := m.findSegment(segments, 1001)
		assert.Equal(t, seg1, found)
	})

	t.Run("not found", func(t *testing.T) {
		found := m.findSegment(segments, 9999)
		assert.Nil(t, found)
	})
}

func TestGrowingFlushManager_CheckAndTriggerSync(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
	}

	tracker := NewCheckpointTracker()
	segMgr := NewMockSegmentManager(t)

	// Create a custom policy that always selects segment 1001
	customPolicy := GrowingSyncPolicyFunc{
		fn: func(buffers []*GrowingSegmentBuffer, ts uint64) []int64 {
			for _, buf := range buffers {
				if buf.SegmentID() == 1001 {
					return []int64{1001}
				}
			}
			return nil
		},
		reason: "test policy",
	}

	// Create mock chunk manager for buildFlushConfig
	mockCM := mock_storage.NewMockChunkManager(t)
	mockCM.EXPECT().RootPath().Return("/test/root").Maybe()

	m := NewGrowingFlushManager(
		1,
		"test-channel",
		schema,
		nil,
		segMgr,
		nil,    // broker - needed for triggerSync but we won't call it in this test
		mockCM, // chunkManager - needed for buildFlushConfig
		nil,
		tracker,
		WithSyncPolicies(customPolicy),
	)

	// Record batches to track segments
	pos := &msgpb.MsgPosition{Timestamp: 100}
	tracker.RecordBatch(1001, 50, pos)

	// Setup mock segment
	seg := NewMockSegment(t)
	seg.EXPECT().ID().Return(int64(1001)).Maybe()
	seg.EXPECT().MemSize().Return(int64(1024)).Maybe()
	seg.EXPECT().RowNum().Return(int64(100)).Maybe()
	seg.EXPECT().Partition().Return(int64(10)).Maybe()
	// FlushData will be called in doSync goroutine
	seg.EXPECT().FlushData(mock.Anything, mock.Anything, mock.Anything, mock.Anything).Return(nil, nil).Maybe()

	segMgr.EXPECT().GetGrowing(int64(1001)).Return(seg).Maybe()

	// checkAndTriggerSync will trigger sync in a goroutine.
	// The actual sync will fail gracefully (no FlushResult returned)
	ctx := context.Background()
	m.checkAndTriggerSync(ctx)

	// Wait briefly for the async goroutine to run
	time.Sleep(50 * time.Millisecond)
}

func TestGrowingFlushManager_DefaultPolicies(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
	}

	m := NewGrowingFlushManager(
		1,
		"test-channel",
		schema,
		nil,
		nil,
		nil,
		nil,
		nil,
		NewCheckpointTracker(),
	)

	policies := m.defaultPolicies()

	// Should have at least FullBufferPolicy and StaleBufferPolicy
	assert.True(t, len(policies) >= 2)

	reasons := make([]string, len(policies))
	for i, p := range policies {
		reasons[i] = p.Reason()
	}

	assert.Contains(t, reasons, "buffer full")
	assert.Contains(t, reasons, "buffer stale")
}

// Test segment mutex for concurrent sync prevention
func TestGrowingFlushManager_SegmentMutex(t *testing.T) {
	paramtable.Init()

	m := &GrowingFlushManager{}

	segID := int64(1001)

	// Get or create mutex
	mutexI, _ := m.syncMu.LoadOrStore(segID, &sync.Mutex{})
	mu := mutexI.(*sync.Mutex)

	// First lock should succeed
	locked := mu.TryLock()
	assert.True(t, locked)

	// Second lock should fail (mutex is held)
	locked2 := mu.TryLock()
	assert.False(t, locked2)

	// After unlock, lock should succeed again
	mu.Unlock()
	locked3 := mu.TryLock()
	assert.True(t, locked3)
	mu.Unlock()
}

// Mock helpers for testing
type testSyncPolicy struct {
	mock.Mock
}

func (p *testSyncPolicy) SelectSegments(buffers []*GrowingSegmentBuffer, ts uint64) []int64 {
	args := p.Called(buffers, ts)
	return args.Get(0).([]int64)
}

func (p *testSyncPolicy) Reason() string {
	args := p.Called()
	return args.String(0)
}

func TestGrowingFlushManager_BuildFlushConfig(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
		},
	}

	tracker := NewCheckpointTracker()

	// Create mock chunk manager
	mockCM := mock_storage.NewMockChunkManager(t)
	mockCM.EXPECT().RootPath().Return("/test/root").Maybe()

	m := NewGrowingFlushManager(
		1,                   // collectionID
		"test-channel",      // channelName
		schema,              // schema
		nil,                 // collectionManager
		nil,                 // segmentManager
		nil,                 // broker
		mockCM,              // chunkManager
		nil,                 // allocator
		tracker,             // checkpointTracker
	)

	// Create mock segment
	seg := NewMockSegment(t)
	seg.EXPECT().ID().Return(int64(1001)).Maybe()
	seg.EXPECT().Partition().Return(int64(10)).Maybe()

	config := m.buildFlushConfig(seg)

	assert.NotNil(t, config)
	assert.Contains(t, config.SegmentBasePath, "/test/root")
	assert.Contains(t, config.SegmentBasePath, "insert_log")
	assert.Contains(t, config.PartitionBasePath, "/test/root")
	assert.Equal(t, int64(1), config.CollectionID)
	assert.Equal(t, int64(10), config.PartitionID)
}

func TestGrowingFlushManager_DoSync_NoUnflushedData(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
	}

	tracker := NewCheckpointTracker()
	segMgr := NewMockSegmentManager(t)

	m := NewGrowingFlushManager(
		1,
		"test-channel",
		schema,
		nil,
		segMgr,
		nil,
		nil,
		nil,
		tracker,
	)

	// Create mock segment with no unflushed data
	seg := NewMockSegment(t)
	seg.EXPECT().ID().Return(int64(1001)).Maybe()
	seg.EXPECT().RowNum().Return(int64(100)).Maybe()

	// Set flushed offset to equal row count (no unflushed data)
	tracker.UpdateFlushedOffset(1001, 100)

	// doSync should return nil when no data to flush
	err := m.doSync(context.Background(), seg)
	assert.NoError(t, err)
}

func TestGrowingFlushManager_DoSync_WithUnflushedData(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
	}

	tracker := NewCheckpointTracker()
	mockCM := mock_storage.NewMockChunkManager(t)
	mockCM.EXPECT().RootPath().Return("/test/root").Maybe()

	m := NewGrowingFlushManager(
		1,
		"test-channel",
		schema,
		nil,
		nil,
		nil,
		mockCM,
		nil,
		tracker,
	)

	// Create mock segment with unflushed data
	seg := NewMockSegment(t)
	seg.EXPECT().ID().Return(int64(1001)).Maybe()
	seg.EXPECT().Partition().Return(int64(10)).Maybe()
	seg.EXPECT().RowNum().Return(int64(100)).Maybe()

	// Set flushed offset less than row count (has unflushed data)
	tracker.UpdateFlushedOffset(1001, 50)

	// Record batch to get checkpoint
	pos := &msgpb.MsgPosition{Timestamp: 200}
	tracker.RecordBatch(1001, 50, pos)

	// FlushData will be called
	flushResult := &FlushResult{
		ManifestPath: "/test/manifest.json",
		NumRows:      50,
	}
	seg.EXPECT().FlushData(mock.Anything, int64(50), int64(100), mock.Anything).Return(flushResult, nil).Once()

	// Without broker, saveManifestAndCheckpoint will fail
	// but the test should still exercise the main logic
	err := m.doSync(context.Background(), seg)
	// Expected to fail because broker is nil
	assert.Error(t, err)
}

func TestGrowingFlushManager_ForceSync(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
	}

	tracker := NewCheckpointTracker()
	segMgr := NewMockSegmentManager(t)

	m := NewGrowingFlushManager(
		1,
		"test-channel",
		schema,
		nil,
		segMgr,
		nil,
		nil,
		nil,
		tracker,
	)

	t.Run("segment not found", func(t *testing.T) {
		segMgr.EXPECT().GetGrowing(int64(9999)).Return(nil).Once()

		err := m.ForceSync(context.Background(), 9999)
		assert.NoError(t, err) // should return nil when segment not found
	})

	t.Run("segment exists but no unflushed data", func(t *testing.T) {
		seg := NewMockSegment(t)
		seg.EXPECT().ID().Return(int64(1001)).Maybe()
		seg.EXPECT().RowNum().Return(int64(100)).Maybe()

		segMgr.EXPECT().GetGrowing(int64(1001)).Return(seg).Once()

		// Set flushed offset equal to row count
		tracker.UpdateFlushedOffset(1001, 100)

		err := m.ForceSync(context.Background(), 1001)
		assert.NoError(t, err)
	})
}

func TestGrowingFlushManager_TriggerSync_AlreadyInProgress(t *testing.T) {
	paramtable.Init()

	schema := &schemapb.CollectionSchema{
		Name: "test_collection",
	}

	m := NewGrowingFlushManager(
		1,
		"test-channel",
		schema,
		nil,
		nil,
		nil,
		nil,
		nil,
		NewCheckpointTracker(),
	)

	// Create mock segment
	seg := NewMockSegment(t)
	seg.EXPECT().ID().Return(int64(1001)).Maybe()

	// Acquire the mutex first to simulate sync in progress
	muInterface, _ := m.syncMu.LoadOrStore(int64(1001), &sync.Mutex{})
	mu := muInterface.(*sync.Mutex)
	mu.Lock()
	defer mu.Unlock()

	// triggerSync should skip because sync is already in progress
	// (this is tested by not expecting any FlushData calls)
	m.triggerSync(context.Background(), seg, "test")

	// Give time for goroutine to potentially start
	time.Sleep(10 * time.Millisecond)

	// If FlushData was not called, the test passes
	// The mock would fail if FlushData was called without expectation
}

func TestGrowingFlushManager_GetCheckpointTracker(t *testing.T) {
	paramtable.Init()

	tracker := NewCheckpointTracker()

	m := NewGrowingFlushManager(
		1,
		"test-channel",
		nil,
		nil,
		nil,
		nil,
		nil,
		nil,
		tracker,
	)

	assert.Equal(t, tracker, m.GetCheckpointTracker())
}
