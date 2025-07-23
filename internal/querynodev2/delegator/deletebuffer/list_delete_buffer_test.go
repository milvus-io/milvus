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

package deletebuffer

import (
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
)

type ListDeleteBufferSuite struct {
	suite.Suite
}

func (s *ListDeleteBufferSuite) TestNewBuffer() {
	buffer := NewListDeleteBuffer[*Item](10, 1000, []string{"1", "dml-1"})

	s.EqualValues(10, buffer.SafeTs())

	ldb, ok := buffer.(*listDeleteBuffer[*Item])
	s.True(ok)
	s.Len(ldb.list, 1)
}

func (s *ListDeleteBufferSuite) TestCache() {
	buffer := NewListDeleteBuffer[*Item](10, 1000, []string{"1", "dml-1"})
	buffer.Put(&Item{
		Ts: 11,
		Data: []BufferItem{
			{
				PartitionID: 200,
				DeleteData:  storage.DeleteData{},
			},
		},
	})

	buffer.Put(&Item{
		Ts: 12,
		Data: []BufferItem{
			{
				PartitionID: 200,
				DeleteData:  storage.DeleteData{},
			},
		},
	})

	s.Equal(2, len(buffer.ListAfter(11)))
	s.Equal(1, len(buffer.ListAfter(12)))
	entryNum, memorySize := buffer.Size()
	s.EqualValues(0, entryNum)
	s.EqualValues(192, memorySize)
}

func (s *ListDeleteBufferSuite) TestTryDiscard() {
	buffer := NewListDeleteBuffer[*Item](10, 1, []string{"1", "dml-1"})
	buffer.Put(&Item{
		Ts: 10,
		Data: []BufferItem{
			{
				PartitionID: 200,
				DeleteData: storage.DeleteData{
					Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(1)},
					Tss:      []uint64{10},
					RowCount: 1,
				},
			},
		},
	})

	buffer.Put(&Item{
		Ts: 20,
		Data: []BufferItem{
			{
				PartitionID: 200,
				DeleteData: storage.DeleteData{
					Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(2)},
					Tss:      []uint64{20},
					RowCount: 1,
				},
			},
		},
	})

	s.Equal(2, len(buffer.ListAfter(10)))
	entryNum, memorySize := buffer.Size()
	s.EqualValues(2, entryNum)
	s.EqualValues(240, memorySize)

	buffer.TryDiscard(10)
	s.Equal(2, len(buffer.ListAfter(10)), "equal ts shall not discard block")
	entryNum, memorySize = buffer.Size()
	s.EqualValues(2, entryNum)
	s.EqualValues(240, memorySize)

	buffer.TryDiscard(9)
	s.Equal(2, len(buffer.ListAfter(10)), "history ts shall not discard any block")
	entryNum, memorySize = buffer.Size()
	s.EqualValues(2, entryNum)
	s.EqualValues(240, memorySize)

	buffer.TryDiscard(20)
	s.Equal(1, len(buffer.ListAfter(10)), "first block shall be discarded")
	entryNum, memorySize = buffer.Size()
	s.EqualValues(1, entryNum)
	s.EqualValues(120, memorySize)

	buffer.TryDiscard(20)
	s.Equal(1, len(buffer.ListAfter(10)), "discard will not happen if there is only one block")
	s.EqualValues(1, entryNum)
	s.EqualValues(120, memorySize)
}

func (s *ListDeleteBufferSuite) TestL0SegmentOperations() {
	buffer := NewListDeleteBuffer[*Item](10, 1000, []string{"1", "dml-1"})

	// Create mock segments with specific IDs
	seg1 := segments.NewMockSegment(s.T())
	seg1.On("ID").Return(int64(1))
	seg1.On("Release", mock.Anything).Return()
	seg1.On("StartPosition").Return(&msgpb.MsgPosition{
		Timestamp: 10,
	})

	seg2 := segments.NewMockSegment(s.T())
	seg2.On("ID").Return(int64(2))
	seg2.On("Release", mock.Anything).Return()
	seg2.On("StartPosition").Return(&msgpb.MsgPosition{
		Timestamp: 20,
	})

	seg3 := segments.NewMockSegment(s.T())
	seg3.On("ID").Return(int64(3))
	seg3.On("Release", mock.Anything).Return()
	seg3.On("StartPosition").Return(&msgpb.MsgPosition{
		Timestamp: 30,
	})

	// Test RegisterL0 with multiple segments
	buffer.RegisterL0(seg1, seg2)
	segments := buffer.ListL0()
	s.Equal(2, len(segments))

	// Verify segment IDs by collecting them first
	ids := make([]int64, 0, len(segments))
	for _, seg := range segments {
		ids = append(ids, seg.ID())
	}
	s.ElementsMatch([]int64{1, 2}, ids, "expected segment IDs 1 and 2 in any order")

	// Test ListL0 with empty buffer
	emptyBuffer := NewListDeleteBuffer[*Item](10, 1000, []string{})
	s.Equal(0, len(emptyBuffer.ListL0()))

	// Test UnRegister
	buffer.UnRegister(seg1.StartPosition().GetTimestamp())
	segments = buffer.ListL0()
	s.Equal(2, len(segments))
	buffer.UnRegister(seg1.StartPosition().GetTimestamp() + 1)
	segments = buffer.ListL0()
	s.Equal(1, len(segments))
	s.Equal(int64(2), segments[0].ID())

	// Verify Release was called on unregistered segment
	seg1.AssertCalled(s.T(), "Release", mock.Anything)

	// Test Clear
	buffer.RegisterL0(seg3)
	s.Equal(2, len(buffer.ListL0()))
	buffer.Clear()
	s.Equal(0, len(buffer.ListL0()))

	// Verify Release was called on all segments
	seg2.AssertCalled(s.T(), "Release", mock.Anything)
	seg3.AssertCalled(s.T(), "Release", mock.Anything)

	// Test RegisterL0 with nil segment (should not panic)
	buffer.RegisterL0(nil)
	s.Equal(0, len(buffer.ListL0()))
}

func TestListDeleteBuffer(t *testing.T) {
	suite.Run(t, new(ListDeleteBufferSuite))
}

func TestListDeleteBuffer_PinUnpinBasic(t *testing.T) {
	// Create a new list delete buffer
	buffer := NewListDeleteBuffer[*Item](1000, 1, []string{"test1", "test2"})

	// Add some test data
	item1 := &Item{Ts: 1000, Data: []BufferItem{
		{
			PartitionID: 200,
			DeleteData: storage.DeleteData{
				Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(1)},
				Tss:      []uint64{1000},
				RowCount: 1,
			},
		},
	}}
	item2 := &Item{Ts: 2000, Data: []BufferItem{
		{
			PartitionID: 200,
			DeleteData: storage.DeleteData{
				Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(2)},
				Tss:      []uint64{2000},
				RowCount: 1,
			},
		},
	}}
	item3 := &Item{Ts: 3000, Data: []BufferItem{
		{
			PartitionID: 200,
			DeleteData: storage.DeleteData{
				Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(3)},
				Tss:      []uint64{3000},
				RowCount: 1,
			},
		},
	}}

	buffer.Put(item1)
	buffer.Put(item2)
	buffer.Put(item3)

	// Verify initial state
	entryNum, _ := buffer.Size()
	assert.Equal(t, int64(3), entryNum)

	// Pin timestamp 1500 for segment 123
	buffer.Pin(1500, 123)

	// Try to discard data before timestamp 2000
	// This should be skipped because there's a pinned timestamp (1500) before cleanTs (2000)
	buffer.TryDiscard(2000)

	// Verify that all data is still there (cleanup was skipped)
	entryNum, _ = buffer.Size()
	assert.Equal(t, int64(3), entryNum)

	// Unpin timestamp 1500 for segment 123
	buffer.Unpin(1500, 123)

	// Try to discard data before timestamp 2000 again
	// Now cleanup should proceed normally
	buffer.TryDiscard(2000)

	// Verify that data before 2000 is cleaned up
	entryNum, _ = buffer.Size()
	assert.Equal(t, int64(2), entryNum)
}

func TestListDeleteBuffer_MultipleSegmentsPinSameTimestamp(t *testing.T) {
	// Create a new list delete buffer
	buffer := NewListDeleteBuffer[*Item](1000, 1, []string{"test1", "test2"})

	// Add test data
	item1 := &Item{Ts: 1000, Data: []BufferItem{
		{
			PartitionID: 200,
			DeleteData: storage.DeleteData{
				Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(1)},
				Tss:      []uint64{1000},
				RowCount: 1,
			},
		},
	}}
	item2 := &Item{Ts: 2000, Data: []BufferItem{
		{
			PartitionID: 200,
			DeleteData: storage.DeleteData{
				Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(2)},
				Tss:      []uint64{2000},
				RowCount: 1,
			},
		},
	}}

	buffer.Put(item1)
	buffer.Put(item2)

	// Multiple segments pin the same timestamp
	buffer.Pin(1500, 123) // Protects data after 1500
	buffer.Pin(1500, 456) // Also protects data after 1500

	// Try to discard data before timestamp 2000
	// This should be skipped because there's a pinned timestamp (1500) before cleanTs (2000)
	buffer.TryDiscard(2000)

	// Verify that all data is still there (cleanup was skipped)
	entryNum, _ := buffer.Size()
	assert.Equal(t, int64(2), entryNum) // All data should still be there

	// Unpin one segment
	buffer.Unpin(1500, 123)

	// Try to discard data before timestamp 2000
	// This should still be skipped because the other segment still has it pinned
	buffer.TryDiscard(2000)

	// Verify that data is still there (other segment still has it pinned)
	entryNum, _ = buffer.Size()
	assert.Equal(t, int64(2), entryNum) // Data should still be there

	// Unpin the other segment
	buffer.Unpin(1500, 456)

	// Try to discard data before timestamp 2000
	// Now cleanup should proceed normally
	buffer.TryDiscard(2000)

	// Verify that data is now cleaned up
	entryNum, _ = buffer.Size()
	assert.Equal(t, int64(1), entryNum) // Only data with ts >= 2000 should remain
}

func TestListDeleteBuffer_PinAfterCleanTs(t *testing.T) {
	// Create a new list delete buffer
	buffer := NewListDeleteBuffer[*Item](1000, 1, []string{"test1", "test2"})

	// Add test data
	item1 := &Item{Ts: 1000, Data: []BufferItem{
		{
			PartitionID: 200,
			DeleteData: storage.DeleteData{
				Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(1)},
				Tss:      []uint64{1000},
				RowCount: 1,
			},
		},
	}}
	item2 := &Item{Ts: 2000, Data: []BufferItem{
		{
			PartitionID: 200,
			DeleteData: storage.DeleteData{
				Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(2)},
				Tss:      []uint64{2000},
				RowCount: 1,
			},
		},
	}}
	item3 := &Item{Ts: 3000, Data: []BufferItem{
		{
			PartitionID: 200,
			DeleteData: storage.DeleteData{
				Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(3)},
				Tss:      []uint64{3000},
				RowCount: 1,
			},
		},
	}}

	buffer.Put(item1)
	buffer.Put(item2)
	buffer.Put(item3)

	// Pin a timestamp that is AFTER the cleanTs
	buffer.Pin(2500, 123) // Protects data after 2500

	// Try to discard data before timestamp 2000
	// This should proceed normally because the pinned timestamp (2500) is AFTER cleanTs (2000)
	buffer.TryDiscard(2000)

	// Verify that data before 2000 is cleaned up
	entryNum, _ := buffer.Size()
	assert.Equal(t, int64(2), entryNum) // Data with ts 2000 and 3000 should remain
}

// TestListDeleteBuffer_EdgeCases tests edge cases for the pinned functionality
func TestListDeleteBuffer_EdgeCases(t *testing.T) {
	// Create a new list delete buffer
	buffer := NewListDeleteBuffer[*Item](1000, 1, []string{"test1", "test2"})

	// Test with empty buffer - TryDiscard should proceed normally
	buffer.TryDiscard(2000)
	entryNum, _ := buffer.Size()
	assert.Equal(t, int64(0), entryNum) // No data to clean

	// Add some data
	item1 := &Item{Ts: 1000, Data: []BufferItem{
		{
			PartitionID: 200,
			DeleteData: storage.DeleteData{
				Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(1)},
				Tss:      []uint64{1000},
				RowCount: 1,
			},
		},
	}}

	buffer.Put(item1)

	// Pin timestamp equal to data timestamp
	buffer.Pin(1000, 123)

	// Try to discard with timestamp equal to pinned timestamp
	// This should be skipped because 1000 == 1000 (not < 1000)
	buffer.TryDiscard(1000)

	// Verify that data is still there
	entryNum, _ = buffer.Size()
	assert.Equal(t, int64(1), entryNum) // Data should still be there

	// Try to discard with timestamp greater than pinned timestamp
	// This should be skipped because 1000 < 2000
	buffer.TryDiscard(2000)

	// Verify that data is still there
	entryNum, _ = buffer.Size()
	assert.Equal(t, int64(1), entryNum) // Data should still be there

	// Try to discard with timestamp less than pinned timestamp
	// This should proceed normally because 1000 > 500
	buffer.TryDiscard(500)

	// Verify that data is still there (because 500 < 1000, no data to clean)
	entryNum, _ = buffer.Size()
	assert.Equal(t, int64(1), entryNum) // Data should still be there
}

// TestListDeleteBuffer_PinUnpinConcurrent tests concurrent pin and unpin operations
func TestListDeleteBuffer_PinUnpinConcurrent(t *testing.T) {
	// Create a new list delete buffer
	buffer := NewListDeleteBuffer[*Item](1000, 1, []string{"test1", "test2"})

	// Add test data
	item1 := &Item{Ts: 1000, Data: []BufferItem{
		{
			PartitionID: 200,
			DeleteData: storage.DeleteData{
				Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(1)},
				Tss:      []uint64{1000},
				RowCount: 1,
			},
		},
	}}
	item2 := &Item{Ts: 2000, Data: []BufferItem{
		{
			PartitionID: 200,
			DeleteData: storage.DeleteData{
				Pks:      []storage.PrimaryKey{storage.NewInt64PrimaryKey(2)},
				Tss:      []uint64{2000},
				RowCount: 1,
			},
		},
	}}
	buffer.Put(item1)
	buffer.Put(item2)

	// Test concurrent pin operations
	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(segmentID int64) {
			defer wg.Done()
			buffer.Pin(1500, segmentID)
		}(int64(i))
	}
	wg.Wait()

	// Try to discard - should be skipped due to pinned timestamp
	buffer.TryDiscard(2000)
	entryNum, _ := buffer.Size()
	assert.Equal(t, int64(2), entryNum) // All data should remain

	// Test concurrent unpin operations
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(segmentID int64) {
			defer wg.Done()
			buffer.Unpin(1500, segmentID)
		}(int64(i))
	}
	wg.Wait()

	// Try to discard again - should proceed normally
	buffer.TryDiscard(2000)
	entryNum, _ = buffer.Size()
	assert.Equal(t, int64(1), entryNum) // Only data with ts >= 2000 should remain
}
