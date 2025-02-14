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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/storage"
)

func TestSkipListDeleteBuffer(t *testing.T) {
	db := NewDeleteBuffer()

	db.Cache(10, []BufferItem{
		{PartitionID: 1},
	})

	result := db.List(0)

	assert.Equal(t, 1, len(result))
	assert.Equal(t, int64(1), result[0][0].PartitionID)

	db.TruncateBefore(11)
	result = db.List(0)
	assert.Equal(t, 0, len(result))
}

type DoubleCacheBufferSuite struct {
	suite.Suite
}

func (s *DoubleCacheBufferSuite) TestNewBuffer() {
	buffer := NewDoubleCacheDeleteBuffer[*Item](10, 1000)

	s.EqualValues(10, buffer.SafeTs())
}

func (s *DoubleCacheBufferSuite) TestCache() {
	buffer := NewDoubleCacheDeleteBuffer[*Item](10, 1000)
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
}

func (s *DoubleCacheBufferSuite) TestPut() {
	buffer := NewDoubleCacheDeleteBuffer[*Item](10, 1)
	buffer.Put(&Item{
		Ts: 11,
		Data: []BufferItem{
			{
				PartitionID: 200,
				DeleteData: storage.DeleteData{
					Pks:      []storage.PrimaryKey{storage.NewVarCharPrimaryKey("test1")},
					Tss:      []uint64{11},
					RowCount: 1,
				},
			},
		},
	})

	buffer.Put(&Item{
		Ts: 12,
		Data: []BufferItem{
			{
				PartitionID: 200,
				DeleteData: storage.DeleteData{
					Pks:      []storage.PrimaryKey{storage.NewVarCharPrimaryKey("test2")},
					Tss:      []uint64{12},
					RowCount: 1,
				},
			},
		},
	})

	s.Equal(2, len(buffer.ListAfter(11)))
	s.Equal(1, len(buffer.ListAfter(12)))
	entryNum, memorySize := buffer.Size()
	s.EqualValues(2, entryNum)
	s.EqualValues(234, memorySize)

	buffer.Put(&Item{
		Ts: 13,
		Data: []BufferItem{
			{
				PartitionID: 200,
				DeleteData: storage.DeleteData{
					Pks:      []storage.PrimaryKey{storage.NewVarCharPrimaryKey("test3")},
					Tss:      []uint64{13},
					RowCount: 1,
				},
			},
		},
	})

	s.Equal(2, len(buffer.ListAfter(11)))
	s.Equal(2, len(buffer.ListAfter(12)))
	s.Equal(1, len(buffer.ListAfter(13)))
	entryNum, memorySize = buffer.Size()
	s.EqualValues(2, entryNum)
	s.EqualValues(234, memorySize)
}

func (s *DoubleCacheBufferSuite) TestL0SegmentOperations() {
	buffer := NewDoubleCacheDeleteBuffer[*Item](10, 1000)

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
	seg3.On("Release", mock.Anything).Return()

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
	emptyBuffer := NewDoubleCacheDeleteBuffer[*Item](10, 1000)
	s.Equal(0, len(emptyBuffer.ListL0()))

	// Test UnRegister
	buffer.UnRegister(15)
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

func TestDoubleCacheDeleteBuffer(t *testing.T) {
	suite.Run(t, new(DoubleCacheBufferSuite))
}
