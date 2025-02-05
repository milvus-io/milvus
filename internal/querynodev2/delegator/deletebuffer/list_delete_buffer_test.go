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

	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/suite"

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

	seg2 := segments.NewMockSegment(s.T())
	seg2.On("ID").Return(int64(2))
	seg2.On("Release", mock.Anything).Return()

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
	emptyBuffer := NewListDeleteBuffer[*Item](10, 1000, []string{})
	s.Equal(0, len(emptyBuffer.ListL0()))

	// Test UnRegister
	buffer.UnRegister(15, 1)
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

	// Test UnRegister with non-existent segment
	buffer.UnRegister(20, 999)

	// Test RegisterL0 with nil segment (should not panic)
	buffer.RegisterL0(nil)
	s.Equal(0, len(buffer.ListL0()))
}

func TestListDeleteBuffer(t *testing.T) {
	suite.Run(t, new(ListDeleteBufferSuite))
}
