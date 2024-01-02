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

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/storage"
)

type ListDeleteBufferSuite struct {
	suite.Suite
}

func (s *ListDeleteBufferSuite) TestNewBuffer() {
	buffer := NewListDeleteBuffer[*Item](10, 1000)

	s.EqualValues(10, buffer.SafeTs())

	ldb, ok := buffer.(*listDeleteBuffer[*Item])
	s.True(ok)
	s.Len(ldb.list, 1)
}

func (s *ListDeleteBufferSuite) TestCache() {
	buffer := NewListDeleteBuffer[*Item](10, 1000)
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

func (s *ListDeleteBufferSuite) TestTryDiscard() {
	buffer := NewListDeleteBuffer[*Item](10, 1)
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

	buffer.TryDiscard(10)
	s.Equal(2, len(buffer.ListAfter(10)), "equal ts shall not discard block")

	buffer.TryDiscard(9)
	s.Equal(2, len(buffer.ListAfter(10)), "history ts shall not discard any block")

	buffer.TryDiscard(20)
	s.Equal(1, len(buffer.ListAfter(10)), "first block shall be discarded")

	buffer.TryDiscard(20)
	s.Equal(1, len(buffer.ListAfter(10)), "discard will not happen if there is only one block")
}

func TestListDeleteBuffer(t *testing.T) {
	suite.Run(t, new(ListDeleteBufferSuite))
}
