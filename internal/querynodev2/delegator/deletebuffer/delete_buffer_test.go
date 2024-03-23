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
	"github.com/stretchr/testify/suite"

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
}

func TestDoubleCacheDeleteBuffer(t *testing.T) {
	suite.Run(t, new(DoubleCacheBufferSuite))
}
