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

package meta

import (
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type BloomFilterSetSuite struct {
	suite.Suite
	bfs *BloomFilterSet
}

func (s *BloomFilterSetSuite) SetupTest() {
	paramtable.Init()
	s.bfs = NewBloomFilterSet()
}

func (s *BloomFilterSetSuite) TearDownSuite() {
	s.bfs = nil
}

func (s *BloomFilterSetSuite) GetFieldData(ids []int64) storage.FieldData {
	fd, err := storage.NewFieldData(schemapb.DataType_Int64, &schemapb.FieldSchema{
		FieldID:      101,
		Name:         "ID",
		IsPrimaryKey: true,
		DataType:     schemapb.DataType_Int64,
	})
	s.Require().NoError(err)

	for _, id := range ids {
		err = fd.AppendRow(id)
		s.Require().NoError(err)
	}
	return fd
}

func (s *BloomFilterSetSuite) TestWriteRead() {
	ids := []int64{1, 2, 3, 4, 5}
	for _, id := range ids {
		s.False(s.bfs.PkExists(storage.NewLocationsCache(storage.NewInt64PrimaryKey(id))), "pk shall not exist before update")
	}

	err := s.bfs.UpdatePKRange(s.GetFieldData(ids))
	s.NoError(err)

	for _, id := range ids {
		s.True(s.bfs.PkExists(storage.NewLocationsCache(storage.NewInt64PrimaryKey(id))), "pk shall return exist after update")
	}
}

func (s *BloomFilterSetSuite) TestRoll() {
	history := s.bfs.GetHistory()

	s.Equal(0, len(history), "history empty for new bfs")

	ids := []int64{1, 2, 3, 4, 5}
	err := s.bfs.UpdatePKRange(s.GetFieldData(ids))
	s.NoError(err)

	newEntry := &storage.PrimaryKeyStats{}

	s.bfs.Roll(newEntry)

	history = s.bfs.GetHistory()
	s.Equal(1, len(history), "history shall have one entry after roll with current data")

	s.bfs.Roll()
	history = s.bfs.GetHistory()
	s.Equal(1, len(history), "history shall have one entry after empty roll")
}

func TestBloomFilterSet(t *testing.T) {
	suite.Run(t, new(BloomFilterSetSuite))
}
