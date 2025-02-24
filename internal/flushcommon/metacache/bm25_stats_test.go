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

package metacache

import (
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

type BM25StatsSetSuite struct {
	suite.Suite
	stats        *SegmentBM25Stats
	bm25FieldIDs []int64
}

func (s *BM25StatsSetSuite) SetupTest() {
	paramtable.Init()
	s.stats = NewEmptySegmentBM25Stats()
	s.bm25FieldIDs = []int64{101, 102}
}

func (suite *BM25StatsSetSuite) TestMergeAndSeralize() {
	statsA := map[int64]*storage.BM25Stats{
		101: {},
	}
	statsA[101].Append(map[uint32]float32{1: 1, 2: 2})

	statsB := map[int64]*storage.BM25Stats{
		101: {},
	}
	statsB[101].Append(map[uint32]float32{1: 1, 2: 2})

	suite.stats.Merge(statsA)
	suite.stats.Merge(statsB)

	blobs, numrows, err := suite.stats.Serialize()
	suite.NoError(err)
	suite.Equal(numrows[101], int64(2))

	storageStats := storage.NewBM25Stats()
	err = storageStats.Deserialize(blobs[101])
	suite.NoError(err)

	suite.Equal(storageStats.NumRow(), int64(2))
}
