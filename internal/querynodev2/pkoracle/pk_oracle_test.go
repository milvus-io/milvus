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

package pkoracle

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestGet(t *testing.T) {
	paramtable.Init()
	pko := NewPkOracle()

	batchSize := 100
	pks := make([]storage.PrimaryKey, 0)
	for i := 0; i < batchSize; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i))
		pks = append(pks, pk)
	}

	bfs := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs.UpdateBloomFilter(pks)
	pko.Register(bfs, 1)

	ret := pko.Exists(bfs, 1)
	assert.True(t, ret)

	ret = pko.Exists(bfs, 2)
	assert.False(t, ret)

	for i := 0; i < batchSize; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i))
		segmentIDs, ok := pko.Get(pk)
		assert.Nil(t, ok)
		assert.Contains(t, segmentIDs, int64(1))
	}

	pko.Remove(WithSegmentIDs(1))

	for i := 0; i < batchSize; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i))
		segmentIDs, ok := pko.Get(pk)
		assert.Nil(t, ok)
		assert.NotContains(t, segmentIDs, int64(1))
	}
}
