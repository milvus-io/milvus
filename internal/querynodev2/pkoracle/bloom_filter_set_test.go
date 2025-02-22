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
	"strconv"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestInt64Pk(t *testing.T) {
	paramtable.Init()
	batchSize := 100
	pks := make([]storage.PrimaryKey, 0)

	for i := 0; i < batchSize; i++ {
		pk := storage.NewInt64PrimaryKey(int64(i))
		pks = append(pks, pk)
	}

	bfs := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs.UpdateBloomFilter(pks)

	for i := 0; i < batchSize; i++ {
		lc := storage.NewLocationsCache(pks[i])
		ret := bfs.MayPkExist(lc)
		assert.True(t, ret)
	}

	assert.Equal(t, int64(1), bfs.ID())
	assert.Equal(t, int64(1), bfs.Partition())
	assert.Equal(t, commonpb.SegmentState_Sealed, bfs.Type())
}

func TestVarCharPk(t *testing.T) {
	paramtable.Init()
	batchSize := 100
	pks := make([]storage.PrimaryKey, 0)

	for i := 0; i < batchSize; i++ {
		pk := storage.NewVarCharPrimaryKey(strconv.FormatInt(int64(i), 10))
		pks = append(pks, pk)
	}

	bfs := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs.UpdateBloomFilter(pks)

	for i := 0; i < batchSize; i++ {
		lc := storage.NewLocationsCache(pks[i])
		ret := bfs.MayPkExist(lc)
		assert.True(t, ret)
	}
}

func TestHistoricalStat(t *testing.T) {
	paramtable.Init()
	batchSize := 100
	pks := make([]storage.PrimaryKey, 0)
	for i := 0; i < batchSize; i++ {
		pk := storage.NewVarCharPrimaryKey(strconv.FormatInt(int64(i), 10))
		pks = append(pks, pk)
	}

	bfs := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
	bfs.UpdateBloomFilter(pks)

	// mock historical bf
	bfs.AddHistoricalStats(bfs.currentStat)
	bfs.AddHistoricalStats(bfs.currentStat)
	bfs.currentStat = nil

	for i := 0; i < batchSize; i++ {
		lc := storage.NewLocationsCache(pks[i])
		ret := bfs.MayPkExist(lc)
		assert.True(t, ret)
	}

	lc := storage.NewBatchLocationsCache(pks)
	ret := bfs.BatchPkExist(lc)
	for i := range ret {
		assert.True(t, ret[i])
	}
}
