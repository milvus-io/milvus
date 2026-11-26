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
	"context"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/initcore"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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
	bfs.UpdatePkCandidate(pks)

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
	bfs.UpdatePkCandidate(pks)

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
	bfs.UpdatePkCandidate(pks)

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

func TestMemSize(t *testing.T) {
	paramtable.Init()

	t.Run("empty bloom filter set", func(t *testing.T) {
		bfs := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
		size := bfs.MemSize()
		assert.Equal(t, int64(0), size)
	})

	t.Run("with current stat only", func(t *testing.T) {
		batchSize := 100
		pks := make([]storage.PrimaryKey, 0)
		for i := 0; i < batchSize; i++ {
			pk := storage.NewInt64PrimaryKey(int64(i))
			pks = append(pks, pk)
		}

		bfs := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
		bfs.UpdatePkCandidate(pks)

		size := bfs.MemSize()
		assert.Greater(t, size, int64(0))
		// Bloom filter size should be related to the configured BloomFilterSize
		// Cap() returns bits, MemSize() converts to bytes
		expectedSize := int64(bfs.currentStat.PkFilter.Cap() / 8)
		assert.Equal(t, expectedSize, size)
	})

	t.Run("with historical stats", func(t *testing.T) {
		batchSize := 100
		pks := make([]storage.PrimaryKey, 0)
		for i := 0; i < batchSize; i++ {
			pk := storage.NewInt64PrimaryKey(int64(i))
			pks = append(pks, pk)
		}

		bfs := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
		bfs.UpdatePkCandidate(pks)

		currentSize := bfs.MemSize()

		// Add historical stats (simulate compaction history)
		bfs.AddHistoricalStats(bfs.currentStat)
		bfs.AddHistoricalStats(bfs.currentStat)

		totalSize := bfs.MemSize()
		// Should be approximately 3x the single bloom filter size
		// (1 current + 2 historical, all pointing to same underlying filter for this test)
		assert.Equal(t, currentSize*3, totalSize)
	})

	t.Run("with nil current stat and historical stats", func(t *testing.T) {
		batchSize := 100
		pks := make([]storage.PrimaryKey, 0)
		for i := 0; i < batchSize; i++ {
			pk := storage.NewInt64PrimaryKey(int64(i))
			pks = append(pks, pk)
		}

		bfs := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
		bfs.UpdatePkCandidate(pks)

		singleSize := bfs.MemSize()

		// Add historical stats and clear current
		bfs.AddHistoricalStats(bfs.currentStat)
		bfs.currentStat = nil

		historicalSize := bfs.MemSize()
		assert.Equal(t, singleSize, historicalSize)
	})

	t.Run("varchar primary key", func(t *testing.T) {
		batchSize := 100
		pks := make([]storage.PrimaryKey, 0)
		for i := 0; i < batchSize; i++ {
			pk := storage.NewVarCharPrimaryKey(strconv.FormatInt(int64(i), 10))
			pks = append(pks, pk)
		}

		bfs := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
		bfs.UpdatePkCandidate(pks)

		size := bfs.MemSize()
		assert.Greater(t, size, int64(0))
	})
}

func TestLazyBloomFilterSetLoadsOnce(t *testing.T) {
	paramtable.Init()
	require.NoError(t, initcore.InitTieredStorage(paramtable.Get()))
	pk := storage.NewInt64PrimaryKey(42)
	stats := NewBloomFilterSet(1, 2, commonpb.SegmentState_Sealed)
	stats.UpdatePkCandidate([]storage.PrimaryKey{pk})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	var loads atomic.Int32
	bfs := NewLazyBloomFilterSet(ctx, 1, 2, func(ctx context.Context, publish func([]*storage.PkStatistics)) error {
		assert.NoError(t, ctx.Err())
		loads.Add(1)
		publish([]*storage.PkStatistics{stats.Stats()})
		return nil
	})
	defer bfs.Refund()
	cancel()

	bfs.Charge()
	require.False(t, bfs.IsResourceCharged())
	require.Zero(t, bfs.MemSize())
	require.Zero(t, loads.Load())

	var readers sync.WaitGroup
	for range 16 {
		readers.Go(func() {
			assert.True(t, bfs.PkCandidateExist())
			assert.True(t, bfs.MayPkExist(storage.NewLocationsCache(pk)))
		})
	}
	readers.Wait()

	require.EqualValues(t, 1, loads.Load())
	require.Equal(t, []bool{true, false}, bfs.BatchPkExist(storage.NewBatchLocationsCache(
		[]storage.PrimaryKey{pk, storage.NewInt64PrimaryKey(43)})))
	require.Equal(t, stats.MemSize(), bfs.MemSize())
	require.True(t, bfs.IsResourceCharged())

	bfs.Refund()
	bfs.Charge()
	require.False(t, bfs.IsResourceCharged())
}

func TestLazyBloomFilterSetRetriesWithoutDroppingPKs(t *testing.T) {
	paramtable.Init()
	require.NoError(t, initcore.InitTieredStorage(paramtable.Get()))
	pk := storage.NewInt64PrimaryKey(42)
	other := storage.NewInt64PrimaryKey(43)
	stats := NewBloomFilterSet(1, 2, commonpb.SegmentState_Sealed)
	stats.UpdatePkCandidate([]storage.PrimaryKey{pk})
	loads := 0
	bfs := NewLazyBloomFilterSet(context.Background(), 1, 2, func(_ context.Context, publish func([]*storage.PkStatistics)) error {
		loads++
		if loads <= 2 {
			return merr.WrapErrServiceInternalMsg("injected stats read failure")
		}
		publish([]*storage.PkStatistics{stats.Stats()})
		return nil
	})
	defer bfs.Refund()

	pks := []storage.PrimaryKey{pk, other}
	require.False(t, bfs.PkCandidateExist())
	require.True(t, bfs.MayPkExist(storage.NewLocationsCache(other)))
	require.Equal(t, []bool{true, true}, bfs.BatchPkExist(storage.NewBatchLocationsCache(pks)))
	require.Nil(t, bfs.GetMinPk())
	require.Zero(t, bfs.MemSize())
	require.False(t, bfs.IsResourceCharged())
	require.Equal(t, 1, loads)
	require.Equal(t, initialBloomFilterRetryDelay, bfs.lazy.retryDelay)

	bfs.lazy.retryAt = time.Time{}
	require.False(t, bfs.PkCandidateExist())
	require.Equal(t, 2, loads)
	require.Equal(t, 2*initialBloomFilterRetryDelay, bfs.lazy.retryDelay)

	bfs.lazy.retryAt = time.Time{}
	require.Equal(t, []bool{true, false}, bfs.BatchPkExist(storage.NewBatchLocationsCache(pks)))
	require.Equal(t, 3, loads)
	require.True(t, bfs.IsResourceCharged())
}

func TestLazyBloomFilterSetRefundDuringLoad(t *testing.T) {
	started, finished := make(chan struct{}), make(chan struct{})
	bfs := NewLazyBloomFilterSet(context.Background(), 1, 2, func(ctx context.Context, publish func([]*storage.PkStatistics)) error {
		close(started)
		<-ctx.Done()
		// A read that finishes concurrently with release cannot install stats.
		publish([]*storage.PkStatistics{{}})
		return nil
	})
	go func() {
		defer close(finished)
		assert.False(t, bfs.PkCandidateExist())
	}()
	<-started
	bfs.Refund()
	<-finished
	require.False(t, bfs.PkCandidateExist())
	require.False(t, bfs.IsResourceCharged())
}
