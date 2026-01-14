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

func TestRemoveReturnsRemovedCandidates(t *testing.T) {
	paramtable.Init()

	t.Run("remove single candidate", func(t *testing.T) {
		pko := NewPkOracle()

		batchSize := 10
		pks := make([]storage.PrimaryKey, 0)
		for i := 0; i < batchSize; i++ {
			pk := storage.NewInt64PrimaryKey(int64(i))
			pks = append(pks, pk)
		}

		bfs := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
		bfs.UpdateBloomFilter(pks)
		pko.Register(bfs, 1)

		removed := pko.Remove(WithSegmentIDs(1))
		assert.Len(t, removed, 1)
		assert.Equal(t, int64(1), removed[0].ID())
	})

	t.Run("remove multiple candidates", func(t *testing.T) {
		pko := NewPkOracle()

		batchSize := 10
		pks := make([]storage.PrimaryKey, 0)
		for i := 0; i < batchSize; i++ {
			pk := storage.NewInt64PrimaryKey(int64(i))
			pks = append(pks, pk)
		}

		// Register multiple candidates
		bfs1 := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
		bfs1.UpdateBloomFilter(pks)
		pko.Register(bfs1, 1)

		bfs2 := NewBloomFilterSet(2, 1, commonpb.SegmentState_Sealed)
		bfs2.UpdateBloomFilter(pks)
		pko.Register(bfs2, 1)

		bfs3 := NewBloomFilterSet(3, 1, commonpb.SegmentState_Sealed)
		bfs3.UpdateBloomFilter(pks)
		pko.Register(bfs3, 1)

		// Remove multiple segments
		removed := pko.Remove(WithSegmentIDs(1, 2))
		assert.Len(t, removed, 2)

		removedIDs := make([]int64, len(removed))
		for i, c := range removed {
			removedIDs[i] = c.ID()
		}
		assert.Contains(t, removedIDs, int64(1))
		assert.Contains(t, removedIDs, int64(2))

		// Verify bfs3 still exists
		assert.True(t, pko.Exists(bfs3, 1))
	})

	t.Run("remove non-existent candidate", func(t *testing.T) {
		pko := NewPkOracle()

		removed := pko.Remove(WithSegmentIDs(999))
		assert.Len(t, removed, 0)
	})

	t.Run("remove with segment type filter", func(t *testing.T) {
		pko := NewPkOracle()

		batchSize := 10
		pks := make([]storage.PrimaryKey, 0)
		for i := 0; i < batchSize; i++ {
			pk := storage.NewInt64PrimaryKey(int64(i))
			pks = append(pks, pk)
		}

		// Register sealed segment
		bfsSealed := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
		bfsSealed.UpdateBloomFilter(pks)
		pko.Register(bfsSealed, 1)

		// Register growing segment
		bfsGrowing := NewBloomFilterSet(2, 1, commonpb.SegmentState_Growing)
		bfsGrowing.UpdateBloomFilter(pks)
		pko.Register(bfsGrowing, 1)

		// Remove only sealed segments
		removed := pko.Remove(WithSegmentType(commonpb.SegmentState_Sealed))
		assert.Len(t, removed, 1)
		assert.Equal(t, int64(1), removed[0].ID())

		// Growing segment should still exist
		assert.True(t, pko.Exists(bfsGrowing, 1))
	})

	t.Run("remove with worker ID filter", func(t *testing.T) {
		pko := NewPkOracle()

		batchSize := 10
		pks := make([]storage.PrimaryKey, 0)
		for i := 0; i < batchSize; i++ {
			pk := storage.NewInt64PrimaryKey(int64(i))
			pks = append(pks, pk)
		}

		// Register on different workers
		bfs1 := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
		bfs1.UpdateBloomFilter(pks)
		pko.Register(bfs1, 1)

		bfs2 := NewBloomFilterSet(2, 1, commonpb.SegmentState_Sealed)
		bfs2.UpdateBloomFilter(pks)
		pko.Register(bfs2, 2)

		// Remove only from worker 1
		removed := pko.Remove(WithWorkerID(1))
		assert.Len(t, removed, 1)
		assert.Equal(t, int64(1), removed[0].ID())

		// bfs2 on worker 2 should still exist
		assert.True(t, pko.Exists(bfs2, 2))
	})

	t.Run("remove all candidates", func(t *testing.T) {
		pko := NewPkOracle()

		batchSize := 10
		pks := make([]storage.PrimaryKey, 0)
		for i := 0; i < batchSize; i++ {
			pk := storage.NewInt64PrimaryKey(int64(i))
			pks = append(pks, pk)
		}

		// Register multiple candidates
		for i := 0; i < 5; i++ {
			bfs := NewBloomFilterSet(int64(i), 1, commonpb.SegmentState_Sealed)
			bfs.UpdateBloomFilter(pks)
			pko.Register(bfs, 1)
		}

		// Remove all (no filter)
		removed := pko.Remove()
		assert.Len(t, removed, 5)
	})
}

func TestRefundRemoved(t *testing.T) {
	paramtable.Init()

	t.Run("refund empty candidates", func(t *testing.T) {
		pko := NewPkOracle()
		// Should not panic with empty slice
		pko.RefundRemoved(nil)
		pko.RefundRemoved([]Candidate{})
	})

	t.Run("refund bloom filter candidates", func(t *testing.T) {
		// Test that RefundRemoved correctly identifies BloomFilterSet and calls Refund
		// Note: We don't set resourceCharged=true to avoid requiring C library initialization
		pko := NewPkOracle()
		batchSize := 10
		pks := make([]storage.PrimaryKey, 0)
		for i := 0; i < batchSize; i++ {
			pk := storage.NewInt64PrimaryKey(int64(i))
			pks = append(pks, pk)
		}

		// Create bloom filter sets (not charged, so Refund() is a no-op)
		bfs1 := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
		bfs1.UpdateBloomFilter(pks)

		bfs2 := NewBloomFilterSet(2, 1, commonpb.SegmentState_Sealed)
		bfs2.UpdateBloomFilter(pks)

		candidates := []Candidate{bfs1, bfs2}

		// Should not panic - Refund() returns early when not charged
		pko.RefundRemoved(candidates)
	})
}

func TestRemoveAndRefundAll(t *testing.T) {
	paramtable.Init()

	t.Run("remove and refund all with empty oracle", func(t *testing.T) {
		pko := NewPkOracle()
		// Should not panic
		pko.RemoveAndRefundAll()
	})

	t.Run("remove and refund all bloom filter candidates", func(t *testing.T) {
		// Test that RemoveAndRefundAll removes and refunds all BloomFilterSet candidates
		// Note: We don't set resourceCharged=true to avoid requiring C library initialization
		pko := NewPkOracle()
		batchSize := 10
		pks := make([]storage.PrimaryKey, 0)
		for i := 0; i < batchSize; i++ {
			pk := storage.NewInt64PrimaryKey(int64(i))
			pks = append(pks, pk)
		}

		// Create and register bloom filter sets (not charged)
		bfs1 := NewBloomFilterSet(1, 1, commonpb.SegmentState_Sealed)
		bfs1.UpdateBloomFilter(pks)
		pko.Register(bfs1, 1)

		bfs2 := NewBloomFilterSet(2, 1, commonpb.SegmentState_Sealed)
		bfs2.UpdateBloomFilter(pks)
		pko.Register(bfs2, 1)

		// Should not panic - Refund() returns early when not charged
		pko.RemoveAndRefundAll()

		// Candidates should be removed from oracle
		assert.False(t, pko.Exists(bfs1, 1))
		assert.False(t, pko.Exists(bfs2, 1))
	})
}
