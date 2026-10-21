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
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
)

func TestVersionedSegments_StaleWriteIsRejected(t *testing.T) {
	s := NewMetaStore(nil)
	_, applied := s.InstallSegment(1, &datapb.SegmentInfo{ID: 1, NumOfRows: 10}, 10)
	require.True(t, applied)

	old, applied := s.InstallSegment(1, &datapb.SegmentInfo{ID: 1, NumOfRows: 5}, 5)
	assert.False(t, applied)
	assert.EqualValues(t, 10, old.GetNumOfRows(), "a rejected write returns the newer value it kept")
	seg, version, ok := s.GetSegmentWithVersion(1)
	require.True(t, ok)
	assert.EqualValues(t, 10, seg.GetNumOfRows())
	assert.EqualValues(t, 10, version)
}

func TestVersionedSegments_ForcedWriteKeepsTheStoredVersion(t *testing.T) {
	s := NewMetaStore(nil)
	s.InstallSegment(1, &datapb.SegmentInfo{ID: 1}, 10)
	s.PutSegment(&datapb.SegmentInfo{ID: 1, NumOfRows: 3})

	seg, version, ok := s.GetSegmentWithVersion(1)
	require.True(t, ok)
	assert.EqualValues(t, 3, seg.GetNumOfRows())
	assert.EqualValues(t, 10, version)
}

func TestVersionedSegments_TombstoneRejectsStaleWritesUntilPruned(t *testing.T) {
	s := NewMetaStore(nil)
	s.InstallSegment(1, &datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch"}, 10)
	require.NotNil(t, s.DropSegment(1, 20))
	assert.Nil(t, s.DropSegment(1, 15), "a stale drop does not apply")

	_, applied := s.InstallSegment(1, &datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch"}, 15)
	assert.False(t, applied, "a write older than the tombstone is rejected")
	_, ok := s.GetSegment(1)
	assert.False(t, ok)
	assert.Nil(t, s.GetSegments(100))

	s.PruneSegment(1)
	_, applied = s.InstallSegment(1, &datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch"}, 1)
	assert.True(t, applied, "the pruned tombstone no longer rejects writes")
	assert.Len(t, s.GetSegments(100), 1)
}

func TestVersionedSegments_DropWithoutVersionLeavesNoTombstone(t *testing.T) {
	s := NewMetaStore(nil)
	s.InstallSegment(1, &datapb.SegmentInfo{ID: 1}, 10)
	require.NotNil(t, s.DropSegment(1, 0))

	_, applied := s.InstallSegment(1, &datapb.SegmentInfo{ID: 1}, 5)
	assert.True(t, applied)
}

func TestVersionedSegments_PublishMergesIntoLiveValues(t *testing.T) {
	s := NewMetaStore(nil)
	s.InstallSegment(1, &datapb.SegmentInfo{ID: 1, LastExpireTime: 50}, 10)
	keepLastExpire := func(current, next *datapb.SegmentInfo) {
		if current.GetLastExpireTime() > next.GetLastExpireTime() {
			next.LastExpireTime = current.GetLastExpireTime()
		}
	}

	results := s.PublishSegments([]*datapb.SegmentInfo{
		{ID: 1, LastExpireTime: 20, NumOfRows: 7},
		{ID: 2, LastExpireTime: 20},
	}, map[int64]int64{1: 11, 2: 11}, keepLastExpire)
	require.Len(t, results, 2)
	assert.True(t, results[0].Applied)
	assert.EqualValues(t, 50, results[0].Old.GetLastExpireTime())
	assert.True(t, results[1].Applied)
	assert.Nil(t, results[1].Old)

	seg, _ := s.GetSegment(1)
	assert.EqualValues(t, 50, seg.GetLastExpireTime(), "the merge kept the newer in-memory value")
	assert.EqualValues(t, 7, seg.GetNumOfRows())
	seg, _ = s.GetSegment(2)
	assert.EqualValues(t, 20, seg.GetLastExpireTime(), "a new segment is installed as published")

	results = s.PublishSegments([]*datapb.SegmentInfo{{ID: 1}}, map[int64]int64{1: 11}, keepLastExpire)
	assert.False(t, results[0].Applied, "the store already holds version 11")
}

func TestVersionedSegments_PublicationIsAtomicForConsistentReaders(t *testing.T) {
	s := NewMetaStore(nil)
	s.InstallSegment(1, &datapb.SegmentInfo{ID: 1}, 1)
	s.InstallSegment(2, &datapb.SegmentInfo{ID: 2}, 1)

	const rounds = 2000
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for v := int64(2); v < rounds; v++ {
			// One mutation stores the same row count in both segments.
			s.PublishSegments([]*datapb.SegmentInfo{
				{ID: 1, NumOfRows: v},
				{ID: 2, NumOfRows: v},
			}, map[int64]int64{1: v, 2: v}, nil)
		}
	}()
	for i := 0; i < rounds; i++ {
		s.ReadConsistent(func() {
			a, _ := s.GetSegment(1)
			b, _ := s.GetSegment(2)
			require.Equal(t, a.GetNumOfRows(), b.GetNumOfRows(), "a consistent reader saw half of a publication")
		})
	}
	wg.Wait()
}

func TestVersionedSegments_UpdateKeepsTheVersion(t *testing.T) {
	s := NewMetaStore(nil)
	s.InstallSegment(1, &datapb.SegmentInfo{ID: 1, NumOfRows: 1}, 10)

	old, updated := s.UpdateSegment(1, func(current *datapb.SegmentInfo) *datapb.SegmentInfo {
		return &datapb.SegmentInfo{ID: 1, NumOfRows: current.GetNumOfRows() + 1}
	})
	assert.EqualValues(t, 1, old.GetNumOfRows())
	assert.EqualValues(t, 2, updated.GetNumOfRows())
	seg, version, _ := s.GetSegmentWithVersion(1)
	assert.EqualValues(t, 2, seg.GetNumOfRows())
	assert.EqualValues(t, 10, version)

	old, updated = s.UpdateSegment(1, func(*datapb.SegmentInfo) *datapb.SegmentInfo { return nil })
	assert.Nil(t, old)
	assert.Nil(t, updated)
	old, updated = s.UpdateSegment(2, func(current *datapb.SegmentInfo) *datapb.SegmentInfo { return current })
	assert.Nil(t, old, "a segment that is not live is not updated")
	assert.Nil(t, updated)
}

func TestVersionedSegments_ClearRemovesEverything(t *testing.T) {
	s := NewMetaStore(nil).(*metaStore)
	s.InstallSegment(1, &datapb.SegmentInfo{ID: 1, CollectionID: 100, InsertChannel: "ch"}, 10)
	s.InstallSegment(2, &datapb.SegmentInfo{ID: 2, CollectionID: 100, InsertChannel: "ch"}, 10)
	s.DropSegment(2, 20)

	s.ClearSegments()
	assert.Zero(t, s.NumSegments())
	assert.Zero(t, bucketCount(s.segCollIdx))
	assert.Zero(t, bucketCount(s.segChanIdx))
	entries := 0
	s.segments.entries.Range(func(int64, *versionedEntry[*datapb.SegmentInfo]) bool {
		entries++
		return true
	})
	assert.Zero(t, entries, "tombstones are cleared too")
	_, applied := s.InstallSegment(2, &datapb.SegmentInfo{ID: 2}, 1)
	assert.True(t, applied)
}

func TestVersionedSegments_IndexesFollowConcurrentChurn(t *testing.T) {
	// Writers add segments of four collections (and channels) and drop each but
	// their last one, so buckets are emptied, removed and recreated while other
	// writers add to them.
	s := NewMetaStore(nil).(*metaStore)
	const writers, rounds, keys = 8, 1000, 4
	var wg sync.WaitGroup
	for w := 0; w < writers; w++ {
		wg.Add(1)
		go func(w int) {
			defer wg.Done()
			for r := 0; r < rounds; r++ {
				id := int64(w*rounds + r)
				key := int64(r % keys)
				s.InstallSegment(id, &datapb.SegmentInfo{ID: id, CollectionID: 100 + key, InsertChannel: fmt.Sprint("ch-", key)}, 1)
				if r != rounds-1 {
					s.DropSegment(id, 2)
				}
			}
		}(w)
	}
	wg.Wait()

	// Every writer keeps its last segment, in collection 100+(rounds-1)%keys.
	lastKey := int64((rounds - 1) % keys)
	require.Equal(t, writers, s.NumSegments())
	assert.Len(t, s.GetSegments(100+lastKey), writers, "every live segment is indexed by its collection")
	assert.Len(t, s.GetSegmentsByChannel(fmt.Sprint("ch-", lastKey)), writers, "every live segment is indexed by its channel")
	assert.Equal(t, writers, bucketLen(s.segCollIdx, 100+lastKey), "the bucket holds no removed segment")
	assert.Equal(t, writers, bucketLen(s.segChanIdx, fmt.Sprint("ch-", lastKey)))
	assert.Equal(t, 1, bucketCount(s.segCollIdx), "emptied collection buckets are removed")
	assert.Equal(t, 1, bucketCount(s.segChanIdx), "emptied channel buckets are removed")
}
