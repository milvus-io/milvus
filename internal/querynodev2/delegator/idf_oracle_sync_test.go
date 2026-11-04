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

package delegator

import (
	"math/rand"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const syncTestVocab = 64

func newSyncTestOracle(t *testing.T, fieldIDs ...int64) *idfOracle {
	functions := make([]*schemapb.FunctionSchema, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		functions = append(functions, &schemapb.FunctionSchema{
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{fieldID - 1},
			OutputFieldIds: []int64{fieldID},
		})
	}
	o := NewIDFOracle("sync-test-channel", functions).(*idfOracle)
	o.dirPath = t.TempDir()
	o.Start()
	t.Cleanup(o.Close)
	return o
}

func randomSyncTestStats(r *rand.Rand, rows int) *storage.BM25Stats {
	stats := storage.NewBM25Stats()
	for i := 0; i < rows; i++ {
		row := map[uint32]float32{}
		for j := 0; j < 4; j++ {
			row[uint32(r.Int31n(syncTestVocab))] += 1
		}
		stats.Append(row)
	}
	return stats
}

func syncTestSnapshot(version int64, segIDs ...int64) *snapshot {
	segments := make([]SegmentEntry, 0, len(segIDs))
	for _, segID := range segIDs {
		segments = append(segments, SegmentEntry{NodeID: 1, SegmentID: segID, TargetVersion: version})
	}
	return &snapshot{dist: []SnapshotItem{{NodeID: 1, Segments: segments}}, targetVersion: version}
}

func assertCurrentField(t *testing.T, o *idfOracle, fieldID int64, expected *storage.BM25Stats) {
	t.Helper()
	current, err := o.current.GetStats(fieldID)
	require.NoError(t, err)
	assert.Equal(t, expected.NumRow(), current.NumRow(), "row count of field %d", fieldID)

	all := map[uint32]float32{}
	for i := uint32(0); i < syncTestVocab; i++ {
		all[i] = 1
	}
	tf := typeutil.CreateAndSortSparseFloatRow(all)
	idf, avgdl, err := o.BuildIDF(fieldID, &schemapb.SparseFloatArray{Contents: [][]byte{tf}, Dim: syncTestVocab})
	require.NoError(t, err)
	assert.Equal(t, expected.GetAvgdl(), avgdl, "avgdl of field %d", fieldID)
	assert.Equal(t, typeutil.SparseFloatBytesToMap(expected.BuildIDF(tf)), typeutil.SparseFloatBytesToMap(idf[0]), "idf of field %d", fieldID)
}

func sumStats(all ...*storage.BM25Stats) *storage.BM25Stats {
	sum := storage.NewBM25Stats()
	for _, s := range all {
		sum.Merge(s)
	}
	return sum
}

func newHandoffDistribution(o *idfOracle) *distribution {
	d := NewDistribution("sync-test-channel", NewChannelQueryView(nil, map[int64]int64{}, []int64{1}, initialTargetVersion))
	d.SetIDFOracle(o)
	return d
}

func TestSyncTargetVersionWhileIDFLocked(t *testing.T) {
	for _, tc := range []struct {
		name       string
		segmentIDs []int64
	}{
		{name: "empty"},
		{name: "drop growing", segmentIDs: []int64{1}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			o := newSyncTestOracle(t, 102)
			o.targetVersion.Store(1)
			d := newHandoffDistribution(o)
			for _, id := range tc.segmentIDs {
				o.RegisterGrowing(id, bm25Stats{102: randomSyncTestStats(rand.New(rand.NewSource(1)), 1)})
				d.AddGrowing(SegmentEntry{NodeID: 1, SegmentID: id, PartitionID: 1})
			}

			o.Lock()
			defer o.Unlock()
			completed := make(chan error, 1)
			go func() {
				d.SyncTargetVersion(&querypb.SyncAction{
					TargetVersion:   2,
					DroppedInTarget: tc.segmentIDs,
				}, []int64{1})
				_, _, _, version, err := d.PinReadableSegments(1)
				if err == nil {
					d.Unpin(version)
				}
				completed <- err
			}()

			select {
			case err := <-completed:
				require.NoError(t, err)
			case <-time.After(5 * time.Second):
				t.Fatal("target publication and segment pinning waited for the IDF lock")
			}
			for _, id := range tc.segmentIDs {
				version, ok := o.growingDropVersions.Get(id)
				require.True(t, ok)
				assert.Equal(t, int64(2), version)
			}
		})
	}
}

func TestSyncDistributionGrowingDropVersions(t *testing.T) {
	r := rand.New(rand.NewSource(15))
	o := newSyncTestOracle(t, 102)
	o.targetVersion.Store(1)
	o.RegisterGrowing(1, bm25Stats{102: randomSyncTestStats(r, 4)})
	kept := randomSyncTestStats(r, 5)
	o.RegisterGrowing(2, bm25Stats{102: kept.Clone()})

	o.LazyRemoveGrowings(0, 1)
	assert.Zero(t, o.growingDropVersions.Len())
	o.LazyRemoveGrowings(2, 1, 999)
	o.LazyRemoveGrowings(3, 1, 2)
	version, ok := o.growingDropVersions.Get(1)
	require.True(t, ok)
	assert.Equal(t, int64(2), version)

	o.UpdateGrowing(1, bm25Stats{102: randomSyncTestStats(r, 2)})
	delta := randomSyncTestStats(r, 3)
	o.UpdateGrowing(2, bm25Stats{102: delta})
	o.next.SetSnapshot(syncTestSnapshot(2))
	require.NoError(t, o.SyncDistribution())
	assertCurrentField(t, o, 102, sumStats(kept, delta))
	assert.NotContains(t, o.growing, int64(1))
	assert.False(t, o.growingDropVersions.Contain(1))
	assert.False(t, o.growingDropVersions.Contain(999))
	version, ok = o.growingDropVersions.Get(2)
	require.True(t, ok)
	assert.Equal(t, int64(3), version)

	o.LazyRemoveGrowings(2, 1, 999)
	assert.False(t, o.growingDropVersions.Contain(1))
	assert.False(t, o.growingDropVersions.Contain(999))
	require.NoError(t, o.SyncDistribution())
	assertCurrentField(t, o, 102, sumStats(kept, delta))
	assert.False(t, o.growingDropVersions.Contain(1))
	assert.False(t, o.growingDropVersions.Contain(999))
	assert.True(t, o.growingDropVersions.Contain(2))

	o.UpdateGrowing(1, bm25Stats{102: randomSyncTestStats(r, 2)})
	o.next.SetSnapshot(syncTestSnapshot(3))
	require.NoError(t, o.SyncDistribution())
	assertCurrentField(t, o, 102, storage.NewBM25Stats())
	assert.Empty(t, o.growing)
	assert.Zero(t, o.growingDropVersions.Len())
}

func TestSyncDistributionStaleTargetWhileIDFLocked(t *testing.T) {
	o := newSyncTestOracle(t, 102)
	o.targetVersion.Store(2)
	o.next.SetSnapshot(syncTestSnapshot(2))
	o.LazyRemoveGrowings(3, 1)

	o.Lock()
	done := make(chan struct{})
	defer func() {
		o.Unlock()
		<-done
	}()
	var syncErr error
	go func() {
		defer close(done)
		syncErr = o.SyncDistribution()
	}()
	select {
	case <-done:
		require.NoError(t, syncErr)
	case <-time.After(5 * time.Second):
		t.Fatal("an already synchronized target waited for the IDF lock")
	}
	version, ok := o.growingDropVersions.Get(1)
	require.True(t, ok)
	assert.Equal(t, int64(3), version)
}

func TestSyncTargetVersionFirstTargetDropsGrowing(t *testing.T) {
	o := newSyncTestOracle(t, 102)
	d := newHandoffDistribution(o)
	o.RegisterGrowing(1, bm25Stats{102: randomSyncTestStats(rand.New(rand.NewSource(16)), 4)})
	d.AddGrowing(SegmentEntry{NodeID: 1, SegmentID: 1, PartitionID: 1})
	d.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:   1,
		DroppedInTarget: []int64{1},
	}, []int64{1})
	require.Equal(t, int64(1), o.TargetVersion())
	assertCurrentField(t, o, 102, storage.NewBM25Stats())
	assert.Empty(t, o.growing)
	assert.Zero(t, o.growingDropVersions.Len())
}
