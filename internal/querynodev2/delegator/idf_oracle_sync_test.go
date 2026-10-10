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
	"bytes"
	"context"
	"fmt"
	"math/rand"
	"os"
	"path"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// These tests change the sealed segments while SyncDistribution is between reading the stats
// of the segments it activates or deactivates (outside the oracle lock) and applying them
// (under the oracle lock), through syncDistributionAfterFetchHook.

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

func syncTestFunctions(fieldIDs ...int64) []*schemapb.FunctionSchema {
	functions := make([]*schemapb.FunctionSchema, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		functions = append(functions, &schemapb.FunctionSchema{
			Type:           schemapb.FunctionType_BM25,
			InputFieldIds:  []int64{fieldID - 1},
			OutputFieldIds: []int64{fieldID},
		})
	}
	return functions
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

// syncTestSegment serves the stats files of one sealed segment from a mock chunk manager.
type syncTestSegment struct {
	id    int64
	stats map[int64]*storage.BM25Stats
	cm    *mocks.ChunkManager
}

func newSyncTestSegment(t *testing.T, id int64, stats map[int64]*storage.BM25Stats) *syncTestSegment {
	seg := &syncTestSegment{id: id, stats: stats, cm: mocks.NewChunkManager(t)}
	for fieldID, s := range stats {
		data, err := s.Serialize()
		require.NoError(t, err)
		seg.cm.EXPECT().Reader(mock.Anything, seg.remotePath(fieldID)).RunAndReturn(
			func(context.Context, string) (storage.FileReader, error) {
				return &bytesFileReader{bytes.NewReader(data)}, nil
			}).Maybe()
	}
	return seg
}

func (s *syncTestSegment) remotePath(fieldID int64) string {
	return fmt.Sprintf("bm25stats/seg_%d/field_%d/0", s.id, fieldID)
}

func (s *syncTestSegment) loadInfo(fieldIDs ...int64) *querypb.SegmentLoadInfo {
	logs := make([]*datapb.FieldBinlog, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		logs = append(logs, bm25LogsForField(fieldID, s.remotePath(fieldID))...)
	}
	return &querypb.SegmentLoadInfo{Bm25Logs: logs}
}

// load registers the segment with the given fields. Before the first target (targetVersion 0)
// the segment is preloaded and active; after it, the segment stays inactive until a sync activates it.
func (s *syncTestSegment) load(t *testing.T, o *idfOracle, fieldIDs ...int64) {
	require.NoError(t, o.LoadSealed(context.Background(), s.id, s.loadInfo(fieldIDs...), s.cm))
}

func syncTestSnapshot(version int64, segIDs ...int64) *snapshot {
	segments := make([]SegmentEntry, 0, len(segIDs))
	for _, segID := range segIDs {
		segments = append(segments, SegmentEntry{NodeID: 1, SegmentID: segID, TargetVersion: version})
	}
	return &snapshot{dist: []SnapshotItem{{NodeID: 1, Segments: segments}}, targetVersion: version}
}

// syncWithHook runs SyncDistribution for the snapshot, running hook once between the
// unlocked stats read and the locked apply.
func syncWithHook(t *testing.T, o *idfOracle, snap *snapshot, hook func()) {
	var once sync.Once
	syncDistributionAfterFetchHook = func() { once.Do(hook) }
	defer func() { syncDistributionAfterFetchHook = nil }()
	o.next.SetSnapshot(snap)
	require.NoError(t, o.SyncDistribution())
	require.Equal(t, snap.targetVersion, o.TargetVersion())
}

// assertCurrentField checks that current holds exactly expected for the field: row count, avgdl and IDF of every token.
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

// A reopen that activates a segment while SyncDistribution is activating it must not count it twice.
func TestSyncDistributionReopenActivatesDuringFetch(t *testing.T) {
	r := rand.New(rand.NewSource(1))
	o := newSyncTestOracle(t, 104)
	o.targetVersion.Store(1)
	seg := newSyncTestSegment(t, 1, map[int64]*storage.BM25Stats{104: randomSyncTestStats(r, 3)})
	seg.load(t, o, 104)

	syncWithHook(t, o, syncTestSnapshot(2, 1), func() {
		require.NoError(t, o.LoadSealedForReopen(context.Background(), 1, seg.loadInfo(104), seg.cm, true))
	})

	assertCurrentField(t, o, 104, seg.stats[104])
}

// A reopen that adds a field to a segment while SyncDistribution is activating it:
// once active, the segment must contribute the added field too.
func TestSyncDistributionReopenAddsFieldToActivatingSegment(t *testing.T) {
	r := rand.New(rand.NewSource(2))
	o := newSyncTestOracle(t, 102, 104)
	o.targetVersion.Store(1)
	seg := newSyncTestSegment(t, 1, map[int64]*storage.BM25Stats{
		102: randomSyncTestStats(r, 2),
		104: randomSyncTestStats(r, 3),
	})
	seg.load(t, o, 102)

	syncWithHook(t, o, syncTestSnapshot(2, 1), func() {
		require.NoError(t, o.LoadSealedForReopen(context.Background(), 1, seg.loadInfo(102, 104), seg.cm, false))
	})

	assertCurrentField(t, o, 102, seg.stats[102])
	assertCurrentField(t, o, 104, seg.stats[104])
}

// A reopen that adds a field to an active segment while SyncDistribution is deactivating and
// removing it: nothing of the segment, including the added field, may stay in current.
func TestSyncDistributionReopenAddsFieldToDeactivatingSegment(t *testing.T) {
	r := rand.New(rand.NewSource(3))
	o := newSyncTestOracle(t, 102, 104)
	seg := newSyncTestSegment(t, 1, map[int64]*storage.BM25Stats{
		102: randomSyncTestStats(r, 2),
		104: randomSyncTestStats(r, 3),
	})
	seg.load(t, o, 102) // preloaded, active
	kept := newSyncTestSegment(t, 2, map[int64]*storage.BM25Stats{
		102: randomSyncTestStats(r, 4),
		104: randomSyncTestStats(r, 5),
	})
	kept.load(t, o, 102, 104)

	syncWithHook(t, o, syncTestSnapshot(1, 2), func() {
		require.NoError(t, o.LoadSealedForReopen(context.Background(), 1, seg.loadInfo(102, 104), seg.cm, false))
	})

	assert.False(t, o.sealed.Contain(1))
	assertCurrentField(t, o, 102, kept.stats[102])
	assertCurrentField(t, o, 104, kept.stats[104])
}

// A reopen that activates a segment the new target drops, while SyncDistribution reads stats:
// the segment is removed, so its stats must not stay in current.
func TestSyncDistributionReopenActivatesRemovedSegmentDuringFetch(t *testing.T) {
	r := rand.New(rand.NewSource(8))
	o := newSyncTestOracle(t, 104)
	o.targetVersion.Store(1)
	dropped := newSyncTestSegment(t, 1, map[int64]*storage.BM25Stats{104: randomSyncTestStats(r, 3)})
	dropped.load(t, o, 104)
	kept := newSyncTestSegment(t, 2, map[int64]*storage.BM25Stats{104: randomSyncTestStats(r, 4)})
	kept.load(t, o, 104)

	syncWithHook(t, o, syncTestSnapshot(2, 2), func() {
		require.NoError(t, o.LoadSealedForReopen(context.Background(), 1, dropped.loadInfo(104), dropped.cm, true))
	})

	assert.False(t, o.sealed.Contain(1))
	assertCurrentField(t, o, 104, kept.stats[104])
}

// SyncFunctions dropping a field while SyncDistribution is activating segments must not bring the field back.
func TestSyncDistributionSyncFunctionsDropsFieldDuringFetch(t *testing.T) {
	r := rand.New(rand.NewSource(4))
	o := newSyncTestOracle(t, 102, 104)
	o.targetVersion.Store(1)
	seg := newSyncTestSegment(t, 1, map[int64]*storage.BM25Stats{
		102: randomSyncTestStats(r, 2),
		104: randomSyncTestStats(r, 3),
	})
	seg.load(t, o, 102, 104)

	syncWithHook(t, o, syncTestSnapshot(2, 1), func() {
		require.NoError(t, o.SyncFunctions(syncTestFunctions(104)))
	})

	_, err := o.current.GetStats(102)
	assert.Error(t, err)
	assertCurrentField(t, o, 104, seg.stats[104])
}

// Without concurrent changes, activating and deactivating many segments matches a serial sum.
func TestSyncDistributionManySegmentsMatchesSerial(t *testing.T) {
	r := rand.New(rand.NewSource(5))
	o := newSyncTestOracle(t, 102)
	o.targetVersion.Store(1)
	const segNum = 60
	segs := make([]*syncTestSegment, 0, segNum)
	for id := int64(1); id <= segNum; id++ {
		seg := newSyncTestSegment(t, id, map[int64]*storage.BM25Stats{102: randomSyncTestStats(r, 1+r.Intn(20))})
		seg.load(t, o, 102)
		segs = append(segs, seg)
	}

	odd, even := []int64{}, []int64{}
	oddStats, evenStats := []*storage.BM25Stats{}, []*storage.BM25Stats{}
	for _, seg := range segs {
		if seg.id%2 == 1 {
			odd, oddStats = append(odd, seg.id), append(oddStats, seg.stats[102])
		} else {
			even, evenStats = append(even, seg.id), append(evenStats, seg.stats[102])
		}
	}

	// the even segments are loaded but not readable yet, so they are kept inactive instead of removed
	first := syncTestSnapshot(2, odd...)
	for _, id := range even {
		first.dist[0].Segments = append(first.dist[0].Segments, SegmentEntry{NodeID: 1, SegmentID: id, TargetVersion: unreadableTargetVersion})
	}
	syncWithHook(t, o, first, func() {})
	assertCurrentField(t, o, 102, sumStats(oddStats...))
	for _, id := range even {
		assert.True(t, o.sealed.Contain(id))
	}

	// the odd segments are deactivated and removed, the even ones activated
	syncWithHook(t, o, syncTestSnapshot(3, even...), func() {})
	assertCurrentField(t, o, 102, sumStats(evenStats...))
	for _, id := range odd {
		assert.False(t, o.sealed.Contain(id))
	}
}

// Two SyncDistribution calls for the same target must count every segment once.
func TestSyncDistributionConcurrentCalls(t *testing.T) {
	r := rand.New(rand.NewSource(6))
	o := newSyncTestOracle(t, 102)
	o.targetVersion.Store(1)
	ids, stats := []int64{}, []*storage.BM25Stats{}
	for id := int64(1); id <= 20; id++ {
		seg := newSyncTestSegment(t, id, map[int64]*storage.BM25Stats{102: randomSyncTestStats(r, 5)})
		seg.load(t, o, 102)
		ids, stats = append(ids, id), append(stats, seg.stats[102])
	}

	o.next.SetSnapshot(syncTestSnapshot(2, ids...))
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			assert.NoError(t, o.SyncDistribution())
		}()
	}
	wg.Wait()
	assertCurrentField(t, o, 102, sumStats(stats...))
}

// A read failure must leave current, the activation state and the target version untouched.
func TestSyncDistributionFetchFailureChangesNothing(t *testing.T) {
	r := rand.New(rand.NewSource(7))
	o := newSyncTestOracle(t, 102)
	active := newSyncTestSegment(t, 1, map[int64]*storage.BM25Stats{102: randomSyncTestStats(r, 4)})
	active.load(t, o, 102) // preloaded, active
	o.targetVersion.Store(1)
	broken := newSyncTestSegment(t, 2, map[int64]*storage.BM25Stats{102: randomSyncTestStats(r, 3)})
	broken.load(t, o, 102)
	require.NoError(t, os.RemoveAll(path.Join(o.dirPath, "2", "102")))

	o.next.SetSnapshot(syncTestSnapshot(2, 1, 2))
	assert.Error(t, o.SyncDistribution())

	assert.Equal(t, int64(1), o.TargetVersion())
	brokenStats, ok := o.sealed.Get(2)
	require.True(t, ok)
	assert.False(t, brokenStats.activate.Load())
	assertCurrentField(t, o, 102, active.stats[102])
}

// After the first target, BuildIDF reads current without the shard locks while UpdateGrowing writes it
// without them too; the oracle lock must keep them apart (run with -race).
func TestBuildIDFConcurrentWithGrowingUpdates(t *testing.T) {
	r := rand.New(rand.NewSource(9))
	o := newSyncTestOracle(t, 102)
	o.targetVersion.Store(1)
	o.RegisterGrowing(1, bm25Stats{102: randomSyncTestStats(r, 5)})

	updates := make([]bm25Stats, 64)
	expected := storage.NewBM25Stats()
	expected.Merge(o.growing[1].bm25Stats[102])
	for i := range updates {
		updates[i] = bm25Stats{102: randomSyncTestStats(r, 3)}
		expected.Merge(updates[i][102])
	}

	tf := typeutil.CreateAndSortSparseFloatRow(map[uint32]float32{1: 1, 2: 1, 3: 1})
	stop := make(chan struct{})
	var readers sync.WaitGroup
	for i := 0; i < 4; i++ {
		readers.Add(1)
		go func() {
			defer readers.Done()
			for {
				select {
				case <-stop:
					return
				default:
					_, _, err := o.BuildIDF(102, &schemapb.SparseFloatArray{Contents: [][]byte{tf}, Dim: syncTestVocab})
					assert.NoError(t, err)
				}
			}
		}()
	}
	for _, u := range updates {
		o.UpdateGrowing(1, u)
	}
	close(stop)
	readers.Wait()

	assertCurrentField(t, o, 102, expected)
}
