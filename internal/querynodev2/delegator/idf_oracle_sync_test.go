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
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
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

// load registers the segment with the given fields. Before the first target (targetVersion 0)
// the segment is preloaded and active; after it, the segment stays inactive until a sync activates it.
func (s *syncTestSegment) load(t *testing.T, o *idfOracle, fieldIDs ...int64) {
	logs := make([]*datapb.FieldBinlog, 0, len(fieldIDs))
	for _, fieldID := range fieldIDs {
		logs = append(logs, &datapb.FieldBinlog{
			FieldID: fieldID,
			Binlogs: []*datapb.Binlog{{LogPath: s.remotePath(fieldID)}},
		})
	}
	require.NoError(t, o.LoadSealed(context.Background(), s.id, logs, s.cm))
}

func syncTestSnapshot(version int64, segIDs ...int64) *snapshot {
	segments := make([]SegmentEntry, 0, len(segIDs))
	for _, segID := range segIDs {
		segments = append(segments, SegmentEntry{NodeID: 1, SegmentID: segID, TargetVersion: version})
	}
	return &snapshot{dist: []SnapshotItem{{NodeID: 1, Segments: segments}}, targetVersion: version}
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

// Activating and deactivating many segments matches a serial sum.
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
	o.next.SetSnapshot(first)
	require.NoError(t, o.SyncDistribution())
	assertCurrentField(t, o, 102, sumStats(oddStats...))
	for _, id := range even {
		assert.True(t, o.sealed.Contain(id))
	}

	// the odd segments are deactivated and removed, the even ones activated
	o.next.SetSnapshot(syncTestSnapshot(3, even...))
	require.NoError(t, o.SyncDistribution())
	assertCurrentField(t, o, 102, sumStats(evenStats...))
	for _, id := range odd {
		assert.False(t, o.sealed.Contain(id))
	}
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

// newHandoffDistribution builds a distribution with the oracle attached, like the delegator does.
func newHandoffDistribution(o *idfOracle) *distribution {
	d := NewDistribution("sync-test-channel", NewChannelQueryView(nil, map[int64]int64{}, []int64{1}, initialTargetVersion))
	d.SetIDFOracle(o)
	return d
}

// syncHandoffTarget publishes a target through distribution.SyncTargetVersion with the given sealed
// segments, which replace the growing segments of the same ID.
func syncHandoffTarget(d *distribution, version int64, sealed ...int64) {
	rows := make(map[int64]int64, len(sealed))
	for _, id := range sealed {
		rows[id] = 1
	}
	d.SyncTargetVersion(&querypb.SyncAction{
		TargetVersion:         version,
		SealedInTarget:        sealed,
		SealedSegmentRowCount: rows,
	}, []int64{1})
}

// Through the real caller, a growing segment handed off to its sealed copy in the first target must count once.
func TestGrowingHandoffFirstTargetCountsOnce(t *testing.T) {
	r := rand.New(rand.NewSource(13))
	o := newSyncTestOracle(t, 102)
	d := newHandoffDistribution(o)
	sealedStats := randomSyncTestStats(r, 6)
	seg := newSyncTestSegment(t, 1, map[int64]*storage.BM25Stats{102: sealedStats})

	o.RegisterGrowing(1, bm25Stats{102: sealedStats.Clone()})
	d.AddGrowing(SegmentEntry{NodeID: 1, SegmentID: 1, PartitionID: 1})
	seg.load(t, o, 102)
	d.AddDistributions(SegmentEntry{NodeID: 1, SegmentID: 1, PartitionID: 1})

	syncHandoffTarget(d, 1, 1)
	require.Equal(t, int64(1), o.TargetVersion())
	assertCurrentField(t, o, 102, sealedStats)
}

// Through the real caller, a growing segment handed off to its sealed copy in a later target must count once.
func TestGrowingHandoffLaterTargetCountsOnce(t *testing.T) {
	r := rand.New(rand.NewSource(14))
	o := newSyncTestOracle(t, 102)
	d := newHandoffDistribution(o)
	base := newSyncTestSegment(t, 1, map[int64]*storage.BM25Stats{102: randomSyncTestStats(r, 4)})
	base.load(t, o, 102)
	d.AddDistributions(SegmentEntry{NodeID: 1, SegmentID: 1, PartitionID: 1})
	syncHandoffTarget(d, 1, 1)
	require.Equal(t, int64(1), o.TargetVersion())

	sealedStats := randomSyncTestStats(r, 6)
	handedOff := newSyncTestSegment(t, 2, map[int64]*storage.BM25Stats{102: sealedStats})
	o.RegisterGrowing(2, bm25Stats{102: sealedStats.Clone()})
	d.AddGrowing(SegmentEntry{NodeID: 1, SegmentID: 2, PartitionID: 1})
	handedOff.load(t, o, 102)
	d.AddDistributions(SegmentEntry{NodeID: 1, SegmentID: 2, PartitionID: 1})

	syncHandoffTarget(d, 2, 1, 2)
	require.Eventually(t, func() bool { return o.TargetVersion() == 2 }, 10*time.Second, 10*time.Millisecond)
	assertCurrentField(t, o, 102, sumStats(base.stats[102], sealedStats))
}
