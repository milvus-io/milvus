// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package index

import (
	"context"
	"path"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/metautil"
	"github.com/milvus-io/milvus/pkg/v3/util/metricsinfo"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func legacyUpdateSegmentIndex(m *IndexMeta, segIdx *model.SegmentIndex) {
	old, _ := m.SegmentBuildInfo.Get(segIdx.BuildID)
	m.taskCounts.replaceTask(old, segIdx)
	indexes, ok := m.SegmentIndexes.Get(segIdx.SegmentID)
	if ok {
		indexes.Insert(segIdx.IndexID, segIdx)
	} else {
		indexes := typeutil.NewConcurrentMap[UniqueID, *model.SegmentIndex]()
		indexes.Insert(segIdx.IndexID, segIdx)
		m.SegmentIndexes.Insert(segIdx.SegmentID, indexes)
	}
	m.SegmentBuildInfo.Add(segIdx)
}

// legacyManifestIndexRecovery is ReloadSegmentIndexesFromManifests' handling
// of completed reads, applied to reads in the given completion order. Every
// empty marker is flushed at once, as with a read limit of one.
func legacyManifestIndexRecovery(ctx context.Context, m *meta.Meta, im *IndexMeta, reads map[string]manifestRecoveryRead, order []string) error {
	rootPath := m.ChunkManager.RootPath()
	segments := make(map[string]*meta.SegmentInfo)
	for _, segment := range m.SelectSegments(ctx) {
		segments[segment.GetManifestPath()] = segment
	}
	for _, manifestPath := range order {
		segment, read := segments[manifestPath], reads[manifestPath]
		indexes, removed, err := meta.LegacyRecoverManifestIndexes(ctx, m, segment, read.entries, read.err, rootPath)
		if err != nil {
			return merr.Wrap(err, "recover segment indexes from manifests")
		}
		if len(indexes) == 0 && !removed {
			if err := m.UpdateSegmentsInfo(ctx, meta.ClearEmptyManifestIndexMarker(segment.GetID(), segment.GetManifestPath())); err != nil {
				return merr.Wrap(err, "persist empty manifest index markers")
			}
		}
		for _, segIdx := range indexes {
			if _, ok := im.SegmentBuildInfo.Get(segIdx.BuildID); ok {
				continue
			}
			legacyUpdateSegmentIndex(im, segIdx)
		}
	}
	return nil
}

const recoveryPartitionID = UniqueID(7)

type manifestRecoveryRead struct {
	entries []packed.ManifestIndexInfo
	err     error
}

// manifestRecoveryFixture is a restart with every shape recovery tells apart:
// catalog rows that win their build ID, two builds of one index in a manifest,
// active, deleted and undefined index definitions, zero sizes, an empty
// manifest, and a dropped segment whose manifest is gone. Optional shapes are a
// build ID repeated across manifests, whose winner depends on completion
// order, and a manifest holding an unusable entry.
type manifestRecoveryFixture struct {
	root     string
	segments []*datapb.SegmentInfo
	reads    map[string]manifestRecoveryRead
	indexes  []*model.Index
	deleted  map[UniqueID][]UniqueID
	rows     []*model.SegmentIndex
}

func newManifestRecoveryFixture(t *testing.T, segmentsPerCollection int, repeatedBuild, invalid bool) *manifestRecoveryFixture {
	f := &manifestRecoveryFixture{
		root:  t.TempDir(),
		reads: make(map[string]manifestRecoveryRead),
		indexes: []*model.Index{
			{CollectionID: 1, FieldID: 101, IndexID: 11, IndexName: "a"},
			{CollectionID: 1, FieldID: 102, IndexID: 12, IndexName: "b"},
			{CollectionID: 2, FieldID: 101, IndexID: 21, IndexName: "a"},
			{CollectionID: 2, FieldID: 102, IndexID: 22, IndexName: "b"},
		},
		deleted: map[UniqueID][]UniqueID{2: {22}},
	}
	// Collection 3 has manifest entries but no index definition.
	indexIDs := map[UniqueID][]UniqueID{1: {11, 12}, 2: {21, 22}, 3: {31}}
	segmentID := UniqueID(1000)
	for _, collectionID := range []UniqueID{1, 2, 3} {
		for range segmentsPerCollection {
			segmentID++
			var entries []packed.ManifestIndexInfo
			for k, indexID := range indexIDs[collectionID] {
				entries = append(entries, f.entry(collectionID, segmentID, indexID, segmentID*10+UniqueID(k)))
			}
			f.addSegment(collectionID, segmentID, commonpb.SegmentState_Flushed, manifestRecoveryRead{entries: entries})
		}
	}

	// Catalog rows win their build ID: a finished row the first manifest also
	// holds, and an unrelated row in another state.
	f.rows = []*model.SegmentIndex{
		{
			CollectionID: 1, PartitionID: recoveryPartitionID, SegmentID: 1001, IndexID: 11, BuildID: 10010,
			IndexState: commonpb.IndexState_Finished, IndexFileKeys: []string{"from-etcd"}, IndexSerializedSize: 5, NodeID: 9,
		},
		{
			CollectionID: 2, PartitionID: recoveryPartitionID, SegmentID: 1, IndexID: 21, BuildID: 7,
			IndexState: commonpb.IndexState_InProgress,
		},
	}

	segmentID = 9000
	twoBuilds := []packed.ManifestIndexInfo{f.entry(1, segmentID, 11, 90001), f.entry(1, segmentID, 11, 90002)}
	f.addSegment(1, segmentID, commonpb.SegmentState_Flushed, manifestRecoveryRead{entries: twoBuilds})
	f.addSegment(1, segmentID+1, commonpb.SegmentState_Flushed, manifestRecoveryRead{})
	f.addSegment(2, segmentID+2, commonpb.SegmentState_Dropped,
		manifestRecoveryRead{err: retry.Unrecoverable(merr.WrapErrIoFailedReason("manifest is gone"))})
	if repeatedBuild {
		// Segment 1002 holds build 10020 too; whichever installs second loses.
		f.addSegment(1, segmentID+3, commonpb.SegmentState_Flushed,
			manifestRecoveryRead{entries: []packed.ManifestIndexInfo{f.entry(1, segmentID+3, 11, 10020)}})
	}
	if invalid {
		unusable := f.entry(2, segmentID+4, 21, 90041)
		unusable.IndexFileKeys = []string{"../escape"}
		f.addSegment(2, segmentID+4, commonpb.SegmentState_Flushed,
			manifestRecoveryRead{entries: []packed.ManifestIndexInfo{f.entry(2, segmentID+4, 22, 90040), unusable}})
	}
	return f
}

func (f *manifestRecoveryFixture) entry(collectionID, segmentID, indexID, buildID UniqueID) packed.ManifestIndexInfo {
	layout := indexpb.IndexStorePathVersion(buildID % 2)
	size := (buildID % 5) * 1000 // Every fifth build is empty.
	return packed.ManifestIndexInfo{
		IndexID:               indexID,
		BuildID:               buildID,
		FieldID:               100 + indexID%10,
		IndexName:             "index",
		IndexType:             "HNSW",
		IndexVersion:          1,
		NumRows:               100,
		SerializedSize:        size,
		MemSize:               2 * size,
		CurrentIndexVersion:   5,
		Path:                  metautil.NewIndexPathBuilder(f.root, layout, collectionID, recoveryPartitionID, segmentID, buildID, 1).BuildPrefix(),
		IndexFileKeys:         []string{"0", "1"},
		IndexStorePathVersion: layout,
	}
}

func (f *manifestRecoveryFixture) addSegment(collectionID, segmentID UniqueID, state commonpb.SegmentState, read manifestRecoveryRead) {
	manifest := packed.MarshalManifestPath(path.Join(f.root, "insert_log", metautil.JoinIDPath(collectionID, recoveryPartitionID, segmentID)), 3)
	f.segments = append(f.segments, &datapb.SegmentInfo{
		ID: segmentID, CollectionID: collectionID, PartitionID: recoveryPartitionID, State: state, NumOfRows: 100,
		StorageVersion: storage.StorageV3, ManifestPath: manifest, ManifestHasIndex: true,
	})
	f.reads[manifest] = read
}

// boot builds a fresh meta in the state recovery starts from, with the stored
// index size gauges reset so that only this meta writes them.
func (f *manifestRecoveryFixture) boot(t *testing.T) (*meta.Meta, *IndexMeta) {
	ctx := context.Background()
	metrics.DataCoordStoredIndexFilesSize.Reset()
	m, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	im := dm.IndexMeta
	m.ChunkManager = storage.NewLocalChunkManager(objectstorage.RootPath(f.root))
	for _, index := range f.indexes {
		require.NoError(t, im.CreateIndex(ctx, model.CloneIndex(index)))
	}
	for collectionID, indexIDs := range f.deleted {
		require.NoError(t, im.MarkIndexAsDeleted(ctx, collectionID, indexIDs))
	}
	for _, row := range f.rows {
		require.NoError(t, im.AddSegmentIndex(ctx, model.CloneSegmentIndex(row)))
	}
	for _, segment := range f.segments {
		require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(proto.Clone(segment).(*datapb.SegmentInfo))))
	}
	return m, im
}

// mockManifestRecoveryReads serves recovery reads from reads and returns their
// submission order. Inline completion makes that the install order when one
// read is outstanding at a time; async completion delivers each read from its
// own goroutine, out of order.
func mockManifestRecoveryReads(t *testing.T, reads map[string]manifestRecoveryRead, async bool) func() []string {
	var mu sync.Mutex
	var order []string
	mock := mockey.Mock(packed.SubmitManifestIndexInfosWithProperties).To(func(_ context.Context, _ *packed.ManifestIOContext, manifest string, properties *packed.ManifestReadProperties, complete func([]packed.ManifestIndexInfo, error)) error {
		read, ok := reads[manifest]
		if !ok || properties == nil {
			return merr.WrapErrServiceInternalMsg("unexpected manifest read %s", manifest)
		}
		mu.Lock()
		order = append(order, manifest)
		mu.Unlock()
		if async {
			go complete(read.entries, read.err)
		} else {
			complete(read.entries, read.err)
		}
		return nil
	}).Build()
	t.Cleanup(func() { mock.UnPatch() })
	return func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(order)
	}
}

// recoveredIndexState is everything recovery writes: the resident records,
// task counts, the stored-size gauges, the task-stats LRU, and the segments'
// index markers.
type recoveredIndexState struct {
	Builds      map[UniqueID]model.SegmentIndex
	BySegment   map[UniqueID]map[UniqueID]UniqueID
	TaskTotal   indexTaskStateCounts
	TaskByIndex map[[2]int64]indexTaskStats
	Gauges      map[string]float64
	LRUKeys     []UniqueID
	LRUValues   []metricsinfo.IndexTaskStats
	Markers     map[UniqueID]bool
}

// waitStoredSizesPublished publishes the stored sizes, or waits for the
// publication recovery started off its own path.
func waitStoredSizesPublished(im *IndexMeta) {
	im.taskCounts.publishStoredSizes()
	im.taskCounts.publishMu.Lock()
	defer im.taskCounts.publishMu.Unlock()
}

func snapshotRecoveredIndexState(t *testing.T, m *meta.Meta, im *IndexMeta) recoveredIndexState {
	s := recoveredIndexState{
		Builds:      make(map[UniqueID]model.SegmentIndex),
		BySegment:   make(map[UniqueID]map[UniqueID]UniqueID),
		TaskByIndex: make(map[[2]int64]indexTaskStats),
		Gauges:      make(map[string]float64),
		Markers:     make(map[UniqueID]bool),
	}
	for _, segIdx := range im.SegmentBuildInfo.List() {
		s.Builds[segIdx.BuildID] = *segIdx
	}
	im.SegmentIndexes.Range(func(segmentID UniqueID, indexes *typeutil.ConcurrentMap[UniqueID, *model.SegmentIndex]) bool {
		builds := make(map[UniqueID]UniqueID)
		indexes.Range(func(indexID UniqueID, segIdx *model.SegmentIndex) bool {
			builds[indexID] = segIdx.BuildID
			return true
		})
		s.BySegment[segmentID] = builds
		return true
	})
	im.taskCounts.mu.RLock()
	s.TaskTotal = im.taskCounts.total
	for key, stats := range im.taskCounts.byIndex {
		s.TaskByIndex[key] = stats
	}
	im.taskCounts.mu.RUnlock()
	waitStoredSizesPublished(im)
	collected := make(chan prometheus.Metric)
	go func() {
		metrics.DataCoordStoredIndexFilesSize.Collect(collected)
		close(collected)
	}()
	for metric := range collected {
		var sample dto.Metric
		require.NoError(t, metric.Write(&sample))
		labels := make([]string, 0, len(sample.GetLabel()))
		for _, label := range sample.GetLabel() {
			labels = append(labels, label.GetName()+"="+label.GetValue())
		}
		s.Gauges[strings.Join(labels, ",")] = sample.GetGauge().GetValue()
	}
	s.LRUKeys = im.SegmentBuildInfo.taskStats.Keys()
	for _, stats := range im.SegmentBuildInfo.taskStats.Values() {
		s.LRUValues = append(s.LRUValues, *stats)
	}
	for _, segment := range m.SelectSegments(context.Background()) {
		s.Markers[segment.GetID()] = segment.GetManifestHasIndex()
	}
	return s
}

// Recovery leaves exactly the state the per-record path left, in the same
// completion order: records, counts, stored sizes and gauges, the task-stats
// LRU in order, and index markers. That holds whether or not the LRU evicts the
// catalog's rows, and also when a manifest with an unusable entry fails startup
// after earlier manifests were installed.
func TestManifestIndexRecoveryMatchesPerRecordInstall(t *testing.T) {
	for _, tc := range []struct {
		name                  string
		segmentsPerCollection int
		invalid               bool
	}{
		{"lru_keeps_catalog_rows", 3, false},
		{"lru_evicts_catalog_rows", 260, false},
		{"unusable_entry_fails_startup", 260, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			old := Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("1")
			defer Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(old)
			ctx := context.Background()
			fixture := newManifestRecoveryFixture(t, tc.segmentsPerCollection, true, tc.invalid)
			order := mockManifestRecoveryReads(t, fixture.reads, false)

			current, currentIndexMeta := fixture.boot(t)
			currentErr := current.ReloadSegmentIndexesFromManifests(ctx)
			got := snapshotRecoveredIndexState(t, current, currentIndexMeta)

			previous, previousIndexMeta := fixture.boot(t)
			previousErr := legacyManifestIndexRecovery(ctx, previous, previousIndexMeta, fixture.reads, order())
			want := snapshotRecoveredIndexState(t, previous, previousIndexMeta)

			if tc.invalid {
				require.ErrorIs(t, currentErr, merr.ErrDataIntegrity)
				require.Error(t, previousErr)
				require.Equal(t, previousErr.Error(), currentErr.Error())
			} else {
				require.NoError(t, currentErr)
				require.NoError(t, previousErr)
				require.Len(t, order(), len(fixture.reads))
			}
			require.Equal(t, want, got)
			require.NotEmpty(t, got.Gauges)
			switch {
			case tc.invalid:
			case tc.segmentsPerCollection > 100:
				require.Len(t, got.LRUKeys, taskStatsLRUCapacity)
				require.NotContains(t, got.LRUKeys, UniqueID(7), "the catalog rows are evicted")
			default:
				require.Contains(t, got.LRUKeys, UniqueID(7), "the catalog rows survive")
			}
		})
	}
}

// Reads that complete out of order, each delivered on its own goroutine, are
// validated and projected concurrently and still install exactly the records
// the per-record path installs. Only the task-stats LRU, which keeps the latest
// installs, depends on completion order.
func TestManifestIndexRecoveryConcurrentReads(t *testing.T) {
	old := Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue("8")
	defer Params.DataCoordCfg.SegmentIndexManifestLoadConcurrency.SwapTempValue(old)
	ctx := context.Background()
	fixture := newManifestRecoveryFixture(t, 260, false, false)
	order := mockManifestRecoveryReads(t, fixture.reads, true)

	current, currentIndexMeta := fixture.boot(t)
	require.NoError(t, current.ReloadSegmentIndexesFromManifests(ctx))
	got := snapshotRecoveredIndexState(t, current, currentIndexMeta)

	previous, previousIndexMeta := fixture.boot(t)
	require.NoError(t, legacyManifestIndexRecovery(ctx, previous, previousIndexMeta, fixture.reads, order()))
	want := snapshotRecoveredIndexState(t, previous, previousIndexMeta)

	require.Len(t, got.LRUKeys, taskStatsLRUCapacity)
	for _, buildID := range got.LRUKeys {
		require.Contains(t, want.Builds, buildID)
	}
	got.LRUKeys, got.LRUValues, want.LRUKeys, want.LRUValues = nil, nil, nil, nil
	require.Equal(t, want, got)
}
