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

package datacoord

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/gc"
	"github.com/milvus-io/milvus/internal/datacoord/index"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
)

// Embed the catalog interface so this fixture implements only the persistence
// operations exercised here. Every persisted task is cloned, including reloads.
type indexGCReadinessCatalog struct {
	metastore.DataCoordCatalog
	mu           sync.Mutex
	indexes      []*model.Index
	stored       map[int64]*model.SegmentIndex
	events       []string
	alterFailure int64
	dropFailure  bool
}

func (c *indexGCReadinessCatalog) ListIndexes(context.Context) ([]*model.Index, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := make([]*model.Index, 0, len(c.indexes))
	for _, idx := range c.indexes {
		ret = append(ret, model.CloneIndex(idx))
	}
	return ret, nil
}

func (c *indexGCReadinessCatalog) ListSegmentIndexes(_ context.Context, collectionID int64) ([]*model.SegmentIndex, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := make([]*model.SegmentIndex, 0, len(c.stored))
	for _, idx := range c.stored {
		if idx.CollectionID == collectionID {
			ret = append(ret, model.CloneSegmentIndex(idx))
		}
	}
	return ret, nil
}

func (c *indexGCReadinessCatalog) AlterSegmentIndexes(_ context.Context, indexes []*model.SegmentIndex) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, idx := range indexes {
		c.events = append(c.events, fmt.Sprintf("mark:%d", idx.IndexID))
		if c.alterFailure == idx.IndexID {
			return errors.New("persist deletion marker failed")
		}
	}
	for _, idx := range indexes {
		c.stored[idx.BuildID] = model.CloneSegmentIndex(idx)
	}
	return nil
}

func (c *indexGCReadinessCatalog) DropSegmentIndex(_ context.Context, _, _, _, buildID int64) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.events = append(c.events, fmt.Sprintf("drop:%d", buildID))
	if c.dropFailure {
		return errors.New("drop segment-index metadata failed")
	}
	delete(c.stored, buildID)
	return nil
}

func newIndexGCReadinessServer(t *testing.T, secondIndex bool) (*Server, *model.Index, *indexGCReadinessCatalog) {
	t.Helper()
	s, idx := newIndexReadinessTestServer([]*datapb.SegmentInfo{
		newIndexLineageSegment(1, 100, commonpb.SegmentState_Dropped),
		newIndexLineageSegment(2, 100, commonpb.SegmentState_Dropped),
		newIndexLineageSegment(3, 150, commonpb.SegmentState_Flushed, 1, 2),
	}, map[int64]commonpb.IndexState{1: commonpb.IndexState_Finished, 2: commonpb.IndexState_Finished, 3: commonpb.IndexState_InProgress})
	idx.IndexName = "gc_index"
	catalog := &indexGCReadinessCatalog{indexes: []*model.Index{idx}, stored: map[int64]*model.SegmentIndex{}}
	for _, id := range []int64{1, 2, 3} {
		indexes, _ := s.indexMeta.SegmentIndexes.Get(id)
		task, _ := indexes.Get(10)
		task.BuildID = id*100 + 10
		task.PartitionID = 1
		catalog.stored[task.BuildID] = model.CloneSegmentIndex(task)
	}
	if secondIndex {
		catalog.indexes = append(catalog.indexes, &model.Index{CollectionID: 1, FieldID: 20, IndexID: 20, IndexName: "other_index"})
		catalog.stored[120] = &model.SegmentIndex{CollectionID: 1, PartitionID: 1, SegmentID: 1, IndexID: 20, BuildID: 120, IndexState: commonpb.IndexState_Finished}
	}
	var err error
	s.indexMeta, err = index.NewIndexMeta(context.Background(), catalog, []int64{1})
	require.NoError(t, err)
	s.meta.IndexCatalog = s.indexMeta
	return s, idx, catalog
}

func patchIndexGCReadinessExternalIO(t *testing.T, s *Server, catalog *indexGCReadinessCatalog, fileFailure bool) {
	t.Helper()
	// Keep the real collector and real index metadata methods. Only storage
	// discovery/removal is substituted so failures are deterministic.
	discovery := mockey.Mock((*gc.GarbageCollector).GetDroppedSegmentIndexFiles).To(func(_ *gc.GarbageCollector, _ context.Context, _ *packed.ManifestIOContext, segmentID int64) ([]*model.SegmentIndex, map[string]struct{}, gc.GcBlockReason) {
		indexes := s.indexMeta.GetAllSegmentIndexes(segmentID)
		sort.Slice(indexes, func(i, j int) bool { return indexes[i].IndexID < indexes[j].IndexID })
		return indexes, map[string]struct{}{"index-artifact": {}}, gc.GcNotBlocked
	}).Build()
	files := mockey.Mock((*gc.GarbageCollector).RemoveDroppedSegmentFiles).To(func(_ *gc.GarbageCollector, _ context.Context, _ *meta.SegmentInfo, _ map[string]struct{}) error {
		catalog.mu.Lock()
		catalog.events = append(catalog.events, "files")
		catalog.mu.Unlock()
		if fileFailure {
			return errors.New("remove files failed")
		}
		return nil
	}).Build()
	t.Cleanup(func() { files.UnPatch(); discovery.UnPatch() })
}

func assertIndexGCReadinessInvalidated(t *testing.T, s *Server, idx *model.Index) {
	t.Helper()
	require.Equal(t, commonpb.IndexState_Unissued, s.indexMeta.GetSegmentIndexState(1, 1, 10).GetState())
	require.Empty(t, s.indexMeta.GetIndexedSegments(1, []int64{1}, []int64{10}))
	require.Empty(t, s.indexMeta.GetSegmentIndexes(1, 1))
	require.Nil(t, s.indexMeta.GetSegmentsIndexStates(1, []int64{1})[1][10])
	require.Len(t, s.indexMeta.GetAllSegmentIndexes(1), 1, "GC retains the durable retry record")
	require.ElementsMatch(t, []int64{3}, indexServingQueryIDs(s))
	assertIndexServingState(t, s, idx, commonpb.IndexState_InProgress)
	progress, err := s.GetIndexBuildProgress(context.Background(), &indexpb.GetIndexBuildProgressRequest{CollectionID: 1, IndexName: idx.IndexName})
	require.NoError(t, err)
	require.EqualValues(t, 100, progress.GetIndexedRows(), "only the other retained ancestor can contribute rows")
	require.EqualValues(t, 150, progress.GetTotalRows())
	require.EqualValues(t, 150, progress.GetPendingIndexRows())
}

func TestIndexGCReadinessMarkerFailurePreventsFiles(t *testing.T) {
	s, _, catalog := newIndexGCReadinessServer(t, true)
	catalog.alterFailure = 20
	patchIndexGCReadinessExternalIO(t, s, catalog, false)
	gcx := &gc.GarbageCollector{Meta: s.meta, Option: gc.GcOption{IndexMeta: s.indexMeta, SnapshotMeta: s.snapshotMeta}}
	gcx.RecycleDroppedSegment(context.Background(), gc.NewGCManifestReadIO(t), 1, s.meta.Segments.GetSegment(1))
	require.Equal(t, []string{"mark:10", "mark:20"}, catalog.events, "every marker must persist before any file deletion")
	require.True(t, catalog.stored[110].IsDeleted)
	require.False(t, catalog.stored[120].IsDeleted, "failed persistence must not mutate the live task")
	require.NotNil(t, s.meta.Segments.GetSegment(1))
	require.Len(t, s.indexMeta.GetAllSegmentIndexes(1), 2)
}

func TestIndexGCReadinessFileFailureLeavesMarkedRecord(t *testing.T) {
	s, idx, catalog := newIndexGCReadinessServer(t, false)
	patchIndexGCReadinessExternalIO(t, s, catalog, true)
	gcx := &gc.GarbageCollector{Meta: s.meta, Option: gc.GcOption{IndexMeta: s.indexMeta, SnapshotMeta: s.snapshotMeta}}
	gcx.RecycleDroppedSegment(context.Background(), gc.NewGCManifestReadIO(t), 1, s.meta.Segments.GetSegment(1))
	require.Equal(t, []string{"mark:10", "files"}, catalog.events)
	require.True(t, catalog.stored[110].IsDeleted)
	assertIndexGCReadinessInvalidated(t, s, idx)
}

func TestIndexGCReadinessMetaFailureSurvivesRestart(t *testing.T) {
	s, idx, catalog := newIndexGCReadinessServer(t, false)
	catalog.dropFailure = true
	patchIndexGCReadinessExternalIO(t, s, catalog, false)
	gcx := &gc.GarbageCollector{Meta: s.meta, Option: gc.GcOption{IndexMeta: s.indexMeta, SnapshotMeta: s.snapshotMeta}}
	gcx.RecycleDroppedSegment(context.Background(), gc.NewGCManifestReadIO(t), 1, s.meta.Segments.GetSegment(1))
	require.Equal(t, []string{"mark:10", "files", "drop:110"}, catalog.events)
	require.True(t, catalog.stored[110].IsDeleted)
	assertIndexGCReadinessInvalidated(t, s, idx)

	// Reload the actual indexMeta from persisted catalog rows, including the
	// Finished state underneath the deletion marker.
	var err error
	s.indexMeta, err = index.NewIndexMeta(context.Background(), catalog, []int64{1})
	require.NoError(t, err)
	s.meta.IndexCatalog = s.indexMeta
	gcx.Option.IndexMeta = s.indexMeta
	assertIndexGCReadinessInvalidated(t, s, idx)
	gcx.RecycleDroppedSegment(context.Background(), gc.NewGCManifestReadIO(t), 1, s.meta.Segments.GetSegment(1))
	require.Equal(t, []string{"mark:10", "files", "drop:110", "files", "drop:110"}, catalog.events, "retry uses the persisted marker without rewriting it")

	// An independently finished current task still reports Finished.
	indexes, _ := s.indexMeta.SegmentIndexes.Get(3)
	output, _ := indexes.Get(10)
	output.IndexState = commonpb.IndexState_Finished
	assertIndexServingState(t, s, idx, commonpb.IndexState_Finished)
}
