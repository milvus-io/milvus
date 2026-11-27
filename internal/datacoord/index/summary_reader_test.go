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
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestSummaryModeRecoveryDoesNotCreateBuildRecords(t *testing.T) {
	ctx := context.Background()
	mt, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, dm.IndexMeta.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10, FieldID: 100}))
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 3, CollectionID: 1, IndexSummaries: []*datapb.SegmentIndexSummary{{IndexId: 10, FieldId: 100, BuildId: 20, HasArtifact: true, SerializedSize: 500}}})))
	require.NoError(t, dm.IndexMeta.UseSegmentSummaries(mt.MetaStore))
	summaries := dm.IndexMeta.GetSegmentIndexSummaries(1, 3)
	require.Contains(t, summaries, int64(10))
	require.EqualValues(t, 20, summaries[10].BuildID)
	require.True(t, summaries[10].HasArtifact)
	require.Equal(t, commonpb.IndexState_Finished, dm.IndexMeta.GetSegmentIndexState(1, 3, 10).GetState())
	require.Empty(t, dm.IndexMeta.GetSegmentIndexTasks(1, 3))
	require.False(t, dm.IndexMeta.HasBuildInfo(20))
	require.Zero(t, dm.IndexMeta.SegmentIndexes.Len())
	require.Empty(t, dm.IndexMeta.GetUnindexedSegments(1, []int64{3}))
	require.Empty(t, dm.IndexMeta.GetUnIndexedSegmentIDsForIndexTask(1, []int64{3}).Collect())
	_, err = mt.ResolveSegmentIndexes(ctx, mt.GetSegment(ctx, 3).SegmentInfo, nil, nil)
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
}

func TestSummaryModeRestartDoesNotReadCompletedManifests(t *testing.T) {
	params := paramtable.Get()
	key := params.DataCoordCfg.SegmentIndexSummariesEnabled.Key
	previous := params.DataCoordCfg.SegmentIndexSummariesEnabled.GetValue()
	params.Save(key, "true")
	t.Cleanup(func() { params.Save(key, previous) })
	meta.WithSegmentIndexManifestWrites(t, true)
	manifests := meta.NewFakeManifestStore(t)
	catalog := datacoord.NewCatalog(meta.NewMetaMemoryKV(), "", "")
	mt, dm := BootMetaForRestart(t, catalog, meta.RestartCollID)
	SeedRestartFixture(t, mt, dm.IndexMeta)
	require.False(t, dm.IndexMeta.HasBuildInfo(meta.RestartBuildID))
	manifests.FailReadsFrom()
	before := manifests.ReadCount
	recovered, recoveredDM := BootMetaForRestart(t, catalog, meta.RestartCollID)
	require.Equal(t, before, manifests.ReadCount)
	require.False(t, recoveredDM.IndexMeta.HasBuildInfo(meta.RestartBuildID))
	require.Zero(t, recoveredDM.IndexMeta.SegmentIndexes.Len())
	require.Equal(t, []int64{meta.RestartSegID}, recoveredDM.IndexMeta.GetIndexedSegments(meta.RestartCollID, []int64{meta.RestartSegID}, []int64{meta.RestartFieldID}))
	_, err := recovered.ResolveSegmentIndexes(t.Context(), recovered.GetSegment(t.Context(), meta.RestartSegID).SegmentInfo, nil, nil)
	require.Error(t, err)
	manifests.ResumeReads()
	records, err := recovered.ResolveSegmentIndexes(t.Context(), recovered.GetSegment(t.Context(), meta.RestartSegID).SegmentInfo, nil, nil)
	require.NoError(t, err)
	require.Equal(t, []string{"0", "1"}, records[meta.RestartIndexID].IndexFileKeys)
	require.False(t, recoveredDM.IndexMeta.HasBuildInfo(meta.RestartBuildID))
}

func TestSummaryModeEmptyCompletionConsumers(t *testing.T) {
	ctx := t.Context()
	mt, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, dm.IndexMeta.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10, FieldID: 100}))
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3})))
	require.NoError(t, dm.IndexMeta.UseSegmentSummaries(mt.MetaStore))
	require.NoError(t, dm.IndexMeta.AddSegmentIndex(ctx, &model.SegmentIndex{SegmentID: 3, CollectionID: 1, PartitionID: 2, IndexID: 10, BuildID: 20, IndexState: commonpb.IndexState_InProgress}))
	require.NoError(t, mt.FinishTask(ctx, &workerpb.IndexTaskInfo{BuildID: 20, State: commonpb.IndexState_Finished}))
	require.Equal(t, []int64{3}, dm.IndexMeta.GetIndexedSegments(1, []int64{3}, []int64{100}))
	records, err := mt.ResolveSegmentIndexes(ctx, mt.GetSegment(ctx, 3).SegmentInfo, nil, nil)
	require.NoError(t, err)
	require.Contains(t, records, int64(10))
	require.False(t, records[10].HasArtifact())
	require.Empty(t, records[10].IndexFileKeys)
	require.False(t, dm.IndexMeta.HasBuildInfo(20))
}

func TestSummaryModeIndexStatePreservesFailureReason(t *testing.T) {
	ctx := t.Context()
	mt, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	im := dm.IndexMeta
	require.NoError(t, im.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10, FieldID: 100}))
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
	})))
	require.NoError(t, im.UseSegmentSummaries(mt.MetaStore))
	require.NoError(t, im.AddSegmentIndex(ctx, &model.SegmentIndex{
		SegmentID: 3, CollectionID: 1, PartitionID: 2, IndexID: 10, BuildID: 20, IndexState: commonpb.IndexState_InProgress,
	}))
	const reason = "worker lost"
	require.NoError(t, mt.FinishTask(ctx, &workerpb.IndexTaskInfo{
		BuildID: 20, State: commonpb.IndexState_Failed, FailReason: reason,
	}))

	single := im.GetSegmentIndexState(1, 3, 10)
	require.Equal(t, commonpb.IndexState_Failed, single.GetState())
	require.Equal(t, reason, single.GetFailReason())
	batch := im.GetSegmentsIndexStates(1, []int64{3})[3][10]
	require.Equal(t, commonpb.IndexState_Failed, batch.GetState())
	require.Equal(t, reason, batch.GetFailReason())
	require.Empty(t, mt.GetSegment(ctx, 3).GetIndexSummaries())
}

type pausedSummaryView struct {
	metacache.MetaView
	started chan struct{}
	resume  chan struct{}
	first   sync.Once
}

func (v *pausedSummaryView) GetSegment(id int64) (*datapb.SegmentInfo, bool) {
	segment, found := v.MetaView.GetSegment(id)
	pause := false
	v.first.Do(func() { pause = true })
	if pause {
		close(v.started)
		<-v.resume
	}
	return segment, found
}

func TestSummaryModeReaderDuringTaskRetirement(t *testing.T) {
	ctx := t.Context()
	mt, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	im := dm.IndexMeta
	require.NoError(t, im.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10, FieldID: 100}))
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
	})))
	view := &pausedSummaryView{MetaView: mt.MetaStore, started: make(chan struct{}), resume: make(chan struct{})}
	require.NoError(t, im.UseSegmentSummaries(view))
	require.NoError(t, im.AddSegmentIndex(ctx, &model.SegmentIndex{
		SegmentID: 3, CollectionID: 1, PartitionID: 2, IndexID: 10, BuildID: 20, IndexState: commonpb.IndexState_InProgress,
	}))

	result := make(chan map[int64]*model.SegmentIndexSummary, 1)
	go func() { result <- im.GetSegmentIndexSummaries(1, 3) }()
	<-view.started
	finished := make(chan error, 1)
	go func() {
		finished <- mt.FinishTask(ctx, &workerpb.IndexTaskInfo{BuildID: 20, State: commonpb.IndexState_Finished})
	}()
	defer func() {
		// Let both goroutines exit even when the publication assertion fails.
		select {
		case <-view.resume:
		default:
			close(view.resume)
		}
	}()
	require.Eventually(t, func() bool {
		segment, _ := mt.MetaStore.GetSegment(3)
		return len(segment.GetIndexSummaries()) == 1
	}, time.Second, time.Millisecond)
	close(view.resume)
	require.Contains(t, <-result, int64(10))
	require.NoError(t, <-finished)
	require.Equal(t, commonpb.IndexState_Finished, im.GetSegmentIndexSummaries(1, 3)[10].IndexState)
}

func TestSummaryModeReconciliationChecksAllActiveIndexes(t *testing.T) {
	store := metacache.NewMetaStore(nil)
	im := &IndexMeta{
		Indexes: map[int64]map[int64]*model.Index{
			1: {10: {IndexID: 10}, 20: {IndexID: 20}, 30: {IndexID: 30, IsDeleted: true}},
		},
		SegmentIndexes: typeutil.NewConcurrentMap[int64, *typeutil.ConcurrentMap[int64, *model.SegmentIndex]](),
		summarySource:  store,
	}
	// Retained deleted tasks suppress rescheduling and take precedence over summaries.
	tasks := typeutil.NewConcurrentMap[int64, *model.SegmentIndex]()
	tasks.Insert(10, &model.SegmentIndex{IndexID: 10, IsDeleted: true})
	im.SegmentIndexes.Insert(1, tasks)
	store.LoadSegments([]*datapb.SegmentInfo{
		{ID: 1, IndexSummaries: []*datapb.SegmentIndexSummary{{IndexId: 10}, {IndexId: 20}}},
		{ID: 2, IndexSummaries: []*datapb.SegmentIndexSummary{{IndexId: 10}, {IndexId: 10}, {IndexId: 30}, {IndexId: 40}}},
		{ID: 3, IndexSummaries: []*datapb.SegmentIndexSummary{{IndexId: 20}}},
		{ID: 4, IndexSummaries: []*datapb.SegmentIndexSummary{{IndexId: 20}, {IndexId: 10}}},
		{ID: 5},
	})
	im.SegmentIndexes.Insert(3, tasks)
	// Empty completions still count. Duplicate, deleted and unrelated IDs cannot
	// stand in for a missing active ID. Segment 6 does not have metadata.
	require.ElementsMatch(t, []int64{2, 5, 6}, im.GetUnIndexedSegmentIDsForIndexTask(1, []int64{1, 2, 3, 4, 5, 6}).Collect())
	require.Equal(t, commonpb.IndexState_Unissued, im.GetSegmentIndexState(1, 1, 10).GetState())
	require.Equal(t, commonpb.IndexState_Finished, im.GetSegmentIndexState(1, 1, 20).GetState())
}

func BenchmarkSummaryIndexReconciliation(b *testing.B) {
	const segmentCount = 1024
	for _, indexCount := range []int{1, 8, 32, 128, 512} {
		b.Run(strconv.Itoa(indexCount)+"Indexes", func(b *testing.B) {
			store := metacache.NewMetaStore(nil)
			im := &IndexMeta{Indexes: map[int64]map[int64]*model.Index{1: {}}, summarySource: store}
			segmentIDs := make([]int64, segmentCount)
			segments := make([]*datapb.SegmentInfo, segmentCount)
			for id := int64(1); id <= int64(indexCount); id++ {
				im.Indexes[1][id] = &model.Index{CollectionID: 1, IndexID: id}
			}
			for i := range segments {
				segmentIDs[i] = int64(i + 1)
				segment := &datapb.SegmentInfo{ID: segmentIDs[i], CollectionID: 1}
				for id := int64(1); id <= int64(indexCount); id++ {
					segment.IndexSummaries = append(segment.IndexSummaries, &datapb.SegmentIndexSummary{IndexId: id, HasArtifact: true})
				}
				segments[i] = segment
			}
			store.LoadSegments(segments)
			b.ReportAllocs()
			for b.Loop() {
				if len(im.GetUnIndexedSegmentIDsForIndexTask(1, segmentIDs)) != 0 {
					b.Fatal("expected every segment to have all index summaries")
				}
			}
		})
	}
}

func BenchmarkLegacyIndexLookup(b *testing.B) {
	const segmentCount, indexCount = 1024, 8
	im := &IndexMeta{
		Indexes:        map[int64]map[int64]*model.Index{1: {}},
		SegmentIndexes: typeutil.NewConcurrentMap[int64, *typeutil.ConcurrentMap[int64, *model.SegmentIndex]](),
	}
	for indexID := int64(1); indexID <= indexCount; indexID++ {
		im.Indexes[1][indexID] = &model.Index{CollectionID: 1, IndexID: indexID}
	}
	segmentIDs := make([]int64, segmentCount)
	for i := range segmentIDs {
		segmentID := int64(i + 1)
		segmentIDs[i] = segmentID
		tasks := typeutil.NewConcurrentMap[int64, *model.SegmentIndex]()
		for indexID := range im.Indexes[1] {
			tasks.Insert(indexID, &model.SegmentIndex{
				CollectionID: 1, SegmentID: segmentID, IndexID: indexID, IndexState: commonpb.IndexState_Finished,
			})
		}
		im.SegmentIndexes.Insert(segmentID, tasks)
	}

	b.Run("Reconcile1024Segments", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if len(im.GetUnIndexedSegmentIDsForIndexTask(1, segmentIDs)) != 0 {
				b.Fatal("expected every segment to be indexed")
			}
		}
	})
	b.Run("SingleIndexState", func(b *testing.B) {
		b.ReportAllocs()
		for b.Loop() {
			if im.GetSegmentIndexState(1, segmentIDs[0], indexCount).GetState() != commonpb.IndexState_Finished {
				b.Fatal("expected a finished index")
			}
		}
	})
}
