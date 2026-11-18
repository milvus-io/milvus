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
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metacache"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/metrics"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestEmptyCompletionKeepsLegacyPublication(t *testing.T) {
	ctx := context.Background()
	mt, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.False(t, dm.IndexMeta.UsesSegmentSummaries())
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2,
		State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
	})))
	require.NoError(t, dm.IndexMeta.AddSegmentIndex(ctx, &model.SegmentIndex{
		SegmentID: 3, CollectionID: 1, PartitionID: 2, IndexID: 10, BuildID: 20,
		IndexState: commonpb.IndexState_InProgress,
	}))
	result := &workerpb.IndexTaskInfo{BuildID: 20, State: commonpb.IndexState_Finished}
	require.NoError(t, mt.FinishTask(ctx, result))
	require.Empty(t, mt.GetSegment(ctx, 3).GetIndexSummaries())
	record, exists := dm.IndexMeta.GetIndexJob(20)
	require.True(t, exists)
	require.Equal(t, commonpb.IndexState_Finished, record.IndexState)
	rows, err := mt.Catalog.ListSegmentIndexes(ctx, 1)
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, commonpb.IndexState_Finished, rows[0].IndexState)
}

func TestSummaryModeEmptyCompletion(t *testing.T) {
	ctx := context.Background()
	mt, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, dm.IndexMeta.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10, FieldID: 100}))
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3})))
	require.NoError(t, dm.IndexMeta.UseSegmentSummaries(mt.MetaStore))
	require.NoError(t, dm.IndexMeta.AddSegmentIndex(ctx, &model.SegmentIndex{SegmentID: 3, CollectionID: 1, PartitionID: 2, IndexID: 10, BuildID: 20, IndexState: commonpb.IndexState_InProgress, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED}))
	record, ok := dm.IndexMeta.GetIndexJob(20)
	require.True(t, ok)
	task := NewIndexBuildTask(record, 1, mt, dm.IndexMeta, nil, nil, nil)
	recorder := &indexDataViewRecorder{}
	mt.DataViewManager = recorder
	result := &workerpb.IndexTaskInfo{BuildID: 20, State: commonpb.IndexState_Finished}
	failure := mockey.Mock((*datacoord.Catalog).Update).Return(merr.ErrServiceUnavailable).Build()
	defer failure.UnPatch()
	err = task.SetJobInfo(result)
	require.ErrorIs(t, err, merr.ErrServiceUnavailable)
	require.Empty(t, mt.GetSegment(ctx, 3).GetIndexSummaries())
	require.True(t, dm.IndexMeta.HasBuildInfo(20))
	require.Empty(t, recorder.calls)
	require.Equal(t, commonpb.IndexState_InProgress, task.IndexState)
	failure.UnPatch()
	require.NoError(t, task.SetJobInfo(result))
	require.Equal(t, []int64{1}, recorder.calls)
	require.Equal(t, commonpb.IndexState_Finished, task.IndexState)
	require.False(t, dm.IndexMeta.HasBuildInfo(20))
	require.Zero(t, dm.IndexMeta.SegmentIndexes.Len())
	rows, err := mt.Catalog.ListSegmentIndexes(ctx, 1)
	require.NoError(t, err)
	require.Empty(t, rows)
	require.NoError(t, task.SetJobInfo(result))
	persisted, err := mt.Catalog.ListSegments(ctx, 1)
	require.NoError(t, err)
	require.Len(t, persisted, 1)
	require.Len(t, persisted[0].GetIndexSummaries(), 1)
}

func TestSummaryModeEmptyCompletionForRetiredSegment(t *testing.T) {
	for _, missing := range []bool{false, true} {
		t.Run(map[bool]string{false: "dropped", true: "missing"}[missing], func(t *testing.T) {
			ctx := t.Context()
			mt, dm, err := NewMemoryMetaWithIndex(t)
			require.NoError(t, err)
			require.NoError(t, dm.IndexMeta.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10, FieldID: 100}))
			if !missing {
				require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
					ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Dropped, StorageVersion: storage.StorageV3,
				})))
			}
			require.NoError(t, dm.IndexMeta.UseSegmentSummaries(mt.MetaStore))
			record := &model.SegmentIndex{CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 10, BuildID: 20, IndexState: commonpb.IndexState_InProgress, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED}
			require.NoError(t, dm.IndexMeta.AddSegmentIndex(ctx, record))
			recorder := &indexDataViewRecorder{}
			mt.DataViewManager = recorder
			task := NewIndexBuildTask(record, 1, mt, dm.IndexMeta, nil, nil, nil)
			require.NoError(t, task.SetJobInfo(&workerpb.IndexTaskInfo{BuildID: 20, State: commonpb.IndexState_Finished}))
			require.Equal(t, []int64{1}, recorder.calls)
			rows, err := mt.Catalog.ListSegmentIndexes(ctx, 1)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, commonpb.IndexState_Finished, rows[0].IndexState)
		})
	}
}

func TestFinishTaskSummaryModeFallback(t *testing.T) {
	for _, tc := range []struct {
		name      string
		state     commonpb.IndexState
		files     []string
		hasIndex  bool
		wantErr   error
		wantState commonpb.IndexState
	}{
		{name: "failed", state: commonpb.IndexState_Failed, hasIndex: true, wantState: commonpb.IndexState_Failed},
		{name: "unpublished artifact", state: commonpb.IndexState_Finished, files: []string{"index.bin"}, hasIndex: true, wantErr: merr.ErrDataIntegrity, wantState: commonpb.IndexState_InProgress},
		{name: "dropped definition", state: commonpb.IndexState_Finished, files: []string{"index.bin"}, wantState: commonpb.IndexState_Finished},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			mt, dm, err := NewMemoryMetaWithIndex(t)
			require.NoError(t, err)
			if tc.hasIndex {
				require.NoError(t, dm.IndexMeta.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10, FieldID: 100}))
			}
			require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
				ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
			})))
			require.NoError(t, dm.IndexMeta.UseSegmentSummaries(mt.MetaStore))
			require.NoError(t, dm.IndexMeta.AddSegmentIndex(ctx, &model.SegmentIndex{
				CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 10, BuildID: 20,
				IndexState: commonpb.IndexState_InProgress, IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
			}))
			err = mt.FinishTask(ctx, &workerpb.IndexTaskInfo{
				BuildID: 20, State: tc.state, IndexFileKeys: tc.files,
				IndexStorePathVersion: indexpb.IndexStorePathVersion_INDEX_STORE_PATH_VERSION_SEGMENT_ROOTED,
			})
			if tc.wantErr == nil {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, tc.wantErr)
			}
			require.Empty(t, mt.GetSegment(ctx, 3).GetIndexSummaries())
			rows, err := mt.Catalog.ListSegmentIndexes(ctx, 1)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, tc.wantState, rows[0].IndexState)
		})
	}
}

func TestSummaryModeRestartPreservesInventory(t *testing.T) {
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
	require.Equal(t, mt.GetSegment(t.Context(), meta.RestartSegID).GetIndexSummaries(), recovered.GetSegment(t.Context(), meta.RestartSegID).GetIndexSummaries())
	require.Len(t, recovered.GetSegment(t.Context(), meta.RestartSegID).GetIndexSummaries(), 1)
}

func TestSummaryModeEmptyCompletionAfterDropIndex(t *testing.T) {
	for _, remove := range []bool{false, true} {
		name := "marked deleted"
		if remove {
			name = "removed by GC"
		}
		t.Run(name, func(t *testing.T) {
			ctx := t.Context()
			mt, dm, err := NewMemoryMetaWithIndex(t)
			require.NoError(t, err)
			im := dm.IndexMeta
			require.NoError(t, im.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10, FieldID: 100}))
			require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
				ID: 3, CollectionID: 1, PartitionID: 2, State: commonpb.SegmentState_Flushed, StorageVersion: storage.StorageV3,
			})))
			require.NoError(t, im.UseSegmentSummaries(mt.MetaStore))
			record := &model.SegmentIndex{
				CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 10, BuildID: 20,
				IndexState: commonpb.IndexState_InProgress,
			}
			require.NoError(t, im.AddSegmentIndex(ctx, record))
			task := NewIndexBuildTask(record, 1, mt, im, nil, nil, nil)
			require.NoError(t, im.MarkIndexAsDeleted(ctx, 1, []int64{10}))
			if remove {
				require.NoError(t, im.RemoveIndex(ctx, 1, 10))
			}
			require.NoError(t, task.SetJobInfo(&workerpb.IndexTaskInfo{BuildID: 20, State: commonpb.IndexState_Finished}))
			persisted, err := mt.Catalog.ListSegments(ctx, 1)
			require.NoError(t, err)
			require.Len(t, persisted, 1)
			require.Empty(t, persisted[0].GetIndexSummaries())
			rows, err := mt.Catalog.ListSegmentIndexes(ctx, 1)
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, commonpb.IndexState_Finished, rows[0].IndexState)
		})
	}
}

func TestSummaryModeRecoveryPublishesStoredIndexSize(t *testing.T) {
	const collectionID = 73501
	im := CreateTestIndexMeta(t, collectionID, make(map[int64]*model.Index))
	im.updateCollectionIndex(&model.Index{CollectionID: collectionID, IndexID: 10, FieldID: 100})
	store := metacache.NewMetaStore(nil)
	store.PutSegment(&datapb.SegmentInfo{
		ID: 3, CollectionID: collectionID, PartitionID: 2,
		IndexSummaries: []*datapb.SegmentIndexSummary{{IndexId: 10, BuildId: 20, SerializedSize: 4096}},
	})
	gauges := metrics.DataCoordStoredIndexFilesSize
	gauge := gauges.WithLabelValues("", "", "73501")
	t.Cleanup(func() { gauges.DeleteLabelValues("", "", "73501") })
	require.NoError(t, im.UseSegmentSummaries(store))
	require.Eventually(t, func() bool { return testutil.ToFloat64(gauge) == 4096 }, time.Second, 5*time.Millisecond)
}

func TestSummaryColumnChangeRetractsCompletedAndActiveBuilds(t *testing.T) {
	mt, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	base := "/tmp/test-index-summary/insert_log/1/2/3"
	require.NoError(t, mt.AddSegment(t.Context(), meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 3, CollectionID: 1, PartitionID: 2, StorageVersion: storage.StorageV3, State: commonpb.SegmentState_Flushed,
		ManifestPath: packed.MarshalManifestPath(base, 1), ManifestHasIndex: true,
		IndexSummaries: []*datapb.SegmentIndexSummary{{IndexId: 10, FieldId: 100, BuildId: 20, IndexVersion: 1, HasArtifact: true}},
	})))
	require.NoError(t, dm.IndexMeta.UseSegmentSummaries(mt.MetaStore))
	// The definition has already been removed; the summary still owns the field identity.
	require.NoError(t, dm.IndexMeta.AddSegmentIndex(t.Context(), &model.SegmentIndex{CollectionID: 1, PartitionID: 2, SegmentID: 3, IndexID: 10, BuildID: 21, IndexState: commonpb.IndexState_InProgress}))
	params := paramtable.Get()
	key := params.DataCoordCfg.SegmentIndexSummariesEnabled.Key
	previous := params.DataCoordCfg.SegmentIndexSummariesEnabled.GetValue()
	params.Save(key, "true")
	t.Cleanup(func() { params.Save(key, previous) })
	commit := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).Return(packed.ManifestUpdateResult{
		ManifestPath: packed.MarshalManifestPath(base, 2), HasIndexes: proto.Bool(false),
	}, nil).Build()
	defer commit.UnPatch()
	require.NoError(t, mt.CommitSegmentManifest(t.Context(), meta.SegmentManifestCommit{
		SegmentID: 3,
		Mutation: meta.ManifestMutation{Type: meta.ManifestMutationCommitUpdates, Updates: &packed.ManifestUpdates{
			ColumnGroups: []packed.ColumnGroupEntry{{Columns: []string{"100"}, Format: "parquet", Files: []packed.ColumnGroupFileEntry{{Path: "data.parquet", StartIndex: 0, EndIndex: 10}}}},
		}},
	}))
	require.Empty(t, mt.GetSegment(t.Context(), 3).GetIndexSummaries())
	require.False(t, dm.IndexMeta.HasBuildInfo(21))
	require.False(t, mt.GetSegment(t.Context(), 3).GetManifestHasIndex())
}

func TestSegmentIndexSummariesPreserveTaskStateAndCompletedBuilds(t *testing.T) {
	for _, summaryMode := range []bool{false, true} {
		name := "legacy"
		if summaryMode {
			name = "summaries"
		}
		t.Run(name, func(t *testing.T) {
			mt, dm, err := NewMemoryMetaWithIndex(t)
			require.NoError(t, err)
			if summaryMode {
				require.NoError(t, dm.IndexMeta.UseSegmentSummaries(mt.MetaStore))
			}
			mt.MetaStore.PutSegment(&datapb.SegmentInfo{
				ID: 3, CollectionID: 1, PartitionID: 2,
				IndexSummaries: []*datapb.SegmentIndexSummary{
					{IndexId: 10, BuildId: 19, HasArtifact: true},
					{IndexId: 11, BuildId: 21, HasArtifact: true},
					{IndexId: 12, BuildId: 22},
				},
			})
			for _, record := range []*model.SegmentIndex{
				{
					SegmentID: 3, CollectionID: 1, PartitionID: 2, IndexID: 10, BuildID: 20,
					IndexState: commonpb.IndexState_Retry, IsDeleted: true, IndexFileKeys: []string{"task-file"},
				},
				{
					SegmentID: 3, CollectionID: 1, PartitionID: 2, IndexID: 11, BuildID: 21,
					IndexState: commonpb.IndexState_Finished, IsDeleted: true, IndexFileKeys: []string{"finished-file"},
				},
			} {
				require.NoError(t, dm.IndexMeta.AddSegmentIndex(t.Context(), record))
			}
			summaries := dm.IndexMeta.GetAllSegmentIndexSummaries(3)
			byBuild := make(map[int64]*model.SegmentIndexSummary)
			for _, summary := range summaries {
				require.EqualValues(t, 1, summary.CollectionID)
				require.EqualValues(t, 2, summary.PartitionID)
				require.EqualValues(t, 3, summary.SegmentID)
				require.NotContains(t, byBuild, summary.BuildID)
				byBuild[summary.BuildID] = summary
			}
			require.Equal(t, commonpb.IndexState_Retry, byBuild[20].IndexState)
			require.True(t, byBuild[20].IsDeleted)
			require.True(t, byBuild[20].HasArtifact)
			require.True(t, byBuild[21].IsDeleted, "task deletion marker wins over the same completed build")
			if summaryMode {
				require.Len(t, summaries, 4)
				require.Equal(t, commonpb.IndexState_Finished, byBuild[19].IndexState)
				require.True(t, byBuild[19].HasArtifact)
				require.False(t, byBuild[22].HasArtifact)
			} else {
				require.Len(t, summaries, 2)
			}
			byBuild[20].IsDeleted = false
			record, exists := dm.IndexMeta.GetIndexJob(20)
			require.True(t, exists)
			require.True(t, record.IsDeleted, "returned summaries are detached from resident task state")
			require.Equal(t, []string{"task-file"}, record.IndexFileKeys)
		})
	}
}
