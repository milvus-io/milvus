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

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/workerpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestSummaryModeRecoveryDoesNotCreateBuildRecords(t *testing.T) {
	ctx := context.Background()
	mt, dm, err := NewMemoryMetaWithIndex(t)
	require.NoError(t, err)
	require.NoError(t, dm.IndexMeta.CreateIndex(ctx, &model.Index{CollectionID: 1, IndexID: 10, FieldID: 100}))
	require.NoError(t, mt.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 3, CollectionID: 1, IndexSummaries: []*datapb.SegmentIndexSummary{{IndexId: 10, FieldId: 100, BuildId: 20, HasArtifact: true, SerializedSize: 500}}})))
	require.NoError(t, dm.IndexMeta.UseSegmentSummaries(mt.MetaStore))
	records := dm.IndexMeta.GetSegmentIndexes(1, 3)
	require.EqualValues(t, 20, records[10].BuildID)
	require.True(t, records[10].HasArtifact())
	require.Empty(t, records[10].IndexFileKeys)
	require.False(t, dm.IndexMeta.HasBuildInfo(20))
	require.Zero(t, dm.IndexMeta.SegmentIndexes.Len())
	require.Empty(t, dm.IndexMeta.GetUnindexedSegments(1, []int64{3}))
	require.Empty(t, dm.IndexMeta.GetUnIndexedSegmentIDsForIndexTask(1, []int64{3}).Collect())
	_, err = mt.ReadCompletedIndexes(ctx, mt.GetSegment(ctx, 3).SegmentInfo)
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
	_, err := recovered.ReadCompletedIndexes(t.Context(), recovered.GetSegment(t.Context(), meta.RestartSegID).SegmentInfo)
	require.Error(t, err)
	manifests.ResumeReads()
	records, err := recovered.ReadCompletedIndexes(t.Context(), recovered.GetSegment(t.Context(), meta.RestartSegID).SegmentInfo)
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
	records, err := mt.ReadCompletedIndexes(ctx, mt.GetSegment(ctx, 3).SegmentInfo)
	require.NoError(t, err)
	require.Contains(t, records, int64(10))
	require.False(t, records[10].HasArtifact())
	require.Empty(t, records[10].IndexFileKeys)
	require.False(t, dm.IndexMeta.HasBuildInfo(20))
}
