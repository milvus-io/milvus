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

package meta

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/datacoord/split"
	"github.com/milvus-io/milvus/internal/metastore"
	catalogkv "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	"github.com/milvus-io/milvus/internal/metastore/model"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

type adoptionPrefixedWalkKV struct{ kv.MetaKv }

func (m *adoptionPrefixedWalkKV) WalkWithPrefix(ctx context.Context, prefix string, n int, fn func([]byte, []byte) error) error {
	return m.MetaKv.WalkWithPrefix(ctx, prefix, n, func(key, value []byte) error { return fn([]byte("/by-dev/meta/"+string(key)), value) })
}

func TestCommittedAdoptionRecoveryPrecedesProjection(t *testing.T) {
	ctx := context.Background()
	original, err := NewMemoryMeta(t)
	require.NoError(t, err)
	require.NoError(t, original.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 1, InsertChannel: "s", SplitTarget: "t", State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, NumOfRows: 1, Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{LogID: 1, EntriesNum: 1}}}}, TransformStartAfterTimetick: 50})))
	concrete := original.Catalog.(*catalogkv.Catalog)
	concrete.MetaKv = &adoptionPrefixedWalkKV{MetaKv: concrete.MetaKv}
	catalog := original.Catalog.(model.SplitAdoptionCatalog)
	record, err := catalog.PrepareSplitAdoption(ctx, &model.SplitAdoption{TaskID: 7, CollectionID: 100, Source: "s", Moves: []model.SplitAdoptionMove{{SegmentID: 10, PartitionID: 1, Target: "t", TransformStart: 100, SourceTransformStart: 50}}})
	require.NoError(t, err)
	reopen := func() *Meta {
		return BootMetaWithLoader(t, original.Catalog, 100, func(metastore.DataCoordCatalog, storage.ChunkManager) DomainMetaLoader { return NoDomainMetas })
	}
	before := reopen()
	require.Equal(t, "s", before.GetSegment(ctx, 10).GetInsertChannel(), "prepared batches are invisible during real MetaStore recovery")
	view := &viewpb.DataViewOfCollection{CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}, ShardSetEpoch: 3, Shards: []*viewpb.DataViewOfShard{{Vchannel: "t", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 1, SegmentIds: []int64{10}, SegmentManifestVersions: []int64{0}, SegmentTransformStartAfterTimeticks: []uint64{100}}}}}}
	// The former coordinator's transaction reaches the store after recovery
	// and an unrelated target flush. It cannot replace the new active view.
	flush := &viewpb.DataViewOfCollection{CollectionId: 100, DataVersion: view.DataVersion, Shards: []*viewpb.DataViewOfShard{{Vchannel: "t", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 1, SegmentIds: []int64{20}}}}}}
	require.NoError(t, concrete.SaveDataView(ctx, flush))
	require.Error(t, catalog.CommitSplitAdoption(ctx, record, view))
	views, err := concrete.ListDataViews(ctx, 100)
	require.NoError(t, err)
	require.Len(t, views, 1)
	require.Equal(t, []int64{20}, views[0].GetShards()[0].GetPartitions()[0].GetSegmentIds())
	record, err = catalog.PrepareSplitAdoption(ctx, record)
	require.NoError(t, err)
	view.DataVersion = &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 2}
	view.Shards[0].Partitions[0].SegmentIds = []int64{10, 20}
	view.Shards[0].Partitions[0].SegmentManifestVersions = []int64{0, 0}
	view.Shards[0].Partitions[0].SegmentTransformStartAfterTimeticks = []uint64{100, 100}
	require.NoError(t, catalog.CommitSplitAdoption(ctx, record, view))
	// No canonical SegmentInfo write and no task-state advancement has occurred.
	// The actual startup loader must install effective ownership before callers
	// can invoke the DataView projector or start metadata GC/scheduling.
	recovered := reopen()
	segment := recovered.GetSegment(ctx, 10)
	require.Equal(t, "t", segment.GetInsertChannel())
	require.EqualValues(t, 100, segment.GetTransformStartAfterTimetick())
	projection, err := recovered.LoadableProjection(ctx, 100, view)
	require.NoError(t, err)
	require.Len(t, projection, 1)
	require.Equal(t, "t", projection[0].VChannel)
	require.NoError(t, catalog.SaveSplitAdoptionState(ctx, record, "Cleaning"))
	recovered = reopen()
	require.Equal(t, "t", recovered.GetSegment(ctx, 10).GetInsertChannel(), "interrupted cleaning replays the same committed ownership")
}

// Split adoption changes ownership, not the data revision. Exercise the real
// compaction-result installer, adoption publication, crash overlay recovery and
// canonical cleanup so index inventory and known logical totals remain usable.
func TestSplitAdoptionPreservesRevisionMetadata(t *testing.T) {
	ctx := context.Background()
	original, err := NewMemoryMeta(t)
	require.NoError(t, err)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}},
		{FieldID: 105, Name: "body", DataType: schemapb.DataType_Text, Nullable: true},
	}}
	original.AddCollection(&CollectionInfo{ID: 100, Schema: schema})
	require.NoError(t, original.AddSegment(ctx, NewSegmentInfo(&datapb.SegmentInfo{
		ID: 1, CollectionID: 100, PartitionID: 1, InsertChannel: "s", State: commonpb.SegmentState_Flushed,
		Level: datapb.SegmentLevel_L1, NumOfRows: 3, TransformStartAfterTimetick: 50,
	})))
	manifest := packed.MarshalManifestPath("/tmp/split-adoption/insert_log/100/1/10", 2)
	stats := &datapb.Statistics{
		InsertBinlogSize: 1200, StatsBinlogSize: 64, InsertBinlogCount: 1,
		FieldLogicalSizes: &datapb.FieldLogicalSizes{
			Version: storage.FieldLogicalSizeVersion, RowCount: 3,
			Fields: map[int64]*datapb.FieldLogicalSize{105: {PayloadBytes: 90003, ValidityBits: 3}},
		},
	}
	task := &datapb.CompactionTask{
		PlanID: 7, CollectionID: 100, InputSegments: []int64{1}, Channel: "s",
		Type: datapb.CompactionType_SplitShardRewriteCompaction, Schema: schema,
		SplitRewrite: &datapb.SplitShardRewritePlan{RoutingModulus: 2, Targets: []*datapb.SplitShardTaskTarget{{Vchannel: "t", Residues: []uint64{0}}, {Vchannel: "u", Residues: []uint64{1}}}},
	}
	_, err = original.CompleteCompactionMutation(ctx, task, &datapb.CompactionPlanResult{Segments: []*datapb.CompactionSegment{{
		SegmentID: 10, NumOfRows: 3, SplitTarget: "t", StorageVersion: storage.StorageV3, Manifest: manifest, Stats: stats,
	}}})
	require.NoError(t, err)
	// An index with no artifact is still completed inventory; losing its summary
	// would incorrectly put it back in the build queue after ownership changes.
	summary := &datapb.SegmentIndexSummary{IndexId: 20, FieldId: 101, BuildId: 30, IndexVersion: 2, IndexType: "FLAT", VectorEngineVersion: 1}
	require.NoError(t, original.UpdateSegmentsInfo(ctx, UpdateIndexSummaries(10, []*datapb.SegmentIndexSummary{summary})))
	assertRevision := func(m *Meta, owner string) {
		segment := m.GetSegment(ctx, 10)
		require.Equal(t, owner, segment.GetInsertChannel())
		require.Equal(t, manifest, segment.GetManifestPath())
		require.Equal(t, packed.ManifestPathKey(manifest), segment.GetFieldLogicalSizesManifest())
		require.True(t, proto.Equal(stats, segment.GetStats()))
		require.True(t, proto.Equal(stats.GetFieldLogicalSizes(), SegmentFieldLogicalSizes(segment.SegmentInfo)), "logical sizes must still be trusted for this manifest")
		require.Len(t, segment.GetIndexSummaries(), 1)
		require.True(t, proto.Equal(summary, segment.GetIndexSummaries()[0]))
		inventory := model.SegmentIndexSummaryFromProto(segment.SegmentInfo, segment.GetIndexSummaries()[0])
		require.Equal(t, commonpb.IndexState_Finished, inventory.IndexState)
		require.EqualValues(t, 100, inventory.CollectionID)
		require.EqualValues(t, 1, inventory.PartitionID)
		require.EqualValues(t, 30, inventory.BuildID)
		require.EqualValues(t, 1264, segment.GetSegmentSize())
	}
	assertRevision(original, "s")
	record, err := original.PrepareSplitAdoption(ctx, &datapb.SplitShardTask{TaskId: 7, CollectionId: 100, SourceVchannel: "s"}, []split.AdoptionMove{{SegmentID: 10, PartitionID: 1, Target: "t", TransformStart: 100}})
	require.NoError(t, err)
	view := &viewpb.DataViewOfCollection{
		CollectionId: 100, DataVersion: &viewpb.DataVersion{StreamingVersion: 1, CompactVersion: 1}, ShardSetEpoch: 3,
		Shards: []*viewpb.DataViewOfShard{{Vchannel: "t", Partitions: []*viewpb.DataViewOfPartition{{PartitionId: 1, SegmentIds: []int64{10}, SegmentManifestVersions: []int64{2}, SegmentTransformStartAfterTimeticks: []uint64{100}}}}},
	}
	require.NoError(t, original.AdoptSplit(ctx, record, view))
	assertRevision(original, "t")
	reopen := func() *Meta {
		return BootMetaWithLoader(t, original.Catalog, 100, func(metastore.DataCoordCatalog, storage.ChunkManager) DomainMetaLoader { return NoDomainMetas })
	}
	recovered := reopen()
	assertRevision(recovered, "t")
	require.Empty(t, recovered.GetSegment(ctx, 10).GetSplitTarget())
	require.EqualValues(t, 100, recovered.GetSegment(ctx, 10).GetTransformStartAfterTimetick())
	require.NoError(t, recovered.FinishSplitAdoption(ctx, record))
	assertRevision(reopen(), "t")
}
