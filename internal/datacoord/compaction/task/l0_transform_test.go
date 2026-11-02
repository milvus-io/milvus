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

package task

import (
	"context"
	"math"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/datacoord/meta"
	"github.com/milvus-io/milvus/internal/dataview"
	kvstreamingnode "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestL0CompactionTransformCursorPrefix(t *testing.T) {
	for _, tc := range []struct {
		name         string
		materialized uint64
		holeStart    uint64
		holePart     int64
		hole         bool
		old          uint64
		want         uint64
	}{
		{name: "complete prefix", materialized: 300, old: 50, want: 200},
		{name: "materialized snapshot lags", materialized: 150, old: 50, want: 150},
		{name: "no persisted proof", materialized: 0, old: 50, want: 50},
		{name: "same partition hole", materialized: 300, hole: true, holeStart: 120, holePart: 1, old: 50, want: 119},
		{name: "other partition irrelevant", materialized: 300, hole: true, holeStart: 120, holePart: 2, old: 50, want: 200},
		{name: "wildcard hole", materialized: 300, hole: true, holeStart: 120, holePart: common.AllPartitionsID, old: 50, want: 119},
		{name: "unknown hole", materialized: 300, hole: true, holeStart: 0, holePart: 1, old: 50, want: 50},
		{name: "hole cannot regress", materialized: 300, hole: true, holeStart: 20, holePart: 1, old: 50, want: 50},
		{name: "unknown target remains unknown", materialized: 300, old: 0, want: 0},
		{name: "never regress target", materialized: 300, old: 250, want: 250},
	} {
		t.Run(tc.name, func(t *testing.T) {
			ctx := t.Context()
			metadata, _, err := newMemoryMetaWithCompactionTask(t)
			require.NoError(t, err)
			metadata.StreamingNodeCatalog = kvstreamingnode.NewCataLog(nil)
			read := mockey.Mock(mockey.GetMethod(metadata.StreamingNodeCatalog, "GetVChannelBase")).To(func(_ context.Context, pchannel, vchannel string) (*streamingpb.VChannelMeta, error) {
				require.Equal(t, "p1", pchannel)
				require.Equal(t, "p1_100v0", vchannel)
				return &streamingpb.VChannelMeta{Vchannel: vchannel, TransformMaterializedTimeTick: tc.materialized}, nil
			}).Build()
			defer read.UnPatch()
			input := &datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 1, InsertChannel: "p1_100v0", Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Flushed, StartPosition: &msgpb.MsgPosition{Timestamp: 100}, DmlPosition: &msgpb.MsgPosition{Timestamp: 200}}
			target := &datapb.SegmentInfo{ID: 20, CollectionID: 100, PartitionID: 1, InsertChannel: "p1_100v0", Level: datapb.SegmentLevel_L1, State: commonpb.SegmentState_Flushed, TransformStartAfterTimetick: tc.old}
			require.NoError(t, metadata.AddSegment(ctx, meta.NewSegmentInfo(input)))
			require.NoError(t, metadata.AddSegment(ctx, meta.NewSegmentInfo(target)))
			if tc.hole {
				hole := &datapb.SegmentInfo{ID: 11, CollectionID: 100, PartitionID: tc.holePart, InsertChannel: "p1_100v0", Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Flushed, StartPosition: &msgpb.MsgPosition{Timestamp: tc.holeStart}, DmlPosition: &msgpb.MsgPosition{Timestamp: 200}}
				require.NoError(t, metadata.AddSegment(ctx, meta.NewSegmentInfo(hole)))
				metadata.SetSegmentsCompacting(ctx, []int64{11}, true)
			}
			task := &L0CompactionTask{meta: metadata}
			// The repair mode deliberately has no useful timestamp certificate.
			task.taskProto.Store(&datapb.CompactionTask{CollectionID: 100, Channel: "p1_100v0", InputSegments: []int64{10}, Pos: &msgpb.MsgPosition{Timestamp: math.MaxUint64}})
			for range 2 {
				require.NoError(t, task.SaveSegmentMeta([]*datapb.CompactionSegment{{SegmentID: 20}}), "no-hit result still certifies the exact target")
				require.Equal(t, tc.want, metadata.GetSegment(ctx, 20).GetTransformStartAfterTimetick())
				require.Equal(t, commonpb.SegmentState_Dropped, metadata.GetSegment(ctx, 10).GetState())
			}
		})
	}
}

func TestL0CompactionCursorPublicationIsAtomic(t *testing.T) {
	ctx := t.Context()
	metadata, _, err := newMemoryMetaWithCompactionTask(t)
	require.NoError(t, err)
	metadata.StreamingNodeCatalog = kvstreamingnode.NewCataLog(nil)
	read := mockey.Mock(mockey.GetMethod(metadata.StreamingNodeCatalog, "GetVChannelBase")).Return(&streamingpb.VChannelMeta{Vchannel: "p1_100v0", TransformMaterializedTimeTick: 200}, nil).Build()
	defer read.UnPatch()
	require.NoError(t, metadata.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 1, InsertChannel: "p1_100v0", State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L0, DmlPosition: &msgpb.MsgPosition{Timestamp: 200}})))
	require.NoError(t, metadata.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 20, CollectionID: 100, PartitionID: 1, InsertChannel: "p1_100v0", State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, TransformStartAfterTimetick: 50, NumOfRows: 1, Binlogs: []*datapb.FieldBinlog{{FieldID: 100, Binlogs: []*datapb.Binlog{{EntriesNum: 1, MemorySize: 8, LogID: 1}}}}})))
	catalog := metadata.Catalog.(dataview.Catalog)
	metadata.DataViewManager, err = dataview.RecoverManager(ctx, catalog, func(context.Context, int64) (bool, error) { return true, nil }, nil, nil, nil)
	require.NoError(t, err)
	_, err = metadata.DataViewManager.OnCreateCollection(ctx, dataview.CreateCollectionDataViewEvent{CollectionID: 100, VChannels: []string{"p1_100v0"}, TransformStarts: map[string]uint64{"p1_100v0": 50}})
	require.NoError(t, err)
	before, err := metadata.DataViewManager.RecomputeNow(ctx, 100, metadata.LoadableProjection)
	require.NoError(t, err)
	task := &L0CompactionTask{meta: metadata}
	task.taskProto.Store(&datapb.CompactionTask{CollectionID: 100, Channel: "p1_100v0", InputSegments: []int64{10}})
	failure := merr.WrapErrServiceUnavailableMsg("injected metadata commit failure")
	commit := mockey.Mock(mockey.GetMethod(metadata.Catalog, "AlterSegments")).Return(failure).Build()
	defer commit.UnPatch()
	err = task.SaveSegmentMeta([]*datapb.CompactionSegment{{SegmentID: 20}})
	require.ErrorIs(t, err, failure)
	require.EqualValues(t, 50, metadata.GetSegment(ctx, 20).GetTransformStartAfterTimetick())
	require.Equal(t, commonpb.SegmentState_Flushed, metadata.GetSegment(ctx, 10).GetState())
	commit.UnPatch()
	require.NoError(t, task.SaveSegmentMeta([]*datapb.CompactionSegment{{SegmentID: 20}}))
	after, err := metadata.DataViewManager.RecomputeNow(ctx, 100, metadata.LoadableProjection)
	require.NoError(t, err)
	require.Equal(t, before.GetCompactVersion()+1, after.GetCompactVersion(), "cursor-only publication must create a new DataView")
	ref, err := metadata.DataViewManager.Latest(ctx, 100)
	require.NoError(t, err)
	defer ref.Deref()
	shard := ref.DataView().GetShards()[0]
	require.EqualValues(t, 50, shard.GetGrowingTransformStartAfterTimetick(), "L0 must not advance W")
	require.EqualValues(t, 200, shard.GetCompactedTransformStartAfterTimetick())
	segments, err := metadata.Catalog.ListSegments(ctx, 100)
	require.NoError(t, err)
	for _, segment := range segments {
		if segment.GetID() == 20 {
			require.EqualValues(t, 200, segment.GetTransformStartAfterTimetick(), "cursor survives persistence")
		}
	}
}

func TestL0CompactionTransformCursorReadFailureAndRestore(t *testing.T) {
	metadata, _, err := newMemoryMetaWithCompactionTask(t)
	require.NoError(t, err)
	metadata.StreamingNodeCatalog = kvstreamingnode.NewCataLog(nil)
	failure := merr.WrapErrServiceUnavailableMsg("injected catalog failure")
	read := mockey.Mock(mockey.GetMethod(metadata.StreamingNodeCatalog, "GetVChannelBase")).Return(nil, failure).Build()
	defer read.UnPatch()
	task := &L0CompactionTask{meta: metadata}
	task.taskProto.Store(&datapb.CompactionTask{})
	_, err = task.transformCursorUpdates(t.Context(), []*datapb.CompactionSegment{{SegmentID: 20}})
	require.ErrorIs(t, err, failure)
	task.taskProto.Store(&datapb.CompactionTask{Type: datapb.CompactionType_RestoreLevel0DeleteCompaction})
	updates, err := task.transformCursorUpdates(t.Context(), []*datapb.CompactionSegment{{SegmentID: 20}})
	require.NoError(t, err)
	require.Empty(t, updates, "restore keeps target WAL coverage, not source L0 timestamps")
}

func TestL0CompactionCursorCommitsWithV3Manifest(t *testing.T) {
	meta.MockManifestUpdateSubmissions(t)
	ctx := t.Context()
	metadata, _, err := newMemoryMetaWithCompactionTask(t)
	require.NoError(t, err)
	metadata.StreamingNodeCatalog = kvstreamingnode.NewCataLog(nil)
	read := mockey.Mock(mockey.GetMethod(metadata.StreamingNodeCatalog, "GetVChannelBase")).Return(&streamingpb.VChannelMeta{Vchannel: "p1_100v0", TransformMaterializedTimeTick: 200}, nil).Build()
	defer read.UnPatch()
	basePath := "/tmp/milvus/insert_log/100/1/20"
	oldManifest := packed.MarshalManifestPath(basePath, 7)
	newManifest := packed.MarshalManifestPath(basePath, 8)
	require.NoError(t, metadata.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 10, CollectionID: 100, PartitionID: 1, InsertChannel: "p1_100v0", State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L0, DmlPosition: &msgpb.MsgPosition{Timestamp: 200}})))
	require.NoError(t, metadata.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 20, CollectionID: 100, PartitionID: 1, InsertChannel: "p1_100v0", State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1, TransformStartAfterTimetick: 50, StorageVersion: storage.StorageV3, ManifestPath: oldManifest})))
	manifest := mockey.Mock(packed.CommitManifestUpdatesWithResultAsync).To(func(_ context.Context, _ *packed.ManifestIOContext, actual string, _ *indexpb.StorageConfig, updates *packed.ManifestUpdates) (packed.ManifestUpdateResult, error) {
		require.Equal(t, oldManifest, actual)
		return meta.MockManifestUpdateResult(newManifest, updates), nil
	}).Build()
	defer manifest.UnPatch()
	task := &L0CompactionTask{meta: metadata}
	task.taskProto.Store(&datapb.CompactionTask{CollectionID: 100, Channel: "p1_100v0", InputSegments: []int64{10}})
	failure := merr.WrapErrServiceUnavailableMsg("injected metadata commit failure")
	commit := mockey.Mock(mockey.GetMethod(metadata.Catalog, "AlterSegments")).Return(failure).Build()
	defer commit.UnPatch()
	require.ErrorIs(t, task.SaveSegmentMeta([]*datapb.CompactionSegment{l0DeltaOutput(20, basePath)}), failure)
	unchanged := metadata.GetSegment(ctx, 20)
	require.Equal(t, oldManifest, unchanged.GetManifestPath())
	require.EqualValues(t, 50, unchanged.GetTransformStartAfterTimetick())
	require.Empty(t, unchanged.GetDeltalogs())
	require.Equal(t, commonpb.SegmentState_Flushed, metadata.GetSegment(ctx, 10).GetState())
	commit.UnPatch()
	for range 2 {
		require.NoError(t, task.SaveSegmentMeta([]*datapb.CompactionSegment{l0DeltaOutput(20, basePath)}))
		published := metadata.GetSegment(ctx, 20)
		require.Equal(t, newManifest, published.GetManifestPath())
		require.EqualValues(t, 200, published.GetTransformStartAfterTimetick())
		require.Len(t, published.GetDeltalogs(), 1)
		require.Equal(t, commonpb.SegmentState_Dropped, metadata.GetSegment(ctx, 10).GetState())
	}
}
