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
	"github.com/milvus-io/milvus/internal/datacoord/session"
	"github.com/milvus-io/milvus/internal/dataview"
	"github.com/milvus-io/milvus/internal/metastore"
	kvcatalog "github.com/milvus-io/milvus/internal/metastore/kv/datacoord"
	kvstreamingnode "github.com/milvus-io/milvus/internal/metastore/kv/streamingnode"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/kv"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
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

// Model an oversized catalog publication interrupted after a cursor-only
// target became durable but before the second target's deltalog was written.
type interruptedL0Catalog struct {
	metastore.DataCoordCatalog
	failure error
}

func (c interruptedL0Catalog) AlterSegments(ctx context.Context, segments []*datapb.SegmentInfo, _ ...metastore.BinlogsIncrement) error {
	for _, s := range segments {
		if s.GetID() == 3 {
			if err := c.DataCoordCatalog.AlterSegments(ctx, []*datapb.SegmentInfo{s}); err != nil {
				return err
			}
			return c.failure
		}
	}
	return merr.WrapErrServiceInternalMsg("missing first target in test catalog publication")
}

func TestCertifiedL0PublicationRecovery(t *testing.T) {
	ctx := t.Context()
	m, domain, err := newMemoryMetaWithCompactionTask(t)
	require.NoError(t, err)
	m.StreamingNodeCatalog = kvstreamingnode.NewCataLog(nil)
	const channel = "p1_100v0"
	for _, s := range []*datapb.SegmentInfo{
		{ID: 1, Level: datapb.SegmentLevel_L0, PartitionID: 1},
		{ID: 2, Level: datapb.SegmentLevel_L0, PartitionID: common.AllPartitionsID},
		{ID: 3, Level: datapb.SegmentLevel_L1, PartitionID: 1, TransformStartAfterTimetick: 50},
		{ID: 4, Level: datapb.SegmentLevel_L1, PartitionID: 2},
	} {
		s.CollectionID = 100
		s.InsertChannel = channel
		s.State = commonpb.SegmentState_Flushed
		require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(s)))
	}
	plan := &datapb.CompactionTask{
		PlanID: 77, TriggerID: 76, CollectionID: 100, PartitionID: common.AllPartitionsID, Channel: channel, Type: datapb.CompactionType_Level0DeleteCompaction,
		Pos: &msgpb.MsgPosition{Timestamp: 200}, MaterializedThrough: 200, InputSegments: []int64{1, 2}, L0Targets: &datapb.L0DispatchTargets{SegmentIds: []int64{3, 4}},
	}
	task := NewL0CompactionTask(plan, nil, m, domain.compactionTaskMeta)
	require.NoError(t, task.SaveTaskMeta())
	outputs := []*datapb.CompactionSegment{{SegmentID: 3}, {SegmentID: 4, Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{{LogID: 88, EntriesNum: 1, MutationCounts: &datapb.DeltaMutationCounts{UpdateKeys: 2}}}}}}}
	require.ErrorIs(t, task.SaveSegmentMeta(outputs[:1]), merr.ErrDataIntegrity)
	// A late manifest registration invalidates the frozen round. Keep every
	// input live until the policy can select a complete group again.
	require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{
		ID: 9, CollectionID: 100, PartitionID: 3, InsertChannel: channel, Level: datapb.SegmentLevel_L0,
		State: commonpb.SegmentState_Flushed, StartPosition: &msgpb.MsgPosition{Timestamp: 140},
		DmlPosition: &msgpb.MsgPosition{Timestamp: 140},
	})))
	require.ErrorIs(t, task.SaveSegmentMeta(outputs), merr.ErrCompactionPlanConflict)
	require.False(t, task.GetTaskProto().GetL0Targets().GetPublished())
	require.NoError(t, m.UpdateSegmentsInfo(ctx, meta.UpdateStatusOperator(9, commonpb.SegmentState_Dropped)))
	require.EqualValues(t, 50, m.GetSegment(ctx, 3).GetTransformStartAfterTimetick())
	catalog := m.Catalog
	partial := merr.WrapErrServiceUnavailableMsg("interrupted target publication")
	m.Catalog = interruptedL0Catalog{catalog, partial}
	require.ErrorIs(t, task.SaveSegmentMeta(outputs), partial)
	m.Catalog = catalog
	restart := func() {
		domain = &compactionTaskTestMetas{}
		m = meta.BootMetaWithLoader(t, catalog, 100, withCompactionTaskTestMetas(domain))
		m.StreamingNodeCatalog = kvstreamingnode.NewCataLog(nil)
		saved, err := catalog.ListCompactionTask(ctx)
		require.NoError(t, err)
		require.Len(t, saved, 1)
		task = NewL0CompactionTask(saved[0], nil, m, domain.compactionTaskMeta)
	}
	restart()
	require.EqualValues(t, 200, m.GetSegment(ctx, 3).GetTransformStartAfterTimetick())
	require.Empty(t, m.GetSegment(ctx, 4).GetDeltalogs())
	require.False(t, task.GetTaskProto().GetL0Targets().GetPublished())
	for _, id := range plan.InputSegments {
		require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, id).GetState())
	}
	failure := merr.WrapErrServiceUnavailableMsg("injected marker failure")
	marker := mockey.Mock(mockey.GetMethod(m.Catalog, "SaveCompactionTask")).Return(failure).Build()
	defer marker.UnPatch()
	require.ErrorIs(t, task.SaveSegmentMeta(outputs), failure)
	require.EqualValues(t, 200, m.GetSegment(ctx, 3).GetTransformStartAfterTimetick())
	require.EqualValues(t, 0, m.GetSegment(ctx, 4).GetTransformStartAfterTimetick())
	require.Len(t, m.GetSegment(ctx, 4).GetDeltalogs()[0].GetBinlogs(), 1)
	require.False(t, task.GetTaskProto().GetL0Targets().GetPublished())
	for _, id := range plan.InputSegments {
		require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, id).GetState())
	}
	marker.UnPatch()
	// A target appearing after the worker attempt is outside the frozen plan.
	require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 5, CollectionID: 100, PartitionID: 2, InsertChannel: channel, State: commonpb.SegmentState_Flushed, Level: datapb.SegmentLevel_L1})))
	frozen, _, err := task.SelectFlushedSegment()
	require.NoError(t, err)
	require.Len(t, frozen, 2)
	require.NoError(t, task.SaveSegmentMeta(outputs))
	require.Len(t, m.GetSegment(ctx, 4).GetDeltalogs()[0].GetBinlogs(), 1)
	require.True(t, task.GetTaskProto().GetL0Targets().GetPublished())
	// Model an interruption between catalog chunks of input retirement.
	require.NoError(t, m.UpdateSegmentsInfo(ctx, meta.UpdateStatusOperator(1, commonpb.SegmentState_Dropped), meta.UpdateCompactedOperator(1)))
	restart()
	require.True(t, task.Process())
	for _, id := range plan.InputSegments {
		require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, id).GetState())
	}
}

func TestCertifiedL0RejectsOldWorkerResult(t *testing.T) {
	ctx := t.Context()
	m, domain, err := newMemoryMetaWithCompactionTask(t)
	require.NoError(t, err)
	m.StreamingNodeCatalog = kvstreamingnode.NewCataLog(nil)
	plan := &datapb.CompactionTask{PlanID: 77, TriggerID: 76, NodeID: 71, CollectionID: 100, PartitionID: 1, Channel: "p1_100v0", Type: datapb.CompactionType_Level0DeleteCompaction, MaterializedThrough: 200, InputSegments: []int64{1}, L0Targets: &datapb.L0DispatchTargets{SegmentIds: []int64{2}}}
	task := NewL0CompactionTask(plan, nil, m, domain.compactionTaskMeta)
	for _, s := range []*datapb.SegmentInfo{{ID: 1, Level: datapb.SegmentLevel_L0}, {ID: 2, Level: datapb.SegmentLevel_L1, TransformStartAfterTimetick: 50}} {
		s.CollectionID = 100
		s.PartitionID = 1
		s.InsertChannel = plan.Channel
		s.State = commonpb.SegmentState_Flushed
		require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(s)))
	}
	cluster := session.NewMockCluster(t)
	for _, tc := range []struct {
		through uint64
		missing bool
	}{{}, {through: 199}, {through: 200, missing: true}, {through: 200}} {
		through := tc.through
		retry := through != 200 || tc.missing
		require.NoError(t, task.UpdateAndSaveTaskMeta(SetNodeID(71), SetState(datapb.CompactionTaskState_executing)))
		res := &datapb.CompactionPlanResult{PlanID: plan.PlanID, Channel: plan.Channel, Type: plan.Type, State: datapb.CompactionTaskState_completed, MaterializedThrough: through, Segments: []*datapb.CompactionSegment{{SegmentID: 2}}}
		if tc.missing {
			res.Segments = nil
		}
		cluster.EXPECT().QueryCompaction(int64(71), &datapb.CompactionStateRequest{PlanID: 77}).Return(res, nil).Once()
		if retry {
			cluster.EXPECT().DropCompaction(int64(71), int64(77)).Return(nil).Once()
		}
		task.QueryTaskOnWorker(cluster)
		if retry {
			require.EqualValues(t, 50, m.GetSegment(ctx, 2).GetTransformStartAfterTimetick())
			require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 1).GetState())
			require.False(t, task.GetTaskProto().GetL0Targets().GetPublished())
		} else {
			require.EqualValues(t, 200, m.GetSegment(ctx, 2).GetTransformStartAfterTimetick())
			require.True(t, task.Process())
			require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 1).GetState())
		}
	}
}

func TestCertifiedL0NoTargets(t *testing.T) {
	ctx := t.Context()
	m, domains, err := newMemoryMetaWithCompactionTask(t)
	require.NoError(t, err)
	plan := &datapb.CompactionTask{PlanID: 8, TriggerID: 6, CollectionID: 1, PartitionID: 1, Channel: "p1_1v0", Type: datapb.CompactionType_Level0DeleteCompaction, Pos: &msgpb.MsgPosition{Timestamp: 100}, MaterializedThrough: 200, InputSegments: []int64{7}}
	require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(&datapb.SegmentInfo{ID: 7, CollectionID: 1, PartitionID: 1, InsertChannel: plan.Channel, Level: datapb.SegmentLevel_L0, State: commonpb.SegmentState_Flushed})))
	task := NewL0CompactionTask(plan, nil, m, domains.compactionTaskMeta)
	task.CreateTaskOnWorker(1, nil)
	require.NotNil(t, task.GetTaskProto().GetL0Targets())
	require.Empty(t, task.GetTaskProto().GetL0Targets().GetSegmentIds())
	require.True(t, task.GetTaskProto().GetL0Targets().GetPublished())
	require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, 7).GetState())
	require.True(t, task.Process())
	require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, 7).GetState())
}

// Cap the actual catalog backend and interrupt a later chunk. All successful
// transactions land in the same backing KV that the restarted coordinator reads.
type interruptedL0KV struct {
	kv.MetaKv
	calls  int
	failAt int
}

func (k *interruptedL0KV) MaxTxnOps() int { return 1 }

func (k *interruptedL0KV) MultiSave(ctx context.Context, values map[string]string) error {
	k.calls++
	if k.calls == k.failAt {
		return merr.WrapErrServiceUnavailableMsg("interrupted L0 metadata chunk")
	}
	return k.MetaKv.MultiSave(ctx, values)
}

func TestCertifiedL0OversizedCatalogPublication(t *testing.T) {
	const collection, sources, targets = 100, 2, 32
	const m0 = 200
	ctx := t.Context()
	m, domain, err := newMemoryMetaWithCompactionTask(t)
	require.NoError(t, err)
	channel := "p1_100v0"
	taskProto := &datapb.CompactionTask{PlanID: 99, TriggerID: 98, Channel: channel, CollectionID: collection, PartitionID: common.AllPartitionsID, MaterializedThrough: m0, L0Targets: &datapb.L0DispatchTargets{}}
	var outputs []*datapb.CompactionSegment
	for i := int64(1); i <= sources+targets; i++ {
		seg := &datapb.SegmentInfo{ID: i, CollectionID: collection, PartitionID: 1, InsertChannel: channel, State: commonpb.SegmentState_Flushed}
		if i <= sources {
			seg.Level = datapb.SegmentLevel_L0
			taskProto.InputSegments = append(taskProto.InputSegments, i)
		} else {
			seg.Level = datapb.SegmentLevel_L1
			seg.StorageVersion = storage.StorageV2
			seg.Stats = &datapb.Statistics{InsertBinlogCount: 1}
			seg.TransformStartAfterTimetick = 50
			taskProto.L0Targets.SegmentIds = append(taskProto.L0Targets.SegmentIds, i)
			outputs = append(outputs, &datapb.CompactionSegment{SegmentID: i, Deltalogs: []*datapb.FieldBinlog{{Binlogs: []*datapb.Binlog{
				{LogID: i * 10, EntriesNum: 1, MutationCounts: &datapb.DeltaMutationCounts{UpdateKeys: 1}},
				{LogID: i*10 + 1, EntriesNum: 1, MutationCounts: &datapb.DeltaMutationCounts{UpdateKeys: 1}},
			}}}})
		}
		require.NoError(t, m.AddSegment(ctx, meta.NewSegmentInfo(seg)))
	}
	catalog := m.Catalog.(*kvcatalog.Catalog)
	kv := &interruptedL0KV{MetaKv: catalog.MetaKv, failAt: targets + 2}
	catalog.MetaKv = kv
	maxOps := paramtable.Get().MetaStoreCfg.MaxEtcdTxnNum
	require.NoError(t, paramtable.Get().Save(maxOps.Key, "1"))
	t.Cleanup(func() { paramtable.Get().Reset(maxOps.Key) })
	task := NewL0CompactionTask(taskProto, nil, m, domain.compactionTaskMeta)
	require.NoError(t, task.SaveTaskMeta())
	kv.calls = 0
	require.ErrorIs(t, task.SaveSegmentMeta(outputs), merr.ErrServiceUnavailable)
	require.Equal(t, kv.failAt, kv.calls)
	domain = &compactionTaskTestMetas{}
	m = meta.BootMetaWithLoader(t, catalog, collection, withCompactionTaskTestMetas(domain))
	saved, err := catalog.ListCompactionTask(ctx)
	require.NoError(t, err)
	require.Len(t, saved, 1)
	require.False(t, saved[0].GetL0Targets().GetPublished())
	task = NewL0CompactionTask(saved[0], nil, m, domain.compactionTaskMeta)
	for _, id := range taskProto.GetInputSegments() {
		require.Equal(t, commonpb.SegmentState_Flushed, m.GetSegment(ctx, id).GetState())
	}
	visible := 0
	for _, id := range taskProto.GetL0Targets().GetSegmentIds() {
		seg := m.GetSegment(ctx, id)
		if seg.GetTransformStartAfterTimetick() == m0 {
			visible++
			require.Len(t, seg.GetDeltalogs(), 1, "cursor precedes delta for target %d", id)
			require.Len(t, seg.GetDeltalogs()[0].GetBinlogs(), 2)
		}
	}
	require.Positive(t, visible)
	require.Less(t, visible, targets)
	require.NoError(t, task.SaveSegmentMeta(outputs))
	require.True(t, task.Process())
	for _, id := range taskProto.GetL0Targets().GetSegmentIds() {
		seg := m.GetSegment(ctx, id)
		require.EqualValues(t, m0, seg.GetTransformStartAfterTimetick())
		require.Len(t, seg.GetDeltalogs()[0].GetBinlogs(), 2)
		require.EqualValues(t, 2, seg.GetStats().GetDeltaBinlogCount())
		require.EqualValues(t, 0, seg.GetStats().GetDeleteNumRows())
		require.EqualValues(t, 1, seg.GetStats().GetInsertBinlogCount())
	}
	for _, id := range taskProto.GetInputSegments() {
		require.Equal(t, commonpb.SegmentState_Dropped, m.GetSegment(ctx, id).GetState())
	}
}
