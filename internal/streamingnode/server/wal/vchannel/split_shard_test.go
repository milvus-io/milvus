// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
package vchannel

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/vchannel/segment"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/walsummary"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

const (
	splitSource  = "p0_1v0"
	splitTarget1 = "p1_1v1"
	splitTarget2 = "p2_1v2"
	splitTaskID  = int64(77)
)

func splitSchema(version int32) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name:    "c",
		Version: version,
		Fields:  []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true}},
	}
}

// newSplitShardCopy builds the copy of a SplitShard message on one vchannel,
// as it is read from the WAL.
func newSplitShardCopy(vchannel string, taskID int64, timetick uint64) message.ImmutableMessage {
	return message.NewSplitShardMessageBuilderV2().WithVChannel(vchannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    1,
			SplitTaskId:     taskID,
			SourceVchannel:  splitSource,
			TargetVchannels: []string{splitTarget1, splitTarget2},
		}).
		WithBody(&message.SplitShardMessageBody{
			GenesisHeader: &message.CreateCollectionMessageHeader{CollectionId: 1, PartitionIds: []int64{10, 11}},
			Genesis:       &message.CreateCollectionRequest{CollectionID: 1, CollectionSchema: splitSchema(3)},
			Routing:       &message.SplitShardRouting{RoutingModulus: 2},
		}).
		MustBuildMutable().WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick))).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick)))
}

func observeOnManager(manager *PChannelRecoveryManager, msg message.ImmutableMessage) {
	owner := message.NewOwnedImmutableMessage(msg, nil)
	dispatch := owner.Clone()
	manager.ObserveMessage(context.Background(), dispatch)
	dispatch.Release()
	owner.Release()
}

func newSplitTestManager(t *testing.T, pchannel string, metas map[string]*streamingpb.VChannelMeta) *PChannelRecoveryManager {
	t.Helper()
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{
		PChannel:       pchannel,
		VChannelMetas:  metas,
		Runtime:        moduleapi.Runtime{Scheduler: &recordingVChannelScheduler{}},
		SummaryManager: walsummary.NewManager(walsummary.ManagerConfig{}),
	})
	require.NoError(t, err)
	t.Cleanup(manager.Close)
	return manager
}

// vchannelMetaOf returns the metadata the module of a vchannel holds.
func vchannelMetaOf(t *testing.T, manager *PChannelRecoveryManager, vchannel string) *streamingpb.VChannelMeta {
	t.Helper()
	module := manager.Module(vchannel)
	require.NotNil(t, module)
	require.NotNil(t, module.vchannelView)
	return module.vchannelView.AssignmentMeta()
}

func newSplitSourceMeta() *streamingpb.VChannelMeta {
	return &streamingpb.VChannelMeta{
		Vchannel: splitSource, State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick: 10, CreateCollectionTimeTick: 5,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 1,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				{PartitionId: 11, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{{
				Schema: splitSchema(3), State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL, CheckpointTimeTick: 5,
			}},
		},
	}
}

func TestSplitShardGenesisCreatesTheVChannelModule(t *testing.T) {
	manager := newSplitTestManager(t, "p1", nil)
	var pinned []uint64
	patch := mockey.Mock((*walsummary.Manager).SetQueryRetention).To(func(_ *walsummary.Manager, vchannel string, startAfter uint64) {
		if vchannel == splitTarget1 {
			pinned = append(pinned, startAfter)
		}
	}).Build()
	defer patch.UnPatch()

	// Only a genesis creates a module. A message of another type does not, and
	// neither does the copy of a SplitShard message that is not a genesis of
	// the vchannel it is addressed to.
	flush := message.NewManualFlushMessageBuilderV2().WithVChannel(splitTarget1).
		WithHeader(&message.ManualFlushMessageHeader{CollectionId: 1}).WithBody(&message.ManualFlushMessageBody{}).
		MustBuildMutable().WithTimeTick(100).IntoImmutableMessage(walimplstest.NewTestMessageID(100))
	observeOnManager(manager, flush)
	require.Nil(t, manager.Module(splitTarget1))
	observeOnManager(manager, newSplitShardCopy(splitSource, splitTaskID, 200))
	require.Nil(t, manager.Module(splitSource))
	observeOnManager(manager, newSplitShardCopy("p1_1v9", splitTaskID, 210))
	require.Nil(t, manager.Module("p1_1v9"))

	observeOnManager(manager, newSplitShardCopy(splitTarget1, splitTaskID, 300))
	meta := vchannelMetaOf(t, manager, splitTarget1)
	require.True(t, proto.Equal(&streamingpb.VChannelMeta{
		Vchannel:            splitTarget1,
		SplitSourceVchannel: splitSource,
		SplitGenesisTaskId:  splitTaskID,
		State:               streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 1,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
				{PartitionId: 11, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{{
				Schema: splitSchema(3), State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL, CheckpointTimeTick: 300,
			}},
		},
		CheckpointTimeTick:       300,
		CreateCollectionTimeTick: 300,
	}, meta), "the genesis of a target builds the metadata a CreateCollection message builds: %v", meta)
	require.True(t, manager.Module(splitTarget1).IsActive())

	// The transform history of the target is retained from its genesis. The
	// target has no event below it, so this retains everything from T_switch.
	require.NotEmpty(t, pinned)
	require.Equal(t, uint64(300), pinned[len(pinned)-1])

	// The write path recovers the target as a vchannel that accepts DML.
	state, ok := manager.RecoverySnapshot().VChannels[splitTarget1]
	require.True(t, ok)
	require.Equal(t, int64(1), state.CollectionID)
	require.ElementsMatch(t, []int64{10, 11}, state.PartitionIDs)
	require.Equal(t, uint64(300), state.SchemaTimeTick)
	require.Zero(t, state.SplitTaskID)
	require.EqualValues(t, splitTaskID, state.SplitGenesisTaskID)
	require.Equal(t, splitSource, state.SplitSourceVChannel)

	// The module is dirty, and its snapshot is what recovery reads back.
	snapshots := manager.ConsumeDirtySnapshots()
	require.Len(t, snapshots, 1)
	persisted := snapshots[0].Payload().(*streamingpb.VChannelMeta)
	require.True(t, proto.Equal(meta, persisted))
	snapshots[0].MarkPersisted()

	// A second delivery of the same genesis, and a genesis appended again at a
	// later time tick, both leave the vchannel as it is.
	observeOnManager(manager, newSplitShardCopy(splitTarget1, splitTaskID, 300))
	observeOnManager(manager, newSplitShardCopy(splitTarget1, splitTaskID, 400))
	require.True(t, proto.Equal(meta, vchannelMetaOf(t, manager, splitTarget1)))
	require.Equal(t, uint64(300), pinned[len(pinned)-1])
}

func TestSplitShardGenesisReplayReproducesTheVChannelMeta(t *testing.T) {
	first := newSplitTestManager(t, "p1", nil)
	observeOnManager(first, newSplitShardCopy(splitTarget1, splitTaskID, 300))
	want := vchannelMetaOf(t, first, splitTarget1)

	// The node restarts before the metadata is persisted: the WAL is replayed
	// from a checkpoint below the genesis, on an empty catalog.
	replayed := newSplitTestManager(t, "p1", nil)
	observeOnManager(replayed, newSplitShardCopy(splitTarget1, splitTaskID, 300))
	require.True(t, proto.Equal(want, vchannelMetaOf(t, replayed, splitTarget1)))

	// The node restarts after the metadata is persisted: the genesis is
	// replayed on top of it and changes nothing.
	recovered := newSplitTestManager(t, "p1", map[string]*streamingpb.VChannelMeta{splitTarget1: want})
	require.True(t, proto.Equal(want, vchannelMetaOf(t, recovered, splitTarget1)))
	observeOnManager(recovered, newSplitShardCopy(splitTarget1, splitTaskID, 300))
	require.True(t, proto.Equal(want, vchannelMetaOf(t, recovered, splitTarget1)))
	require.Empty(t, recovered.ConsumeDirtySnapshots())
}

func TestSplitShardFenceRecordsTheLineageOnce(t *testing.T) {
	manager := newSplitTestManager(t, "p0", map[string]*streamingpb.VChannelMeta{splitSource: newSplitSourceMeta()})
	require.Zero(t, manager.RecoverySnapshot().VChannels[splitSource].SplitTaskID)

	observeOnManager(manager, newSplitShardCopy(splitSource, splitTaskID, 200))
	meta := vchannelMetaOf(t, manager, splitSource)
	wantLineage := &streamingpb.VChannelSplitLineage{
		SplitTaskId:     splitTaskID,
		TargetVchannels: []string{splitTarget1, splitTarget2},
		SwitchTimeTick:  200,
	}
	require.True(t, proto.Equal(wantLineage, meta.GetSplitLineage()))
	require.Equal(t, uint64(200), meta.GetCheckpointTimeTick())
	// The source is not dropped and not retired: it stays NORMAL and active,
	// with its collection, partitions and schemas untouched.
	require.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, meta.GetState())
	require.True(t, manager.Module(splitSource).IsActive())
	require.True(t, proto.Equal(newSplitSourceMeta().GetCollectionInfo(), meta.GetCollectionInfo()))
	require.Equal(t, uint64(5), meta.GetCreateCollectionTimeTick())

	// The write path recovers it as registered and fenced.
	state, ok := manager.RecoverySnapshot().VChannels[splitSource]
	require.True(t, ok)
	require.Equal(t, splitTaskID, state.SplitTaskID)
	require.ElementsMatch(t, []int64{10, 11}, state.PartitionIDs)

	// The copy appended again, and a copy of another split, move the
	// checkpoint and leave the record as it is: T_switch stays 200.
	observeOnManager(manager, newSplitShardCopy(splitSource, splitTaskID, 250))
	observeOnManager(manager, newSplitShardCopy(splitSource, splitTaskID+1, 260))
	meta = vchannelMetaOf(t, manager, splitSource)
	require.True(t, proto.Equal(wantLineage, meta.GetSplitLineage()))
	require.Equal(t, uint64(260), meta.GetCheckpointTimeTick())
	require.Equal(t, splitTaskID, manager.RecoverySnapshot().VChannels[splitSource].SplitTaskID)

	// A message that is local to the cluster is still observed after the
	// fence: a forwarded schema is recorded.
	schemaChange := message.NewSchemaChangeMessageBuilderV2().WithVChannel(splitSource).WithUnreplicable().
		WithHeader(&message.SchemaChangeMessageHeader{CollectionId: 1}).
		WithBody(&message.SchemaChangeMessageBody{Schema: splitSchema(4)}).
		MustBuildMutable().WithTimeTick(300).IntoImmutableMessage(walimplstest.NewTestMessageID(300))
	observeOnManager(manager, schemaChange)
	meta = vchannelMetaOf(t, manager, splitSource)
	require.Len(t, meta.GetCollectionInfo().GetSchemas(), 2)
	require.Equal(t, int32(4), meta.GetCollectionInfo().GetSchemas()[1].GetSchema().GetVersion())
	require.True(t, proto.Equal(wantLineage, meta.GetSplitLineage()))
}

func TestSplitShardFenceReplayReproducesTheVChannelMeta(t *testing.T) {
	first := newSplitTestManager(t, "p0", map[string]*streamingpb.VChannelMeta{splitSource: newSplitSourceMeta()})
	observeOnManager(first, newSplitShardCopy(splitSource, splitTaskID, 200))
	want := vchannelMetaOf(t, first, splitSource)

	// Restart before the record is persisted: replay from the old metadata.
	replayed := newSplitTestManager(t, "p0", map[string]*streamingpb.VChannelMeta{splitSource: newSplitSourceMeta()})
	observeOnManager(replayed, newSplitShardCopy(splitSource, splitTaskID, 200))
	require.True(t, proto.Equal(want, vchannelMetaOf(t, replayed, splitSource)))

	// Restart after the record is persisted: the write path is fenced before
	// any message is replayed, and the replay changes nothing.
	recovered := newSplitTestManager(t, "p0", map[string]*streamingpb.VChannelMeta{splitSource: want})
	require.Equal(t, splitTaskID, recovered.RecoverySnapshot().VChannels[splitSource].SplitTaskID)
	observeOnManager(recovered, newSplitShardCopy(splitSource, splitTaskID, 200))
	require.True(t, proto.Equal(want, vchannelMetaOf(t, recovered, splitSource)))
	require.Empty(t, recovered.ConsumeDirtySnapshots())
}

func TestSplitShardFenceIsPersistedAcrossAPendingPartitionDrop(t *testing.T) {
	view := NewVChannelViewFromMeta(newSplitSourceMeta())
	drop := message.MustAsImmutableDropPartitionMessageV1(message.NewDropPartitionMessageBuilderV1().WithVChannel(splitSource).
		WithHeader(&message.DropPartitionMessageHeader{CollectionId: 1, PartitionId: 10}).WithBody(&message.DropPartitionRequest{}).
		MustBuildMutable().WithTimeTick(20).IntoImmutableMessage(walimplstest.NewTestMessageID(20)))
	require.True(t, view.ObserveDropPartitionMessageV1(drop))
	require.Nil(t, view.SplitLineage())

	fence := message.MustAsImmutableSplitShardMessageV2(newSplitShardCopy(splitSource, splitTaskID, 30))
	require.True(t, view.ObserveSplitShardFenceMessageV2(fence))
	// The same message again is below the checkpoint and is not observed.
	require.False(t, view.ObserveSplitShardFenceMessageV2(fence))
	require.Equal(t, uint64(30), view.SplitLineage().GetSwitchTimeTick())

	// The snapshot that is published while the drop is pending is the one
	// taken before the drop. It must carry the record already: a restart from
	// it must not recover a source that accepts DML.
	snapshot, _ := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	require.Equal(t, uint64(10), snapshot.GetCheckpointTimeTick())
	require.Equal(t, splitTaskID, snapshot.GetSplitLineage().GetSplitTaskId())
	recovered := NewVChannelViewFromMeta(snapshot)
	state, ok := recovered.WritePathRecoveryState()
	require.True(t, ok)
	require.Equal(t, splitTaskID, state.SplitTaskID)

	// A vchannel that is closing does not record a split.
	closing := NewVChannelViewFromMeta(newSplitSourceMeta())
	dropCollection := message.MustAsImmutableDropCollectionMessageV1(message.NewDropCollectionMessageBuilderV1().WithVChannel(splitSource).
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: 1}).WithBody(&message.DropCollectionRequest{}).
		MustBuildMutable().WithTimeTick(20).IntoImmutableMessage(walimplstest.NewTestMessageID(20)))
	require.True(t, closing.ObserveDropCollectionMessageV1(dropCollection))
	require.False(t, closing.ObserveSplitShardFenceMessageV2(fence))
	require.Nil(t, closing.SplitLineage())
}

func TestSplitShardFenceSealsTheGrowingSegmentsOfTheSource(t *testing.T) {
	ctx := context.Background()
	scheduler := &recordingVChannelScheduler{}
	lifecycle := segment.NewSegmentLifecycleWriter(nil, 1)
	var committed []*streamingpb.SegmentAssignmentMeta
	patch := mockey.Mock(mockey.GetMethod(lifecycle, "CommitL1SegmentWithTransformStart")).To(func(_ context.Context, meta *streamingpb.SegmentAssignmentMeta, _ uint64) (*viewpb.DataVersion, error) {
		committed = append(committed, proto.Clone(meta).(*streamingpb.SegmentAssignmentMeta))
		return &viewpb.DataVersion{StreamingVersion: 1}, nil
	}).Build()
	defer patch.UnPatch()

	metas := map[int64]*streamingpb.SegmentAssignmentMeta{
		1: newMaterializationBlockerMeta(1, 40, false),
		2: newMaterializationBlockerMeta(2, 50, false),
	}
	for _, meta := range metas {
		meta.State = streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING
		meta.Vchannel = splitSource
	}
	module, err := NewModule(ModuleConfig{
		PChannel: "p0", VChannel: splitSource,
		VChannelMeta:     newSplitSourceMeta(),
		Segments:         metas,
		SegmentLifecycle: lifecycle,
		SummaryManager:   walsummary.NewManager(walsummary.ManagerConfig{}),
		Runtime:          moduleapi.Runtime{Scheduler: scheduler},
	})
	require.NoError(t, err)
	require.Len(t, module.RecoverySnapshot().GrowingSegments, 2)

	raw := newSplitShardCopy(splitSource, splitTaskID, 100)
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(raw)
	retained := owner.Clone()
	require.True(t, module.ObserveMessage(ctx, retained))
	retained.Release()
	owner.Release()

	// Every growing segment is sealed at the fence. The fence holds its
	// message until they are flushed and the deletes of the vchannel are
	// materialized: one task per segment and one for L0.
	require.Empty(t, module.RecoverySnapshot().GrowingSegments)
	require.Zero(t, tracker.CompletedPoint().TimeTick)
	require.Len(t, scheduler.tasks, 3, "two L1 flushes and one L0 materialization")
	for _, task := range scheduler.tasks {
		require.NoError(t, task.Execute(ctx))
	}
	// The sealed segments are flushed through the normal path, and they stay
	// segments of the source vchannel.
	require.Len(t, committed, 2)
	for _, meta := range committed {
		require.Equal(t, splitSource, meta.GetVchannel())
	}
	require.Equal(t, uint64(100), module.l0Materializer.MaterializedTimeTick())
	require.Equal(t, uint64(100), tracker.CompletedPoint().TimeTick)

	// The source is still an active vchannel with its lineage recorded.
	require.True(t, module.IsActive())
	require.Equal(t, uint64(100), module.vchannelView.SplitLineage().GetSwitchTimeTick())
}
