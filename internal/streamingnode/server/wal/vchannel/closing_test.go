package vchannel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

func TestRecoveredFinalCommitPublishesTombstone(t *testing.T) {
	manager := newCleanupTestManager(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		map[int64]*streamingpb.SegmentAssignmentMeta{1: {
			SegmentId: 1, PartitionId: 10, Vchannel: "v1",
			State:              streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_FLUSHED,
			CheckpointTimeTick: 10, SealedAtDataVersion: &viewpb.DataVersion{StreamingVersion: 7},
		}})
	require.Empty(t, manager.ConsumeDirtySnapshots())
	require.Empty(t, manager.RecoverySnapshot().GrowingSegments)
	snapshots := manager.ConsumeDirtySnapshots()
	require.Len(t, snapshots, 1, "recovery must enqueue its terminal metadata change")
	require.Equal(t, streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_TOMBSTONED,
		snapshots[0].Payload().(*streamingpb.SegmentAssignmentMeta).State)
	snapshots[0].MarkPersisted()
	require.Empty(t, manager.ConsumeDirtySnapshots())
}

func TestPartitionDropFencesLaterMetadataUntilCompletion(t *testing.T) {
	view := NewVChannelViewFromMeta(&streamingpb.VChannelMeta{
		Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, CheckpointTimeTick: 10,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 1,
			Partitions:   []*streamingpb.PartitionInfoOfVChannel{{PartitionId: 1, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL}, {PartitionId: 2, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL}},
			Schemas:      []*streamingpb.CollectionSchemaOfVChannel{{Schema: &schemapb.CollectionSchema{Name: "old"}, State: streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL, CheckpointTimeTick: 1}},
		},
	})
	drop := func(partition int64, tt uint64) message.ImmutableDropPartitionMessageV1 {
		return message.MustAsImmutableDropPartitionMessageV1(message.NewDropPartitionMessageBuilderV1().WithVChannel("v1").
			WithHeader(&message.DropPartitionMessageHeader{CollectionId: 1, PartitionId: partition}).WithBody(&message.DropPartitionRequest{}).
			MustBuildMutable().WithTimeTick(tt).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt))))
	}
	create := func(partition int64, tt uint64) {
		msg := message.NewCreatePartitionMessageBuilderV1().WithVChannel("v1").
			WithHeader(&message.CreatePartitionMessageHeader{CollectionId: 1, PartitionId: partition}).WithBody(&message.CreatePartitionRequest{}).
			MustBuildMutable().WithTimeTick(tt).IntoImmutableMessage(walimplstest.NewTestMessageID(int64(tt)))
		require.True(t, view.ObserveCreatePartitionMessageV1(message.MustAsImmutableCreatePartitionMessageV1(msg)))
	}
	first := drop(1, 20)
	require.True(t, view.ObserveDropPartitionMessageV1(first))
	snapshot, _ := view.ConsumeDirtyAndGetSnapshot()
	require.Nil(t, snapshot, "closing itself does not dirty stable metadata")
	create(3, 25)
	require.True(t, view.ObserveDropPartitionMessageV1(drop(2, 30)))
	create(4, 35)
	view.SetTransformMaterializedTimeTick(18)
	snapshot, _ = view.ConsumeDirtyAndGetSnapshot()
	require.Equal(t, uint64(10), snapshot.CheckpointTimeTick)
	require.Equal(t, uint64(18), snapshot.TransformMaterializedTimeTick)
	require.Len(t, snapshot.CollectionInfo.Partitions, 2, "later metadata cannot cross pending Drop")
	require.Equal(t, streamingpb.PartitionState_PARTITION_STATE_NORMAL, snapshot.CollectionInfo.Partitions[0].State)
	// A crash here recovers the old lifecycle and can replay Drop. No runtime
	// closing marker leaked into either the snapshot or the persisted checkpoint.
	recovered := NewVChannelViewFromMeta(snapshot)
	require.True(t, recovered.ObserveDropPartitionMessageV1(first))
	view.MarkSnapshotPersisted(snapshot)
	view.CompleteDrop(20)
	snapshot, _ = view.ConsumeDirtyAndGetSnapshot()
	require.Equal(t, uint64(25), snapshot.CheckpointTimeTick)
	require.Len(t, snapshot.CollectionInfo.Partitions, 3)
	require.Equal(t, streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, snapshot.CollectionInfo.Partitions[0].State)
	require.Equal(t, streamingpb.PartitionState_PARTITION_STATE_NORMAL, snapshot.CollectionInfo.Partitions[1].State)
	view.MarkSnapshotPersisted(snapshot)
	view.CompleteDrop(30)
	snapshot, _ = view.ConsumeDirtyAndGetSnapshot()
	require.Equal(t, uint64(35), snapshot.CheckpointTimeTick)
	require.Len(t, snapshot.CollectionInfo.Partitions, 4)
	require.Equal(t, streamingpb.PartitionState_PARTITION_STATE_TOMBSTONED, snapshot.CollectionInfo.Partitions[1].State)
}

func TestCollectionDropWaitsForL0BeforePublishingTombstone(t *testing.T) {
	scheduler := &recordingVChannelScheduler{}
	module, err := NewModule(ModuleConfig{
		PChannel: "p1", VChannel: "v1", Runtime: moduleapi.Runtime{Scheduler: scheduler},
		VChannelMeta: &streamingpb.VChannelMeta{
			Vchannel: "v1", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 10, CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: 1},
		},
	})
	require.NoError(t, err)
	raw := message.NewDropCollectionMessageBuilderV1().WithVChannel("v1").
		WithHeader(&message.DropCollectionMessageHeader{CollectionId: 1}).WithBody(&message.DropCollectionRequest{}).
		MustBuildMutable().WithTimeTick(20).WithLastConfirmed(walimplstest.NewTestMessageID(19)).IntoImmutableMessage(walimplstest.NewTestMessageID(20))
	tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
	owner := tracker.Track(raw)
	dispatch := owner.Clone()
	require.True(t, module.ObserveMessage(context.Background(), dispatch))
	dispatch.Release()
	owner.Release()
	require.False(t, module.IsActive())
	require.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, module.vchannelView.AssignmentMeta().State)
	require.Empty(t, module.ConsumeDirtySnapshots())
	require.Zero(t, tracker.CompletedPoint().TimeTick)
	require.Len(t, scheduler.tasks, 1)
	// Later PChannel FlushAll must not create new work for a closing collection.
	flush := message.NewFlushAllMessageBuilderV2().WithVChannel("p1").
		WithHeader(&message.FlushAllMessageHeader{}).WithBody(&message.FlushAllMessageBody{}).
		MustBuildMutable().WithTimeTick(30).IntoImmutableMessage(walimplstest.NewTestMessageID(30))
	flushOwner := message.NewOwnedImmutableMessage(flush, nil)
	flushDispatch := flushOwner.Clone()
	require.True(t, module.ObserveMessage(context.Background(), flushDispatch))
	flushDispatch.Release()
	flushOwner.Release()
	require.Len(t, scheduler.tasks, 1)
	require.NoError(t, scheduler.tasks[0].Execute(context.Background()))
	snapshots := module.ConsumeDirtySnapshots()
	require.Len(t, snapshots, 1)
	meta := snapshots[0].Payload().(*streamingpb.VChannelMeta)
	require.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, meta.State)
	require.Equal(t, uint64(20), meta.CheckpointTimeTick)
	require.Equal(t, uint64(20), meta.TransformMaterializedTimeTick)
	require.Equal(t, uint64(20), tracker.CompletedPoint().TimeTick)
}
