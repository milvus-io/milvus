package vchannel

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/messageack"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/moduleapi"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/utility"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/walimplstest"
)

const (
	splitSourceVChannel = "v1"
	splitTargetVChannel = "v1-target1"
)

// newSplitShardMessage builds one replica of a SplitShard broadcast on
// `vchannel`. switchTimeTick > 0 stamps the source's extra append response on
// the record, which is how a re-driven first fence carries the tick of the
// attempt that actually fenced the shard.
func newSplitShardMessage(vchannel string, timetick uint64, switchTimeTick uint64) message.ImmutableMessage {
	mutable := message.NewSplitShardMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    1,
			SplitTaskId:     7,
			SourceVchannel:  splitSourceVChannel,
			TargetVchannels: []string{splitTargetVChannel, "v1-target2"},
			PartitionIds:    []int64{10},
		}).
		WithBody(&message.SplitShardMessageBody{
			Genesis: &message.CreateCollectionRequest{
				CollectionSchema: &schemapb.CollectionSchema{Name: "collection"},
			},
		}).
		MustBuildMutable()
	if switchTimeTick != 0 {
		extra, err := anypb.New(&message.SplitShardExtraResponse{SplitTimeTick: switchTimeTick})
		if err != nil {
			panic(err)
		}
		message.SetAppendExtra(mutable, extra)
	}
	return mutable.
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick) - 1)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick)))
}

// splitShardMessageOf is newSplitShardMessage already narrowed to the
// specialized accessor the view's observers take.
func splitShardMessageOf(vchannel string, timetick uint64, switchTimeTick uint64) message.ImmutableSplitShardMessageV2 {
	return message.MustAsImmutableSplitShardMessageV2(newSplitShardMessage(vchannel, timetick, switchTimeTick))
}

// newRoutingCommit builds the AlterCollection routing commit of a split, whose
// post-image names `vchannels`. A commit that does not name the vchannel it is
// observed on retires it.
func newRoutingCommit(vchannel string, timetick uint64, vchannels []string) message.ImmutableMessage {
	raw := message.NewAlterCollectionMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: 1,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{message.FieldMaskCollectionShardSplitRouting},
			},
		}).
		WithBody(&message.AlterCollectionMessageBody{
			Updates: &message.AlterCollectionMessageUpdates{
				VirtualChannelNames: vchannels,
				SplitTaskId:         7,
			},
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmed(walimplstest.NewTestMessageID(int64(timetick) - 1)).
		IntoImmutableMessage(walimplstest.NewTestMessageID(int64(timetick)))
	return raw
}

// routingCommitOf is newRoutingCommit already narrowed to the specialized
// accessor the view's observers take.
func routingCommitOf(vchannel string, timetick uint64, vchannels []string) message.ImmutableAlterCollectionMessageV2 {
	return message.MustAsImmutableAlterCollectionMessageV2(newRoutingCommit(vchannel, timetick, vchannels))
}

func newSplitTestView() *VChannelView {
	return NewVChannelViewFromMeta(&streamingpb.VChannelMeta{
		Vchannel:           splitSourceVChannel,
		State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CheckpointTimeTick: 10,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: 1,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 10, State: streamingpb.PartitionState_PARTITION_STATE_NORMAL},
			},
			Schemas: []*streamingpb.CollectionSchemaOfVChannel{
				{
					Schema:             &schemapb.CollectionSchema{Name: "collection"},
					State:              streamingpb.VChannelSchemaState_VCHANNEL_SCHEMA_STATE_NORMAL,
					CheckpointTimeTick: 1,
				},
			},
		},
	})
}

// TestSourceFenceIsRecordedAndSurvivesTheSnapshot: the fence is a field of the
// source's meta, not a state, so the write-path snapshot still reports the
// source as a live vchannel -- carrying the fence, which is what rebuilds the
// DoAppend gate after a restart.
func TestSourceFenceIsRecordedAndSurvivesTheSnapshot(t *testing.T) {
	view := newSplitTestView()
	require.True(t, view.ObserveSplitShardSourceMessageV2(splitShardMessageOf(splitSourceVChannel, 20, 0)))

	timeTick, taskID := view.SplitFence()
	assert.Equal(t, uint64(20), timeTick)
	assert.Equal(t, int64(7), taskID)
	// The vchannel stays NORMAL: it still has to observe the collection's DDL
	// and drain its own data until adoption retires it.
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, view.AssignmentMeta().GetState())

	state, ok := view.WritePathRecoveryState()
	require.True(t, ok, "a fenced source is still reported by the write-path snapshot")
	assert.Equal(t, uint64(20), state.SplitFenceTimeTick)
	assert.Equal(t, int64(7), state.SplitFenceTaskID)
	assert.Equal(t, []int64{10}, state.PartitionIDs)

	snapshot, _ := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot, "the fence must be persisted")
	assert.Equal(t, uint64(20), snapshot.GetSplitFenceTimeTick())
	assert.Equal(t, int64(7), snapshot.GetSplitFenceTaskId())
	assert.Equal(t, uint64(20), snapshot.GetCheckpointTimeTick())

	// A restart rebuilds the same gate out of the persisted meta.
	recovered := NewVChannelViewFromMeta(snapshot)
	recoveredTimeTick, recoveredTaskID := recovered.SplitFence()
	assert.Equal(t, uint64(20), recoveredTimeTick)
	assert.Equal(t, int64(7), recoveredTaskID)
}

// TestSourceFenceTakesTheStampedSwitchTimeTick: the fence the shard manager
// installed before the append is T_switch. When that first append failed, the
// record that reaches the WAL is a re-drive at a later tick that reports the
// first attempt's tick; every consumer must read T_switch from the stamp.
func TestSourceFenceTakesTheStampedSwitchTimeTick(t *testing.T) {
	view := newSplitTestView()
	require.True(t, view.ObserveSplitShardSourceMessageV2(splitShardMessageOf(splitSourceVChannel, 40, 20)))

	timeTick, _ := view.SplitFence()
	assert.Equal(t, uint64(20), timeTick, "T_switch is the stamped tick, not the record's own")
	assert.Equal(t, uint64(40), view.AssignmentMeta().GetCheckpointTimeTick(), "the checkpoint is the record's own tick")
}

// TestSourceReFenceKeepsTheFirstSwitchTimeTick: the broadcaster re-drives a
// split whose source replica was not acknowledged. Moving T_switch forward
// would invalidate what DataCoord already recorded against the first fence.
func TestSourceReFenceKeepsTheFirstSwitchTimeTick(t *testing.T) {
	view := newSplitTestView()
	require.True(t, view.ObserveSplitShardSourceMessageV2(splitShardMessageOf(splitSourceVChannel, 20, 0)))
	assert.False(t, view.ObserveSplitShardSourceMessageV2(splitShardMessageOf(splitSourceVChannel, 30, 0)),
		"a re-fence records nothing new")

	timeTick, _ := view.SplitFence()
	assert.Equal(t, uint64(20), timeTick)
}

// TestTargetGenesisSeedsTheVChannelMeta: the TARGET replica is the genesis of
// a brand-new vchannel and seeds its meta exactly as CreateCollection does.
func TestTargetGenesisSeedsTheVChannelMeta(t *testing.T) {
	meta := NewVChannelMetaFromSplitShardTargetMessage(splitShardMessageOf(splitTargetVChannel, 20, 0))
	assert.Equal(t, splitTargetVChannel, meta.GetVchannel())
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, meta.GetState())
	assert.Equal(t, int64(1), meta.GetCollectionInfo().GetCollectionId())
	require.Len(t, meta.GetCollectionInfo().GetPartitions(), 1)
	assert.Equal(t, int64(10), meta.GetCollectionInfo().GetPartitions()[0].GetPartitionId())
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_NORMAL, meta.GetCollectionInfo().GetPartitions()[0].GetState())
	require.Len(t, meta.GetCollectionInfo().GetSchemas(), 1)
	assert.Equal(t, "collection", meta.GetCollectionInfo().GetSchemas()[0].GetSchema().GetName())
	assert.Equal(t, uint64(20), meta.GetCheckpointTimeTick())
	// No fence: the target is not a source of anything.
	assert.Zero(t, meta.GetSplitFenceTimeTick())
}

// TestManagerCreatesAModuleForATargetGenesis: a shard split's targets are new
// vchannels, so the pchannel manager has to build a module for the TARGET
// replica just as it does for CreateCollection -- and must NOT build one for
// the SOURCE replica of a vchannel it does not hold.
func TestManagerCreatesAModuleForATargetGenesis(t *testing.T) {
	manager, err := NewPChannelRecoveryManager(PChannelManagerConfig{PChannel: "p1"})
	require.NoError(t, err)
	t.Cleanup(manager.Close)

	observe := func(msg message.ImmutableMessage) {
		tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
		owner := tracker.Track(msg)
		dispatch := owner.Clone()
		manager.ObserveMessage(context.Background(), dispatch)
		dispatch.Release()
		owner.Release()
	}

	observe(newSplitShardMessage(splitTargetVChannel, 20, 0))
	module := manager.Module(splitTargetVChannel)
	require.NotNil(t, module, "the target replica is a genesis")
	assert.True(t, module.IsActive())

	observe(newSplitShardMessage(splitSourceVChannel, 20, 0))
	assert.Nil(t, manager.Module(splitSourceVChannel), "the source replica is a fence, not a genesis")
}

// TestRetiringRoutingCommitDropsTheFencedSource: the routing commit that
// delists a fenced source is its drop. Upstream's pending-drop machinery then
// tombstones it once its own L0 and L1 dependencies have finished -- nothing
// of the split's waits for it.
func TestRetiringRoutingCommitDropsTheFencedSource(t *testing.T) {
	scheduler := &recordingVChannelScheduler{}
	module, err := NewModule(ModuleConfig{
		PChannel: "p1", VChannel: splitSourceVChannel,
		Runtime: moduleapi.Runtime{Scheduler: scheduler},
		VChannelMeta: &streamingpb.VChannelMeta{
			Vchannel: splitSourceVChannel, State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 10,
			CollectionInfo:     &streamingpb.CollectionInfoOfVChannel{CollectionId: 1},
		},
	})
	require.NoError(t, err)

	observe := func(msg message.ImmutableMessage) {
		tracker := messageack.NewTracker(utility.WALCheckpoint{}, nil, nil)
		owner := tracker.Track(msg)
		dispatch := owner.Clone()
		require.True(t, module.ObserveMessage(context.Background(), dispatch))
		dispatch.Release()
		owner.Release()
	}

	observe(newSplitShardMessage(splitSourceVChannel, 20, 0))
	timeTick, _ := module.vchannelView.SplitFence()
	require.Equal(t, uint64(20), timeTick)
	require.True(t, module.IsActive(), "a fenced source is still live metadata")
	// The fence itself is ordinary dirty metadata; persist it so the drop that
	// follows is the only pending change.
	fenceSnapshots := module.ConsumeDirtySnapshots()
	require.Len(t, fenceSnapshots, 1)
	require.Equal(t, uint64(20),
		fenceSnapshots[0].Payload().(*streamingpb.VChannelMeta).GetSplitFenceTimeTick())
	fenceSnapshots[0].MarkPersisted()
	require.Empty(t, module.ConsumeDirtySnapshots())

	// The adoption commit grows the collection's vchannel list with the targets
	// and delists the spent source in the same post-image.
	observe(newRoutingCommit(splitSourceVChannel, 30,
		[]string{splitTargetVChannel, "v1-target2"}))
	require.False(t, module.IsActive(), "the delisting commit closes the source")
	_, ok := module.vchannelView.WritePathRecoveryState()
	require.False(t, ok, "a closing vchannel is withheld from the write path")
	// The drop is only published once the vchannel's own dependencies finish:
	// the L0 materialization frontier has to pass the drop tick. Two boundaries
	// scheduled L0 work here, the fence and the drop itself.
	require.Empty(t, module.ConsumeDirtySnapshots())
	require.NotEmpty(t, scheduler.tasks)
	// The fence is its own L0 boundary, so the materializer batches up to it
	// first and the drop's batch is only scheduled when that one completes.
	for i := 0; i < len(scheduler.tasks); i++ {
		require.NoError(t, scheduler.tasks[i].Execute(context.Background()))
	}

	snapshots := module.ConsumeDirtySnapshots()
	require.Len(t, snapshots, 1)
	meta := snapshots[0].Payload().(*streamingpb.VChannelMeta)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_TOMBSTONED, meta.GetState())
	assert.Equal(t, uint64(30), meta.GetCheckpointTimeTick())
}

// TestRoutingCommitThatKeepsTheVChannelIsNotARetire: the same message type
// commits routing on every shard of the collection. Only the vchannel the
// post-image omits is retired; one it still names takes the ordinary apply,
// and a vchannel that was never fenced is never dropped by it.
func TestRoutingCommitThatKeepsTheVChannelIsNotARetire(t *testing.T) {
	view := newSplitTestView()
	require.True(t, view.ObserveSplitShardSourceMessageV2(splitShardMessageOf(splitSourceVChannel, 20, 0)))
	assert.False(t, view.ObserveRetireVChannel(routingCommitOf(splitSourceVChannel, 30,
		[]string{splitSourceVChannel, splitTargetVChannel})), "the post-image still names it")
	assert.True(t, view.IsActive())

	// An unfenced vchannel is not retirable: a commit that merely fails to
	// mention it must not drop a live shard.
	unfenced := newSplitTestView()
	assert.False(t, unfenced.ObserveRetireVChannel(routingCommitOf(splitSourceVChannel, 30,
		[]string{splitTargetVChannel})))
	assert.True(t, unfenced.IsActive())
}

// TestRetireIsIdempotent: the commit can be redelivered, and a second retire
// must not queue a second drop of the same vchannel.
func TestRetireIsIdempotent(t *testing.T) {
	view := newSplitTestView()
	require.True(t, view.ObserveSplitShardSourceMessageV2(splitShardMessageOf(splitSourceVChannel, 20, 0)))
	require.True(t, view.ObserveRetireVChannel(routingCommitOf(splitSourceVChannel, 30, []string{splitTargetVChannel})))
	assert.False(t, view.ObserveRetireVChannel(routingCommitOf(splitSourceVChannel, 30, []string{splitTargetVChannel})))
	assert.False(t, view.ObserveRetireVChannel(routingCommitOf(splitSourceVChannel, 40, []string{splitTargetVChannel})))
}

// TestFenceIsPersistedAcrossAPendingPartitionDrop: a partition drop does not
// close the vchannel, so a fence can land while one is still pending -- and
// while it is, the stable snapshot published for persistence is the pre-image
// taken before the fence. The fence has to be written into it, or the recovery
// checkpoint passes the fence record while no persisted meta carries the fence
// and a crash recovers a writable source.
func TestFenceIsPersistedAcrossAPendingPartitionDrop(t *testing.T) {
	view := newSplitTestView()
	drop := message.MustAsImmutableDropPartitionMessageV1(
		message.NewDropPartitionMessageBuilderV1().WithVChannel(splitSourceVChannel).
			WithHeader(&message.DropPartitionMessageHeader{CollectionId: 1, PartitionId: 10}).
			WithBody(&message.DropPartitionRequest{}).
			MustBuildMutable().WithTimeTick(15).
			IntoImmutableMessage(walimplstest.NewTestMessageID(15)))
	require.True(t, view.ObserveDropPartitionMessageV1(drop))
	require.True(t, view.ObserveSplitShardSourceMessageV2(splitShardMessageOf(splitSourceVChannel, 20, 0)))

	snapshot, _ := view.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot, "the fence must dirty the stable pre-image")
	assert.Equal(t, uint64(20), snapshot.GetSplitFenceTimeTick())
	assert.Equal(t, int64(7), snapshot.GetSplitFenceTaskId())
	// Still the pre-image in every other respect: the partition drop has not
	// completed, so its tombstone is not published and the checkpoint has not
	// moved past it.
	assert.Equal(t, uint64(10), snapshot.GetCheckpointTimeTick())
	require.Len(t, snapshot.GetCollectionInfo().GetPartitions(), 1)
	assert.Equal(t, streamingpb.PartitionState_PARTITION_STATE_NORMAL,
		snapshot.GetCollectionInfo().GetPartitions()[0].GetState())

	// A restart on that snapshot rebuilds the fence, which is the whole point.
	recovered := NewVChannelViewFromMeta(snapshot)
	timeTick, taskID := recovered.SplitFence()
	assert.Equal(t, uint64(20), timeTick)
	assert.Equal(t, int64(7), taskID)
}
