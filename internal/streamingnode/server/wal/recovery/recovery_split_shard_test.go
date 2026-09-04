package recovery

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

func newSplitShardMessage(vchannel string, collectionID int64, timetick uint64) message.ImmutableSplitShardMessageV2 {
	msg := message.NewSplitShardMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId: collectionID,
			SplitTaskId:  100,
			Targets: []*message.SplitShardTarget{
				{Vchannel: vchannel + "-target1", Routing: &schemapb.HashRouting{Buckets: []uint64{0}}},
				{Vchannel: vchannel + "-target2", Routing: &schemapb.HashRouting{Buckets: []uint64{1}}},
			},
		}).
		WithBody(&message.SplitShardMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID()
	return message.MustAsImmutableSplitShardMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(3)))
}

func newCreateVChannelMessage(vchannel string, collectionID int64, partitionIDs []int64, timetick uint64) message.ImmutableCreateVChannelMessageV2 {
	msg := message.NewCreateVChannelMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CreateVChannelMessageHeader{
			CollectionId:         collectionID,
			PartitionIds:         partitionIDs,
			SplitTaskId:          100,
			SplitSourceVchannels: []string{"v1"},
			Routing:              &schemapb.HashRouting{Buckets: []uint64{0}},
			RoutingModulus:       2,
		}).
		WithBody(&message.CreateCollectionRequest{
			CollectionSchema: &schemapb.CollectionSchema{Name: "col"},
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID()
	return message.MustAsImmutableCreateVChannelMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(4)))
}

func TestRecoveryStorageHandleCreateVChannel(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	// the genesis of a target vchannel is exempt from the vchannel-not-found
	// check and seeds a new vchannel meta exactly as create collection does.
	rs.handleMessage(context.Background(), newCreateVChannelMessage("v2", 7, []int64{8}, 100))
	info, ok := rs.vchannels["v2"]
	assert.True(t, ok)
	assert.Equal(t, int64(7), info.meta.CollectionInfo.CollectionId)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, info.meta.State)
	assert.Equal(t, "col", info.meta.CollectionInfo.Schemas[0].Schema.GetName())
	assert.True(t, info.dirty)

	// re-applying the genesis is idempotent.
	rs.handleCreateVChannel(newCreateVChannelMessage("v2", 7, []int64{8}, 200))
}

func TestRecoveryStorageHandleSplitShard(t *testing.T) {
	rs := newTestRecoveryStorage(t)
	addActiveVChannel(rs, "v1", 1, []int64{2})
	addGrowingSegment(rs, 1001, 1, 2, "v1")

	rs.handleSplitShard(newSplitShardMessage("v1", 1, 100))

	// the vchannel is fenced by shard split and the state is persisted.
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, rs.vchannels["v1"].meta.State)
	// T_switch is persisted so an already-fenced re-fence can return it after a crash.
	assert.Equal(t, uint64(100), rs.vchannels["v1"].meta.SplitTimeTick)
	// the growing segments are flushed defensively.
	assert.False(t, rs.segments[1001].IsGrowing())

	// a split message on an unknown vchannel takes no effect.
	rs.handleSplitShard(newSplitShardMessage("v999", 999, 200))
}

func TestVChannelRecoveryInfoObserveSplitShard(t *testing.T) {
	info := &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel:           "v1",
			State:              streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CheckpointTimeTick: 50,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}

	// a message older than the checkpoint is ignored.
	info.ObserveSplitShard(newSplitShardMessage("v1", 1, 10))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, info.meta.State)
	assert.False(t, info.dirty)

	// the split message fences the vchannel.
	info.ObserveSplitShard(newSplitShardMessage("v1", 1, 100))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, info.meta.State)
	assert.Equal(t, uint64(100), info.meta.CheckpointTimeTick)
	assert.True(t, info.dirty)
	// the splitted vchannel is still active: it serves replay until dropped.
	assert.True(t, info.IsActive())

	// idempotent: a second split message takes no effect.
	info.dirty = false
	info.ObserveSplitShard(newSplitShardMessage("v1", 1, 200))
	assert.Equal(t, uint64(100), info.meta.CheckpointTimeTick)
	assert.False(t, info.dirty)

	// the SPLITTED state is persisted in the snapshot and the vchannel
	// meta must not be removed from the catalog (the fence must survive
	// restarts until the vchannel is really dropped).
	info.dirty = true
	snapshot, shouldBeRemoved := info.ConsumeDirtyAndGetSnapshot()
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, snapshot.State)
	assert.False(t, shouldBeRemoved)

	// a dropped vchannel never goes back to splitted.
	dropped := &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel: "v2",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 2,
			},
		},
	}
	dropped.ObserveSplitShard(newSplitShardMessage("v2", 2, 100))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, dropped.meta.State)
}

func newDropVChannelMessage(vchannel string, collectionID int64, timetick uint64) message.ImmutableDropVChannelMessageV2 {
	msg := message.NewDropVChannelMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.DropVChannelMessageHeader{
			CollectionId: collectionID,
			SplitTaskId:  100,
		}).
		WithBody(&message.DropVChannelMessageBody{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID()
	return message.MustAsImmutableDropVChannelMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(5)))
}

// TestRecoveryStorageSplitAndDropAreScopedToOneVChannel pins the scope of both
// teardown paths.
//
// A recovery storage covers one PCHANNEL, and a collection's other shards can
// live on it too. Scoping either handler by collection id would seal segments
// belonging to shards that are still taking writes and that no message asked to
// seal -- a silent, collection-wide flush triggered by one shard's split.
func TestRecoveryStorageSplitAndDropAreScopedToOneVChannel(t *testing.T) {
	rs := newTestRecoveryStorage(t)
	addActiveVChannel(rs, "v0", 1, []int64{2})
	addActiveVChannel(rs, "v1", 1, []int64{2})
	addGrowingSegment(rs, 1001, 1, 2, "v0")
	addGrowingSegment(rs, 1002, 1, 2, "v1")

	// Fencing v0 must leave v1's growing segment alone.
	rs.handleSplitShard(newSplitShardMessage("v0", 1, 100))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, rs.vchannels["v0"].meta.State)
	assert.False(t, rs.segments[1001].IsGrowing())
	assert.True(t, rs.segments[1002].IsGrowing(), "the sibling shard is still live and must not be sealed")
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, rs.vchannels["v1"].meta.State)

	// Retiring v0 must likewise leave v1 alone.
	rs.handleDropVChannel(context.Background(), newDropVChannelMessage("v0", 1, 200))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, rs.vchannels["v0"].meta.State)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, rs.vchannels["v1"].meta.State)
	assert.True(t, rs.segments[1002].IsGrowing())
}

func TestRecoveryStorageHandleDropVChannel(t *testing.T) {
	rs := newTestRecoveryStorage(t)
	addActiveVChannel(rs, "v0", 1, []int64{2})
	addGrowingSegment(rs, 1001, 1, 2, "v0")

	rs.handleDropVChannel(context.Background(), newDropVChannelMessage("v0", 1, 100))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, rs.vchannels["v0"].meta.State)
	// Flushed unconditionally: a replay can recreate GROWING segments after the
	// vchannel was marked dropped, so the teardown must not assume there is
	// nothing left.
	assert.False(t, rs.segments[1001].IsGrowing())

	// Replaying the teardown is harmless, and one for a vchannel that is gone
	// entirely must not panic either.
	rs.handleDropVChannel(context.Background(), newDropVChannelMessage("v0", 1, 200))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, rs.vchannels["v0"].meta.State)
	assert.NotPanics(t, func() {
		rs.handleDropVChannel(context.Background(), newDropVChannelMessage("v-gone", 1, 300))
	})
}

// TestRecoveryStorageDropVChannelReplayIsNotAnInconsistency: once a retired
// vchannel's meta is garbage-collected, a WAL replay from an older checkpoint
// meets its DropVChannel again. That is a replay, not a broken invariant, and
// must not be reported as one -- exactly as DropCollection is not.
func TestRecoveryStorageDropVChannelReplayIsNotAnInconsistency(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	reasons := make([]string, 0)
	mockDetect := mockey.Mock((*recoveryStorageImpl).detectInconsistency).To(
		func(r *recoveryStorageImpl, ctx context.Context, msg message.ImmutableMessage, reason string, extra ...mlog.Field) {
			reasons = append(reasons, reason)
		}).Build()
	defer mockDetect.UnPatch()

	rs.handleMessage(context.Background(), newDropVChannelMessage("v-already-gc-ed", 1, 100))
	assert.Empty(t, reasons)

	// A message type that genuinely requires the vchannel still reports it, so
	// the exemption is not a blanket one.
	addGrowingSegment(rs, 2001, 1, 2, "v-already-gc-ed")
	rs.handleMessage(context.Background(), newSplitShardMessage("v-already-gc-ed", 1, 100))
	assert.Equal(t, []string{"vchannel not found"}, reasons)
}

// TestVChannelRecoveryInfoSplitTimeTickIsPersisted: T_switch has to reach the
// catalog, because the whole point of recording it is to answer a re-fence
// after the streamingnode that recorded it has restarted.
func TestVChannelRecoveryInfoSplitTimeTickIsPersisted(t *testing.T) {
	info := &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}
	info.ObserveSplitShard(newSplitShardMessage("v1", 1, 4242))

	snapshot, shouldBeRemoved := info.ConsumeDirtyAndGetSnapshot()
	assert.False(t, shouldBeRemoved)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, snapshot.State)
	assert.Equal(t, uint64(4242), snapshot.SplitTimeTick,
		"T_switch must be persisted; a re-fence after restart reads it back from here")
}
