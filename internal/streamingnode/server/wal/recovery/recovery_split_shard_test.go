package recovery

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
)

// newSplitShardMessage builds one replica of a SplitShard broadcast, landing
// on the given vchannel. Its role is decided from whether vchannel is the
// source or one of the targets, exactly as the production dispatch decides it.
func newSplitShardMessage(vchannel, source string, targets []string, collectionID int64, partitionIDs []int64, timetick uint64) message.ImmutableSplitShardMessageV2 {
	splitTargets := make([]*message.SplitShardTarget, 0, len(targets))
	for i, target := range targets {
		splitTargets = append(splitTargets, &message.SplitShardTarget{
			Vchannel: target,
			Routing:  &schemapb.HashRouting{Buckets: []uint64{uint64(i)}},
		})
	}
	msg := message.NewSplitShardMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    collectionID,
			SplitTaskId:     100,
			PartitionIds:    partitionIDs,
			SourceVchannels: []string{source},
			Targets:         splitTargets,
		}).
		WithBody(&message.SplitShardMessageBody{
			Genesis: &msgpb.CreateCollectionRequest{
				CollectionSchema: &schemapb.CollectionSchema{Name: "col"},
			},
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID()
	return message.MustAsImmutableSplitShardMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(3)))
}

// newRetireMessage builds the AlterCollection replica that retires a vchannel:
// a shard-split routing commit whose new vchannel list omits it.
func newRetireMessage(vchannel string, collectionID int64, kept []string, timetick uint64) message.ImmutableAlterCollectionMessageV2 {
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: collectionID,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionShardSplitRouting}},
		}).
		WithBody(&message.AlterCollectionMessageBody{
			Updates: &message.AlterCollectionMessageUpdates{
				VirtualChannelNames: kept,
			},
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID()
	return message.MustAsImmutableAlterCollectionMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(5)))
}

// TestRecoveryStorageSplitShardOnTargetSeedsTheVChannel: the genesis replica
// landing on a target vchannel is exempt from the vchannel-not-found check
// and seeds a new vchannel meta exactly as create collection does.
func TestRecoveryStorageSplitShardOnTargetSeedsTheVChannel(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	rs.handleMessage(context.Background(), newSplitShardMessage("v2", "v1", []string{"v2", "v3"}, 7, []int64{8}, 100))
	info, ok := rs.vchannels["v2"]
	assert.True(t, ok)
	assert.Equal(t, int64(7), info.meta.CollectionInfo.CollectionId)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, info.meta.State)
	assert.Equal(t, "col", info.meta.CollectionInfo.Schemas[0].Schema.GetName())
	assert.Len(t, info.meta.CollectionInfo.Partitions, 1)
	assert.Equal(t, int64(8), info.meta.CollectionInfo.Partitions[0].PartitionId)
	assert.True(t, info.dirty)

	// re-applying the genesis is idempotent.
	rs.handleSplitShard(context.Background(), newSplitShardMessage("v2", "v1", []string{"v2", "v3"}, 7, []int64{8}, 200))
}

// TestRecoveryStorageSplitShardOnSourceMarksSplitted: the fence replica on
// the source vchannel flips its state to SPLITTED and flushes its segments.
func TestRecoveryStorageSplitShardOnSourceMarksSplitted(t *testing.T) {
	rs := newTestRecoveryStorage(t)
	addActiveVChannel(rs, "v1", 1, []int64{2})
	addGrowingSegment(rs, 1001, 1, 2, "v1")

	rs.handleSplitShard(context.Background(), newSplitShardMessage("v1", "v1", []string{"v1-target1", "v1-target2"}, 1, []int64{2}, 100))

	// the vchannel is fenced by shard split and the state is persisted.
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, rs.vchannels["v1"].meta.State)
	// T_switch is persisted so an already-fenced re-fence can return it after a crash.
	assert.Equal(t, uint64(100), rs.vchannels["v1"].meta.SplitTimeTick)
	// the growing segments are flushed defensively.
	assert.False(t, rs.segments[1001].IsGrowing())

	// a split message whose source is an unknown vchannel takes no effect.
	rs.handleSplitShard(context.Background(), newSplitShardMessage("v999", "v999", []string{"v999-target1"}, 999, nil, 200))
}

// TestRecoveryStorageSplitShardOnUnknownVChannelIsAnInconsistency: a replica
// whose vchannel is neither the source nor one of the targets, and is not
// otherwise registered, is a misroute and must be reported, not silently
// dropped.
func TestRecoveryStorageSplitShardOnUnknownVChannelIsAnInconsistency(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	reasons := make([]string, 0)
	mockDetect := mockey.Mock((*recoveryStorageImpl).detectInconsistency).To(
		func(r *recoveryStorageImpl, ctx context.Context, msg message.ImmutableMessage, reason string, extra ...mlog.Field) {
			reasons = append(reasons, reason)
		}).Build()
	defer mockDetect.UnPatch()

	rs.handleMessage(context.Background(), newSplitShardMessage("v-unrelated", "v1", []string{"v2", "v3"}, 1, nil, 100))
	// Two independent checks each catch the same misroute from a different
	// angle: handleMessage's generic "is this vchannel known at all" check,
	// and handleSplitShard's own "does the header name this vchannel a
	// source or a target" check -- both true, and both worth reporting.
	assert.Equal(t, []string{"vchannel not found", "split shard replica of unknown role"}, reasons)
	_, ok := rs.vchannels["v-unrelated"]
	assert.False(t, ok)
}

// TestRecoveryStorageSplitShardOfUnknownRoleOnARegisteredVChannelIsAnInconsistency:
// a replica landing on a vchannel that IS registered, but whose header lists
// it as neither a source nor a target, skips the generic "vchannel not found"
// check in handleMessage (the vchannel is known) and must instead be caught
// inside handleSplitShard itself -- the asymmetry this closes is that the
// source and target arms both act, but there was no arm, and so no report,
// for every other role a SplitShard replica could land with.
func TestRecoveryStorageSplitShardOfUnknownRoleOnARegisteredVChannelIsAnInconsistency(t *testing.T) {
	rs := newTestRecoveryStorage(t)
	addActiveVChannel(rs, "v9", 1, []int64{2})

	reasons := make([]string, 0)
	mockDetect := mockey.Mock((*recoveryStorageImpl).detectInconsistency).To(
		func(r *recoveryStorageImpl, ctx context.Context, msg message.ImmutableMessage, reason string, extra ...mlog.Field) {
			reasons = append(reasons, reason)
		}).Build()
	defer mockDetect.UnPatch()

	rs.handleMessage(context.Background(), newSplitShardMessage("v9", "v1", []string{"v2", "v3"}, 1, nil, 100))
	assert.Equal(t, []string{"split shard replica of unknown role"}, reasons)
}

// TestRecoveryStorageSplitShardOnBystanderIsIgnored: the broadcast now covers
// every vchannel of the collection, so a replica landing on a vchannel that
// is registered but is neither the source nor one of the targets -- a
// bystander shard of the same collection -- must be ignored: no state change
// on the bystander's own vchannel meta, and (unlike a genuine misroute) no
// inconsistency reported.
func TestRecoveryStorageSplitShardOnBystanderIsIgnored(t *testing.T) {
	rs := newTestRecoveryStorage(t)
	addActiveVChannel(rs, "p0_1v3", 1, []int64{2})

	reasons := make([]string, 0)
	mockDetect := mockey.Mock((*recoveryStorageImpl).detectInconsistency).To(
		func(r *recoveryStorageImpl, ctx context.Context, msg message.ImmutableMessage, reason string, extra ...mlog.Field) {
			reasons = append(reasons, reason)
		}).Build()
	defer mockDetect.UnPatch()

	// "p0_1v3" is collection 1's third shard: neither "p0_1v0" (the source)
	// nor "p0_1v1"/"p0_1v2" (the targets).
	rs.handleMessage(context.Background(), newSplitShardMessage("p0_1v3", "p0_1v0", []string{"p0_1v1", "p0_1v2"}, 1, []int64{2}, 100))
	assert.Empty(t, reasons)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, rs.vchannels["p0_1v3"].meta.State)
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
	info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 10))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, info.meta.State)
	assert.False(t, info.dirty)

	// the split message fences the vchannel.
	info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 100))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, info.meta.State)
	assert.Equal(t, uint64(100), info.meta.CheckpointTimeTick)
	assert.True(t, info.dirty)
	// the splitted vchannel is still active: it serves replay until dropped.
	assert.True(t, info.IsActive())

	// idempotent: a second split message takes no effect.
	info.dirty = false
	info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 200))
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
	dropped.ObserveSplitShard(newSplitShardMessage("v2", "v2", []string{"v2-target1"}, 2, nil, 100))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, dropped.meta.State)
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
	rs.handleSplitShard(context.Background(), newSplitShardMessage("v0", "v0", []string{"v0-target1"}, 1, nil, 100))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, rs.vchannels["v0"].meta.State)
	assert.False(t, rs.segments[1001].IsGrowing())
	assert.True(t, rs.segments[1002].IsGrowing(), "the sibling shard is still live and must not be sealed")
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, rs.vchannels["v1"].meta.State)

	// Retiring v0 must likewise leave v1 alone.
	rs.handleMessage(context.Background(), newRetireMessage("v0", 1, []string{"v1"}, 200))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, rs.vchannels["v0"].meta.State)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, rs.vchannels["v1"].meta.State)
	assert.True(t, rs.segments[1002].IsGrowing())
}

// TestVChannelRecoveryInfoObserveDropVChannel exercises ObserveDropVChannel
// directly: it now takes the plain timetick that retires a vchannel (the
// AlterCollection replica no longer owns this call, handleAlterCollection's
// state guard does), so the checkpoint and idempotency guards need their own
// coverage independent of that caller.
func TestVChannelRecoveryInfoObserveDropVChannel(t *testing.T) {
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

	// a timetick older than the checkpoint is ignored.
	info.ObserveDropVChannel(10)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, info.meta.State)
	assert.False(t, info.dirty)

	// the retire moves the vchannel to DROPPED.
	info.ObserveDropVChannel(100)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, info.meta.State)
	assert.Equal(t, uint64(100), info.meta.CheckpointTimeTick)
	assert.True(t, info.dirty)

	// idempotent: a second retire takes no effect.
	info.dirty = false
	info.ObserveDropVChannel(200)
	assert.Equal(t, uint64(100), info.meta.CheckpointTimeTick)
	assert.False(t, info.dirty)
}

// TestRecoveryStorageAlterCollectionRetiresTheDelistedVChannel: a shard-split
// routing commit that delists this vchannel retires it, exactly as
// DropVChannel once did.
func TestRecoveryStorageAlterCollectionRetiresTheDelistedVChannel(t *testing.T) {
	rs := newTestRecoveryStorage(t)
	addActiveVChannel(rs, "v0", 1, []int64{2})
	addGrowingSegment(rs, 1001, 1, 2, "v0")

	rs.handleMessage(context.Background(), newRetireMessage("v0", 1, []string{"v1"}, 100))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, rs.vchannels["v0"].meta.State)
	// Flushed unconditionally: a replay can recreate GROWING segments after the
	// vchannel was marked dropped, so the teardown must not assume there is
	// nothing left.
	assert.False(t, rs.segments[1001].IsGrowing())

	// Replaying the teardown is harmless, and one for a vchannel that is gone
	// entirely must not panic either.
	rs.handleMessage(context.Background(), newRetireMessage("v0", 1, []string{"v1"}, 200))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, rs.vchannels["v0"].meta.State)
	assert.NotPanics(t, func() {
		rs.handleMessage(context.Background(), newRetireMessage("v-gone", 1, []string{"v1"}, 300))
	})
}

// TestRecoveryStorageRetireReplayIsNotAnInconsistency: once a retired
// vchannel's meta is garbage-collected, a WAL replay from an older checkpoint
// meets its retiring AlterCollection again. That is a replay, not a broken
// invariant, and must not be reported as one -- exactly as DropCollection is
// not.
func TestRecoveryStorageRetireReplayIsNotAnInconsistency(t *testing.T) {
	rs := newTestRecoveryStorage(t)

	reasons := make([]string, 0)
	mockDetect := mockey.Mock((*recoveryStorageImpl).detectInconsistency).To(
		func(r *recoveryStorageImpl, ctx context.Context, msg message.ImmutableMessage, reason string, extra ...mlog.Field) {
			reasons = append(reasons, reason)
		}).Build()
	defer mockDetect.UnPatch()

	rs.handleMessage(context.Background(), newRetireMessage("v-already-gc-ed", 1, []string{"v1"}, 100))
	assert.Empty(t, reasons)

	// A message type that genuinely requires the vchannel still reports it, so
	// the exemption is not a blanket one.
	addGrowingSegment(rs, 2001, 1, 2, "v-already-gc-ed")
	rs.handleMessage(context.Background(), newSplitShardMessage("v-already-gc-ed", "v-already-gc-ed", []string{"v-target1"}, 1, nil, 100))
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
	info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 4242))

	snapshot, shouldBeRemoved := info.ConsumeDirtyAndGetSnapshot()
	assert.False(t, shouldBeRemoved)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, snapshot.State)
	assert.Equal(t, uint64(4242), snapshot.SplitTimeTick,
		"T_switch must be persisted; a re-fence after restart reads it back from here")
}
