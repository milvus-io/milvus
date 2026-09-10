package recovery

import (
	"context"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/metastore"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/mocks/streaming/mock_walimpls"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
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

	// a re-fence on an already-SPLITTED vchannel raises T_switch to the
	// newer tick instead of taking no effect.
	info.dirty = false
	info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 200))
	assert.Equal(t, uint64(200), info.meta.CheckpointTimeTick)
	assert.Equal(t, uint64(200), info.meta.SplitTimeTick)
	assert.True(t, info.dirty)

	// the SPLITTED state is persisted in the snapshot and the vchannel
	// meta must not be removed from the catalog (the fence must survive
	// restarts until the vchannel is really dropped).
	snapshot, shouldBeRemoved := info.ConsumeDirtyAndGetSnapshot(0)
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

	// Retiring v0 must likewise leave v1 alone. v0 stays SPLITTED -- the
	// source must never go DROPPED -- and is only marked Retired.
	rs.handleMessage(context.Background(), newRetireMessage("v0", 1, []string{"v1"}, 200))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, rs.vchannels["v0"].meta.State)
	assert.True(t, rs.vchannels["v0"].meta.Retired)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, rs.vchannels["v1"].meta.State)
	assert.True(t, rs.segments[1002].IsGrowing())
}

// TestVChannelRecoveryInfoRetireMarksASplittedVChannel exercises ObserveRetire
// directly, which replaces the old ObserveDropVChannel: a retire only ever
// touches an already-SPLITTED (fenced) vchannel, marking it Retired without
// ever moving it to DROPPED.
func TestVChannelRecoveryInfoRetireMarksASplittedVChannel(t *testing.T) {
	// a vchannel that has not been fenced yet (NORMAL) is untouched -- in the
	// real flow a retire always follows a fence, but a stray one must still
	// be a safe no-op.
	normal := &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel: "v-normal",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}
	normal.ObserveRetire(100)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, normal.meta.State)
	assert.False(t, normal.meta.Retired)
	assert.False(t, normal.dirty)

	// a SPLITTED (fenced) vchannel is the only one a retire actually acts on:
	// Retired flips true and the meta is marked dirty, but the state stays
	// SPLITTED -- the source must never go DROPPED.
	splitted := &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel:      "v-splitted",
			State:         streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED,
			SplitTimeTick: 2000,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}
	splitted.ObserveRetire(2500)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, splitted.meta.State)
	assert.True(t, splitted.meta.Retired)
	assert.True(t, splitted.dirty)

	// idempotent: a replayed retire takes no further effect.
	splitted.dirty = false
	splitted.ObserveRetire(3000)
	assert.True(t, splitted.meta.Retired)
	assert.False(t, splitted.dirty)

	// a vchannel that is already gone (DROPPED) is untouched by a stray retire.
	dropped := &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel: "v-dropped",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}
	dropped.ObserveRetire(100)
	assert.False(t, dropped.meta.Retired)
	assert.False(t, dropped.dirty)
}

// TestObserveDropCollectionClearsRetired: a genuine DropCollection reaching a
// vchannel that a shard split had already retired must still be a real drop
// -- clearing Retired is what keeps dropAllVirtualChannel from mistaking the
// resulting DROPPED meta for a split source being locally collected, and
// wrongly skipping the DataCoord notification a real drop needs.
func TestObserveDropCollectionClearsRetired(t *testing.T) {
	info := &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel:      "v1",
			State:         streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED,
			Retired:       true,
			SplitTimeTick: 100,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}
	info.ObserveDropCollection(buildDropCollectionMsg("v1", 1, 200, 200))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, info.meta.State)
	assert.False(t, info.meta.Retired)
}

// TestObserveSplitShardRaisesTheFenceTick: the shard manager's own T_switch
// bookkeeping treats the latest fence record of the task as authoritative, so
// a re-fence on an already-SPLITTED vchannel must raise SplitTimeTick to a
// larger tick, and a stale (older) re-fence must never move it backwards.
func TestObserveSplitShardRaisesTheFenceTick(t *testing.T) {
	info := &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}

	// the first fence keeps today's behaviour: it sets T_switch outright.
	info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 2000))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, info.meta.State)
	assert.Equal(t, uint64(2000), info.meta.SplitTimeTick)

	// a later re-fence raises T_switch to the newer tick.
	info.dirty = false
	info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 3000))
	assert.Equal(t, uint64(3000), info.meta.SplitTimeTick)
	assert.True(t, info.dirty)

	// a stale re-fence (an older tick than the one already recorded) leaves
	// T_switch exactly where the newer fence put it.
	info.dirty = false
	info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 2500))
	assert.Equal(t, uint64(3000), info.meta.SplitTimeTick)
	assert.False(t, info.dirty)
}

// TestRecoveryStorageAlterCollectionRetiresTheDelistedVChannel: a shard-split
// routing commit that delists this vchannel retires it -- once the vchannel
// is already fenced (SPLITTED) by the earlier SplitShard broadcast. The
// source is never moved to DROPPED: it stays SPLITTED, and the routing
// commit only sets the Retired flag that later lets it be collected once the
// flusher has drained past the fence.
func TestRecoveryStorageAlterCollectionRetiresTheDelistedVChannel(t *testing.T) {
	rs := newTestRecoveryStorage(t)
	addActiveVChannel(rs, "v0", 1, []int64{2})
	addGrowingSegment(rs, 1001, 1, 2, "v0")

	// The retire routing commit always follows the fence in the real flow;
	// the fence itself already flushed the growing segments.
	rs.handleSplitShard(context.Background(), newSplitShardMessage("v0", "v0", []string{"v0-target1"}, 1, nil, 50))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, rs.vchannels["v0"].meta.State)
	assert.False(t, rs.segments[1001].IsGrowing())

	rs.handleMessage(context.Background(), newRetireMessage("v0", 1, []string{"v1"}, 100))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, rs.vchannels["v0"].meta.State,
		"retiring must never move the source to DROPPED")
	assert.True(t, rs.vchannels["v0"].meta.Retired)

	// Replaying the teardown is harmless, and one for a vchannel that is gone
	// entirely must not panic either.
	rs.handleMessage(context.Background(), newRetireMessage("v0", 1, []string{"v1"}, 200))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, rs.vchannels["v0"].meta.State)
	assert.True(t, rs.vchannels["v0"].meta.Retired)
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

	snapshot, shouldBeRemoved := info.ConsumeDirtyAndGetSnapshot(0)
	assert.False(t, shouldBeRemoved)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, snapshot.State)
	assert.Equal(t, uint64(4242), snapshot.SplitTimeTick,
		"T_switch must be persisted; a re-fence after restart reads it back from here")
}

// TestConsumeDirtyAndGetSnapshotRewritesRetiredSplittedToDroppedForRemoval:
// the case that matters most is exactly the one where dirty is already
// false -- Retired was set and persisted on an earlier round, and only later
// does flusherCheckpointTimeTick independently catch up to the fence. That
// round still has to emit a snapshot, rewritten to DROPPED (with Retired
// left true), or the catalog would never see the write that deletes the row.
func TestConsumeDirtyAndGetSnapshotRewritesRetiredSplittedToDroppedForRemoval(t *testing.T) {
	info := &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel:      "v1",
			State:         streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED,
			Retired:       true,
			SplitTimeTick: 2000,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
		dirty: false,
	}

	snapshot, shouldBeRemoved := info.ConsumeDirtyAndGetSnapshot(2000)
	assert.True(t, shouldBeRemoved)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, snapshot.State)
	assert.True(t, snapshot.Retired)
}

// newRecoveryStorageForRetireGCTest builds a bare recoveryStorageImpl carrying
// exactly one retired-and-SPLITTED vchannel whose flusher checkpoint sits at
// flusherCheckpointTimeTick, plus enough resource wiring for persistDirtySnapshot
// to run: a catalog mock that records the persisted snapshot, and a mixcoord
// mock that deliberately has no DropVirtualChannel expectation -- calling it
// unexpectedly fails the test, which is exactly the assertion this test needs.
// The vchannel starts dirty so the snapshot actually flows through
// dropAllVirtualChannel and SaveRecoverySnapshot in both cases -- exercising
// the real path, not a shortcut that never emits a snapshot at all.
func newRecoveryStorageForRetireGCTest(t *testing.T, flusherCheckpointTimeTick uint64) (rs *recoveryStorageImpl, persistedVChannels *map[string]*streamingpb.VChannelMeta) {
	persisted := make(map[string]*streamingpb.VChannelMeta)
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().SaveRecoverySnapshot(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, snapshot *metastore.WALRecoverySnapshot) error {
			for k, v := range snapshot.VChannels {
				persisted[k] = v
			}
			return nil
		})

	mixCoord := mocks.NewMockMixCoordClient(t)
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(mixCoord)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog), resource.OptMixCoordClient(f))

	truncator := mock_walimpls.NewMockWALImpls(t)
	truncator.EXPECT().Truncate(mock.Anything, mock.Anything).Return(nil).Maybe()

	channel := types.PChannelInfo{Name: "test-pchannel"}
	rs = &recoveryStorageImpl{
		cfg:     newConfig(),
		channel: channel,
		checkpoint: &WALCheckpoint{
			MessageID: rmq.NewRmqID(10),
			TimeTick:  10,
		},
		segments:         map[int64]*segmentRecoveryInfo{},
		vchannels:        map[string]*vchannelRecoveryInfo{},
		dirtyCounter:     1, // force consumeDirtySnapshot to actually run.
		metrics:          newRecoveryStorageMetrics(channel),
		truncator:        truncator,
		persistNotifier:  make(chan struct{}, 1),
		retiredVChannels: map[string]struct{}{"v0": {}},
	}
	rs.vchannels["v0"] = &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel:      "v0",
			State:         streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED,
			Retired:       true,
			SplitTimeTick: 2000,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
		dirty: true,
		flusherCheckpoint: &WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  flusherCheckpointTimeTick,
		},
	}
	return rs, &persisted
}

// TestRetiredSplittedVChannelIsRemovedOnlyAfterTheFlusherPassesTheFence: a
// retired SPLITTED vchannel is only safe to collect once the flusher has
// actually drained past T_switch -- collecting it earlier could still lose
// data the flusher has not consumed yet. Collecting it means the catalog row
// is actually deleted (the snapshot is rewritten to State=DROPPED, which is
// what the catalog's own removal logic keys off), but DataCoord must never
// be called: the source's own catalog collection is a purely local decision,
// and dropAllVirtualChannel skips a DROPPED-with-Retired entry for exactly
// that reason.
func TestRetiredSplittedVChannelIsRemovedOnlyAfterTheFlusherPassesTheFence(t *testing.T) {
	t.Run("flusher checkpoint has not passed the fence yet", func(t *testing.T) {
		rs, persisted := newRecoveryStorageForRetireGCTest(t, 1999)

		err := rs.persistDirtySnapshot(context.Background(), mlog.InfoLevel)
		assert.NoError(t, err)

		_, ok := rs.vchannels["v0"]
		assert.True(t, ok, "the retired meta must stay in the catalog until the flusher drains past the fence")
		// No removal write happened: the persisted snapshot still carries it
		// as SPLITTED, not DROPPED.
		assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, (*persisted)["v0"].State)
	})

	t.Run("flusher checkpoint has passed the fence", func(t *testing.T) {
		rs, persisted := newRecoveryStorageForRetireGCTest(t, 2000)

		err := rs.persistDirtySnapshot(context.Background(), mlog.InfoLevel)
		assert.NoError(t, err)

		_, ok := rs.vchannels["v0"]
		assert.False(t, ok, "a retired vchannel drained past the fence must be collected from the catalog")
		// The catalog actually receives the delete write: State is rewritten
		// to DROPPED (what the catalog's own removal logic keys off) with
		// Retired left true (what tells dropAllVirtualChannel this is not a
		// genuine drop). DataCoord's DropVirtualChannel is never reached --
		// it is deliberately left unconfigured on the mixcoord mock above,
		// so any call to it would have already failed this test.
		require.Contains(t, *persisted, "v0")
		assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, (*persisted)["v0"].State)
		assert.True(t, (*persisted)["v0"].Retired)
	})
}

// TestRetiredSourceIsCollectedOnAQuietPChannel: UpdateFlusherCheckpoint is the
// only signal that the flusher has passed a fence, and it is neither a WAL
// message (so it never bumps dirtyCounter) nor does it call notifyPersist on
// its own. Without retiredVChannels keeping the persist gate open,
// consumeDirtySnapshot would short-circuit forever on a pchannel where
// nothing else ever becomes dirty again -- the retired vchannel would sit
// collectable, but never actually get collected.
func TestRetiredSourceIsCollectedOnAQuietPChannel(t *testing.T) {
	rs, persisted := newRecoveryStorageForRetireGCTest(t, 1999)

	// First persist round: the flusher checkpoint (1999) has not passed the
	// fence (2000) yet, so the meta stays -- this also clears dirtyCounter
	// and consumes the vchannel's own dirty flag, exactly like the previous
	// round in TestRetiredSplittedVChannelIsRemovedOnlyAfterTheFlusherPassesTheFence.
	err := rs.persistDirtySnapshot(context.Background(), mlog.InfoLevel)
	require.NoError(t, err)
	_, ok := rs.vchannels["v0"]
	require.True(t, ok)
	require.Equal(t, 0, rs.dirtyCounter)

	// No further WAL message ever arrives on this pchannel: dirtyCounter
	// stays at 0. UpdateFlusherCheckpoint is the only thing that moves, and
	// it must wake the persist loop on its own once the new checkpoint
	// crosses the fence.
	rs.UpdateFlusherCheckpoint("v0", &WALCheckpoint{
		MessageID: rmq.NewRmqID(2),
		TimeTick:  2000,
	})
	assert.Equal(t, 0, rs.dirtyCounter, "no WAL message was ever observed on this pchannel")
	select {
	case <-rs.persistNotifier:
	default:
		t.Fatal("UpdateFlusherCheckpoint must notifyPersist once a pending retirement's fence is passed")
	}

	// The next persist round -- reached because the gate now also considers
	// the pending retirement, not because of dirtyCounter -- collects it.
	err = rs.persistDirtySnapshot(context.Background(), mlog.InfoLevel)
	assert.NoError(t, err)
	_, ok = rs.vchannels["v0"]
	assert.False(t, ok, "a retired source must be collected on an otherwise quiet pchannel too")
	require.Contains(t, *persisted, "v0")
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, (*persisted)["v0"].State)
	assert.True(t, (*persisted)["v0"].Retired)
	assert.Empty(t, rs.retiredVChannels, "the pending-retirement tracking must be cleaned up once collected")
}

// TestUpdateFlusherCheckpointNotifiesOnlyWhenAPendingRetirementIsPastItsFence
// pins down every reason UpdateFlusherCheckpoint must NOT wake the persist
// loop, complementing TestRetiredSourceIsCollectedOnAQuietPChannel's positive
// case.
func TestUpdateFlusherCheckpointNotifiesOnlyWhenAPendingRetirementIsPastItsFence(t *testing.T) {
	newRS := func() *recoveryStorageImpl {
		rs := &recoveryStorageImpl{
			vchannels:        map[string]*vchannelRecoveryInfo{},
			persistNotifier:  make(chan struct{}, 1),
			retiredVChannels: map[string]struct{}{},
		}
		rs.vchannels["v0"] = &vchannelRecoveryInfo{
			meta: &streamingpb.VChannelMeta{
				Vchannel:      "v0",
				State:         streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED,
				Retired:       true,
				SplitTimeTick: 2000,
				CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
					CollectionId: 1,
				},
			},
		}
		rs.retiredVChannels["v0"] = struct{}{}
		return rs
	}
	assertNoNotify := func(t *testing.T, rs *recoveryStorageImpl) {
		t.Helper()
		select {
		case <-rs.persistNotifier:
			t.Fatal("must not wake the persist loop")
		default:
		}
	}

	t.Run("unknown vchannel", func(t *testing.T) {
		rs := newRS()
		assert.NotPanics(t, func() {
			rs.UpdateFlusherCheckpoint("v-unknown", &WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 100})
		})
		assertNoNotify(t, rs)
	})

	t.Run("an out-of-order checkpoint is rejected", func(t *testing.T) {
		rs := newRS()
		rs.vchannels["v0"].flusherCheckpoint = &WALCheckpoint{MessageID: rmq.NewRmqID(5), TimeTick: 5000}
		rs.UpdateFlusherCheckpoint("v0", &WALCheckpoint{MessageID: rmq.NewRmqID(3), TimeTick: 2000})
		assertNoNotify(t, rs)
	})

	t.Run("no pending retirement at all", func(t *testing.T) {
		rs := newRS()
		delete(rs.retiredVChannels, "v0")
		rs.UpdateFlusherCheckpoint("v0", &WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 2000})
		assertNoNotify(t, rs)
	})

	t.Run("pchannel-wide minimum is still unknown", func(t *testing.T) {
		rs := newRS()
		// A second vchannel with no flusher checkpoint at all yet makes the
		// pchannel-wide minimum nil, per getFlusherCheckpointLocked's contract.
		rs.vchannels["v1"] = &vchannelRecoveryInfo{
			meta: &streamingpb.VChannelMeta{
				Vchannel: "v1",
				State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
				CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
					CollectionId: 1,
				},
			},
		}
		rs.UpdateFlusherCheckpoint("v0", &WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 2000})
		assertNoNotify(t, rs)
	})

	t.Run("pending retirement has not reached its fence yet", func(t *testing.T) {
		rs := newRS()
		rs.UpdateFlusherCheckpoint("v0", &WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 1999})
		assertNoNotify(t, rs)
	})
}

// TestDropAllVirtualChannelSkipsRetiredSplitSources: dropAllVirtualChannel
// must tell a genuine drop (DataCoord needs to hear about it) apart from a
// retired split source that merely borrows the DROPPED state to get deleted
// from the catalog (see ConsumeDirtyAndGetSnapshot) -- only the former may
// ever reach DataCoord's DropVirtualChannel.
func TestDropAllVirtualChannelSkipsRetiredSplitSources(t *testing.T) {
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	mixCoord := mocks.NewMockMixCoordClient(t)
	var droppedChannels []string
	mixCoord.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *datapb.DropVirtualChannelRequest, opts ...grpc.CallOption) (*datapb.DropVirtualChannelResponse, error) {
			droppedChannels = append(droppedChannels, req.GetChannelName())
			return &datapb.DropVirtualChannelResponse{Status: merr.Success()}, nil
		})
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(mixCoord)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(snCatalog), resource.OptMixCoordClient(f))

	rs := &recoveryStorageImpl{
		cfg:     newConfig(),
		channel: types.PChannelInfo{Name: "test-pchannel"},
		metrics: newRecoveryStorageMetrics(types.PChannelInfo{Name: "test-pchannel"}),
	}

	err := rs.dropAllVirtualChannel(context.Background(), map[string]*streamingpb.VChannelMeta{
		// a genuine drop: DataCoord must be told.
		"v-dropped": {
			Vchannel: "v-dropped",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
		},
		// a retired split source borrowing DROPPED to get collected from the
		// catalog: DataCoord must never hear about it.
		"v-retired-source": {
			Vchannel: "v-retired-source",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_DROPPED,
			Retired:  true,
		},
		// neither DROPPED nor removable: untouched either way.
		"v-normal": {
			Vchannel: "v-normal",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, []string{"v-dropped"}, droppedChannels)
}
