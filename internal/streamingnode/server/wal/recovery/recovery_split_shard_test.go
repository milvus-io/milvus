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
	msg := message.NewSplitShardMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    collectionID,
			SplitTaskId:     100,
			PartitionIds:    partitionIDs,
			SourceVchannel:  source,
			TargetVchannels: targets,
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
	// The genesis position is recorded and persisted: a flusher recovering the
	// target before datacoord has a position for it starts from here.
	assertSplitGenesis := func(meta *streamingpb.VChannelMeta) {
		genesis := meta.GetSplitGenesisCheckpoint()
		require.NotNil(t, genesis)
		assert.True(t, message.MustUnmarshalMessageID(genesis.GetMessageId()).EQ(rmq.NewRmqID(3)))
		assert.Equal(t, uint64(100), genesis.GetTimeTick())
	}
	assertSplitGenesis(info.meta)
	persisted, _ := info.ConsumeDirtyAndGetSnapshot()
	assertSplitGenesis(persisted)
	assertSplitGenesis(newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{persisted})["v2"].meta)

	// re-applying the genesis is idempotent, and does not move the genesis.
	rs.handleSplitShard(context.Background(), newSplitShardMessage("v2", "v1", []string{"v2", "v3"}, 7, []int64{8}, 200))
	assertSplitGenesis(rs.vchannels["v2"].meta)

	// A CreateCollection vchannel records no split genesis.
	created := message.MustAsImmutableCreateCollectionMessageV1(
		message.CreateTestCreateCollectionMessage(t, 9, 100, rmq.NewRmqID(1)).IntoImmutableMessage(rmq.NewRmqID(2)))
	assert.Nil(t, newVChannelRecoveryInfoFromCreateCollectionMessage(created).meta.GetSplitGenesisCheckpoint())
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

// TestRecoveryStorageSplitShardOnAnotherShardOfTheCollectionIsAnInconsistency:
// the broadcast reaches only the source, the targets and the control channel,
// so a replica landing on another registered shard of the SAME collection is a
// misroute: reported, and the shard's own meta left untouched.
func TestRecoveryStorageSplitShardOnAnotherShardOfTheCollectionIsAnInconsistency(t *testing.T) {
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
	assert.Equal(t, []string{"split shard replica of unknown role"}, reasons)
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

	// the SPLITTED state is persisted in the snapshot and the vchannel
	// meta must not be removed from the catalog (the fence must survive
	// restarts until the vchannel is really dropped).
	snapshot, shouldBeRemoved := info.ConsumeDirtyAndGetSnapshot()
	require.NotNil(t, snapshot)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, snapshot.State)
	assert.Equal(t, uint64(100), snapshot.SplitTimeTick)
	assert.False(t, shouldBeRemoved)

	// a re-fence on an already-SPLITTED vchannel takes no effect: T_switch
	// stays the first fence's tick and nothing is left to persist.
	info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 200))
	assert.Equal(t, uint64(100), info.meta.CheckpointTimeTick)
	assert.Equal(t, uint64(100), info.meta.SplitTimeTick)
	assert.False(t, info.dirty)

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

// newDropPartitionMessage builds one replica of a DropPartition broadcast,
// landing on the given vchannel.
func newDropPartitionMessage(vchannel string, collectionID, partitionID int64, timetick uint64) message.ImmutableMessage {
	return message.NewDropPartitionMessageBuilderV1().
		WithVChannel(vchannel).
		WithHeader(&message.DropPartitionMessageHeader{
			CollectionId: collectionID,
			PartitionId:  partitionID,
		}).
		WithBody(&msgpb.DropPartitionRequest{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID().
		IntoImmutableMessage(rmq.NewRmqID(4))
}

// TestRecoveryStorageDropPartitionIsScopedToOneVChannel: a DropPartition
// broadcast puts one replica on every vchannel of the collection, and two of
// them can share this pchannel -- here a fenced split source and the target
// that took its pchannel. The replica addressed to the source must seal only
// the source's segments of that partition (the fence already did; sealing
// again is idempotent) and leave the target's growing segment of the same
// partition alone: an insert into it appended between the two replicas would
// otherwise find no growing segment. The target's own replica seals it.
func TestRecoveryStorageDropPartitionIsScopedToOneVChannel(t *testing.T) {
	rs := newTestRecoveryStorage(t)
	addActiveVChannel(rs, "v0", 1, []int64{2, 3})
	addGrowingSegment(rs, 1001, 1, 2, "v0")
	rs.handleSplitShard(context.Background(), newSplitShardMessage("v0", "v0", []string{"v0-target1"}, 1, []int64{2, 3}, 100))
	rs.handleSplitShard(context.Background(), newSplitShardMessage("v0-target1", "v0", []string{"v0-target1"}, 1, []int64{2, 3}, 101))
	require.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, rs.vchannels["v0"].meta.State)
	require.False(t, rs.segments[1001].IsGrowing(), "the fence seals the source's growing segment")
	addGrowingSegment(rs, 1002, 1, 2, "v0-target1")
	addGrowingSegment(rs, 1003, 1, 3, "v0-target1")

	// The replica addressed to the fenced source: its partition list is
	// updated, the target's growing segments are untouched.
	rs.handleMessage(context.Background(), newDropPartitionMessage("v0", 1, 2, 150))
	assert.False(t, rs.vchannels["v0"].IsPartitionActive(2))
	assert.True(t, rs.vchannels["v0-target1"].IsPartitionActive(2))
	assert.True(t, rs.segments[1002].IsGrowing(), "the target's segment of the dropped partition must be sealed by the target's own replica")
	assert.True(t, rs.segments[1003].IsGrowing())

	// The replica addressed to the target seals its segment of that partition
	// only.
	rs.handleMessage(context.Background(), newDropPartitionMessage("v0-target1", 1, 2, 151))
	assert.False(t, rs.vchannels["v0-target1"].IsPartitionActive(2))
	assert.False(t, rs.segments[1002].IsGrowing())
	assert.True(t, rs.segments[1003].IsGrowing(), "another partition's segment is not the drop's")
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
	normal.ObserveRetire()
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
	splitted.ObserveRetire()
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, splitted.meta.State)
	assert.True(t, splitted.meta.Retired)
	assert.True(t, splitted.dirty)

	// idempotent: a replayed retire takes no further effect.
	splitted.dirty = false
	splitted.ObserveRetire()
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
	dropped.ObserveRetire()
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

// TestObserveSplitShardKeepsTheFirstFenceTick: like the shard manager's
// tombstone, SplitTimeTick is the tick of the task's FIRST fence record, so a
// re-fence on an already-SPLITTED vchannel moves nothing -- neither to a later
// tick nor to an earlier one -- and marks nothing dirty.
func TestObserveSplitShardKeepsTheFirstFenceTick(t *testing.T) {
	info := &vchannelRecoveryInfo{
		meta: &streamingpb.VChannelMeta{
			Vchannel: "v1",
			State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
			CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
				CollectionId: 1,
			},
		},
	}

	// the first fence keeps today's behavior: it sets T_switch outright.
	info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 2000))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, info.meta.State)
	assert.Equal(t, uint64(2000), info.meta.SplitTimeTick)

	// a later re-fence of the same task leaves T_switch at the first fence.
	info.dirty = false
	info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 3000))
	assert.Equal(t, uint64(2000), info.meta.SplitTimeTick)
	assert.Equal(t, uint64(2000), info.meta.CheckpointTimeTick)
	assert.False(t, info.dirty)

	// so does one at an in-between tick.
	info.ObserveSplitShard(newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, nil, 2500))
	assert.Equal(t, uint64(2000), info.meta.SplitTimeTick)
	assert.False(t, info.dirty)
}

// TestRetiredSourceIsCollectedAfterASameTaskReFence is the recovery half of
// pinning T_switch to the first fence. The source's data sync service drained
// past the first fence (2000) and closed, so the flusher checkpoint for it stops
// there for good. The broadcaster then re-drives the same task's fence at a
// later tick. Had the re-fence raised SplitTimeTick, the retire collection --
// which waits for the flusher checkpoint to pass SplitTimeTick -- could never
// happen and the row would pin WAL truncation; with the tick pinned the retired
// source is still collected.
func TestRetiredSourceIsCollectedAfterASameTaskReFence(t *testing.T) {
	rs, persisted, _ := newRecoveryStorageForRetireGCTest(t, 2000)
	rs.vchannels["v0"].meta.SplitTaskId = 100

	rs.handleSplitShard(context.Background(), newSplitShardMessage("v0", "v0", []string{"v0-t1", "v0-t2"}, 1, nil, 3000))
	require.Equal(t, uint64(2000), rs.vchannels["v0"].meta.SplitTimeTick, "a re-fence must not move T_switch")

	require.NoError(t, rs.persistDirtySnapshot(context.Background(), mlog.InfoLevel))
	_, ok := rs.vchannels["v0"]
	assert.False(t, ok, "the retired source must still be collected once the flusher passed the first fence")
	require.Contains(t, *persisted, "v0")
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, (*persisted)["v0"].State)
	assert.True(t, (*persisted)["v0"].Retired)
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

	snapshot, shouldBeRemoved := info.ConsumeDirtyAndGetSnapshot()
	assert.False(t, shouldBeRemoved)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, snapshot.State)
	assert.Equal(t, uint64(4242), snapshot.SplitTimeTick,
		"T_switch must be persisted; a re-fence after restart reads it back from here")
}

// TestConsumeDirtyAndGetSnapshotRewritesRetiredSplittedToDroppedForRemoval:
// the case that matters most is exactly the one where dirty is already
// false -- Retired was set and persisted on an earlier round, and only later
// does the vchannel's flusher checkpoint independently catch up to the fence. That
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
		// Its own checkpoint reached the fence.
		flusherCheckpoint: &WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 2000},
	}

	snapshot, shouldBeRemoved := info.ConsumeDirtyAndGetSnapshot()
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
func newRecoveryStorageForRetireGCTest(t *testing.T, flusherCheckpointTimeTick uint64) (rs *recoveryStorageImpl, persistedVChannels *map[string]*streamingpb.VChannelMeta, saveCalls *int) {
	persisted := make(map[string]*streamingpb.VChannelMeta)
	// Counted, not only recorded: a snapshot carrying nothing but the
	// checkpoint leaves `persisted` empty, so the count is the only thing that
	// can tell "wrote nothing" apart from "did not write".
	calls := 0
	snCatalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	snCatalog.EXPECT().SaveRecoverySnapshot(mock.Anything, mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, s string, snapshot *metastore.WALRecoverySnapshot) error {
			calls++
			for k, v := range snapshot.VChannels {
				persisted[k] = v
			}
			return nil
		}).Maybe()

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
	return rs, &persisted, &calls
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
		rs, persisted, _ := newRecoveryStorageForRetireGCTest(t, 1999)

		err := rs.persistDirtySnapshot(context.Background(), mlog.InfoLevel)
		assert.NoError(t, err)

		_, ok := rs.vchannels["v0"]
		assert.True(t, ok, "the retired meta must stay in the catalog until the flusher drains past the fence")
		// No removal write happened: the persisted snapshot still carries it
		// as SPLITTED, not DROPPED.
		assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, (*persisted)["v0"].State)
	})

	t.Run("flusher checkpoint has passed the fence", func(t *testing.T) {
		rs, persisted, _ := newRecoveryStorageForRetireGCTest(t, 2000)

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
	rs, persisted, _ := newRecoveryStorageForRetireGCTest(t, 1999)

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

// TestGracefulCloseCollectsADrainedRetirement: the graceful-shutdown persist
// loop runs for as long as isDirty() holds. A retirement typically drains
// long after its meta was last persisted (dirtyCounter back to 0), so the one
// removal write that finally deletes the row from the catalog only makes it
// out on shutdown if isDirty() counts a drained retirement as dirty.
func TestGracefulCloseCollectsADrainedRetirement(t *testing.T) {
	rs, persisted, _ := newRecoveryStorageForRetireGCTest(t, 2000)
	// Everything else is already persisted: only the pending retirement,
	// whose flusher checkpoint has just passed the fence, is left.
	rs.dirtyCounter = 0
	rs.vchannels["v0"].dirty = false
	require.True(t, rs.isDirty(), "a drained retirement must keep the storage dirty")

	require.NoError(t, rs.persistDritySnapshotWhenClosing())

	assert.True(t, rs.gracefulClosed)
	_, ok := rs.vchannels["v0"]
	assert.False(t, ok, "the drained retirement must be collected before the graceful close returns")
	require.Contains(t, *persisted, "v0")
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, (*persisted)["v0"].State)
	assert.True(t, (*persisted)["v0"].Retired)
	assert.False(t, rs.isDirty(), "nothing is left to persist once the retirement is collected")
}

// TestUndrainedRetirementWritesNoSnapshotOnAQuietPChannel: the persist gate
// and isDirty must agree on what a pending retirement is worth. A retirement
// the flusher has not drained past is nothing consumeDirtySnapshot can act on
// -- ConsumeDirtyAndGetSnapshot leaves it exactly where it is -- so if the
// gate merely asked whether ANY retirement is pending, every persistInterval
// of the whole redistribution window (hours) would write a snapshot carrying
// nothing but the checkpoint to etcd, on a pchannel where nothing changed.
func TestUndrainedRetirementWritesNoSnapshotOnAQuietPChannel(t *testing.T) {
	// 1999 < the fence at 2000: retired, pending, not yet collectable.
	rs, persisted, saveCalls := newRecoveryStorageForRetireGCTest(t, 1999)
	// Everything else has already been persisted: this is the quiet pchannel
	// the periodic tick keeps waking on.
	rs.dirtyCounter = 0
	rs.vchannels["v0"].dirty = false
	require.False(t, rs.isDirty(), "an undrained retirement is not dirty")

	for i := 0; i < 3; i++ {
		require.NoError(t, rs.persistDirtySnapshot(context.Background(), mlog.DebugLevel))
	}

	assert.Zero(t, *saveCalls, "an undrained retirement must not write a snapshot on every persist tick")
	assert.Empty(t, *persisted)
	assert.Contains(t, rs.retiredVChannels, "v0", "the retirement is still pending, only not actionable")
	_, ok := rs.vchannels["v0"]
	assert.True(t, ok)

	// And once it drains, the very next round does write -- the gate is not
	// simply closed for retirements.
	rs.vchannels["v0"].flusherCheckpoint = &WALCheckpoint{MessageID: rmq.NewRmqID(2), TimeTick: 2000}
	require.NoError(t, rs.persistDirtySnapshot(context.Background(), mlog.DebugLevel))
	assert.Equal(t, 1, *saveCalls)
	require.Contains(t, *persisted, "v0")
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, (*persisted)["v0"].State)
}

// TestIsDirtyIgnoresAnUndrainedRetirement: only a *collectable* retirement
// counts as dirty. A retirement the flusher has not drained past is nothing a
// persist round can act on, and calling it dirty would spin the
// graceful-shutdown loop -- which persists for as long as isDirty() holds --
// until the graceful timeout expires.
func TestIsDirtyIgnoresAnUndrainedRetirement(t *testing.T) {
	newRS := func() *recoveryStorageImpl {
		rs := &recoveryStorageImpl{
			vchannels:        map[string]*vchannelRecoveryInfo{},
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
			flusherCheckpoint: &WALCheckpoint{MessageID: rmq.NewRmqID(1), TimeTick: 1999},
		}
		return rs
	}

	t.Run("the fence has not been passed yet", func(t *testing.T) {
		assert.False(t, newRS().isDirty())
	})

	t.Run("the pchannel-wide minimum is still unknown", func(t *testing.T) {
		rs := newRS()
		rs.vchannels["v0"].flusherCheckpoint = nil
		assert.False(t, rs.isDirty())
	})

	t.Run("no pending retirement at all", func(t *testing.T) {
		rs := newRS()
		delete(rs.retiredVChannels, "v0")
		assert.False(t, rs.isDirty())
	})

	t.Run("the retired vchannel is already gone", func(t *testing.T) {
		rs := newRS()
		// Another vchannel keeps the pchannel-wide minimum well past the
		// fence, so only the missing meta itself can rule the retirement out.
		rs.vchannels["v1"] = &vchannelRecoveryInfo{
			meta: &streamingpb.VChannelMeta{
				Vchannel: "v1",
				State:    streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
				CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
					CollectionId: 1,
				},
			},
			flusherCheckpoint: &WALCheckpoint{MessageID: rmq.NewRmqID(9), TimeTick: 9000},
		}
		delete(rs.vchannels, "v0")
		assert.False(t, rs.isDirty())
	})
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

	t.Run("an unrelated vchannel without a checkpoint does not hold it back", func(t *testing.T) {
		rs := newRS()
		// A second vchannel with no flusher checkpoint at all yet makes the
		// pchannel-wide minimum nil (getFlusherCheckpointLocked's contract,
		// which bounds truncation only); the retirement is judged by v0's own
		// checkpoint, which has just reached its fence.
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
		select {
		case <-rs.persistNotifier:
		default:
			t.Fatal("must wake the persist loop: v0 drained past its own fence")
		}
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

// TestHandleCreateSegmentOnASplittedVChannelIsSkipped: a CreateSegment
// landing on a fenced source is refused by the shard interceptor's name gate
// on the write side, so one observed here can only come from a replay.
// Registering it would leave a GROWING assignment meta that nothing ever
// seals: the fence record, the only thing that seals the source's segments,
// has already been consumed. It is skipped, exactly as one on a DROPPED
// vchannel is. Whether it is also reported depends on its tick: at or before
// the fence gate it is the pre-fence history a restart legitimately replays
// (snapshot persisted SPLITTED, LastConfirmed checkpoint before the
// CreateSegment, the fence replayed after it), so it is skipped quietly; after
// the gate nothing but a WAL offset reset delivers it, and it is reported as
// an inconsistency.
func TestHandleCreateSegmentOnASplittedVChannelIsSkipped(t *testing.T) {
	rs := newTestRecoveryStorage(t)
	addActiveVChannel(rs, "v1", 1, []int64{2})
	addActiveVChannel(rs, "v2", 3, []int64{4})
	rs.handleSplitShard(context.Background(), newSplitShardMessage("v1", "v1", []string{"v1-target1"}, 1, []int64{2}, 100))
	require.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, rs.vchannels["v1"].meta.State)

	reasons := make([]string, 0)
	mockDetect := mockey.Mock((*recoveryStorageImpl).detectInconsistency).To(
		func(r *recoveryStorageImpl, ctx context.Context, msg message.ImmutableMessage, reason string, extra ...mlog.Field) {
			reasons = append(reasons, reason)
		}).Build()
	defer mockDetect.UnPatch()

	newCreateSegment := func(vchannel string, collectionID, partitionID, segmentID int64, timetick uint64) message.ImmutableCreateSegmentMessageV2 {
		msg := message.NewCreateSegmentMessageBuilderV2().
			WithVChannel(vchannel).
			WithHeader(&message.CreateSegmentMessageHeader{
				CollectionId:   collectionID,
				SegmentId:      segmentID,
				PartitionId:    partitionID,
				StorageVersion: 1,
				MaxSegmentSize: 1024,
			}).
			WithBody(&message.CreateSegmentMessageBody{}).
			MustBuildMutable().
			WithTimeTick(timetick).
			WithLastConfirmed(rmq.NewRmqID(int64(timetick))).
			IntoImmutableMessage(rmq.NewRmqID(int64(timetick)))
		return message.MustAsImmutableCreateSegmentMessageV2(msg)
	}

	// The fence was observed at tick 100, which is the gate.
	require.Equal(t, uint64(100), rs.vchannels["v1"].fenceGate())

	// A restart replay: CreateSegments from before the fence, and one at the
	// fence's own tick, are skipped without a report.
	for _, tick := range []uint64{50, 100} {
		rs.handleCreateSegment(context.Background(), newCreateSegment("v1", 1, 2, 1000+int64(tick), tick))
		_, registered := rs.segments[1000+int64(tick)]
		assert.False(t, registered, "a pre-fence segment replayed onto a fenced source must not be registered growing")
	}
	assert.Empty(t, reasons, "the pre-fence history replayed on a restart is not an inconsistency")

	// A CreateSegment ticked after the gate has no fence behind it.
	rs.handleCreateSegment(context.Background(), newCreateSegment("v1", 1, 2, 1200, 200))
	_, registered := rs.segments[1200]
	assert.False(t, registered, "a segment created on a fenced source must not be registered growing")
	assert.Equal(t, []string{"create segment after the fence of a splitted vchannel"}, reasons)

	// A vchannel still taking writes registers as before, with no report.
	rs.handleCreateSegment(context.Background(), newCreateSegment("v2", 3, 4, 2001, 210))
	segment, registered := rs.segments[2001]
	require.True(t, registered)
	assert.True(t, segment.IsGrowing())
	assert.Len(t, reasons, 1)

	// A vchannel reloaded already SPLITTED takes the gate its meta reseeds
	// (SplitFenceGate: the larger of T_switch and the checkpoint tick), which
	// is the same gate the flusher closes the source on.
	rs.vchannels["v3"] = newVChannelRecoveryInfoFromVChannelMeta([]*streamingpb.VChannelMeta{{
		Vchannel:           "v3",
		State:              streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED,
		SplitTimeTick:      300,
		CheckpointTimeTick: 350,
	}})["v3"]
	require.Equal(t, uint64(350), rs.vchannels["v3"].fenceGate())
	rs.handleCreateSegment(context.Background(), newCreateSegment("v3", 5, 6, 3001, 320))
	_, registered = rs.segments[3001]
	assert.False(t, registered)
	assert.Len(t, reasons, 1, "a replay at or before the reseeded gate is quiet")
	rs.handleCreateSegment(context.Background(), newCreateSegment("v3", 5, 6, 3002, 360))
	_, registered = rs.segments[3002]
	assert.False(t, registered)
	assert.Equal(t, []string{
		"create segment after the fence of a splitted vchannel",
		"create segment after the fence of a splitted vchannel",
	}, reasons)
}
