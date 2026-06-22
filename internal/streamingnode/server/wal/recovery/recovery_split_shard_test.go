package recovery

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
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
				{Vchannel: vchannel + "-target1", KeyRange: &message.KeyRange{Upper: []byte{0x80}}},
				{Vchannel: vchannel + "-target2", KeyRange: &message.KeyRange{Lower: []byte{0x80}}},
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
			CollectionId:        collectionID,
			PartitionIds:        partitionIDs,
			SplitTaskId:         100,
			SplitSourceVchannel: "v1",
			KeyRange:            &message.KeyRange{Upper: []byte{0x80}},
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
	rs.handleMessage(newCreateVChannelMessage("v2", 7, []int64{8}, 100))
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
