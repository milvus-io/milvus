package recovery

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
)

func TestNewVChannelRecoveryInfoFromVChannelMeta(t *testing.T) {
	meta := []*streamingpb.VChannelMeta{
		{Vchannel: "vchannel-1"},
		{Vchannel: "vchannel-2"},
	}

	info := newVChannelRecoveryInfoFromVChannelMeta(meta)

	assert.Len(t, info, 2)
	assert.NotNil(t, info["vchannel-1"])
	assert.NotNil(t, info["vchannel-2"])
	assert.False(t, info["vchannel-1"].dirty)
	assert.False(t, info["vchannel-2"].dirty)

	snapshot, shouldBeRemoved := info["vchannel-1"].ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, shouldBeRemoved)
}

func TestNewVChannelRecoveryInfoFromCreateCollectionMessage(t *testing.T) {
	// CreateCollection
	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{
			CollectionId: 100,
			PartitionIds: []int64{101, 102},
		}).
		WithBody(&msgpb.CreateCollectionRequest{
			CollectionName: "test-collection",
			CollectionID:   100,
			PartitionIDs:   []int64{101, 102},
		}).
		WithVChannel("vchannel-1").
		MustBuildMutable()
	msgID := rmq.NewRmqID(1)
	ts := uint64(12345)
	immutableMsg := msg.WithTimeTick(ts).WithLastConfirmed(msgID).IntoImmutableMessage(msgID)
	info := newVChannelRecoveryInfoFromCreateCollectionMessage(message.MustAsImmutableCreateCollectionMessageV1(immutableMsg))

	assert.Equal(t, "vchannel-1", info.meta.Vchannel)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, info.meta.State)
	assert.Equal(t, ts, info.meta.CheckpointTimeTick)
	assert.Len(t, info.meta.CollectionInfo.Partitions, 2)
	assert.True(t, info.dirty)

	snapshot, shouldBeRemoved := info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.False(t, shouldBeRemoved)
	assert.False(t, info.dirty)

	snapshot, shouldBeRemoved = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, shouldBeRemoved)

	// CreatePartition
	msg3 := message.NewCreatePartitionMessageBuilderV1().
		WithHeader(&message.CreatePartitionMessageHeader{
			CollectionId: 100,
			PartitionId:  103,
		}).
		WithBody(&msgpb.CreatePartitionRequest{
			CollectionName: "test-collection",
			CollectionID:   100,
			PartitionID:    103,
		}).
		WithVChannel("vchannel-1").
		MustBuildMutable()
	msgID3 := rmq.NewRmqID(3)
	ts += 1
	immutableMsg3 := msg3.WithTimeTick(ts).WithLastConfirmed(msgID3).IntoImmutableMessage(msgID3)

	info.ObserveCreatePartition(message.MustAsImmutableCreatePartitionMessageV1(immutableMsg3))
	// idempotent
	info.ObserveCreatePartition(message.MustAsImmutableCreatePartitionMessageV1(immutableMsg3))
	assert.Equal(t, "vchannel-1", info.meta.Vchannel)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, info.meta.State)
	assert.Equal(t, ts, info.meta.CheckpointTimeTick)
	assert.Len(t, info.meta.CollectionInfo.Partitions, 3)
	assert.True(t, info.dirty)

	snapshot, shouldBeRemoved = info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.False(t, shouldBeRemoved)
	assert.False(t, info.dirty)

	snapshot, shouldBeRemoved = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, shouldBeRemoved)
	assert.False(t, info.dirty)

	ts += 1
	immutableMsg3 = msg3.WithTimeTick(ts).WithLastConfirmed(msgID3).IntoImmutableMessage(msgID3)
	// idempotent
	info.ObserveCreatePartition(message.MustAsImmutableCreatePartitionMessageV1(immutableMsg3))
	assert.Len(t, info.meta.CollectionInfo.Partitions, 3)
	snapshot, shouldBeRemoved = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, shouldBeRemoved)
	assert.False(t, info.dirty)

	snapshot, shouldBeRemoved = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, shouldBeRemoved)
	assert.False(t, info.dirty)

	// DropPartition
	msg4 := message.NewDropPartitionMessageBuilderV1().
		WithHeader(&message.DropPartitionMessageHeader{
			CollectionId: 100,
			PartitionId:  101,
		}).
		WithBody(&msgpb.DropPartitionRequest{
			CollectionName: "test-collection",
			CollectionID:   100,
			PartitionID:    101,
		}).
		WithVChannel("vchannel-1").
		MustBuildMutable()
	msgID4 := rmq.NewRmqID(4)
	ts += 1
	immutableMsg4 := msg4.WithTimeTick(ts).WithLastConfirmed(msgID4).IntoImmutableMessage(msgID4)

	info.ObserveDropPartition(message.MustAsImmutableDropPartitionMessageV1(immutableMsg4))
	// idempotent
	info.ObserveDropPartition(message.MustAsImmutableDropPartitionMessageV1(immutableMsg4))
	assert.Equal(t, "vchannel-1", info.meta.Vchannel)
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, info.meta.State)
	assert.Equal(t, ts, info.meta.CheckpointTimeTick)
	assert.Len(t, info.meta.CollectionInfo.Partitions, 2)
	assert.NotContains(t, info.meta.CollectionInfo.Partitions, int64(101))
	assert.True(t, info.dirty)

	snapshot, shouldBeRemoved = info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.False(t, shouldBeRemoved)
	assert.False(t, info.dirty)

	snapshot, shouldBeRemoved = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, shouldBeRemoved)
	assert.False(t, info.dirty)

	ts += 1
	immutableMsg4 = msg4.WithTimeTick(ts).WithLastConfirmed(msgID4).IntoImmutableMessage(msgID4)
	// idempotent
	info.ObserveDropPartition(message.MustAsImmutableDropPartitionMessageV1(immutableMsg4))
	assert.Len(t, info.meta.CollectionInfo.Partitions, 2)
	snapshot, shouldBeRemoved = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, shouldBeRemoved)
	assert.False(t, info.dirty)

	snapshot, shouldBeRemoved = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.False(t, shouldBeRemoved)
	assert.False(t, info.dirty)

	// DropCollection
	msg2 := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&msgpb.DropCollectionRequest{
			CollectionName: "test-collection",
			CollectionID:   100,
		}).
		WithVChannel("vchannel-1").
		MustBuildMutable()
	msgID2 := rmq.NewRmqID(2)
	ts += 1
	immutableMsg2 := msg2.WithTimeTick(ts).WithLastConfirmed(msgID2).IntoImmutableMessage(msgID2)

	info.ObserveDropCollection(message.MustAsImmutableDropCollectionMessageV1(immutableMsg2))
	assert.Equal(t, streamingpb.VChannelState_VCHANNEL_STATE_DROPPED, info.meta.State)
	assert.Equal(t, ts, info.meta.CheckpointTimeTick)
	assert.Len(t, info.meta.CollectionInfo.Partitions, 2)
	assert.True(t, info.dirty)

	snapshot, shouldBeRemoved = info.ConsumeDirtyAndGetSnapshot()
	assert.NotNil(t, snapshot)
	assert.True(t, shouldBeRemoved)
	assert.False(t, info.dirty)

	snapshot, shouldBeRemoved = info.ConsumeDirtyAndGetSnapshot()
	assert.Nil(t, snapshot)
	assert.True(t, shouldBeRemoved)
}
