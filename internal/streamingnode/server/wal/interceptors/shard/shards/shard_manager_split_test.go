package shards

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

func newTestSplitShardImmutableMessage(vchannel string, collectionID int64, timetick uint64) message.ImmutableSplitShardMessageV2 {
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
	return message.MustAsImmutableSplitShardMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(2)))
}

func newTestShardManagerWithVChannelState(t *testing.T, state streamingpb.VChannelState, splitTimeTick uint64) ShardManager {
	paramtable.Init()
	resource.InitForTest(t)
	w := mock_wal.NewMockWAL(t)
	w.EXPECT().Available().RunAndReturn(func() <-chan struct{} {
		return make(chan struct{})
	}).Maybe()
	w.EXPECT().Append(mock.Anything, mock.Anything).Return(&types.AppendResult{
		MessageID: rmq.NewRmqID(1),
		TimeTick:  1000,
	}, nil).Maybe()
	f := syncutil.NewFuture[wal.WAL]()
	f.Set(w)

	return RecoverShardManager(&ShardManagerRecoverParam{
		ChannelInfo: types.PChannelInfo{Name: "test_channel", Term: 1},
		WAL:         f,
		InitialRecoverSnapshot: &recovery.RecoverySnapshot{
			VChannels: map[string]*streamingpb.VChannelMeta{
				"v1": {
					Vchannel:      "v1",
					State:         state,
					SplitTimeTick: splitTimeTick,
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 1,
						Partitions: []*streamingpb.PartitionInfoOfVChannel{
							{PartitionId: 2},
						},
					},
				},
			},
			Checkpoint: &recovery.WALCheckpoint{TimeTick: 100},
		},
		TxnManager: &mockedTxnManager{},
	})
}

func TestShardManagerSplitShard(t *testing.T) {
	m := newTestShardManagerWithVChannelState(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0)

	// the vchannel accepts DML before the split.
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(1))
	// an unknown collection is reported as not found.
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(999), ErrCollectionNotFound)

	// a split message on an unknown collection takes no effect.
	m.SplitShard(newTestSplitShardImmutableMessage("v999", 999, 2000))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(1))

	// an unfenced or unknown collection has no T_switch.
	assert.Zero(t, m.GetSplitTimeTick(1))
	assert.Zero(t, m.GetSplitTimeTick(999))

	// the split message fences the vchannel and records T_switch.
	m.SplitShard(newTestSplitShardImmutableMessage("v1", 1, 2000))
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1), ErrVChannelFenced)
	assert.Equal(t, uint64(2000), m.GetSplitTimeTick(1))

	// the fence is idempotent and T_switch stays at the first fence.
	m.SplitShard(newTestSplitShardImmutableMessage("v1", 1, 3000))
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1), ErrVChannelFenced)
	assert.Equal(t, uint64(2000), m.GetSplitTimeTick(1))
}

func TestShardManagerRecoverSplittedVChannel(t *testing.T) {
	// a vchannel recovered in SPLITTED state keeps rejecting DML and restores
	// T_switch, so an already-fenced re-fence can return it after a crash.
	m := newTestShardManagerWithVChannelState(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 2000)
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1), ErrVChannelFenced)
	assert.Equal(t, uint64(2000), m.GetSplitTimeTick(1))
}

func newTestCreateVChannelImmutableMessage(vchannel string, collectionID int64, partitionIDs []int64, timetick uint64) message.ImmutableCreateVChannelMessageV2 {
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
	return message.MustAsImmutableCreateVChannelMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(3)))
}

func TestShardManagerCreateVChannel(t *testing.T) {
	m := newTestShardManagerWithVChannelState(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0)
	// a shard split target vchannel of a new collection is registered for DML,
	// exactly as a create collection genesis would register it.
	m.CreateVChannel(newTestCreateVChannelImmutableMessage("v2", 7, []int64{8}, 2000))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(7))

	// a target without a schema still registers the collection.
	m.CreateVChannel(newTestCreateVChannelImmutableMessageNoSchema("v3", 9, []int64{10}, 2500))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(9))

	// re-registering an already-known collection is a no-op (idempotent).
	m.CreateVChannel(newTestCreateVChannelImmutableMessage("v1-dup", 1, []int64{2}, 3000))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(1))
}

func newTestCreateVChannelImmutableMessageNoSchema(vchannel string, collectionID int64, partitionIDs []int64, timetick uint64) message.ImmutableCreateVChannelMessageV2 {
	msg := message.NewCreateVChannelMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CreateVChannelMessageHeader{
			CollectionId:        collectionID,
			PartitionIds:        partitionIDs,
			SplitTaskId:         100,
			SplitSourceVchannel: "v1",
			KeyRange:            &message.KeyRange{Lower: []byte{0x80}},
		}).
		WithBody(&message.CreateCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID()
	return message.MustAsImmutableCreateVChannelMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(4)))
}
