package shards

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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
				{Vchannel: vchannel + "-target1", Routing: &schemapb.HashRouting{Buckets: []uint64{0}}},
				{Vchannel: vchannel + "-target2", Routing: &schemapb.HashRouting{Buckets: []uint64{1}}},
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
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(1, "v1"))
	// an unknown collection is reported as not found.
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(999, "v999"), ErrCollectionNotFound)

	// a split message on an unknown collection takes no effect.
	m.SplitShard(newTestSplitShardImmutableMessage("v999", 999, 2000))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(1, "v1"))

	// a split message naming another vchannel of the SAME collection takes no
	// effect either. The entry is keyed by collection id, so a fence that
	// ignored the name would fence whatever vchannel happens to hold the slot.
	m.SplitShard(newTestSplitShardImmutableMessage("v1-successor", 1, 2000))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(1, "v1"))
	assert.Zero(t, m.GetSplitTimeTick(1, "v1"))

	// an unfenced or unknown collection has no T_switch.
	assert.Zero(t, m.GetSplitTimeTick(1, "v1"))
	assert.Zero(t, m.GetSplitTimeTick(999, "v999"))

	// the split message fences the vchannel and records T_switch.
	m.SplitShard(newTestSplitShardImmutableMessage("v1", 1, 2000))
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1"), ErrVChannelFenced)
	assert.Equal(t, uint64(2000), m.GetSplitTimeTick(1, "v1"))

	// the fence is idempotent and T_switch stays at the first fence.
	m.SplitShard(newTestSplitShardImmutableMessage("v1", 1, 3000))
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1"), ErrVChannelFenced)
	assert.Equal(t, uint64(2000), m.GetSplitTimeTick(1, "v1"))
}

func TestShardManagerRecoverSplittedVChannel(t *testing.T) {
	// a vchannel recovered in SPLITTED state keeps rejecting DML and restores
	// T_switch, so an already-fenced re-fence can return it after a crash.
	m := newTestShardManagerWithVChannelState(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 2000)
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1"), ErrVChannelFenced)
	assert.Equal(t, uint64(2000), m.GetSplitTimeTick(1, "v1"))
}

func newTestCreateVChannelImmutableMessage(vchannel string, collectionID int64, partitionIDs []int64, timetick uint64) message.ImmutableCreateVChannelMessageV2 {
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
	return message.MustAsImmutableCreateVChannelMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(3)))
}

func TestShardManagerCreateVChannel(t *testing.T) {
	m := newTestShardManagerWithVChannelState(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0)
	// a shard split target vchannel of a new collection is registered for DML,
	// exactly as a create collection genesis would register it.
	m.CreateVChannel(newTestCreateVChannelImmutableMessage("v2", 7, []int64{8}, 2000))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(7, "v2"))

	// a target without a schema still registers the collection.
	m.CreateVChannel(newTestCreateVChannelImmutableMessageNoSchema("v3", 9, []int64{10}, 2500))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(9, "v3"))

	// replaying the genesis of an already-registered vchannel is a no-op.
	m.CreateVChannel(newTestCreateVChannelImmutableMessage("v2", 7, []int64{8}, 3000))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(7, "v2"))

	// a genesis for a DIFFERENT vchannel of a collection this pchannel already
	// holds must not take the incumbent's place, and must not be mistaken for
	// an idempotent replay: the newcomer stays unregistered and the incumbent
	// keeps serving.
	m.CreateVChannel(newTestCreateVChannelImmutableMessage("v1-successor", 1, []int64{2}, 3000))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(1, "v1"))
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1-successor"), ErrCollectionNotFound)
}

// TestShardManagerVChannelAdmissionChecks pins the three admission predicates
// the interceptor consults before it appends a split message.
func TestShardManagerVChannelAdmissionChecks(t *testing.T) {
	m := newTestShardManagerWithVChannelState(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0)

	// creation: free slot, idempotent replay, and the conflicting newcomer.
	assert.NoError(t, m.CheckIfVChannelCanBeCreated(7, "v2"))
	assert.ErrorIs(t, m.CheckIfVChannelCanBeCreated(1, "v1"), ErrCollectionExists)
	assert.ErrorIs(t, m.CheckIfVChannelCanBeCreated(1, "v1-successor"), ErrVChannelConflict)

	// teardown: a live shard must never be torn down...
	assert.ErrorIs(t, m.CheckIfVChannelCanBeDropped(1, "v1"), ErrVChannelNotFenced)
	// ...while a teardown for a vchannel this pchannel does not hold is a
	// replay and must still be allowed through, because the recovery storage
	// and the flusher are keyed by vchannel and still have work to do.
	assert.NoError(t, m.CheckIfVChannelCanBeDropped(1, "v1-successor"))
	assert.NoError(t, m.CheckIfVChannelCanBeDropped(999, "v999"))

	// once fenced, the teardown is admitted.
	m.SplitShard(newTestSplitShardImmutableMessage("v1", 1, 2000))
	assert.NoError(t, m.CheckIfVChannelCanBeDropped(1, "v1"))
}

// TestResolveVChannelCollision pins the recovery-time tie-break.
//
// The registration map holds one entry per collection per pchannel. If two
// vchannels of one collection are recovered onto the same pchannel -- a fenced
// source whose teardown has not been observed yet, plus its successor -- the
// winner used to be whichever the map iteration reached last, so a restart
// could leave the live shard unwritable at random and leave it that way.
func TestResolveVChannelCollision(t *testing.T) {
	normal := func(vchannel string) *CollectionInfo {
		return &CollectionInfo{VChannel: vchannel, State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL}
	}
	splitted := func(vchannel string) *CollectionInfo {
		return &CollectionInfo{VChannel: vchannel, State: streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED}
	}

	// The shard that can still take writes wins, whichever order they arrive in.
	winner, loser := resolveVChannelCollision(splitted("v0"), normal("v7"))
	assert.Equal(t, "v7", winner.VChannel)
	assert.Equal(t, "v0", loser.VChannel)
	winner, loser = resolveVChannelCollision(normal("v7"), splitted("v0"))
	assert.Equal(t, "v7", winner.VChannel)
	assert.Equal(t, "v0", loser.VChannel)

	// When the state does not separate them the name does, so the answer does
	// not depend on the order the snapshot happened to be walked in.
	winner, _ = resolveVChannelCollision(normal("v7"), normal("v0"))
	assert.Equal(t, "v0", winner.VChannel)
	winner, _ = resolveVChannelCollision(normal("v0"), normal("v7"))
	assert.Equal(t, "v0", winner.VChannel)
	winner, _ = resolveVChannelCollision(splitted("v7"), splitted("v0"))
	assert.Equal(t, "v0", winner.VChannel)
}

func newTestCreateVChannelImmutableMessageNoSchema(vchannel string, collectionID int64, partitionIDs []int64, timetick uint64) message.ImmutableCreateVChannelMessageV2 {
	msg := message.NewCreateVChannelMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.CreateVChannelMessageHeader{
			CollectionId:         collectionID,
			PartitionIds:         partitionIDs,
			SplitTaskId:          100,
			SplitSourceVchannels: []string{"v1"},
			Routing:              &schemapb.HashRouting{Buckets: []uint64{1}},
			RoutingModulus:       2,
		}).
		WithBody(&message.CreateCollectionRequest{}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID()
	return message.MustAsImmutableCreateVChannelMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(4)))
}

// A retired source must keep answering "fenced", because that is the only signal
// the proxy acts on: invalidate the routing cache, refetch, re-resolve, retry.
// Answering terminally instead fails a write that one refresh would complete.
func TestShardManagerRetiredSourceStillAnswersFenced(t *testing.T) {
	m := newTestShardManagerWithVChannelState(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0)

	// fence, then retire.
	m.SplitShard(newTestSplitShardImmutableMessage("v1", 1, 4000))
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1"), ErrVChannelFenced)
	assert.Equal(t, uint64(4000), m.GetSplitTimeTick(1, "v1"))

	dropMsg, err := message.NewDropVChannelMessageBuilderV2().
		WithVChannel("v1").
		WithHeader(&message.DropVChannelMessageHeader{CollectionId: 1}).
		WithBody(&message.DropVChannelMessageBody{}).
		BuildMutable()
	require.NoError(t, err)
	m.DropVChannel(message.MustAsImmutableDropVChannelMessageV2(
		dropMsg.WithTimeTick(5000).WithLastConfirmedUseMessageID().IntoImmutableMessage(rmq.NewRmqID(2))))
	// the registration is gone...
	assert.ErrorIs(t, m.CheckIfCollectionExists(1), ErrCollectionNotFound)
	// ...but the name is still fenced, and T_switch still recoverable.
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1"), ErrVChannelFenced)
	assert.Equal(t, uint64(4000), m.GetSplitTimeTick(1, "v1"))

	// a vchannel this pchannel never held stays terminal: no refresh sends the
	// write anywhere.
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v-never"), ErrCollectionNotFound)

	// a successor landing on the freed slot is writable, and does not inherit
	// the predecessor's fence.
	m.CreateVChannel(newTestCreateVChannelImmutableMessage("v1-successor", 1, []int64{2}, 6000))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(1, "v1-successor"))
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1"), ErrVChannelFenced)
}
