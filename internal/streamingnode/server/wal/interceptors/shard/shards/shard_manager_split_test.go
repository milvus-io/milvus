package shards

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"

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
	return newTestSplitShardImmutableMessageOfTask(vchannel, collectionID, 100, timetick)
}

func newTestSplitShardImmutableMessageOfTask(vchannel string, collectionID int64, splitTaskID int64, timetick uint64) message.ImmutableSplitShardMessageV2 {
	msg := message.NewSplitShardMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId: collectionID,
			SplitTaskId:  splitTaskID,
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
	return newTestShardManagerFromSnapshot(t,
		map[string]*streamingpb.VChannelMeta{
			"v1": newTestVChannelMeta("v1", 1, state, splitTimeTick),
		}, nil)
}

// newTestVChannelMeta is one vchannel of collection `collectionID` holding a
// single partition, as the recovery snapshot carries it.
func newTestVChannelMeta(vchannel string, collectionID int64, state streamingpb.VChannelState, splitTimeTick uint64) *streamingpb.VChannelMeta {
	return &streamingpb.VChannelMeta{
		Vchannel:      vchannel,
		State:         state,
		SplitTimeTick: splitTimeTick,
		SplitTaskId:   100,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
			CollectionId: collectionID,
			Partitions: []*streamingpb.PartitionInfoOfVChannel{
				{PartitionId: 2},
			},
		},
	}
}

func newTestShardManagerFromSnapshot(t *testing.T, vchannels map[string]*streamingpb.VChannelMeta, segments map[int64]*streamingpb.SegmentAssignmentMeta) ShardManager {
	paramtable.Init()
	resource.InitForTest(t)
	w := mock_wal.NewMockWAL(t)
	w.EXPECT().Unavailable().RunAndReturn(func() <-chan struct{} {
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
			VChannels:          vchannels,
			SegmentAssignments: segments,
			Checkpoint:         &recovery.WALCheckpoint{TimeTick: 100},
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
	assert.Zero(t, m.GetSplitFence(1, "v1").TimeTick)

	// an unfenced or unknown collection has no T_switch.
	assert.Zero(t, m.GetSplitFence(1, "v1").TimeTick)
	assert.Zero(t, m.GetSplitFence(999, "v999").TimeTick)

	// the split message fences the vchannel and records T_switch.
	m.SplitShard(newTestSplitShardImmutableMessage("v1", 1, 2000))
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1"), ErrVChannelFenced)
	assert.Equal(t, uint64(2000), m.GetSplitFence(1, "v1").TimeTick)
}

// newTestSplitShardGenesisImmutableMessage builds the TARGET replica of a split
// broadcast: the genesis of a new vchannel, which is what registers it here.
func newTestSplitShardGenesisImmutableMessage(vchannel string, collectionID int64, partitionIDs []int64, timetick uint64) message.ImmutableSplitShardMessageV2 {
	return newTestSplitShardGenesisImmutableMessageWithBody(vchannel, collectionID, partitionIDs, timetick,
		&message.CreateCollectionRequest{CollectionSchema: &schemapb.CollectionSchema{Name: "col"}})
}

func newTestSplitShardGenesisImmutableMessageWithBody(vchannel string, collectionID int64, partitionIDs []int64, timetick uint64, genesis *message.CreateCollectionRequest) message.ImmutableSplitShardMessageV2 {
	msg := message.NewSplitShardMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.SplitShardMessageHeader{
			CollectionId:    collectionID,
			PartitionIds:    partitionIDs,
			SplitTaskId:     100,
			SourceVchannels: []string{"v1"},
			RoutingModulus:  2,
			Targets: []*message.SplitShardTarget{
				{Vchannel: vchannel, Routing: &schemapb.HashRouting{Buckets: []uint64{0}}},
			},
		}).
		WithBody(&message.SplitShardMessageBody{Genesis: genesis}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID()
	return message.MustAsImmutableSplitShardMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(3)))
}

// newTestRetireImmutableMessage builds the routing commit that retires a
// vchannel: the shard-split routing mask plus a vchannel list without it.
func newTestRetireImmutableMessage(vchannel string, collectionID int64, kept []string, timetick uint64) message.ImmutableAlterCollectionMessageV2 {
	msg := message.NewAlterCollectionMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.AlterCollectionMessageHeader{
			CollectionId: collectionID,
			UpdateMask:   &fieldmaskpb.FieldMask{Paths: []string{message.FieldMaskCollectionShardSplitRouting}},
		}).
		WithBody(&message.AlterCollectionMessageBody{
			Updates: &message.AlterCollectionMessageUpdates{VirtualChannelNames: kept},
		}).
		MustBuildMutable().
		WithTimeTick(timetick).
		WithLastConfirmedUseMessageID()
	return message.MustAsImmutableAlterCollectionMessageV2(msg.IntoImmutableMessage(rmq.NewRmqID(2)))
}

func TestShardManagerCreateVChannel(t *testing.T) {
	m := newTestShardManagerWithVChannelState(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0)
	// a shard split target vchannel of a new collection is registered for DML,
	// exactly as a create collection genesis would register it.
	m.CreateVChannel(newTestSplitShardGenesisImmutableMessage("v2", 7, []int64{8}, 2000))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(7, "v2"))

	// a target without a schema still registers the collection.
	m.CreateVChannel(newTestSplitShardGenesisImmutableMessageNoSchema("v3", 9, []int64{10}, 2500))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(9, "v3"))

	// replaying the genesis of an already-registered vchannel is a no-op.
	m.CreateVChannel(newTestSplitShardGenesisImmutableMessage("v2", 7, []int64{8}, 3000))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(7, "v2"))

	// a genesis for a DIFFERENT vchannel of a collection this pchannel already
	// holds must not take the incumbent's place, and must not be mistaken for
	// an idempotent replay: the newcomer stays unregistered and the incumbent
	// keeps serving.
	m.CreateVChannel(newTestSplitShardGenesisImmutableMessage("v1-successor", 1, []int64{2}, 3000))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(1, "v1"))
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1-successor"), ErrCollectionNotFound)
}

// A genesis body may carry the schema as the pre-2.6.1 serialized bytes
// rather than the CollectionSchema message: the interceptor admits either form.
// The shard manager must resolve both exactly as CreateCollection does, or the
// target is registered with a nil schema and every versioned insert to it fails
// with ErrCollectionSchemaNotFound until a restart rebuilds the entry from the
// persisted meta, which is the very "different before and after a restart"
// case the interceptor's schema guard exists to prevent.
func TestShardManagerCreateVChannelResolvesTheSchemaBytesForm(t *testing.T) {
	m := newTestShardManagerWithVChannelState(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0)
	schema := &schemapb.CollectionSchema{Name: "col", Version: 3, Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	bs, err := proto.Marshal(schema)
	require.NoError(t, err)

	m.CreateVChannel(newTestSplitShardGenesisImmutableMessageWithBody("v9", 9, []int64{10}, 2000,
		&message.CreateCollectionRequest{Schema: bs}))

	version, err := m.CheckWritableAndSchemaVersion("v9", &message.InsertMessageHeader{CollectionId: 9, SchemaVersion: proto.Int32(3)})
	require.NoError(t, err)
	assert.Equal(t, int32(3), version)

	// the same body through CreateCollection resolves identically.
	cmsg := message.NewCreateCollectionMessageBuilderV1().
		WithVChannel("v8").
		WithHeader(&message.CreateCollectionMessageHeader{CollectionId: 8, PartitionIds: []int64{10}}).
		WithBody(&message.CreateCollectionRequest{Schema: bs}).
		MustBuildMutable().
		WithTimeTick(2000).
		WithLastConfirmedUseMessageID()
	m.CreateCollection(message.MustAsImmutableCreateCollectionMessageV1(cmsg.IntoImmutableMessage(rmq.NewRmqID(4))))
	version, err = m.CheckWritableAndSchemaVersion("v8", &message.InsertMessageHeader{CollectionId: 8, SchemaVersion: proto.Int32(3)})
	require.NoError(t, err)
	assert.Equal(t, int32(3), version)
}

// TestShardManagerVChannelAdmissionChecks pins the three admission predicates
// the interceptor consults before it appends a split message.
func TestShardManagerVChannelAdmissionChecks(t *testing.T) {
	m := newTestShardManagerWithVChannelState(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0)

	// creation: free slot, idempotent replay, and the conflicting newcomer.
	assert.NoError(t, m.CheckIfVChannelCanBeCreated(7, "v2"))
	assert.ErrorIs(t, m.CheckIfVChannelCanBeCreated(1, "v1"), ErrCollectionExists)
	assert.ErrorIs(t, m.CheckIfVChannelCanBeCreated(1, "v1-successor"), ErrVChannelConflict)

	// once the incumbent is fenced its slot is free, so the newcomer that was
	// refused a moment ago is admitted -- no teardown message in between.
	m.SplitShard(newTestSplitShardImmutableMessage("v1", 1, 2000))
	assert.NoError(t, m.CheckIfVChannelCanBeCreated(1, "v1-successor"))
}

func newTestSplitShardGenesisImmutableMessageNoSchema(vchannel string, collectionID int64, partitionIDs []int64, timetick uint64) message.ImmutableSplitShardMessageV2 {
	return newTestSplitShardGenesisImmutableMessageWithBody(vchannel, collectionID, partitionIDs, timetick,
		&message.CreateCollectionRequest{})
}

// TestShardManagerSplitShardFreesTheSlot pins what the fence does to this
// pchannel's single registration slot.
//
// The fence is the last thing that happens on the source: no DML follows it,
// its growing segments were sealed while the message was being built, and the
// only reader left is a proxy holding a stale route. So the registration goes
// at the fence -- every partition manager dropped, the entry removed -- and the
// slot is free for the next vchannel of the collection immediately, without
// waiting for a routing commit to come back and reclaim it. What survives is
// the name-keyed tombstone, which is all a stale route needs: SHARD_FENCED,
// refresh, retry.
func TestShardManagerSplitShardFreesTheSlot(t *testing.T) {
	m := newTestShardManagerWithVChannelState(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0)

	m.SplitShard(newTestSplitShardImmutableMessage("v1", 1, 4000))

	// the registration is gone, so the slot is free.
	assert.ErrorIs(t, m.CheckIfCollectionExists(1), ErrCollectionNotFound)
	assert.NoError(t, m.CheckIfVChannelCanBeCreated(1, "v1-successor"))
	// ...and the tombstone still answers, both questions.
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1"), ErrVChannelFenced)
	assert.Equal(t, SplitFence{TimeTick: 4000, TaskID: 100}, m.GetSplitFence(1, "v1"))
	// a vchannel this pchannel never held stays terminal: no refresh sends the
	// write anywhere.
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v-never"), ErrCollectionNotFound)

	// a successor landing on the freed slot is writable, and does not inherit
	// the predecessor's fence.
	m.CreateVChannel(newTestSplitShardGenesisImmutableMessage("v1-successor", 1, []int64{2}, 6000))
	assert.NoError(t, m.CheckIfVChannelCanBeWritten(1, "v1-successor"))
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1"), ErrVChannelFenced)
	assert.Equal(t, uint64(4000), m.GetSplitFence(1, "v1").TimeTick)
}

// TestShardManagerSplitShardAgainRaisesTheFenceTick pins T_switch's definition:
// the tick of the LATEST fence record of the task, not the first.
//
// The broadcaster re-drives a split whose source replica landed but whose task
// was not yet persisted, so one task can place several fence records. Every one
// of them seals the same data -- the vchannel took no DML after the first --
// so the interval between them is empty and taking the larger tick is safe.
// Taking the larger one is also necessary: DataCoord records the tick of the
// last record it saw acked, and a shard manager that kept the first would
// disagree with it about T_switch.
func TestShardManagerSplitShardAgainRaisesTheFenceTick(t *testing.T) {
	m := newTestShardManagerWithVChannelState(t, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0)

	m.SplitShard(newTestSplitShardImmutableMessage("v1", 1, 2000))
	assert.Equal(t, SplitFence{TimeTick: 2000, TaskID: 100}, m.GetSplitFence(1, "v1"))

	// the same task fences again: T_switch moves up.
	m.SplitShard(newTestSplitShardImmutableMessage("v1", 1, 3000))
	assert.Equal(t, SplitFence{TimeTick: 3000, TaskID: 100}, m.GetSplitFence(1, "v1"))

	// a replay of an older record never moves it back.
	m.SplitShard(newTestSplitShardImmutableMessage("v1", 1, 2500))
	assert.Equal(t, SplitFence{TimeTick: 3000, TaskID: 100}, m.GetSplitFence(1, "v1"))

	// another task's fence is refused on the append path (one active task per
	// source); should one ever get this far it must not move a fence it did
	// not place, or the two tasks would carve the source at different ticks.
	m.SplitShard(newTestSplitShardImmutableMessageOfTask("v1", 1, 101, 4000))
	assert.Equal(t, SplitFence{TimeTick: 3000, TaskID: 100}, m.GetSplitFence(1, "v1"))
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1"), ErrVChannelFenced)
}

// TestShardManagerRecoverSplittedVChannelSeedsOnlyTheTombstone: a restart must
// land on the same state the fence left behind, or the slot a live successor
// took would be claimed back by a source that has nothing left to do.
func TestShardManagerRecoverSplittedVChannelSeedsOnlyTheTombstone(t *testing.T) {
	m := newTestShardManagerWithVChannelState(t, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 2000)

	// no registration is rebuilt for a fenced vchannel...
	assert.ErrorIs(t, m.CheckIfCollectionExists(1), ErrCollectionNotFound)
	assert.NoError(t, m.CheckIfVChannelCanBeCreated(1, "v1-successor"))
	// ...only the tombstone, which keeps answering both questions.
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1"), ErrVChannelFenced)
	assert.Equal(t, SplitFence{TimeTick: 2000, TaskID: 100}, m.GetSplitFence(1, "v1"))
}

// TestShardManagerRecoverSkipsSegmentsOfAFencedVChannel: the fence flushes
// every segment of the source in the same message, so a growing one still in
// the snapshot means the meta was persisted in parts. It cannot be rebuilt --
// a fenced vchannel keeps no registration to attach it to -- and it must not be
// attached to whatever entry sits under its collection id, which would hand a
// successor a segment that is not its own. Skipping it is also what keeps the
// recovery from panicking on a collection it can no longer find.
func TestShardManagerRecoverSkipsSegmentsOfAFencedVChannel(t *testing.T) {
	m := newTestShardManagerFromSnapshot(t,
		map[string]*streamingpb.VChannelMeta{
			"v1": newTestVChannelMeta("v1", 1, streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, 2000),
		},
		map[int64]*streamingpb.SegmentAssignmentMeta{
			1001: {
				CollectionId: 1,
				PartitionId:  2,
				SegmentId:    1001,
				Vchannel:     "v1",
				State:        streamingpb.SegmentAssignmentState_SEGMENT_ASSIGNMENT_STATE_GROWING,
				Stat: &streamingpb.SegmentAssignmentStat{
					MaxBinarySize:         100,
					ModifiedBinarySize:    50,
					CreateSegmentTimeTick: 101,
				},
			},
		})

	assert.ErrorIs(t, m.CheckIfCollectionExists(1), ErrCollectionNotFound)
	assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1"), ErrVChannelFenced)
	// the segment was not rebuilt anywhere: there is no partition manager left
	// on this pchannel that could hold it.
	assert.ErrorIs(t, m.CheckIfSegmentCanBeFlushed(PartitionUniqueKey{CollectionID: 1, PartitionID: 2}, 1001), ErrCollectionNotFound)
}

// TestShardManagerRecoverTwoLiveVChannelsOfOneCollection: the registration map
// holds one entry per collection per pchannel, and a split's source no longer
// competes for it (it is fenced, so it is not rebuilt at all). Two LIVE
// vchannels of one collection are therefore a placement that should not exist
// -- but if the snapshot carries one, which of them keeps the entry must not
// depend on the order the map happened to be walked in, or a restart would
// leave a different shard unwritable each time.
func TestShardManagerRecoverTwoLiveVChannelsOfOneCollection(t *testing.T) {
	for i := 0; i < 8; i++ {
		m := newTestShardManagerFromSnapshot(t,
			map[string]*streamingpb.VChannelMeta{
				"v0": newTestVChannelMeta("v0", 1, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0),
				"v1": newTestVChannelMeta("v1", 1, streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, 0),
			}, nil)
		assert.NoError(t, m.CheckIfVChannelCanBeWritten(1, "v0"))
		// the loser is not fenced, only unregistered: a route to it is a wrong
		// route, and stays terminal.
		assert.ErrorIs(t, m.CheckIfVChannelCanBeWritten(1, "v1"), ErrCollectionNotFound)
	}
}
