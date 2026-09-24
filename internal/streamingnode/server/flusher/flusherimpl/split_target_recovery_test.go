//go:build test
// +build test

package flusherimpl

import (
	"context"
	"math"
	"testing"

	rawkafka "github.com/confluentinc/confluent-kafka-go/kafka"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message/adaptor"
	msgkafka "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/kafka"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// splitTargetMeta is the recovery meta of a shard split target vchannel whose
// genesis replica landed at genesisID/genesisTick.
func splitTargetMeta(vchannel string, collectionID int64, genesisID message.MessageID, genesisTick uint64) *streamingpb.VChannelMeta {
	return &streamingpb.VChannelMeta{
		Vchannel:       vchannel,
		State:          streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
		CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: collectionID},
		SplitGenesisCheckpoint: &streamingpb.WALCheckpoint{
			MessageId: message.MustMarshalMessageID(genesisID),
			TimeTick:  genesisTick,
		},
	}
}

// TestSplitTargetGenesisRecoveryInfoMatchesTheGenesisSpawn: a target recovered
// before datacoord knows it starts from exactly the position its data sync
// service is spawned from when the genesis replica is consumed, and nothing
// but a live split target gets such a recovery info.
func TestSplitTargetGenesisRecoveryInfoMatchesTheGenesisSpawn(t *testing.T) {
	genesis := newFlusherSplitShardMessage(t, "v2", "v1", []string{"v2", "v3"}, 7, 100)
	meta := splitTargetMeta("v2", 7, genesis.LastConfirmedMessageID(), genesis.TimeTick())

	info := splitTargetGenesisRecoveryInfo("v2", meta)
	require.NotNil(t, info)
	assert.NoError(t, merr.Error(info.GetStatus()))
	assert.Equal(t, int64(7), info.GetInfo().GetCollectionID())
	assert.Equal(t, "v2", info.GetInfo().GetChannelName())
	assert.Empty(t, info.GetSegmentsNotCreatedByStreaming())
	assert.True(t, proto.Equal(
		genesisSeekPosition(genesis.VChannel(), genesis.LastConfirmedMessageID(), genesis.TimeTick()),
		info.GetInfo().GetSeekPosition()))

	// Not a split target: a CreateCollection vchannel, no meta, a dropped target.
	assert.Nil(t, splitTargetGenesisRecoveryInfo("v2", &streamingpb.VChannelMeta{
		Vchannel: "v2", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL,
	}))
	assert.Nil(t, splitTargetGenesisRecoveryInfo("v2", nil))
	dropped := proto.Clone(meta).(*streamingpb.VChannelMeta)
	dropped.State = streamingpb.VChannelState_VCHANNEL_STATE_DROPPED
	assert.Nil(t, splitTargetGenesisRecoveryInfo("v2", dropped))
	// A target that was itself split again before datacoord knew it still has
	// data to flush up to its fence, so it is recovered too.
	splitted := proto.Clone(meta).(*streamingpb.VChannelMeta)
	splitted.State = streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED
	assert.NotNil(t, splitTargetGenesisRecoveryInfo("v2", splitted))
}

// TestWALFlusher_RecoversAnUnseededSplitTargetFromItsGenesis reproduces a
// StreamingNode restart after a split target's genesis landed but before the
// SplitShard ack callback's CommitShardSplit seeded its checkpoint: datacoord
// answers ErrChannelNotAvailable for the target. The flusher must still build
// the target's data sync service, from the genesis position, and scan the WAL
// from no later than that position. Every vchannel skipped before keeps being
// skipped: one whose collection datacoord does not know (no genesis recorded)
// and a split target datacoord has dropped.
func TestWALFlusher_RecoversAnUnseededSplitTargetFromItsGenesis(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()

	const collectionID = int64(100)
	positionOf := func(vchannel string, id int64, tick uint64) *msgpb.MsgPosition {
		return &msgpb.MsgPosition{
			ChannelName: vchannel,
			MsgID:       adaptor.MustGetMQWrapperIDFromMessage(rmq.NewRmqID(id)).Serialize(),
			Timestamp:   tick,
		}
	}
	answers := map[string]*datapb.GetChannelRecoveryInfoResponse{
		"normal": {Info: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "normal", SeekPosition: positionOf("normal", 10, 1000)}},
		// seeded by CommitShardSplit (or by its own checkpoint report): datacoord's answer wins.
		"seeded-target":   {Info: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "seeded-target", SeekPosition: positionOf("seeded-target", 20, 2000)}},
		"unseeded-target": {Status: merr.Status(merr.WrapErrChannelNotAvailable("unseeded-target", "start position is nil"))},
		"unknown-create":  {Status: merr.Status(merr.WrapErrChannelNotAvailable("unknown-create", "start position is nil"))},
		"dropped-target":  {Info: &datapb.VchannelInfo{ChannelName: "dropped-target", SeekPosition: &msgpb.MsgPosition{Timestamp: math.MaxUint64}}},
	}
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *datapb.GetChannelRecoveryInfoRequest, _ ...grpc.CallOption) (*datapb.GetChannelRecoveryInfoResponse, error) {
			return answers[req.GetVchannel()], nil
		})
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	resource.InitForTest(t,
		resource.OptMixCoordClient(fMixcoord),
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
	)

	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().GetSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "ID", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "Vector", DataType: schemapb.DataType_FloatVector},
		},
	}, nil).Maybe()
	l := newMockWAL(t, true)
	flusher := &WALFlusherImpl{
		wal:             syncutil.NewFuture[wal.WAL](),
		logger:          mlog.With(),
		RecoveryStorage: rs,
	}
	flusher.wal.Set(l)

	snapshot := &recovery.RecoverySnapshot{
		VChannels: map[string]*streamingpb.VChannelMeta{
			"normal":          {Vchannel: "normal", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: collectionID}},
			"seeded-target":   splitTargetMeta("seeded-target", collectionID, rmq.NewRmqID(15), 1500),
			"unseeded-target": splitTargetMeta("unseeded-target", collectionID, rmq.NewRmqID(5), 500),
			"unknown-create":  {Vchannel: "unknown-create", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: collectionID}},
			"dropped-target":  splitTargetMeta("dropped-target", collectionID, rmq.NewRmqID(1), 100),
		},
		Checkpoint: &recovery.WALCheckpoint{MessageID: rmq.NewRmqID(30), TimeTick: 3000},
	}

	infos, checkpoint, err := flusher.getRecoveryInfos(context.Background(), []string{
		"normal", "seeded-target", "unseeded-target", "unknown-create", "dropped-target",
	}, snapshot.VChannels)
	require.NoError(t, err)
	assert.ElementsMatch(t, []string{"normal", "seeded-target", "unseeded-target"}, keysOf(infos))
	assert.True(t, proto.Equal(answers["seeded-target"].GetInfo().GetSeekPosition(), infos["seeded-target"].GetInfo().GetSeekPosition()))
	assert.True(t, proto.Equal(genesisSeekPosition("unseeded-target", rmq.NewRmqID(5), 500), infos["unseeded-target"].GetInfo().GetSeekPosition()))
	// The scan starts no later than the unseeded target's genesis, so its writes are replayed into its service.
	require.NotNil(t, checkpoint)
	assert.True(t, checkpoint.EQ(rmq.NewRmqID(5)), "checkpoint %s", checkpoint)

	fc, checkpoint, err := flusher.buildFlusherComponents(context.Background(), l, snapshot)
	require.NoError(t, err)
	defer fc.Close()
	assert.True(t, checkpoint.EQ(rmq.NewRmqID(5)), "checkpoint %s", checkpoint)
	assert.True(t, fc.hasDataSyncService("normal"))
	assert.True(t, fc.hasDataSyncService("seeded-target"))
	assert.True(t, fc.hasDataSyncService("unseeded-target"), "the unseeded split target must get its data sync service")
	assert.False(t, fc.hasDataSyncService("unknown-create"))
	assert.False(t, fc.hasDataSyncService("dropped-target"))
}

// TestWALFlusher_SplitTargetRecoversNoEarlierThanItsGenesis: a split target on
// a pchannel in the collection's start-position table gets datacoord's
// collection creation position before CommitShardSplit seeds it. The flusher
// must recover it from its genesis instead, and the pchannel scan must start
// from the corrected position; a position datacoord recorded for the target
// itself, and every vchannel without a recorded genesis, are left alone.
func TestWALFlusher_SplitTargetRecoversNoEarlierThanItsGenesis(t *testing.T) {
	const collectionID = int64(100)
	positionOf := func(vchannel string, id int64, tick uint64) *msgpb.MsgPosition {
		return &msgpb.MsgPosition{
			ChannelName: vchannel,
			MsgID:       adaptor.MustGetMQWrapperIDFromMessage(rmq.NewRmqID(id)).Serialize(),
			Timestamp:   tick,
		}
	}
	answers := map[string]*datapb.GetChannelRecoveryInfoResponse{
		// The collection's creation start position: timestamp 0, an old message id.
		"target-on-start-pchannel": {Info: &datapb.VchannelInfo{
			CollectionID: collectionID, ChannelName: "target-on-start-pchannel",
			SeekPosition: positionOf("target-on-start-pchannel", 3, 0), DroppedSegmentIds: []int64{7},
		}},
		// Same tick as the genesis but an earlier message id.
		"target-earlier-id": {Info: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "target-earlier-id", SeekPosition: positionOf("target-earlier-id", 40, 450)}},
		// Seeded by CommitShardSplit from the append result: same tick, later id.
		"target-seeded-at-genesis": {Info: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "target-seeded-at-genesis", SeekPosition: positionOf("target-seeded-at-genesis", 51, 500)}},
		// A real, later checkpoint.
		"target-later-checkpoint": {Info: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "target-later-checkpoint", SeekPosition: positionOf("target-later-checkpoint", 60, 600)}},
		"plain":                   {Info: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "plain", SeekPosition: positionOf("plain", 2, 0)}},
	}
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *datapb.GetChannelRecoveryInfoRequest, _ ...grpc.CallOption) (*datapb.GetChannelRecoveryInfoResponse, error) {
			return proto.Clone(answers[req.GetVchannel()]).(*datapb.GetChannelRecoveryInfoResponse), nil
		})
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	resource.InitForTest(t, resource.OptMixCoordClient(fMixcoord))
	flusher := newTestWALFlusher(nil)
	flusher.wal = syncutil.NewFuture[wal.WAL]()
	flusher.wal.Set(newMockWAL(t, true))

	metas := map[string]*streamingpb.VChannelMeta{
		"target-on-start-pchannel": splitTargetMeta("target-on-start-pchannel", collectionID, rmq.NewRmqID(50), 500),
		"target-earlier-id":        splitTargetMeta("target-earlier-id", collectionID, rmq.NewRmqID(45), 450),
		"target-seeded-at-genesis": splitTargetMeta("target-seeded-at-genesis", collectionID, rmq.NewRmqID(50), 500),
		"target-later-checkpoint":  splitTargetMeta("target-later-checkpoint", collectionID, rmq.NewRmqID(50), 500),
		"plain":                    {Vchannel: "plain", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: collectionID}},
	}
	targets := []string{"target-on-start-pchannel", "target-earlier-id", "target-seeded-at-genesis", "target-later-checkpoint"}

	infos, checkpoint, err := flusher.getRecoveryInfos(context.Background(), targets, metas)
	require.NoError(t, err)
	start := infos["target-on-start-pchannel"].GetInfo()
	assert.True(t, proto.Equal(genesisSeekPosition("target-on-start-pchannel", rmq.NewRmqID(50), 500), start.GetSeekPosition()))
	assert.Equal(t, []int64{7}, start.GetDroppedSegmentIds(), "only the seek position is replaced")
	assert.True(t, proto.Equal(genesisSeekPosition("target-earlier-id", rmq.NewRmqID(45), 450), infos["target-earlier-id"].GetInfo().GetSeekPosition()))
	for _, kept := range []string{"target-seeded-at-genesis", "target-later-checkpoint"} {
		assert.True(t, proto.Equal(answers[kept].GetInfo().GetSeekPosition(), infos[kept].GetInfo().GetSeekPosition()), kept)
	}
	// The scan starts from the corrected minimum (45), not the creation position (3).
	require.NotNil(t, checkpoint)
	assert.True(t, checkpoint.EQ(rmq.NewRmqID(45)), "checkpoint %s", checkpoint)

	// A vchannel without a recorded genesis keeps datacoord's answer, and the
	// scan start follows it.
	infos, checkpoint, err = flusher.getRecoveryInfos(context.Background(), append(targets, "plain"), metas)
	require.NoError(t, err)
	assert.True(t, proto.Equal(answers["plain"], infos["plain"]))
	assert.True(t, checkpoint.EQ(rmq.NewRmqID(2)), "checkpoint %s", checkpoint)

	// No info at all is filled in from the genesis rather than dereferenced.
	clamped := flusher.recoverNoEarlierThanSplitGenesis(context.Background(), "target-on-start-pchannel",
		metas["target-on-start-pchannel"], message.WALNameRocksmq, &datapb.GetChannelRecoveryInfoResponse{})
	assert.Equal(t, collectionID, clamped.GetInfo().GetCollectionID())
	assert.True(t, proto.Equal(genesisSeekPosition("target-on-start-pchannel", rmq.NewRmqID(50), 500), clamped.GetInfo().GetSeekPosition()))
	// Message ids of another WAL are not compared; only the tick decides.
	assert.False(t, positionBeforeSplitGenesis(message.WALNameKafka, positionOf("x", 40, 500), metas["target-on-start-pchannel"].GetSplitGenesisCheckpoint()))
	assert.False(t, positionBeforeSplitGenesis(message.WALNameRocksmq, &msgpb.MsgPosition{Timestamp: 500}, metas["target-on-start-pchannel"].GetSplitGenesisCheckpoint()))
}

// TestWALFlusher_SplitTargetGenesisOfASwitchedAwayWALIsNeverDecoded pins what
// keeps a split target's genesis from reaching the scan start after a WAL
// backend switch (AlterWAL) that landed between the target's genesis and a
// restart, before CommitShardSplit seeded the target.
//
// split_genesis_checkpoint is never rewritten by the switch, so it still holds
// an id of the old WAL. The switch's ADVANCE_CHECKPOINT stage
// (openerAdaptorImpl.handleAlterWALAdvanceCheckpointsStage) gives datacoord a
// checkpoint on the new WAL for every vchannel of the pchannel's catalog, the
// target included, at the pchannel checkpoint's tick; that tick is at or after
// the AlterWAL message's, which is after the genesis (nothing is appended to
// the old WAL behind an AlterWAL). So datacoord answers a position that is not
// before the genesis by tick, the old-WAL id is not compared, and the answer and
// the scan start stay on the new WAL. The old id is only unmarshaled, by its own
// WAL's registered unmarshaler.
func TestWALFlusher_SplitTargetGenesisOfASwitchedAwayWALIsNeverDecoded(t *testing.T) {
	const (
		collectionID    = int64(100)
		genesisTick     = uint64(500)
		switchTick      = uint64(3000)
		newWALInitialID = rawkafka.Offset(0)
	)
	switchPosition := func(vchannel string) *msgpb.MsgPosition {
		return &msgpb.MsgPosition{
			ChannelName: vchannel,
			MsgID:       adaptor.MustGetMQWrapperIDFromMessage(msgkafka.NewKafkaID(newWALInitialID)).Serialize(),
			Timestamp:   switchTick,
			WALName:     commonpb.WALName_Kafka,
		}
	}
	answers := map[string]*datapb.GetChannelRecoveryInfoResponse{
		"target": {Info: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "target", SeekPosition: switchPosition("target")}},
		"source": {Info: &datapb.VchannelInfo{CollectionID: collectionID, ChannelName: "source", SeekPosition: switchPosition("source")}},
	}
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, req *datapb.GetChannelRecoveryInfoRequest, _ ...grpc.CallOption) (*datapb.GetChannelRecoveryInfoResponse, error) {
			return proto.Clone(answers[req.GetVchannel()]).(*datapb.GetChannelRecoveryInfoResponse), nil
		})
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	resource.InitForTest(t, resource.OptMixCoordClient(fMixcoord))

	newWAL := mock_wal.NewMockWAL(t)
	newWAL.EXPECT().WALName().Return(message.WALNameKafka).Maybe()
	flusher := newTestWALFlusher(nil)
	flusher.wal = syncutil.NewFuture[wal.WAL]()
	flusher.wal.Set(newWAL)

	metas := map[string]*streamingpb.VChannelMeta{
		// The genesis landed on the old WAL (RocksMQ) before the switch.
		"target": splitTargetMeta("target", collectionID, rmq.NewRmqID(50), genesisTick),
		"source": {Vchannel: "source", State: streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, CollectionInfo: &streamingpb.CollectionInfoOfVChannel{CollectionId: collectionID}},
	}
	require.Equal(t, message.WALNameRocksmq, message.MustUnmarshalMessageID(metas["target"].GetSplitGenesisCheckpoint().GetMessageId()).WALName())

	infos, checkpoint, err := flusher.getRecoveryInfos(context.Background(), []string{"target", "source"}, metas)
	require.NoError(t, err)
	assert.True(t, proto.Equal(answers["target"], infos["target"]), "the switch's checkpoint of the target is kept")
	require.NotNil(t, checkpoint)
	assert.Equal(t, message.WALNameKafka, checkpoint.WALName())
	assert.True(t, checkpoint.EQ(msgkafka.NewKafkaID(newWALInitialID)), "checkpoint %s", checkpoint)
}

func keysOf[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}
