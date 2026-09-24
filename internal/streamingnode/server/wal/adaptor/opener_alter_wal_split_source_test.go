package adaptor

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

// TestHandleAlterWALAdvanceCheckpointsStageSeedsADrainedSplitSource: the
// advance stage seeds a new-WAL position for a drained SPLITTED source exactly
// as for every other vchannel, although the FLUSHING wait skipped it.
//
// The two stages answer different questions. The wait asks whether every byte
// is persisted, and a drained source's frozen checkpoint says nothing about
// that. The seed decides where the source is read from on the new backend: the
// flusher rebuilds every vchannel of the recovery snapshot on the next open,
// the source included, from the position DataCoord holds for it -- decoded
// under the NEW WAL's name (getRecoveryInfos). Left unseeded, that position
// would still be an old-WAL message id and could not be decoded, and the
// delegators that keep serving the source until adoption would seek it the
// same way. Seeded at the switch tick the rebuilt service acks at once, past
// its reseeded close gate, and closes again; DataCoord's drain predicate,
// checkpoint at or past T_switch, keeps holding.
func TestHandleAlterWALAdvanceCheckpointsStageSeedsADrainedSplitSource(t *testing.T) {
	channel := types.PChannelInfo{
		Name:       "alter-wal-split-source-test",
		Term:       1,
		AccessMode: types.AccessModeRW,
	}
	vchannels := []*streamingpb.VChannelMeta{
		{Vchannel: channel.Name + "_1v0", State: streamingpb.VChannelState_VCHANNEL_STATE_NORMAL, CheckpointTimeTick: 90},
		// A source fenced at 100 whose data sync service closed once it drained.
		{Vchannel: channel.Name + "_2v0", State: streamingpb.VChannelState_VCHANNEL_STATE_SPLITTED, SplitTimeTick: 100, CheckpointTimeTick: 100, SplitTaskId: 7},
	}

	catalog := mock_metastore.NewMockStreamingNodeCataLog(t)
	catalog.EXPECT().ListVChannel(mock.Anything, channel.Name).Return(vchannels, nil)
	catalog.EXPECT().SaveConsumeCheckpoint(mock.Anything, channel.Name, mock.Anything).Return(nil)

	var seeded *datapb.UpdateChannelCheckpointRequest
	mixCoord := mocks.NewMockMixCoordClient(t)
	mixCoord.EXPECT().UpdateChannelCheckpoint(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *datapb.UpdateChannelCheckpointRequest, _ ...grpc.CallOption) (*commonpb.Status, error) {
			seeded = req
			return merr.Success(), nil
		})
	mixCoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).RunAndReturn(
		func(_ context.Context, req *datapb.GetChannelRecoveryInfoRequest, _ ...grpc.CallOption) (*datapb.GetChannelRecoveryInfoResponse, error) {
			return &datapb.GetChannelRecoveryInfoResponse{
				Status: merr.Success(),
				Info:   &datapb.VchannelInfo{ChannelName: req.GetVchannel(), SeekPosition: seededPosition(seeded, req.GetVchannel())},
			}, nil
		})
	f := syncutil.NewFuture[internaltypes.MixCoordClient]()
	f.Set(mixCoord)
	resource.InitForTest(t, resource.OptStreamingNodeCatalog(catalog), resource.OptMixCoordClient(f))

	previousDefaultWALName := message.GetDefaultWALName()
	defer message.RegisterDefaultWALName(previousDefaultWALName)

	snapshot := &recovery.RecoverySnapshot{
		Checkpoint: &recovery.WALCheckpoint{
			MessageID: rmq.NewRmqID(1),
			TimeTick:  140,
			AlterWalState: &streamingpb.AlterWALState{
				TargetWalName: commonpb.WALName_Kafka,
				TimeTick:      140,
				Stage:         streamingpb.AlterWALStage_ADVANCE_CHECKPOINT,
			},
		},
	}

	err := (&openerAdaptorImpl{}).handleAlterWALAdvanceCheckpointsStage(
		context.Background(),
		&wal.OpenOption{Channel: channel},
		snapshot,
	)
	require.NoError(t, err)
	require.NotNil(t, seeded)

	positions := make(map[string]*commonpb.WALName)
	for _, pos := range seeded.GetChannelCheckpoints() {
		walName := pos.GetWALName()
		positions[pos.GetChannelName()] = &walName
		assert.Equal(t, uint64(140), pos.GetTimestamp(), pos.GetChannelName())
		assert.Equal(t, commonpb.WALName_Kafka, walName, pos.GetChannelName())
	}
	assert.Len(t, positions, 2)
	assert.Contains(t, positions, vchannels[0].Vchannel)
	assert.Contains(t, positions, vchannels[1].Vchannel, "a drained split source is seeded on the new WAL like every other vchannel")
}

// seededPosition is the position the advance stage seeded for vchannel, nil
// when it seeded none.
func seededPosition(req *datapb.UpdateChannelCheckpointRequest, vchannel string) *msgpb.MsgPosition {
	for _, pos := range req.GetChannelCheckpoints() {
		if pos.GetChannelName() == vchannel {
			return pos
		}
	}
	return nil
}
