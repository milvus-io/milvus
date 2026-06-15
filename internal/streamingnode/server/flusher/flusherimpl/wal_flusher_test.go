//go:build test
// +build test

package flusherimpl

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/flushcommon/pipeline"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/wal/mock_recovery"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/adaptor/rate"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/recovery"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/util"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

func TestMain(m *testing.M) {
	defaultCollectionNotFoundTolerance = 2

	paramtable.Init()
	paramtable.SetRole(typeutil.StandaloneRole)
	paramtable.Get().MQCfg.Type.SwapTempValue(message.WALNameRocksmq.String())
	util.InitAndSelectWALName()
	if code := m.Run(); code != 0 {
		os.Exit(code)
	}
}

func TestWALFlusher(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()

	mixcoord := newMockMixcoord(t, false)
	mixcoord.EXPECT().AllocSegment(mock.Anything, mock.Anything).Return(&datapb.AllocSegmentResponse{
		Status: merr.Status(nil),
	}, nil)
	mixcoord.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(&datapb.DropVirtualChannelResponse{
		Status: merr.Status(nil),
	}, nil)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().GetSchema(mock.Anything, mock.Anything, mock.Anything).Return(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "ID", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "Vector", DataType: schemapb.DataType_FloatVector},
		},
	}, nil)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(nil)
	rs.EXPECT().Close().Return()
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
	)
	l := newMockWAL(t, false)
	rateLimitComponent := rate.NewWALRateLimitComponent(l.Channel())
	defer rateLimitComponent.Close()
	param := &RecoverWALFlusherParam{
		ChannelInfo: l.Channel(),
		WAL:         syncutil.NewFuture[wal.WAL](),
		RecoverySnapshot: &recovery.RecoverySnapshot{
			VChannels: map[string]*streamingpb.VChannelMeta{
				"vchannel-1": {
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 100,
					},
				},
				"vchannel-2": {
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 100,
					},
				},
				"vchannel-3": {
					CollectionInfo: &streamingpb.CollectionInfoOfVChannel{
						CollectionId: 100,
					},
				},
			},
			Checkpoint: &recovery.WALCheckpoint{
				TimeTick: 0,
			},
		},
		RecoveryStorage:    rs,
		RateLimitComponent: rateLimitComponent,
	}
	param.WAL.Set(l)
	flusher := RecoverWALFlusher(param)
	time.Sleep(5 * time.Second)
	flusher.Close()
}

func TestWALFlusher_DispatchDefersAckSyncUpDropCollectionObserve(t *testing.T) {
	resource.InitForTest(t, resource.OptChunkManager(mock_storage.NewMockChunkManager(t)))

	rs := mock_recovery.NewMockRecoveryStorage(t)

	flusher := newTestWALFlusher(rs)
	flusher.flusherComponents.dataServices["vchannel-1"] = newDataSyncServiceWrapper(
		"vchannel-1",
		make(chan *msgstream.MsgPack, 1),
		&pipeline.DataSyncService{},
		0,
	)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).
		RunAndReturn(func(context.Context, message.ImmutableMessage) error {
			_, ok := flusher.flusherComponents.dataServices["vchannel-1"]
			require.False(t, ok)
			return errors.New("observe failed")
		}).
		Once()

	msg := newAckSyncUpDropCollectionMessage(t, "vchannel-1")

	require.ErrorContains(t, flusher.dispatch(msg), "observe failed")
}

func TestWALFlusher_DispatchObservesAckSyncUpTruncateCollectionBeforeHandling(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(errors.New("observe failed")).Once()

	flusher := newTestWALFlusher(rs)
	msg := newAckSyncUpTruncateCollectionMessage(t, "vchannel-1")

	require.ErrorContains(t, flusher.dispatch(msg), "observe failed")
}

func TestWALFlusher_DispatchObservesTruncateCollectionBeforeHandlingWithoutAckSyncUp(t *testing.T) {
	rs := mock_recovery.NewMockRecoveryStorage(t)
	rs.EXPECT().ObserveMessage(mock.Anything, mock.Anything).Return(errors.New("observe failed")).Once()

	flusher := newTestWALFlusher(rs)
	flusher.flusherComponents = nil
	msg := newTruncateCollectionMessage(t, "vchannel-1")

	require.ErrorContains(t, flusher.dispatch(msg), "observe failed")
}

func newTestWALFlusher(rs recovery.RecoveryStorage) *WALFlusherImpl {
	return &WALFlusherImpl{
		notifier:        syncutil.NewAsyncTaskNotifier[struct{}](),
		logger:          log.With(),
		RecoveryStorage: rs,
		flusherComponents: &flusherComponents{
			dataServices: make(map[string]*dataSyncServiceWrapper),
			logger:       log.With(),
			rs:           rs,
		},
	}
}

func newAckSyncUpDropCollectionMessage(t *testing.T, vchannel string) message.ImmutableMessage {
	t.Helper()
	broadcast := message.NewDropCollectionMessageBuilderV1().
		WithHeader(&message.DropCollectionMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&msgpb.DropCollectionRequest{
			Base: &commonpb.MsgBase{},
		}).
		WithBroadcast([]string{vchannel}, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast().
		WithBroadcastID(1)
	msgs := broadcast.SplitIntoMutableMessage()
	require.Len(t, msgs, 1)
	return msgs[0].
		WithTimeTick(100).
		WithLastConfirmed(rmq.NewRmqID(1)).
		IntoImmutableMessage(rmq.NewRmqID(2))
}

func newAckSyncUpTruncateCollectionMessage(t *testing.T, vchannel string) message.ImmutableMessage {
	t.Helper()
	broadcast := message.NewTruncateCollectionMessageBuilderV2().
		WithHeader(&message.TruncateCollectionMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&message.TruncateCollectionMessageBody{}).
		WithBroadcast([]string{vchannel}, message.OptBuildBroadcastAckSyncUp()).
		MustBuildBroadcast().
		WithBroadcastID(1)
	msgs := broadcast.SplitIntoMutableMessage()
	require.Len(t, msgs, 1)
	return msgs[0].
		WithTimeTick(100).
		WithLastConfirmed(rmq.NewRmqID(1)).
		IntoImmutableMessage(rmq.NewRmqID(2))
}

func newTruncateCollectionMessage(t *testing.T, vchannel string) message.ImmutableMessage {
	t.Helper()
	return message.NewTruncateCollectionMessageBuilderV2().
		WithVChannel(vchannel).
		WithHeader(&message.TruncateCollectionMessageHeader{
			CollectionId: 100,
		}).
		WithBody(&message.TruncateCollectionMessageBody{}).
		MustBuildMutable().
		WithTimeTick(100).
		WithLastConfirmed(rmq.NewRmqID(1)).
		IntoImmutableMessage(rmq.NewRmqID(2))
}

func newMockMixcoord(t *testing.T, maybe bool) *mocks.MockMixCoordClient {
	mixcoord := mocks.NewMockMixCoordClient(t)
	mixcoord.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(&datapb.DropVirtualChannelResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil)
	expect := mixcoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *datapb.GetChannelRecoveryInfoRequest, option ...grpc.CallOption,
		) (*datapb.GetChannelRecoveryInfoResponse, error) {
			switch request.Vchannel {
			case "vchannel-3":
				return &datapb.GetChannelRecoveryInfoResponse{
					Status: merr.Status(merr.ErrCollectionNotFound),
				}, nil
			case "vchannel-2":
				return &datapb.GetChannelRecoveryInfoResponse{
					Status: merr.Status(merr.ErrChannelNotAvailable),
				}, nil
			}
			messageID := 1
			b := make([]byte, 8)
			common.Endian.PutUint64(b, uint64(messageID))
			return &datapb.GetChannelRecoveryInfoResponse{
				Info: &datapb.VchannelInfo{
					ChannelName:  request.GetVchannel(),
					SeekPosition: &msgpb.MsgPosition{MsgID: b},
				},
				Schema: &schemapb.CollectionSchema{
					Fields: []*schemapb.FieldSchema{
						{FieldID: 100, Name: "ID", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
						{FieldID: 101, Name: "Vector", DataType: schemapb.DataType_FloatVector},
					},
				},
			}, nil
		})
	if maybe {
		expect.Maybe()
	}
	return mixcoord
}

func newMockWAL(t *testing.T, maybe bool) *mock_wal.MockWAL {
	w := mock_wal.NewMockWAL(t)
	w.EXPECT().WALName().Return(message.WALNameRocksmq).Maybe()
	w.EXPECT().Channel().Return(types.PChannelInfo{Name: "pchannel"}).Maybe()
	read := w.EXPECT().Read(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, option wal.ReadOption) (wal.Scanner, error) {
			handler := option.MesasgeHandler
			scanner := mock_wal.NewMockScanner(t)
			ch := make(chan message.ImmutableMessage, 4)
			msg := message.CreateTestCreateCollectionMessage(t, 2, 100, rmq.NewRmqID(100))
			ch <- msg.IntoImmutableMessage(rmq.NewRmqID(105))
			msg = message.CreateTestCreateSegmentMessage(t, 2, 101, rmq.NewRmqID(101))
			ch <- msg.IntoImmutableMessage(rmq.NewRmqID(106))
			msg = message.CreateTestTimeTickSyncMessage(t, 2, 102, rmq.NewRmqID(101))
			ch <- msg.IntoImmutableMessage(rmq.NewRmqID(107))
			msg = message.CreateTestDropCollectionMessage(t, 2, 103, rmq.NewRmqID(104))
			ch <- msg.IntoImmutableMessage(rmq.NewRmqID(108))
			scanner.EXPECT().Chan().RunAndReturn(func() <-chan message.ImmutableMessage {
				return ch
			})
			scanner.EXPECT().Close().RunAndReturn(func() error {
				handler.Close()
				return nil
			})
			return scanner, nil
		})
	if maybe {
		read.Maybe()
	}
	return w
}
