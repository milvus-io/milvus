package flusherimpl

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/mock_metastore"
	"github.com/milvus-io/milvus/internal/mocks/mock_storage"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal/interceptors"
	internaltypes "github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/streamingutil"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/proto/datapb"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	if code := m.Run(); code != 0 {
		os.Exit(code)
	}
}

func TestWALFlusher(t *testing.T) {
	streamingutil.SetStreamingServiceEnabled()
	defer streamingutil.UnsetStreamingServiceEnabled()

	mixcoord := newMockMixcoord(t, false)
	mixcoord.EXPECT().GetPChannelInfo(mock.Anything, mock.Anything).Return(&rootcoordpb.GetPChannelInfoResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_Success,
		},
		Collections: []*rootcoordpb.CollectionInfoOnPChannel{
			{
				CollectionId: 100,
				Vchannel:     "vchannel-1",
			},
			{
				CollectionId: 100,
				Vchannel:     "vchannel-2",
			},
		},
	}, nil)
	snMeta := mock_metastore.NewMockStreamingNodeCataLog(t)
	snMeta.EXPECT().GetConsumeCheckpoint(mock.Anything, mock.Anything).Return(nil, nil)
	snMeta.EXPECT().SaveConsumeCheckpoint(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	fMixcoord := syncutil.NewFuture[internaltypes.MixCoordClient]()
	fMixcoord.Set(mixcoord)
	resource.InitForTest(
		t,
		resource.OptMixCoordClient(fMixcoord),
		resource.OptStreamingNodeCatalog(snMeta),
		resource.OptChunkManager(mock_storage.NewMockChunkManager(t)),
	)
	l := newMockWAL(t, false)
	param := &interceptors.InterceptorBuildParam{
		WAL: syncutil.NewFuture[wal.WAL](),
	}
	param.WAL.Set(l)
	flusher := RecoverWALFlusher(param)
	time.Sleep(5 * time.Second)
	flusher.Close()
}

func newMockMixcoord(t *testing.T, maybe bool) *mocks.MockMixCoordClient {
	mixcoord := mocks.NewMockMixCoordClient(t)
	failureCnt := atomic.NewInt32(2)
	mixcoord.EXPECT().DropVirtualChannel(mock.Anything, mock.Anything).Return(&datapb.DropVirtualChannelResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
	}, nil)
	expect := mixcoord.EXPECT().GetChannelRecoveryInfo(mock.Anything, mock.Anything).RunAndReturn(
		func(ctx context.Context, request *datapb.GetChannelRecoveryInfoRequest, option ...grpc.CallOption,
		) (*datapb.GetChannelRecoveryInfoResponse, error) {
			if failureCnt.Dec() > 0 {
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
	walName := w.EXPECT().WALName().Return("rocksmq")
	if maybe {
		walName.Maybe()
	}
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
