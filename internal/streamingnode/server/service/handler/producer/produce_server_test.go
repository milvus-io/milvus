package producer

import (
	"context"
	"io"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"go.uber.org/atomic"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_walmanager"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestCreateProduceServer(t *testing.T) {
	manager := mock_walmanager.NewMockManager(t)
	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)

	// No metadata in context should report error
	grpcProduceServer.EXPECT().Context().Return(context.Background())
	assertCreateProduceServerFail(t, manager, grpcProduceServer)

	// wal not exist should report error.
	meta, _ := metadata.FromOutgoingContext(contextutil.WithCreateProducer(context.Background(), &streamingpb.CreateProducerRequest{
		Pchannel: &streamingpb.PChannelInfo{
			Name: "test",
			Term: 1,
		},
	}))
	ctx := metadata.NewIncomingContext(context.Background(), meta)
	grpcProduceServer.ExpectedCalls = nil
	grpcProduceServer.EXPECT().Context().Return(ctx)
	manager.EXPECT().GetAvailableWAL(types.PChannelInfo{Name: "test", Term: 1}).Return(nil, errors.New("wal not exist"))
	assertCreateProduceServerFail(t, manager, grpcProduceServer)

	// Return error if create scanner failed.
	l := mock_wal.NewMockWAL(t)
	l.EXPECT().WALName().Return("test")
	manager.ExpectedCalls = nil
	l.EXPECT().WALName().Return("test")
	manager.EXPECT().GetAvailableWAL(types.PChannelInfo{Name: "test", Term: 1}).Return(l, nil)
	grpcProduceServer.EXPECT().Send(mock.Anything).Return(errors.New("send created failed"))
	assertCreateProduceServerFail(t, manager, grpcProduceServer)

	// Passed.
	grpcProduceServer.EXPECT().Send(mock.Anything).Unset()
	grpcProduceServer.EXPECT().Send(mock.Anything).Return(nil)

	l.EXPECT().Channel().Return(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})
	server, err := CreateProduceServer(manager, grpcProduceServer)
	assert.NoError(t, err)
	assert.NotNil(t, server)
}

func TestProduceSendArm(t *testing.T) {
	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grpcProduceServer.EXPECT().Context().Return(ctx)

	success := atomic.NewInt32(0)
	produceFailure := atomic.NewBool(false)
	grpcProduceServer.EXPECT().Send(mock.Anything).RunAndReturn(func(pr *streamingpb.ProduceResponse) error {
		if !produceFailure.Load() {
			success.Inc()
			return nil
		}
		return errors.New("send failure")
	})

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Available().Return(make(<-chan struct{}))

	p := &ProduceServer{
		wal: wal,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:           log.With(),
		produceMessageCh: make(chan *streamingpb.ProduceMessageResponse, 10),
		appendWG:         sync.WaitGroup{},
	}

	// test send arm success.
	ch := make(chan error)
	go func() {
		ch <- p.sendLoop()
	}()

	p.produceMessageCh <- &streamingpb.ProduceMessageResponse{
		RequestId: 1,
		Response: &streamingpb.ProduceMessageResponse_Result{
			Result: &streamingpb.ProduceMessageResponseResult{
				Id: &messagespb.MessageID{
					Id: walimplstest.NewTestMessageID(1).Marshal(),
				},
			},
		},
	}
	close(p.produceMessageCh)
	assert.Nil(t, <-ch)
	assert.Equal(t, int32(2), success.Load())

	// test send arm failure
	p = &ProduceServer{
		wal: wal,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:           log.With(),
		produceMessageCh: make(chan *streamingpb.ProduceMessageResponse, 10),
		appendWG:         sync.WaitGroup{},
	}

	ch = make(chan error)
	go func() {
		ch <- p.sendLoop()
	}()

	success.Store(0)
	produceFailure.Store(true)

	p.produceMessageCh <- &streamingpb.ProduceMessageResponse{
		RequestId: 1,
		Response: &streamingpb.ProduceMessageResponse_Result{
			Result: &streamingpb.ProduceMessageResponseResult{
				Id: &messagespb.MessageID{
					Id: walimplstest.NewTestMessageID(1).Marshal(),
				},
			},
		},
	}
	assert.Error(t, <-ch)

	// test send arm failure
	p = &ProduceServer{
		wal: wal,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:           log.With(),
		produceMessageCh: make(chan *streamingpb.ProduceMessageResponse, 10),
		appendWG:         sync.WaitGroup{},
	}

	ch = make(chan error)
	go func() {
		ch <- p.sendLoop()
	}()
	cancel()
	assert.Error(t, <-ch)
}

func TestProduceServerRecvArm(t *testing.T) {
	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	recvCh := make(chan *streamingpb.ProduceRequest)
	grpcProduceServer.EXPECT().Recv().RunAndReturn(func() (*streamingpb.ProduceRequest, error) {
		req, ok := <-recvCh
		if ok {
			return req, nil
		}
		return nil, io.EOF
	})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grpcProduceServer.EXPECT().Context().Return(ctx)

	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})
	l.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, mm message.MutableMessage, f func(*wal.AppendResult, error)) {
		msgID := walimplstest.NewTestMessageID(1)
		f(&wal.AppendResult{
			MessageID: msgID,
			TimeTick:  100,
		}, nil)
	})
	l.EXPECT().IsAvailable().Return(true)

	p := &ProduceServer{
		wal: l,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:           log.With(),
		produceMessageCh: make(chan *streamingpb.ProduceMessageResponse, 10),
		appendWG:         sync.WaitGroup{},
		metrics:          newProducerMetrics(l.Channel()),
	}

	// Test send arm
	ch := make(chan error)
	go func() {
		ch <- p.recvLoop()
	}()

	req := &streamingpb.ProduceRequest{
		Request: &streamingpb.ProduceRequest_Produce{
			Produce: &streamingpb.ProduceMessageRequest{
				RequestId: 1,
				Message: &messagespb.Message{
					Payload: []byte("test"),
					Properties: map[string]string{
						"_v": "1",
						"_t": strconv.FormatInt(int64(message.MessageTypeTimeTick), 10),
					},
				},
			},
		},
	}
	recvCh <- req

	msg := <-p.produceMessageCh
	assert.Equal(t, int64(1), msg.RequestId)
	assert.NotNil(t, msg.Response.(*streamingpb.ProduceMessageResponse_Result).Result.Id)

	// Test send error.
	l.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Unset()
	l.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, mm message.MutableMessage, f func(*wal.AppendResult, error)) {
		f(nil, errors.New("append error"))
	})

	req.Request.(*streamingpb.ProduceRequest_Produce).Produce.RequestId = 2
	recvCh <- req
	msg = <-p.produceMessageCh
	assert.Equal(t, int64(2), msg.RequestId)
	assert.NotNil(t, msg.Response.(*streamingpb.ProduceMessageResponse_Error).Error)

	// Test send close and EOF.
	recvCh <- &streamingpb.ProduceRequest{
		Request: &streamingpb.ProduceRequest_Close{},
	}
	p.appendWG.Wait()

	close(recvCh)
	// produceMessageCh should be closed.
	<-p.produceMessageCh
	// recvLoop should closed.
	err := <-ch
	assert.NoError(t, err)

	p = &ProduceServer{
		wal: l,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:           log.With(),
		produceMessageCh: make(chan *streamingpb.ProduceMessageResponse),
		appendWG:         sync.WaitGroup{},
	}

	// Test recv failure.
	grpcProduceServer.EXPECT().Recv().Unset()
	grpcProduceServer.EXPECT().Recv().RunAndReturn(func() (*streamingpb.ProduceRequest, error) {
		return nil, io.ErrUnexpectedEOF
	})

	assert.ErrorIs(t, p.recvLoop(), io.ErrUnexpectedEOF)
}

func assertCreateProduceServerFail(t *testing.T, manager walmanager.Manager, grpcProduceServer streamingpb.StreamingNodeHandlerService_ProduceServer) {
	server, err := CreateProduceServer(manager, grpcProduceServer)
	assert.Nil(t, server)
	assert.Error(t, err)
}

func testChannelShouldBeBlocked[T any](t *testing.T, ch <-chan T, d time.Duration) {
	// should be blocked.
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	select {
	case <-ch:
		t.Errorf("should be block")
	case <-ctx.Done():
	}
}
