package producer

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/server/walmanager"
	mock_wal "github.com/milvus-io/milvus/internal/mocks/lognode/server/wal"
	mock_walmanager "github.com/milvus-io/milvus/internal/mocks/lognode/server/walmanager"
	mock_logpb "github.com/milvus-io/milvus/internal/mocks/proto/logpb"
	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/contextutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mq/msgstream/mqwrapper"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestCreateProduceServer(t *testing.T) {
	manager := mock_walmanager.NewMockManager(t)
	grpcProduceServer := mock_logpb.NewMockLogNodeHandlerService_ProduceServer(t)

	// No metadata in context should report error
	grpcProduceServer.EXPECT().Context().Return(context.Background())
	assertCreateProduceServerFail(t, manager, grpcProduceServer)

	// wal not exist should report error.
	meta, _ := metadata.FromOutgoingContext(contextutil.WithCreateProducer(context.Background(), &logpb.CreateProducerRequest{
		ChannelName: "test",
		Term:        1,
	}))
	ctx := metadata.NewIncomingContext(context.Background(), meta)
	grpcProduceServer.ExpectedCalls = nil
	grpcProduceServer.EXPECT().Context().Return(ctx)
	manager.EXPECT().GetAvailableWAL("test", int64(1)).Return(nil, errors.New("wal not exist"))
	assertCreateProduceServerFail(t, manager, grpcProduceServer)

	// Return error if create scanner failed.
	l := mock_wal.NewMockWAL(t)
	manager.ExpectedCalls = nil
	manager.EXPECT().GetAvailableWAL("test", int64(1)).Return(l, nil)
	grpcProduceServer.EXPECT().Send(mock.Anything).Return(errors.New("send created failed"))
	assertCreateProduceServerFail(t, manager, grpcProduceServer)

	// Passed.
	grpcProduceServer.EXPECT().Send(mock.Anything).Unset()
	grpcProduceServer.EXPECT().Send(mock.Anything).Return(nil)

	l.EXPECT().Channel().Return(&logpb.PChannelInfo{
		Name: "test",
		Term: 1,
	})
	server, err := CreateProduceServer(manager, grpcProduceServer)
	assert.NoError(t, err)
	assert.NotNil(t, server)
}

func TestProduceSendArm(t *testing.T) {
}

func TestProduceServerRecvArm(t *testing.T) {
	grpcProduceServer := mock_logpb.NewMockLogNodeHandlerService_ProduceServer(t)
	recvCh := make(chan *logpb.ProduceRequest)
	grpcProduceServer.EXPECT().Recv().RunAndReturn(func() (*logpb.ProduceRequest, error) {
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
	l.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, mm message.MutableMessage, f func(mqwrapper.MessageID, error)) {
		msgID := &server.RmqID{MessageID: 1}
		f(msgID, nil)
	})

	p := &ProduceServer{
		wal: l,
		produceServer: &produceGrpcServerHelper{
			LogNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:           log.With(),
		produceMessageCh: make(chan *logpb.ProduceMessageResponse, 10),
		appendWG:         sync.WaitGroup{},
	}

	// Test send arm
	ch := make(chan error)
	go func() {
		ch <- p.recvLoop()
	}()

	req := &logpb.ProduceRequest{
		Request: &logpb.ProduceRequest_Produce{
			Produce: &logpb.ProduceMessageRequest{
				RequestID: 1,
				Message: &logpb.Message{
					Payload:    []byte("test"),
					Properties: map[string]string{},
				},
			},
		},
	}
	recvCh <- req

	msg := <-p.produceMessageCh
	assert.Equal(t, int64(1), msg.RequestID)
	assert.NotNil(t, msg.Response.(*logpb.ProduceMessageResponse_Result).Result.Id)

	// Test send error.
	l.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Unset()
	l.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, mm message.MutableMessage, f func(mqwrapper.MessageID, error)) {
		f(nil, errors.New("append error"))
	})

	req.Request.(*logpb.ProduceRequest_Produce).Produce.RequestID = 2
	recvCh <- req
	msg = <-p.produceMessageCh
	assert.Equal(t, int64(2), msg.RequestID)
	assert.NotNil(t, msg.Response.(*logpb.ProduceMessageResponse_Error).Error)

	// Test send close and EOF.
	recvCh <- &logpb.ProduceRequest{
		Request: &logpb.ProduceRequest_Close{},
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
			LogNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:           log.With(),
		produceMessageCh: make(chan *logpb.ProduceMessageResponse),
		appendWG:         sync.WaitGroup{},
	}

	// Test recv failure.
	grpcProduceServer.EXPECT().Recv().Unset()
	grpcProduceServer.EXPECT().Recv().RunAndReturn(func() (*logpb.ProduceRequest, error) {
		return nil, io.ErrUnexpectedEOF
	})

	assert.ErrorIs(t, p.recvLoop(), io.ErrUnexpectedEOF)

	// Test Context Cancel.
	grpcProduceServer.EXPECT().Recv().Unset()
	grpcProduceServer.EXPECT().Recv().RunAndReturn(func() (*logpb.ProduceRequest, error) {
		return &logpb.ProduceRequest{
			Request: &logpb.ProduceRequest_Produce{
				Produce: &logpb.ProduceMessageRequest{
					RequestID: 1,
					Message: &logpb.Message{
						Payload:    []byte("test"),
						Properties: map[string]string{},
					},
				},
			},
		}, nil
	})
	ch = make(chan error)
	go func() {
		ch <- p.recvLoop()
	}()

	testChannelShouldBeBlocked(t, ch, 1*time.Second)
	// cancel the context.
	cancel()
	assert.ErrorIs(t, <-ch, context.Canceled)
}

func assertCreateProduceServerFail(t *testing.T, manager walmanager.Manager, grpcProduceServer logpb.LogNodeHandlerService_ProduceServer) {
	server, err := CreateProduceServer(manager, grpcProduceServer)
	assert.Nil(t, server)
	assert.Error(t, err)
}

func testChannelShouldBeBlocked[T any](t *testing.T, ch <-chan T, d time.Duration) {
	// should be blocked.
	ctx, cancel := context.WithTimeout(context.Background(), d)
	defer cancel()
	select {
	case _ = <-ch:
		t.Errorf("should be block")
	case <-ctx.Done():
	}
}
