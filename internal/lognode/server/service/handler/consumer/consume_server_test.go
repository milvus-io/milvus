package consumer

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus/internal/lognode/server/walmanager"
	mock_wal "github.com/milvus-io/milvus/internal/mocks/lognode/server/wal"
	mock_walmanager "github.com/milvus-io/milvus/internal/mocks/lognode/server/walmanager"
	mock_logpb "github.com/milvus-io/milvus/internal/mocks/proto/logpb"
	mock_message "github.com/milvus-io/milvus/internal/mocks/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/mq/mqimpl/rocksmq/server"
	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/message"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/service/contextutil"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestCreateConsumeServer(t *testing.T) {
	manager := mock_walmanager.NewMockManager(t)
	grpcConsumeServer := mock_logpb.NewMockLogNodeHandlerService_ConsumeServer(t)

	// No metadata in context should report error
	grpcConsumeServer.EXPECT().Context().Return(context.Background())
	assertCreateConsumeServerFail(t, manager, grpcConsumeServer)

	// wal not exist should report error.
	meta, _ := metadata.FromOutgoingContext(contextutil.WithCreateConsumer(context.Background(), &logpb.CreateConsumerRequest{
		ChannelName:   "test",
		Term:          1,
		DeliverPolicy: logpb.NewDeliverAll(),
	}))
	ctx := metadata.NewIncomingContext(context.Background(), meta)
	grpcConsumeServer.ExpectedCalls = nil
	grpcConsumeServer.EXPECT().Context().Return(ctx)
	manager.EXPECT().GetAvailableWAL("test", int64(1)).Return(nil, errors.New("wal not exist"))
	assertCreateConsumeServerFail(t, manager, grpcConsumeServer)

	// Return error if create scanner failed.
	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Read(mock.Anything, mock.Anything).Return(nil, errors.New("create scanner failed"))
	manager.ExpectedCalls = nil
	manager.EXPECT().GetAvailableWAL("test", int64(1)).Return(l, nil)
	assertCreateConsumeServerFail(t, manager, grpcConsumeServer)

	// Return error if send created failed.
	grpcConsumeServer.EXPECT().Send(mock.Anything).Return(errors.New("send created failed"))
	l.ExpectedCalls = nil
	s := mock_wal.NewMockScanner(t)
	l.EXPECT().Read(mock.Anything, mock.Anything).Return(s, nil)
	assertCreateConsumeServerFail(t, manager, grpcConsumeServer)

	// Passed.
	grpcConsumeServer.EXPECT().Send(mock.Anything).Unset()
	grpcConsumeServer.EXPECT().Send(mock.Anything).Return(nil)

	l.EXPECT().Channel().Return(&logpb.PChannelInfo{
		Name: "test",
		Term: 1,
	})
	server, err := CreateConsumeServer(manager, grpcConsumeServer)
	assert.NoError(t, err)
	assert.NotNil(t, server)
}

func TestConsumeServerRecvArm(t *testing.T) {
	grpcConsumerServer := mock_logpb.NewMockLogNodeHandlerService_ConsumeServer(t)
	server := &ConsumeServer{
		consumeServer: &consumeGrpcServerHelper{
			LogNodeHandlerService_ConsumeServer: grpcConsumerServer,
		},
		logger: log.With(),
	}
	recvCh := make(chan *logpb.ConsumeRequest)
	grpcConsumerServer.EXPECT().Recv().RunAndReturn(func() (*logpb.ConsumeRequest, error) {
		req, ok := <-recvCh
		if ok {
			return req, nil
		}
		return nil, io.EOF
	})

	// Test recv arm
	recvFailureCh := typeutil.NewChanSignal[error]()
	ch := make(chan error)
	go func() {
		ch <- server.recvLoop(recvFailureCh)
	}()

	// should be blocked.
	testChannelShouldBeBlocked(t, ch, 500*time.Millisecond)
	testChannelShouldBeBlocked(t, recvFailureCh.Chan(), 500*time.Millisecond)

	// cancelConsumerCh should be closed after receiving close request.
	recvCh <- &logpb.ConsumeRequest{
		Request: &logpb.ConsumeRequest_Close{},
	}
	<-recvFailureCh.Chan()
	testChannelShouldBeBlocked(t, ch, 500*time.Millisecond)

	// Test io.EOF
	close(recvCh)
	assert.NoError(t, <-ch)

	// Test unexpected recv error.
	grpcConsumerServer.EXPECT().Recv().Unset()
	grpcConsumerServer.EXPECT().Recv().Return(nil, io.ErrUnexpectedEOF)
	recvFailureCh = typeutil.NewChanSignal[error]()
	assert.ErrorIs(t, server.recvLoop(recvFailureCh), io.ErrUnexpectedEOF)
}

func TestConsumerServeSendArm(t *testing.T) {
	grpcConsumerServer := mock_logpb.NewMockLogNodeHandlerService_ConsumeServer(t)
	scanner := mock_wal.NewMockScanner(t)
	s := &ConsumeServer{
		consumeServer: &consumeGrpcServerHelper{
			LogNodeHandlerService_ConsumeServer: grpcConsumerServer,
		},
		logger:  log.With(),
		scanner: scanner,
	}
	ctx, cancel := context.WithCancel(context.Background())
	grpcConsumerServer.EXPECT().Context().Return(ctx)
	grpcConsumerServer.EXPECT().Send(mock.Anything).RunAndReturn(func(cr *logpb.ConsumeResponse) error { return nil }).Times(2)

	scanCh := make(chan message.ImmutableMessage)
	scanner.EXPECT().Chan().Return(scanCh)
	scanner.EXPECT().Close().Return(nil).Times(3)

	// Test send arm
	recvFailureCh := typeutil.NewChanSignal[error]()
	ch := make(chan error)
	go func() {
		ch <- s.sendLoop(recvFailureCh)
	}()

	// should be blocked.
	testChannelShouldBeBlocked(t, ch, 500*time.Millisecond)

	// test send.
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().MessageID().Return(&server.RmqID{})
	msg.EXPECT().EstimateSize().Return(0)
	msg.EXPECT().Payload().Return([]byte{})
	properties := mock_message.NewMockRProperties(t)
	properties.EXPECT().ToRawMap().Return(map[string]string{})
	msg.EXPECT().Properties().Return(properties)
	scanCh <- msg

	// test scanner broken.
	scanner.EXPECT().Error().Return(io.EOF)
	close(scanCh)
	assert.ErrorIs(t, <-ch, io.EOF)

	// test cancel by client.
	scanner.EXPECT().Chan().Unset()
	scanner.EXPECT().Chan().Return(make(<-chan message.ImmutableMessage))
	go func() {
		ch <- s.sendLoop(recvFailureCh)
	}()
	// should be blocked.
	testChannelShouldBeBlocked(t, ch, 500*time.Millisecond)
	recvFailureCh.Release()
	assert.NoError(t, <-ch)

	// test cancel by server context.
	recvFailureCh = typeutil.NewChanSignal[error]()
	go func() {
		ch <- s.sendLoop(recvFailureCh)
	}()
	testChannelShouldBeBlocked(t, ch, 500*time.Millisecond)
	cancel()
	assert.ErrorIs(t, <-ch, context.Canceled)
}

func assertCreateConsumeServerFail(t *testing.T, manager walmanager.Manager, grpcConsumeServer logpb.LogNodeHandlerService_ConsumeServer) {
	server, err := CreateConsumeServer(manager, grpcConsumeServer)
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
