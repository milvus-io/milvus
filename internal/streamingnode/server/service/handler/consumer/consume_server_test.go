package consumer

import (
	"context"
	"io"
	"testing"
	"time"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_walmanager"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/log"
	"github.com/milvus-io/milvus/pkg/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/mocks/streaming/util/mock_message"
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

func TestCreateConsumeServer(t *testing.T) {
	manager := mock_walmanager.NewMockManager(t)
	grpcConsumeServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ConsumeServer(t)

	// No metadata in context should report error
	grpcConsumeServer.EXPECT().Context().Return(context.Background())
	assertCreateConsumeServerFail(t, manager, grpcConsumeServer)

	// wal not exist should report error.
	meta, _ := metadata.FromOutgoingContext(contextutil.WithCreateConsumer(context.Background(), &streamingpb.CreateConsumerRequest{
		Pchannel: &streamingpb.PChannelInfo{
			Name: "test",
			Term: 1,
		},
	}))
	ctx := metadata.NewIncomingContext(context.Background(), meta)
	grpcConsumeServer.ExpectedCalls = nil
	grpcConsumeServer.EXPECT().Context().Return(ctx)
	manager.EXPECT().GetAvailableWAL(types.PChannelInfo{Name: "test", Term: int64(1)}).Return(nil, errors.New("wal not exist"))
	assertCreateConsumeServerFail(t, manager, grpcConsumeServer)

	// Return error if send created failed.
	l := mock_wal.NewMockWAL(t)
	manager.ExpectedCalls = nil
	l.EXPECT().WALName().Return("test")
	manager.EXPECT().GetAvailableWAL(types.PChannelInfo{Name: "test", Term: int64(1)}).Return(l, nil)
	grpcConsumeServer.EXPECT().Send(mock.Anything).Return(errors.New("send created failed"))
	assertCreateConsumeServerFail(t, manager, grpcConsumeServer)

	// Return error if recv failed.
	grpcConsumeServer.EXPECT().Send(mock.Anything).Unset()
	grpcConsumeServer.EXPECT().Send(mock.Anything).Return(nil)
	grpcConsumeServer.EXPECT().Recv().Return(nil, io.ErrUnexpectedEOF)
	assertCreateConsumeServerFail(t, manager, grpcConsumeServer)

	// Return error if create scanner failed.
	grpcConsumeServer.EXPECT().Recv().Unset()
	grpcConsumeServer.EXPECT().Recv().Return(&streamingpb.ConsumeRequest{
		Request: &streamingpb.ConsumeRequest_CreateVchannelConsumer{
			CreateVchannelConsumer: &streamingpb.CreateVChannelConsumerRequest{
				Vchannel: "test",
			},
		},
	}, nil)
	l = mock_wal.NewMockWAL(t)
	l.EXPECT().Read(mock.Anything, mock.Anything).Return(nil, errors.New("create scanner failed"))
	l.EXPECT().WALName().Return("test")
	manager.ExpectedCalls = nil
	manager.EXPECT().GetAvailableWAL(types.PChannelInfo{Name: "test", Term: int64(1)}).Return(l, nil)
	assertCreateConsumeServerFail(t, manager, grpcConsumeServer)

	// Return error if send created failed.
	grpcConsumeServer.EXPECT().Send(mock.Anything).Unset()
	i := 0
	grpcConsumeServer.EXPECT().Send(mock.Anything).RunAndReturn(func(cr *streamingpb.ConsumeResponse) error {
		if i >= 1 {
			return io.ErrUnexpectedEOF
		}
		i++
		return nil
	})
	l.EXPECT().Read(mock.Anything, mock.Anything).Unset()
	s := mock_wal.NewMockScanner(t)
	s.EXPECT().Close().Return(nil)
	l.EXPECT().Read(mock.Anything, mock.Anything).Return(s, nil)
	assertCreateConsumeServerFail(t, manager, grpcConsumeServer)

	// Passed.
	grpcConsumeServer.EXPECT().Send(mock.Anything).Unset()
	grpcConsumeServer.EXPECT().Send(mock.Anything).Return(nil)

	l.EXPECT().Channel().Return(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})
	server, err := CreateConsumeServer(manager, grpcConsumeServer)
	assert.NoError(t, err)
	assert.NotNil(t, server)
}

func TestConsumeServerRecvArm(t *testing.T) {
	grpcConsumerServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ConsumeServer(t)
	server := &ConsumeServer{
		consumeServer: &consumeGrpcServerHelper{
			StreamingNodeHandlerService_ConsumeServer: grpcConsumerServer,
		},
		logger:  log.With(),
		closeCh: make(chan struct{}),
		metrics: newConsumerMetrics("test"),
	}
	recvCh := make(chan *streamingpb.ConsumeRequest)
	grpcConsumerServer.EXPECT().Recv().RunAndReturn(func() (*streamingpb.ConsumeRequest, error) {
		req, ok := <-recvCh
		if ok {
			return req, nil
		}
		return nil, io.EOF
	})

	// Test recv arm
	ch := make(chan error)
	go func() {
		ch <- server.recvLoop()
	}()

	// should be blocked.
	testChannelShouldBeBlocked(t, ch, 500*time.Millisecond)
	testChannelShouldBeBlocked(t, server.closeCh, 500*time.Millisecond)

	// cancelConsumerCh should be closed after receiving close request.
	recvCh <- &streamingpb.ConsumeRequest{
		Request: &streamingpb.ConsumeRequest_Close{},
	}
	close(recvCh)
	<-server.closeCh
	assert.NoError(t, <-ch)

	// Test unexpected recv error.
	grpcConsumerServer.EXPECT().Recv().Unset()
	grpcConsumerServer.EXPECT().Recv().Return(nil, io.ErrUnexpectedEOF)
	server.closeCh = make(chan struct{})
	assert.ErrorIs(t, server.recvLoop(), io.ErrUnexpectedEOF)
}

func TestConsumerServeSendArm(t *testing.T) {
	grpcConsumerServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ConsumeServer(t)
	scanner := mock_wal.NewMockScanner(t)
	s := &ConsumeServer{
		consumeServer: &consumeGrpcServerHelper{
			StreamingNodeHandlerService_ConsumeServer: grpcConsumerServer,
		},
		logger:  log.With(),
		scanner: scanner,
		closeCh: make(chan struct{}),
		metrics: newConsumerMetrics("test"),
	}
	ctx, cancel := context.WithCancel(context.Background())
	grpcConsumerServer.EXPECT().Context().Return(ctx)
	grpcConsumerServer.EXPECT().Send(mock.Anything).RunAndReturn(func(cr *streamingpb.ConsumeResponse) error { return nil }).Times(7)

	scanCh := make(chan message.ImmutableMessage, 5)
	scanner.EXPECT().Chan().Return(scanCh)
	scanner.EXPECT().Close().Return(nil).Times(3)

	// Test send arm
	ch := make(chan error)
	go func() {
		ch <- s.sendLoop()
	}()

	// should be blocked.
	testChannelShouldBeBlocked(t, ch, 500*time.Millisecond)

	// test send.
	msg := mock_message.NewMockImmutableMessage(t)
	msg.EXPECT().MessageID().Return(walimplstest.NewTestMessageID(1))
	msg.EXPECT().EstimateSize().Return(0)
	msg.EXPECT().Payload().Return([]byte{})
	properties := mock_message.NewMockRProperties(t)
	properties.EXPECT().ToRawMap().Return(map[string]string{})
	msg.EXPECT().Properties().Return(properties)
	scanCh <- msg

	// test send txn message.
	txnMsg := mock_message.NewMockImmutableTxnMessage(t)
	txnMsg.EXPECT().Begin().Return(msg)
	txnMsg.EXPECT().RangeOver(mock.Anything).RunAndReturn(func(f func(message.ImmutableMessage) error) error {
		for i := 0; i < 3; i++ {
			if err := f(msg); err != nil {
				return err
			}
		}
		return nil
	})
	txnMsg.EXPECT().Commit().Return(msg)
	scanCh <- txnMsg

	// test scanner broken.
	scanner.EXPECT().Error().Return(io.EOF)
	close(scanCh)
	err := <-ch
	sErr := status.AsStreamingError(err)
	assert.Equal(t, streamingpb.StreamingCode_STREAMING_CODE_INNER, sErr.Code)

	// test cancel by client.
	scanner.EXPECT().Chan().Unset()
	scanner.EXPECT().Chan().Return(make(<-chan message.ImmutableMessage))
	go func() {
		ch <- s.sendLoop()
	}()
	// should be blocked.
	testChannelShouldBeBlocked(t, ch, 500*time.Millisecond)
	close(s.closeCh)
	assert.NoError(t, <-ch)

	// test cancel by server context.
	s.closeCh = make(chan struct{})
	go func() {
		ch <- s.sendLoop()
	}()
	testChannelShouldBeBlocked(t, ch, 500*time.Millisecond)
	cancel()
	assert.ErrorIs(t, <-ch, context.Canceled)
}

func assertCreateConsumeServerFail(t *testing.T, manager walmanager.Manager, grpcConsumeServer streamingpb.StreamingNodeHandlerService_ConsumeServer) {
	server, err := CreateConsumeServer(manager, grpcConsumeServer)
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
