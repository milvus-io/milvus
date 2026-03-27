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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_wal"
	"github.com/milvus-io/milvus/internal/mocks/streamingnode/server/mock_walmanager"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/streamingnode/server/wal"
	"github.com/milvus-io/milvus/internal/streamingnode/server/walmanager"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	"github.com/milvus-io/milvus/pkg/v2/log"
	"github.com/milvus-io/milvus/pkg/v2/mocks/proto/mock_streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/ratelimit"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestMain(m *testing.M) {
	paramtable.Init()
	m.Run()
}

func TestCreateProduceServer(t *testing.T) {
	resource.InitForTest(t)
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
	l.EXPECT().WALName().Return(message.WALNameTest)
	manager.ExpectedCalls = nil
	l.EXPECT().WALName().Return(message.WALNameTest)
	l.EXPECT().Register(mock.Anything).Return()
	l.EXPECT().Unregister(mock.Anything).Return().Maybe()
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
				Id: &commonpb.MessageID{
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
				Id: &commonpb.MessageID{
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
			MessageID:              msgID,
			LastConfirmedMessageID: msgID,
			TimeTick:               100,
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

func TestProduceServerSendLoop_RateLimitMessage(t *testing.T) {
	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grpcProduceServer.EXPECT().Context().Return(ctx)

	rateLimitSent := atomic.NewBool(false)
	grpcProduceServer.EXPECT().Send(mock.Anything).RunAndReturn(func(pr *streamingpb.ProduceResponse) error {
		if pr.GetRateLimit() != nil {
			rateLimitSent.Store(true)
		}
		return nil
	})

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Available().Return(make(<-chan struct{}))

	p := &ProduceServer{
		wal: wal,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:             log.With(),
		produceMessageCh:   make(chan *streamingpb.ProduceMessageResponse, 10),
		rateLimitMessageCh: make(chan ratelimit.RateLimitState, 10),
		appendWG:           sync.WaitGroup{},
	}

	ch := make(chan error)
	go func() {
		ch <- p.sendLoop()
	}()

	// Send rate limit message
	p.rateLimitMessageCh <- ratelimit.RateLimitState{
		State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
		Rate:  1024,
	}

	// Wait a bit for processing
	time.Sleep(50 * time.Millisecond)
	assert.True(t, rateLimitSent.Load())

	close(p.produceMessageCh)
	assert.NoError(t, <-ch)
}

func TestProduceServerSendLoop_RateLimitMessageError(t *testing.T) {
	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grpcProduceServer.EXPECT().Context().Return(ctx)

	grpcProduceServer.EXPECT().Send(mock.Anything).RunAndReturn(func(pr *streamingpb.ProduceResponse) error {
		if pr.GetRateLimit() != nil {
			return errors.New("rate limit send error")
		}
		return nil
	})

	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Available().Return(make(<-chan struct{}))

	p := &ProduceServer{
		wal: wal,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:             log.With(),
		produceMessageCh:   make(chan *streamingpb.ProduceMessageResponse, 10),
		rateLimitMessageCh: make(chan ratelimit.RateLimitState, 10),
		appendWG:           sync.WaitGroup{},
	}

	ch := make(chan error)
	go func() {
		ch <- p.sendLoop()
	}()

	// Send rate limit message
	p.rateLimitMessageCh <- ratelimit.RateLimitState{
		State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
		Rate:  1024,
	}

	err := <-ch
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "rate limit send error")
}

func TestProduceServerSendLoop_WALUnavailable(t *testing.T) {
	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grpcProduceServer.EXPECT().Context().Return(ctx)

	closedSent := atomic.NewBool(false)
	grpcProduceServer.EXPECT().Send(mock.Anything).RunAndReturn(func(pr *streamingpb.ProduceResponse) error {
		if pr.GetClose() != nil {
			closedSent.Store(true)
		}
		return nil
	})

	availableCh := make(chan struct{})
	wal := mock_wal.NewMockWAL(t)
	wal.EXPECT().Available().Return(availableCh)

	p := &ProduceServer{
		wal: wal,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:           log.With(),
		produceMessageCh: make(chan *streamingpb.ProduceMessageResponse, 10),
		appendWG:         sync.WaitGroup{},
	}

	ch := make(chan error)
	go func() {
		ch <- p.sendLoop()
	}()

	// Signal WAL unavailable
	close(availableCh)

	// Wait for graceful shutdown
	err := <-ch
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "send loop is stopped for close of wal")
	assert.True(t, closedSent.Load())
}

func TestProduceServerRecvLoop_InvalidMessage(t *testing.T) {
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

	ch := make(chan error)
	go func() {
		ch <- p.recvLoop()
	}()

	// Send message with invalid type (no properties means invalid type)
	req := &streamingpb.ProduceRequest{
		Request: &streamingpb.ProduceRequest_Produce{
			Produce: &streamingpb.ProduceMessageRequest{
				RequestId: 1,
				Message: &messagespb.Message{
					Payload:    []byte("test"),
					Properties: map[string]string{}, // Empty properties means no message type
				},
			},
		},
	}
	recvCh <- req

	msg := <-p.produceMessageCh
	assert.Equal(t, int64(1), msg.RequestId)
	assert.NotNil(t, msg.Response.(*streamingpb.ProduceMessageResponse_Error).Error)

	close(recvCh)
	err := <-ch
	assert.NoError(t, err)
}

func TestProduceServerRecvLoop_UnknownRequestType(t *testing.T) {
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
	grpcProduceServer.EXPECT().Context().Return(ctx).Maybe()

	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})

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

	ch := make(chan error)
	go func() {
		ch <- p.recvLoop()
	}()

	// Send unknown request type (nil Request is treated as unknown)
	req := &streamingpb.ProduceRequest{
		Request: nil,
	}
	recvCh <- req

	// Should skip without error
	close(recvCh)
	err := <-ch
	assert.NoError(t, err)
}

func TestProduceServerRecvLoop_WALUnavailable(t *testing.T) {
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
	grpcProduceServer.EXPECT().Context().Return(ctx).Maybe()

	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})
	l.EXPECT().IsAvailable().Return(false) // WAL is unavailable
	l.EXPECT().Register(mock.Anything).Return().Maybe()
	l.EXPECT().Unregister(mock.Anything).Return().Maybe()

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

	ch := make(chan error)
	go func() {
		ch <- p.recvLoop()
	}()

	// Send produce request when WAL is unavailable
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

	// Should not send any response since handleProduce returns early
	close(recvCh)
	err := <-ch
	assert.NoError(t, err)

	// produceMessageCh should be empty (closed by recvLoop)
	_, ok := <-p.produceMessageCh
	assert.False(t, ok)
}

func TestProduceServerUpdateRateLimitState(t *testing.T) {
	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	grpcProduceServer.EXPECT().Context().Return(context.Background())

	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})
	l.EXPECT().Register(mock.Anything).Return().Maybe()
	l.EXPECT().Unregister(mock.Anything).Return().Maybe()

	p := &ProduceServer{
		wal: l,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:             log.With(),
		produceMessageCh:   make(chan *streamingpb.ProduceMessageResponse, 10),
		rateLimitMessageCh: make(chan ratelimit.RateLimitState, 10),
		appendWG:           sync.WaitGroup{},
		metrics:            newProducerMetrics(l.Channel()),
	}

	// Test successful update
	state := ratelimit.RateLimitState{
		State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
		Rate:  1024,
	}
	p.UpdateRateLimitState(state)

	select {
	case receivedState := <-p.rateLimitMessageCh:
		assert.Equal(t, state, receivedState)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("rate limit state not received")
	}
}

func TestProduceServerUpdateRateLimitState_NonBlocking(t *testing.T) {
	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	grpcProduceServer.EXPECT().Context().Return(context.Background())

	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})

	// Use channel with buffer size 1 to test non-blocking behavior
	rateLimitCh := make(chan ratelimit.RateLimitState, 1)

	p := &ProduceServer{
		wal: l,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:             log.With(),
		produceMessageCh:   make(chan *streamingpb.ProduceMessageResponse, 10),
		rateLimitMessageCh: rateLimitCh,
		appendWG:           sync.WaitGroup{},
		metrics:            newProducerMetrics(l.Channel()),
	}

	// Fill the channel first
	oldState := ratelimit.RateLimitState{
		State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
		Rate:  512,
	}
	rateLimitCh <- oldState

	// Test non-blocking: this call should not block even though channel is full
	done := make(chan struct{})
	go func() {
		newState := ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT,
			Rate:  2048,
		}
		p.UpdateRateLimitState(newState)
		close(done)
	}()

	// Should complete without blocking
	select {
	case <-done:
		// Expected: non-blocking
	case <-time.After(100 * time.Millisecond):
		t.Fatal("UpdateRateLimitState should be non-blocking")
	}

	// The channel should contain the latest state
	select {
	case receivedState := <-p.rateLimitMessageCh:
		assert.Equal(t, streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT, receivedState.State)
		assert.Equal(t, int64(2048), receivedState.Rate)
	case <-time.After(100 * time.Millisecond):
		t.Fatal("should receive the latest state")
	}
}

func TestProduceServerUpdateRateLimitState_OnlyKeepLatest(t *testing.T) {
	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	grpcProduceServer.EXPECT().Context().Return(context.Background())

	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})

	// Use channel with buffer size 1
	rateLimitCh := make(chan ratelimit.RateLimitState, 1)

	p := &ProduceServer{
		wal: l,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:             log.With(),
		produceMessageCh:   make(chan *streamingpb.ProduceMessageResponse, 10),
		rateLimitMessageCh: rateLimitCh,
		appendWG:           sync.WaitGroup{},
		metrics:            newProducerMetrics(l.Channel()),
	}

	// Rapidly send multiple states
	for i := 0; i < 10; i++ {
		state := ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
			Rate:  int64(i * 100),
		}
		p.UpdateRateLimitState(state)
	}

	// Should only have one state in the channel (the latest one)
	receivedCount := 0
	var lastState ratelimit.RateLimitState
	for {
		select {
		case state := <-p.rateLimitMessageCh:
			lastState = state
			receivedCount++
		default:
			goto done
		}
	}
done:
	// Should only receive 1 state since channel buffer is 1
	assert.Equal(t, 1, receivedCount)
	// The state should be the latest (rate = 900)
	assert.Equal(t, int64(900), lastState.Rate)
}

func TestProduceServerUpdateRateLimitState_ContextCanceled(t *testing.T) {
	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // Cancel immediately
	grpcProduceServer.EXPECT().Context().Return(ctx)

	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})

	rateLimitCh := make(chan ratelimit.RateLimitState, 1)

	p := &ProduceServer{
		wal: l,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:             log.With(),
		produceMessageCh:   make(chan *streamingpb.ProduceMessageResponse, 10),
		rateLimitMessageCh: rateLimitCh,
		appendWG:           sync.WaitGroup{},
		metrics:            newProducerMetrics(l.Channel()),
	}

	// Should return early when context is canceled
	done := make(chan struct{})
	go func() {
		state := ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
			Rate:  1024,
		}
		p.UpdateRateLimitState(state)
		close(done)
	}()

	select {
	case <-done:
		// Expected: should return immediately
	case <-time.After(100 * time.Millisecond):
		t.Fatal("UpdateRateLimitState should return immediately when context is canceled")
	}

	// Channel should be empty since context was canceled
	select {
	case <-p.rateLimitMessageCh:
		t.Fatal("should not receive state when context is canceled")
	default:
		// Expected
	}
}

func TestProduceServerUpdateRateLimitState_ContextCanceledDuringDrain(t *testing.T) {
	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	grpcProduceServer.EXPECT().Context().Return(ctx)

	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})

	// Use channel with buffer size 1
	rateLimitCh := make(chan ratelimit.RateLimitState, 1)

	p := &ProduceServer{
		wal: l,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:             log.With(),
		produceMessageCh:   make(chan *streamingpb.ProduceMessageResponse, 10),
		rateLimitMessageCh: rateLimitCh,
		appendWG:           sync.WaitGroup{},
		metrics:            newProducerMetrics(l.Channel()),
	}

	// Fill the channel first
	oldState := ratelimit.RateLimitState{
		State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_SLOWDOWN,
		Rate:  512,
	}
	rateLimitCh <- oldState

	// Cancel context - this will trigger the second done check after draining
	cancel()

	// This call should still be non-blocking and should exit at second done check
	done := make(chan struct{})
	go func() {
		newState := ratelimit.RateLimitState{
			State: streamingpb.WALRateLimitState_WAL_RATE_LIMIT_STATE_REJECT,
			Rate:  2048,
		}
		p.UpdateRateLimitState(newState)
		close(done)
	}()

	select {
	case <-done:
		// Expected: should return
	case <-time.After(100 * time.Millisecond):
		t.Fatal("UpdateRateLimitState should not block")
	}

	// Channel should be empty since old state was drained and context was canceled before new state could be sent
	select {
	case state := <-p.rateLimitMessageCh:
		assert.Equal(t, oldState, state)
	default:
		t.Fatal("should not have state in channel")
	}
}

func TestProduceServerSendProduceResult_ContextCanceled(t *testing.T) {
	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	grpcProduceServer.EXPECT().Context().Return(ctx)

	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})

	p := &ProduceServer{
		wal: l,
		produceServer: &produceGrpcServerHelper{
			StreamingNodeHandlerService_ProduceServer: grpcProduceServer,
		},
		logger:           log.With(),
		produceMessageCh: make(chan *streamingpb.ProduceMessageResponse), // unbuffered
		appendWG:         sync.WaitGroup{},
		metrics:          newProducerMetrics(l.Channel()),
	}

	// Cancel context before sending
	cancel()
	time.Sleep(10 * time.Millisecond)

	// This should not block and should log warning
	msgID := walimplstest.NewTestMessageID(1)
	p.sendProduceResult(1, &wal.AppendResult{
		MessageID:              msgID,
		LastConfirmedMessageID: msgID,
		TimeTick:               100,
	}, nil)

	// Channel should not receive the message
	select {
	case <-p.produceMessageCh:
		t.Fatal("should not receive message after context canceled")
	case <-time.After(100 * time.Millisecond):
		// Expected
	}
}

func TestProduceServerExecute(t *testing.T) {
	grpcProduceServer := mock_streamingpb.NewMockStreamingNodeHandlerService_ProduceServer(t)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	grpcProduceServer.EXPECT().Context().Return(ctx)

	recvCh := make(chan *streamingpb.ProduceRequest)
	grpcProduceServer.EXPECT().Recv().RunAndReturn(func() (*streamingpb.ProduceRequest, error) {
		req, ok := <-recvCh
		if ok {
			return req, nil
		}
		return nil, io.EOF
	})

	sendCallCount := atomic.NewInt32(0)
	grpcProduceServer.EXPECT().Send(mock.Anything).RunAndReturn(func(pr *streamingpb.ProduceResponse) error {
		sendCallCount.Inc()
		return nil
	})

	l := mock_wal.NewMockWAL(t)
	l.EXPECT().Channel().Return(types.PChannelInfo{
		Name: "test",
		Term: 1,
	})
	l.EXPECT().Available().Return(make(<-chan struct{}))
	l.EXPECT().Register(mock.Anything).Return().Maybe()
	l.EXPECT().Unregister(mock.Anything).Return().Maybe()
	l.EXPECT().IsAvailable().Return(true)
	l.EXPECT().AppendAsync(mock.Anything, mock.Anything, mock.Anything).Run(func(ctx context.Context, mm message.MutableMessage, f func(*wal.AppendResult, error)) {
		msgID := walimplstest.NewTestMessageID(1)
		f(&wal.AppendResult{
			MessageID:              msgID,
			LastConfirmedMessageID: msgID,
			TimeTick:               100,
		}, nil)
	})

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

	ch := make(chan error)
	go func() {
		ch <- p.Execute()
	}()

	// Send a produce request
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

	// Wait for response processing
	time.Sleep(100 * time.Millisecond)

	// Close the recv channel to trigger EOF
	close(recvCh)

	// Wait for Execute to complete
	err := <-ch
	assert.NoError(t, err)

	// Should have sent at least 2 messages: produce response + close response
	assert.GreaterOrEqual(t, sendCallCount.Load(), int32(2))
}
