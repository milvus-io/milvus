package replicate

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/apache/pulsar-client-go/pulsar"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/messagespb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	pulsar2 "github.com/milvus-io/milvus/pkg/v3/streaming/walimpls/impls/pulsar"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// createContextWithClusterID creates a context with cluster ID in metadata (simulating incoming context)
func createContextWithClusterID(clusterID string) context.Context {
	if clusterID == "" {
		return context.Background()
	}
	md := metadata.New(map[string]string{
		"cluster-id": clusterID,
	})
	return metadata.NewIncomingContext(context.Background(), md)
}

func TestReplicateStreamServer_Execute(t *testing.T) {
	ctx := createContextWithClusterID("test-cluster")
	mockStreamServer := newMockReplicateStreamServer(ctx)

	const msgCount = replicateRespChanLength * 10

	// Setup WAL mock
	replicateService := mock_streaming.NewMockReplicateService(t)
	tt := uint64(1)
	replicateService.EXPECT().Append(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, msg message.ReplicateMutableMessage) (*types.AppendResult, error) {
			defer func() { tt++ }()
			return &types.AppendResult{
				TimeTick: tt,
			}, nil
		})
	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Replicate().Return(replicateService)
	streaming.SetWALForTest(mockWAL)

	server, err := CreateReplicateServer(mockStreamServer)
	assert.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Execute()
		assert.NoError(t, err)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < msgCount; i++ {
			tt := uint64(i + 1)
			messageID := pulsar2.NewPulsarID(pulsar.EarliestMessageID())
			msg := message.NewInsertMessageBuilderV1().
				WithVChannel("test-vchannel").
				WithHeader(&messagespb.InsertMessageHeader{}).
				WithBody(&msgpb.InsertRequest{}).
				MustBuildMutable().WithTimeTick(tt).
				WithLastConfirmed(messageID)
			milvusMsg := message.ImmutableMessageToMilvusMessage(commonpb.WALName_Pulsar.String(), msg.IntoImmutableMessage(messageID))
			mockStreamServer.SendRequest(&milvuspb.ReplicateRequest{
				Request: &milvuspb.ReplicateRequest_ReplicateMessage{
					ReplicateMessage: &milvuspb.ReplicateMessage{
						Message: milvusMsg,
					},
				},
			})
		}
	}()

	for i := 0; i < msgCount; i++ {
		tt := uint64(i + 1)
		sentResp := mockStreamServer.GetSentResponse()
		assert.NotNil(t, sentResp)
		assert.Equal(t, tt, sentResp.GetReplicateConfirmedMessageInfo().GetConfirmedTimeTick())
	}

	// Close the stream to stop execution
	mockStreamServer.CloseSend()
	wg.Wait()
}

func TestReplicateStreamServer_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(createContextWithClusterID("test-cluster"))
	mockStreamServer := newMockReplicateStreamServer(ctx)

	server, err := CreateReplicateServer(mockStreamServer)
	assert.NoError(t, err)

	// Test send loop with canceled context
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Execute()
		assert.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled))
	}()

	// Cancel context
	cancel()
	wg.Wait()
}

func TestReplicateStreamServer_IgnoredMessageSendsConfirmation(t *testing.T) {
	ctx := createContextWithClusterID("test-cluster")
	mockStreamServer := newMockReplicateStreamServer(ctx)

	const msgCount = 5

	// Setup WAL mock to return IgnoredOperation error
	replicateService := mock_streaming.NewMockReplicateService(t)
	replicateService.EXPECT().Append(mock.Anything, mock.Anything).
		RunAndReturn(func(ctx context.Context, msg message.ReplicateMutableMessage) (*types.AppendResult, error) {
			return nil, status.NewIgnoreOperation("message type is configured to be skipped")
		})
	mockWAL := mock_streaming.NewMockWALAccesser(t)
	mockWAL.EXPECT().Replicate().Return(replicateService)
	streaming.SetWALForTest(mockWAL)

	server, err := CreateReplicateServer(mockStreamServer)
	assert.NoError(t, err)

	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.Execute()
		assert.NoError(t, err)
	}()

	// Send messages that will be ignored
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < msgCount; i++ {
			tt := uint64(i + 1)
			messageID := pulsar2.NewPulsarID(pulsar.EarliestMessageID())
			msg := message.NewInsertMessageBuilderV1().
				WithVChannel("test-vchannel").
				WithHeader(&messagespb.InsertMessageHeader{}).
				WithBody(&msgpb.InsertRequest{}).
				MustBuildMutable().WithTimeTick(tt).
				WithLastConfirmed(messageID)
			milvusMsg := message.ImmutableMessageToMilvusMessage(commonpb.WALName_Pulsar.String(), msg.IntoImmutableMessage(messageID))
			mockStreamServer.SendRequest(&milvuspb.ReplicateRequest{
				Request: &milvuspb.ReplicateRequest_ReplicateMessage{
					ReplicateMessage: &milvuspb.ReplicateMessage{
						Message: milvusMsg,
					},
				},
			})
		}
	}()

	// Verify ConfirmedTimeTick is sent even for ignored messages
	for i := 0; i < msgCount; i++ {
		tt := uint64(i + 1)
		sentResp := mockStreamServer.GetSentResponse()
		assert.NotNil(t, sentResp, "expected ConfirmedTimeTick for ignored message %d", i)
		assert.Equal(t, tt, sentResp.GetReplicateConfirmedMessageInfo().GetConfirmedTimeTick())
	}

	// Close the stream to stop execution
	mockStreamServer.CloseSend()
	wg.Wait()
}

func TestReplicateStreamServer_recvLoop_RecvError(t *testing.T) {
	ctx := createContextWithClusterID("test-cluster")
	mockStreamServer := newMockReplicateStreamServer(ctx)
	mockStreamServer.recvError = errors.New("recv error")

	server, err := CreateReplicateServer(mockStreamServer)
	assert.NoError(t, err)

	// Test recv loop with recv error
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		defer wg.Done()
		err := server.recvLoop()
		assert.Error(t, err)
		assert.Equal(t, "recv error", err.Error())
	}()

	wg.Wait()
}

func TestReplicateStreamServer_ExecuteRecvError(t *testing.T) {
	stream := newMockReplicateStreamServer(createContextWithClusterID("test-cluster"))
	stream.recvError = errors.New("receive failed")
	server, err := CreateReplicateServer(stream)
	require.NoError(t, err)
	server.replicateRespCh <- &milvuspb.ReplicateResponse{
		Response: &milvuspb.ReplicateResponse_ReplicateConfirmedMessageInfo{
			ReplicateConfirmedMessageInfo: &milvuspb.ReplicateConfirmedMessageInfo{ConfirmedTimeTick: 42},
		},
	}
	require.ErrorIs(t, server.Execute(), stream.recvError)
	require.EqualValues(t, 42, stream.GetSentResponse().GetReplicateConfirmedMessageInfo().GetConfirmedTimeTick())
}

func TestReplicateStreamServer_ExecuteAppendError(t *testing.T) {
	for _, appendErr := range []*status.StreamingError{
		status.NewUnrecoverableError("collection not found"),
		status.NewReplicateViolation("txn is in progress"),
		status.NewOnShutdownError("wal is closing"),
	} {
		t.Run(appendErr.Code.String(), func(t *testing.T) {
			stream := newMockReplicateStreamServer(createContextWithClusterID("test-cluster"))
			replicateService := mock_streaming.NewMockReplicateService(t)
			replicateService.EXPECT().Append(mock.Anything, mock.Anything).Return(nil, appendErr).Once()
			mockWAL := mock_streaming.NewMockWALAccesser(t)
			mockWAL.EXPECT().Replicate().Return(replicateService)
			streaming.SetWALForTest(mockWAL)

			messageID := pulsar2.NewPulsarID(pulsar.EarliestMessageID())
			msg := message.NewInsertMessageBuilderV1().
				WithVChannel("test-vchannel").
				WithHeader(&messagespb.InsertMessageHeader{}).
				WithBody(&msgpb.InsertRequest{}).
				MustBuildMutable().WithTimeTick(42).WithLastConfirmed(messageID)
			stream.SendRequest(&milvuspb.ReplicateRequest{
				Request: &milvuspb.ReplicateRequest_ReplicateMessage{
					ReplicateMessage: &milvuspb.ReplicateMessage{
						SourceClusterId: "source",
						Message: message.ImmutableMessageToMilvusMessage(
							commonpb.WALName_Pulsar.String(), msg.IntoImmutableMessage(messageID)),
					},
				},
			})
			server, err := CreateReplicateServer(stream)
			require.NoError(t, err)
			err = server.Execute()
			require.Error(t, err)
			// Check that the error code and cause survive gRPC status conversion.
			wireErr := grpcstatus.Convert(err).Err()
			got := status.AsStreamingError(status.ConvertStreamingError("CreateReplicateStream", wireErr))
			require.Equal(t, appendErr.Code, got.Code)
			require.Equal(t, appendErr.Cause, got.Cause)
			require.Equal(t, appendErr.IsUnrecoverable(), got.IsUnrecoverable())
			require.Empty(t, stream.sentResponses, "rejected messages must not be confirmed")
		})
	}
}

func TestReplicateStreamServerRejectsNilReplicateMessage(t *testing.T) {
	ctx := createContextWithClusterID("test-cluster")
	mockStreamServer := newMockReplicateStreamServer(ctx)

	server, err := CreateReplicateServer(mockStreamServer)
	require.NoError(t, err)

	require.NotPanics(t, func() {
		err = server.handleReplicateMessage(&milvuspb.ReplicateRequest_ReplicateMessage{})
	})
	require.Error(t, err)
	assert.ErrorIs(t, err, merr.ErrParameterMissing)
}

// mockReplicateStreamServer implements the milvuspb.MilvusService_CreateReplicateStreamServer interface
type mockReplicateStreamServer struct {
	ctx           context.Context
	sendError     error
	recvError     error
	recvRequests  chan *milvuspb.ReplicateRequest
	sentResponses chan *milvuspb.ReplicateResponse
	closeCh       chan struct{}
	timeout       time.Duration
}

func newMockReplicateStreamServer(ctx context.Context) *mockReplicateStreamServer {
	return &mockReplicateStreamServer{
		ctx:           ctx,
		recvRequests:  make(chan *milvuspb.ReplicateRequest, 128),
		sentResponses: make(chan *milvuspb.ReplicateResponse, 128),
		closeCh:       make(chan struct{}, 1),
		timeout:       10 * time.Second,
	}
}

func (m *mockReplicateStreamServer) Send(resp *milvuspb.ReplicateResponse) error {
	if m.sendError != nil {
		return m.sendError
	}
	m.sentResponses <- resp
	return nil
}

func (m *mockReplicateStreamServer) Recv() (*milvuspb.ReplicateRequest, error) {
	if m.recvError != nil {
		return nil, m.recvError
	}
	select {
	case <-m.closeCh:
		return nil, io.EOF
	case req := <-m.recvRequests:
		return req, nil
	case <-time.After(m.timeout):
		return nil, errors.New("recv timeout")
	}
}

func (m *mockReplicateStreamServer) RecvMsg(msg interface{}) error {
	return nil
}

func (m *mockReplicateStreamServer) SendMsg(msg interface{}) error {
	return nil
}

func (m *mockReplicateStreamServer) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockReplicateStreamServer) Trailer() metadata.MD {
	return nil
}

func (m *mockReplicateStreamServer) SendHeader(md metadata.MD) error {
	return nil
}

func (m *mockReplicateStreamServer) SetHeader(md metadata.MD) error {
	return nil
}

func (m *mockReplicateStreamServer) SetTrailer(md metadata.MD) {
}

func (m *mockReplicateStreamServer) Context() context.Context {
	return m.ctx
}

func (m *mockReplicateStreamServer) CloseSend() error {
	close(m.closeCh)
	return nil
}

func (m *mockReplicateStreamServer) SendRequest(req *milvuspb.ReplicateRequest) {
	m.recvRequests <- req
}

func (m *mockReplicateStreamServer) GetSentResponse() *milvuspb.ReplicateResponse {
	select {
	case resp := <-m.sentResponses:
		return resp
	case <-time.After(m.timeout):
		return nil
	}
}
