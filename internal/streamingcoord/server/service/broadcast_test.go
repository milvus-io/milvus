package service

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/coordinator/snmanager"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_balancer"
	"github.com/milvus-io/milvus/internal/mocks/streamingcoord/server/mock_broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/balancer/balance"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/broadcaster/broadcast"
	"github.com/milvus-io/milvus/internal/streamingcoord/server/resource"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	streamtypes "github.com/milvus-io/milvus/pkg/v2/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
)

func TestBroadcastService(t *testing.T) {
	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()
	// Set up the balancer
	b := mock_balancer.NewMockBalancer(t)
	b.EXPECT().WaitUntilWALbasedDDLReady(mock.Anything).Return(nil).Maybe()
	b.EXPECT().WatchChannelAssignments(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, cb balancer.WatchChannelAssignmentsCallback) error {
		<-ctx.Done()
		return ctx.Err()
	})
	b.EXPECT().Close().Return().Maybe()
	balance.Register(b)

	fb := syncutil.NewFuture[broadcaster.Broadcaster]()
	mba := mock_broadcaster.NewMockBroadcastAPI(t)
	mba.EXPECT().Close().Return()
	mb := mock_broadcaster.NewMockBroadcaster(t)
	fb.Set(mb)
	mba.EXPECT().Broadcast(mock.Anything, mock.Anything).Return(&streamtypes.BroadcastAppendResult{}, nil)
	mb.EXPECT().WithResourceKeys(mock.Anything, mock.Anything).Return(mba, nil)
	mb.EXPECT().Ack(mock.Anything, mock.Anything).Return(nil)
	mb.EXPECT().LegacyAck(mock.Anything, mock.Anything, mock.Anything).Return(nil)
	mb.EXPECT().Close().Return().Maybe()
	broadcast.Register(mb)

	msg := message.NewCreateCollectionMessageBuilderV1().
		WithHeader(&message.CreateCollectionMessageHeader{}).
		WithBody(&msgpb.CreateCollectionRequest{}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()

	service := NewBroadcastService()
	service.Broadcast(context.Background(), &streamingpb.BroadcastRequest{
		Message: msg.IntoMessageProto(),
	})
	service.Ack(context.Background(), &streamingpb.BroadcastAckRequest{
		BroadcastId: 1,
		Vchannel:    "v1",
	})
	service.Ack(context.Background(), &streamingpb.BroadcastAckRequest{
		BroadcastId: 1,
		Vchannel:    "v1",
		Message: &commonpb.ImmutableMessage{
			Id:         walimplstest.NewTestMessageID(1).IntoProto(),
			Payload:    []byte("payload"),
			Properties: map[string]string{"key": "value"},
		},
	})

	ctx, cancel := context.WithCancel(context.Background())
	reached := make(chan struct{})
	done := make(chan struct{})
	mb.EXPECT().Ack(mock.Anything, mock.Anything).Unset()
	mb.EXPECT().Ack(mock.Anything, mock.Anything).RunAndReturn(func(ctx context.Context, msg message.ImmutableMessage) error {
		close(reached)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-done:
			return nil
		}
	})
	go func() {
		<-reached
		cancel()
		time.Sleep(10 * time.Millisecond)
		close(done)
	}()
	_, err := service.Ack(ctx, &streamingpb.BroadcastAckRequest{
		BroadcastId: 1,
		Vchannel:    "v1",
		Message: &commonpb.ImmutableMessage{
			Id:         walimplstest.NewTestMessageID(1).IntoProto(),
			Payload:    []byte("payload"),
			Properties: map[string]string{"key": "value"},
		},
	})

	assert.NoError(t, err)
}

// TestBroadcastService_ForwardImportToDataCoord tests that import messages from old proxies
// are forwarded to DataCoord.ImportV2 for backward compatibility during rolling upgrades.
func TestBroadcastService_ForwardImportToDataCoord(t *testing.T) {
	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()

	// Set up mock MixCoordClient
	mockMixCoordClient := mocks.NewMockMixCoordClient(t)
	mixCoordClientFuture := syncutil.NewFuture[types.MixCoordClient]()
	mixCoordClientFuture.Set(mockMixCoordClient)

	// Initialize resource with mock MixCoordClient
	resource.InitForTest(resource.OptMixCoordClient(mixCoordClientFuture))

	// Expect ImportV2 to be called with the correct parameters
	mockMixCoordClient.EXPECT().ImportV2(mock.Anything, mock.MatchedBy(func(req *internalpb.ImportRequestInternal) bool {
		return req.CollectionID == 100 &&
			req.CollectionName == "test_collection" &&
			len(req.PartitionIDs) == 1 && req.PartitionIDs[0] == 200 &&
			len(req.ChannelNames) == 2 &&
			len(req.Files) == 1 && req.Files[0].Paths[0] == "/path/to/file1.json" &&
			req.JobID == 123
	})).Return(&internalpb.ImportResponse{
		Status: &commonpb.Status{ErrorCode: commonpb.ErrorCode_Success},
		JobID:  "123",
	}, nil)

	// Build an import message that simulates what an old proxy would send
	importMsg := message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Import,
				Timestamp: 0,
			},
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{200},
			Files: []*msgpb.ImportFile{
				{Id: 1, Paths: []string{"/path/to/file1.json"}},
			},
			Options: map[string]string{"key": "value"},
			Schema: &schemapb.CollectionSchema{
				Name:        "test_collection",
				Description: "test schema",
			},
			JobID: 123,
		}).
		WithBroadcast([]string{"v1", "v2"}).
		MustBuildBroadcast()

	service := NewBroadcastService()
	resp, err := service.Broadcast(context.Background(), &streamingpb.BroadcastRequest{
		Message: importMsg.IntoMessageProto(),
	})

	assert.NoError(t, err)
	assert.NotNil(t, resp)
	// Import forwarding returns a response with empty results since it doesn't actually broadcast
	assert.Equal(t, uint64(0), resp.BroadcastId)
	assert.Empty(t, resp.Results)
}

// TestBroadcastService_ForwardImportToDataCoord_Error tests error handling when ImportV2 fails.
func TestBroadcastService_ForwardImportToDataCoord_Error(t *testing.T) {
	broadcast.ResetBroadcaster()
	snmanager.ResetStreamingNodeManager()

	// Set up mock MixCoordClient
	mockMixCoordClient := mocks.NewMockMixCoordClient(t)
	mixCoordClientFuture := syncutil.NewFuture[types.MixCoordClient]()
	mixCoordClientFuture.Set(mockMixCoordClient)

	// Initialize resource with mock MixCoordClient
	resource.InitForTest(resource.OptMixCoordClient(mixCoordClientFuture))

	// Expect ImportV2 to return an error response
	mockMixCoordClient.EXPECT().ImportV2(mock.Anything, mock.Anything).Return(&internalpb.ImportResponse{
		Status: &commonpb.Status{
			ErrorCode: commonpb.ErrorCode_UnexpectedError,
			Reason:    "test error",
		},
	}, nil)

	// Build an import message
	importMsg := message.NewImportMessageBuilderV1().
		WithHeader(&message.ImportMessageHeader{}).
		WithBody(&msgpb.ImportMsg{
			Base: &commonpb.MsgBase{
				MsgType:   commonpb.MsgType_Import,
				Timestamp: 0,
			},
			CollectionID:   100,
			CollectionName: "test_collection",
			PartitionIDs:   []int64{200},
			Files: []*msgpb.ImportFile{
				{Id: 1, Paths: []string{"/path/to/file1.json"}},
			},
			JobID: 456,
		}).
		WithBroadcast([]string{"v1"}).
		MustBuildBroadcast()

	service := NewBroadcastService()
	resp, err := service.Broadcast(context.Background(), &streamingpb.BroadcastRequest{
		Message: importMsg.IntoMessageProto(),
	})

	assert.Error(t, err)
	assert.Nil(t, resp)
	assert.Contains(t, err.Error(), "test error")
}
