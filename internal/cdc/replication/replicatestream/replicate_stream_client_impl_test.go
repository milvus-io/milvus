// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package replicatestream

import (
	"context"
	"strconv"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	milvuspb "github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/cdc/cluster"
	"github.com/milvus-io/milvus/internal/cdc/resource"
	"github.com/milvus-io/milvus/internal/distributed/streaming"
	"github.com/milvus-io/milvus/internal/mocks/distributed/mock_streaming"
	mock_message "github.com/milvus-io/milvus/pkg/v2/mocks/streaming/util/mock_message"
	streamingpb "github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	message "github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/walimplstest"
)

func TestReplicateStreamClient_Replicate(t *testing.T) {
	ctx := context.Background()
	targetCluster := &commonpb.MilvusCluster{
		ClusterId: "test-cluster",
		ConnectionParam: &commonpb.ConnectionParam{
			Uri:   "localhost:19530",
			Token: "test-token",
		},
	}

	// Setup mocks
	mockStreamClient := newMockReplicateStreamClient(t)

	mockMilvusClient := cluster.NewMockMilvusClient(t)
	mockMilvusClient.EXPECT().CreateReplicateStream(mock.Anything).Return(mockStreamClient, nil)

	mockClusterClient := cluster.NewMockClusterClient(t)
	mockClusterClient.EXPECT().CreateMilvusClient(mock.Anything, mock.Anything).
		Return(mockMilvusClient, nil)

	resource.InitForTest(t,
		resource.OptClusterClient(mockClusterClient),
	)

	wal := mock_streaming.NewMockWALAccesser(t)
	streaming.SetWALForTest(wal)

	replicateInfo := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "test-source-channel",
		TargetChannelName: "test-target-channel",
		TargetCluster:     targetCluster,
	}
	replicateClient := NewReplicateStreamClient(ctx, replicateInfo)

	// Test Replicate method
	const msgCount = pendingMessageQueueLength * 10
	go func() {
		for i := 0; i < msgCount; i++ {
			mockMsg := mock_message.NewMockImmutableMessage(t)
			tt := uint64(i + 1)
			messageID := walimplstest.NewTestMessageID(int64(tt))
			mockMsg.EXPECT().TimeTick().Return(tt)
			mockMsg.EXPECT().EstimateSize().Return(1024)
			mockMsg.EXPECT().MessageType().Return(message.MessageTypeInsert)
			mockMsg.EXPECT().MessageID().Return(messageID)
			mockMsg.EXPECT().IntoImmutableMessageProto().Return(&commonpb.ImmutableMessage{
				Id:         messageID.IntoProto(),
				Payload:    []byte("test-payload"),
				Properties: map[string]string{"key": "value"},
			})
			mockMsg.EXPECT().MarshalLogObject(mock.Anything).Return(nil)

			err := replicateClient.Replicate(mockMsg)
			assert.NoError(t, err)
		}
	}()

	// recv the confirm message
	for i := 0; i < msgCount; i++ {
		mockStreamClient.ExpectRecv()
	}
	assert.Equal(t, msgCount, mockStreamClient.GetRecvCount())
	assert.Equal(t, 0, replicateClient.(*replicateStreamClient).pendingMessages.Len())
	replicateClient.Close()
}

func TestReplicateStreamClient_Replicate_ContextCanceled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	targetCluster := &commonpb.MilvusCluster{
		ClusterId: "test-cluster",
		ConnectionParam: &commonpb.ConnectionParam{
			Uri:   "localhost:19530",
			Token: "test-token",
		},
	}

	// Setup mocks
	mockStreamClient := newMockReplicateStreamClient(t)
	mockMilvusClient := cluster.NewMockMilvusClient(t)
	mockMilvusClient.EXPECT().CreateReplicateStream(mock.Anything).Return(mockStreamClient, nil).Maybe()
	mockMilvusClient.EXPECT().Close(mock.Anything).Return(nil).Maybe()

	mockClusterClient := cluster.NewMockClusterClient(t)
	mockClusterClient.EXPECT().CreateMilvusClient(mock.Anything, mock.Anything).
		Return(mockMilvusClient, nil).Maybe()

	resource.InitForTest(t,
		resource.OptClusterClient(mockClusterClient),
	)

	wal := mock_streaming.NewMockWALAccesser(t)
	streaming.SetWALForTest(wal)

	replicateInfo := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "test-source-channel",
		TargetChannelName: "test-target-channel",
		TargetCluster:     targetCluster,
	}
	client := NewReplicateStreamClient(ctx, replicateInfo)
	defer client.Close()

	// Cancel context
	cancel()

	// Test Replicate method with canceled context
	mockMsg := mock_message.NewMockImmutableMessage(t)
	err := client.Replicate(mockMsg)
	assert.NoError(t, err) // Should return nil when context is canceled
}

func TestReplicateStreamClient_Reconnect(t *testing.T) {
	ctx := context.Background()
	targetCluster := &commonpb.MilvusCluster{
		ClusterId: "test-cluster",
		ConnectionParam: &commonpb.ConnectionParam{
			Uri:   "localhost:19530",
			Token: "test-token",
		},
	}

	const reconnectTimes = 3
	reconnectCount := 0
	// Setup mocks with error to trigger retry logic
	mockStreamClient := newMockReplicateStreamClient(t)
	mockMilvusClient := cluster.NewMockMilvusClient(t)
	mockMilvusClient.EXPECT().CreateReplicateStream(mock.Anything).RunAndReturn(func(ctx context.Context, opts ...grpc.CallOption) (milvuspb.MilvusService_CreateReplicateStreamClient, error) {
		reconnectCount++
		if reconnectCount < reconnectTimes {
			return nil, assert.AnError
		}
		return mockStreamClient, nil
	}).Times(reconnectTimes) // expect to be called reconnectTimes times

	mockClusterClient := cluster.NewMockClusterClient(t)
	mockClusterClient.EXPECT().CreateMilvusClient(mock.Anything, mock.Anything).
		Return(mockMilvusClient, nil)

	resource.InitForTest(t,
		resource.OptClusterClient(mockClusterClient),
	)

	wal := mock_streaming.NewMockWALAccesser(t)
	streaming.SetWALForTest(wal)

	// Create client which will start internal retry loop
	replicateInfo := &streamingpb.ReplicatePChannelMeta{
		SourceChannelName: "test-source-channel",
		TargetChannelName: "test-target-channel",
		TargetCluster:     targetCluster,
	}
	replicateClient := NewReplicateStreamClient(ctx, replicateInfo)

	// Replicate after reconnected
	const msgCount = 100
	go func() {
		for i := 0; i < msgCount; i++ {
			tt := uint64(i + 1)
			mockMsg := mock_message.NewMockImmutableMessage(t)
			mockMsg.EXPECT().TimeTick().Return(tt)
			messageID := walimplstest.NewTestMessageID(int64(tt))
			mockMsg.EXPECT().MessageID().Return(messageID)
			mockMsg.EXPECT().EstimateSize().Return(1024)
			mockMsg.EXPECT().MessageType().Return(message.MessageTypeInsert)
			mockMsg.EXPECT().IntoImmutableMessageProto().Return(&commonpb.ImmutableMessage{
				Id:         messageID.IntoProto(),
				Payload:    []byte("test-payload"),
				Properties: map[string]string{"key": "value"},
			})
			mockMsg.EXPECT().MarshalLogObject(mock.Anything).Return(nil)

			err := replicateClient.Replicate(mockMsg)
			assert.NoError(t, err)
		}
	}()

	for i := 0; i < msgCount; i++ {
		mockStreamClient.ExpectRecv()
	}
	assert.Equal(t, msgCount, mockStreamClient.GetRecvCount())
	assert.Equal(t, 0, replicateClient.(*replicateStreamClient).pendingMessages.Len())
	replicateClient.Close()
}

// mockReplicateStreamClient implements the milvuspb.MilvusService_CreateReplicateStreamClient interface
type mockReplicateStreamClient struct {
	sendError error
	recvError error

	ch           chan *milvuspb.ReplicateRequest
	expectRecvCh chan struct{}
	recvCount    int

	t       *testing.T
	timeout time.Duration

	closeCh chan struct{}
}

func newMockReplicateStreamClient(t *testing.T) *mockReplicateStreamClient {
	return &mockReplicateStreamClient{
		ch:           make(chan *milvuspb.ReplicateRequest, 128),
		expectRecvCh: make(chan struct{}, 128),
		t:            t,
		timeout:      10 * time.Second,
		closeCh:      make(chan struct{}, 1),
	}
}

func (m *mockReplicateStreamClient) Send(req *milvuspb.ReplicateRequest) error {
	if m.sendError != nil {
		return m.sendError
	}
	m.ch <- req
	return m.sendError
}

func (m *mockReplicateStreamClient) Recv() (*milvuspb.ReplicateResponse, error) {
	if m.recvError != nil {
		return nil, m.recvError
	}
	select {
	case <-m.closeCh:
		return nil, nil
	case req := <-m.ch:
		// use id as time tick in mock
		timeTick, err := strconv.ParseUint(req.GetReplicateMessage().GetMessage().GetId().GetId(), 10, 64)
		if err != nil {
			panic(err)
		}
		m.expectRecvCh <- struct{}{}
		return &milvuspb.ReplicateResponse{
			Response: &milvuspb.ReplicateResponse_ReplicateConfirmedMessageInfo{
				ReplicateConfirmedMessageInfo: &milvuspb.ReplicateConfirmedMessageInfo{
					ConfirmedTimeTick: timeTick,
				},
			},
		}, nil
	case <-time.After(m.timeout):
		assert.Fail(m.t, "recv timeout")
		return nil, assert.AnError
	}
}

func (m *mockReplicateStreamClient) ExpectRecv() {
	select {
	case <-m.expectRecvCh:
		m.recvCount++
		return
	case <-time.After(m.timeout):
		assert.Fail(m.t, "expect recv timeout")
	}
}

func (m *mockReplicateStreamClient) GetRecvCount() int {
	return m.recvCount
}

func (m *mockReplicateStreamClient) RecvMsg(msg interface{}) error {
	return nil
}

func (m *mockReplicateStreamClient) SendMsg(msg interface{}) error {
	return nil
}

func (m *mockReplicateStreamClient) Header() (metadata.MD, error) {
	return nil, nil
}

func (m *mockReplicateStreamClient) Trailer() metadata.MD {
	return nil
}

func (m *mockReplicateStreamClient) CloseSend() error {
	close(m.closeCh)
	return nil
}

func (m *mockReplicateStreamClient) Context() context.Context {
	return context.Background()
}
