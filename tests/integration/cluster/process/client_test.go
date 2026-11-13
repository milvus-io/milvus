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

package process

import (
	"context"
	"io"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/util/syncutil"
)

type healthyStreamingNode struct {
	streamingpb.UnimplementedStreamingNodeStateServiceServer
}

func (*healthyStreamingNode) GetComponentStates(context.Context, *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{State: &milvuspb.ComponentInfo{
		NodeID: 5, StateCode: commonpb.StateCode_Healthy,
	}}, nil
}

func TestStreamingNodeReadinessUsesIndependentStateService(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer()
	// There is deliberately no QueryNode service on this server.
	streamingpb.RegisterStreamingNodeStateServiceServer(server, &healthyStreamingNode{})
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	t.Cleanup(func() { _ = listener.Close() })
	conn, err := grpc.NewClient("passthrough:///streamingnode",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() }))
	require.NoError(t, err)
	client := newStreamingNodeClient(conn)
	t.Cleanup(func() { _ = client.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	state, err := client.GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
	require.NoError(t, err)
	require.Equal(t, int64(5), state.GetState().GetNodeID())
	require.Equal(t, commonpb.StateCode_Healthy, state.GetState().GetStateCode())
}

func TestWaitForReadyPropagatesCancellation(t *testing.T) {
	// GetClient must leave the unresolved future when its caller cancels.
	mp := &MilvusProcess{
		notifier: syncutil.NewAsyncTaskNotifier[error](),
		client:   syncutil.NewFuture[io.Closer](),
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	require.ErrorIs(t, mp.waitForReady(ctx), context.Canceled)
}
