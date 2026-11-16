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

package adaptor

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/internal/types"
	"github.com/milvus-io/milvus/internal/util/componentutil"
	"github.com/milvus-io/milvus/internal/util/grpcclient"
	"github.com/milvus-io/milvus/pkg/v2/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v2/streaming/util/message"
	"github.com/milvus-io/milvus/pkg/v2/streaming/walimpls/impls/rmq"
	"github.com/milvus-io/milvus/pkg/v2/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/syncutil"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type recoveryCoordinatorServer struct {
	rootcoordpb.UnimplementedRootCoordServer
}

func (*recoveryCoordinatorServer) GetComponentStates(context.Context, *milvuspb.GetComponentStatesRequest) (*milvuspb.ComponentStates, error) {
	return &milvuspb.ComponentStates{Status: merr.Success(), State: &milvuspb.ComponentInfo{StateCode: commonpb.StateCode_Healthy}}, nil
}

func (*recoveryCoordinatorServer) AllocTimestamp(_ context.Context, req *rootcoordpb.AllocTimestampRequest) (*rootcoordpb.AllocTimestampResponse, error) {
	return &rootcoordpb.AllocTimestampResponse{Status: merr.Success(), Timestamp: 1000, Count: req.GetCount()}, nil
}

// Exercise the production grpcclient, health checker, future, TSO allocator and
// sendFirstTimeTick together. Discovery and durable WAL storage are test inputs.
type recoveryCoordinatorClient struct {
	types.MixCoordClient
	base *grpcclient.ClientBase[rootcoordpb.RootCoordClient]
}

func (c *recoveryCoordinatorClient) GetComponentStates(ctx context.Context, req *milvuspb.GetComponentStatesRequest, opts ...grpc.CallOption) (*milvuspb.ComponentStates, error) {
	ret, err := c.base.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) {
		return client.GetComponentStates(ctx, req, opts...)
	})
	if err != nil {
		return nil, err
	}
	return ret.(*milvuspb.ComponentStates), nil
}

func (c *recoveryCoordinatorClient) AllocTimestamp(ctx context.Context, req *rootcoordpb.AllocTimestampRequest, opts ...grpc.CallOption) (*rootcoordpb.AllocTimestampResponse, error) {
	ret, err := c.base.ReCall(ctx, func(client rootcoordpb.RootCoordClient) (any, error) { return client.AllocTimestamp(ctx, req, opts...) })
	if err != nil {
		return nil, err
	}
	return ret.(*rootcoordpb.AllocTimestampResponse), nil
}

func TestCoordinatorRecoveryFirstTimeTick(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	address := listener.Addr().String()
	var discovered atomic.Int64
	discovered.Store(96)
	var mismatches atomic.Int32
	newSession := make(chan time.Time, 1)
	startServer := func(listener net.Listener, serverID int64) *grpc.Server {
		validate := interceptor.ServerIDValidationUnaryServerInterceptor(func() int64 { return serverID })
		server := grpc.NewServer(grpc.UnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
			resp, err := validate(ctx, req, info, handler)
			// Publish the replacement session before returning the second stale
			// RPC. Registration time must not depend on when the client looks
			// it up: otherwise a lazy reconnect hides its own backoff delay.
			if serverID == 99 && grpcclient.IsServerIDMismatchErr(err) && mismatches.Add(1) == 2 {
				discovered.Store(99)
				newSession <- time.Now()
			}
			return resp, err
		}))
		rootcoordpb.RegisterRootCoordServer(server, &recoveryCoordinatorServer{})
		go server.Serve(listener)
		t.Cleanup(server.Stop)
		return server
	}
	oldServer := startServer(listener, 96)
	base := &grpcclient.ClientBase[rootcoordpb.RootCoordClient]{
		DialTimeout: time.Second, MaxAttempts: 10, InitialBackoff: 0.1, MaxBackoff: 2,
		ClientMaxRecvSize: 1 << 20, ClientMaxSendSize: 1 << 20,
	}
	base.SetRole(typeutil.MixCoordRole)
	base.SetGetAddrFunc(func() (string, error) {
		base.SetNodeID(discovered.Load())
		return address, nil
	})
	base.SetNewGrpcClientFunc(func(conn *grpc.ClientConn) rootcoordpb.RootCoordClient { return rootcoordpb.NewRootCoordClient(conn) })
	t.Cleanup(func() { require.NoError(t, base.Close()) })
	client := &recoveryCoordinatorClient{base: base}
	_, err = client.GetComponentStates(context.Background(), &milvuspb.GetComponentStatesRequest{})
	require.NoError(t, err)

	oldServer.Stop()
	listener, err = net.Listen("tcp", address)
	require.NoError(t, err)
	startServer(listener, 99)
	// Retain the existing client connection across the server restart. The
	// real server interceptor rejects its old identity; no error is synthesized.

	future := syncutil.NewFuture[types.MixCoordClient]()
	resource.InitForTest(t, resource.OptMixCoordClient(future))
	t.Cleanup(resource.Resource().TimeTickInspector().Close)
	wal := newFirstTimeTickWALImpls(func(context.Context, message.MutableMessage) (message.MessageID, error) { return rmq.NewRmqID(1), nil })
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	done := make(chan error, 1)
	go func() { _, err := sendFirstTimeTick(ctx, wal, nil); done <- err }()
	started := time.Now()
	require.NoError(t, componentutil.WaitForComponentHealthy(ctx, client, "mixcoord", 10, 200*time.Millisecond))
	healthy := time.Now()
	require.Zero(t, wal.appendCount.Load(), "WAL append must still wait for the client future")
	futureReady := time.Now()
	future.Set(client)
	require.NoError(t, <-done)
	firstTimeTick := time.Now()
	registered := <-newSession
	require.Less(t, healthy.Sub(registered), 100*time.Millisecond, "new identity must not incur the next 200ms backoff")
	require.EqualValues(t, 1, wal.appendCount.Load())
	t.Logf("new session published=%s, healthy=%s, future publication=%s, first TimeTick=%s; new identity to healthy=%s", registered.Sub(started), healthy.Sub(started), futureReady.Sub(started), firstTimeTick.Sub(started), healthy.Sub(registered))
}
