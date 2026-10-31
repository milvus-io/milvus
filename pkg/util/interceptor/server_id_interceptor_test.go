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

package interceptor

import (
	"context"
	"fmt"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/health"
	"google.golang.org/grpc/health/grpc_health_v1"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func TestServerIDInjectionRefreshesTargetID(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	var serverID, clientID atomic.Int64
	serverID.Store(10)
	clientID.Store(10)
	server := grpc.NewServer(
		grpc.UnaryInterceptor(ServerIDValidationUnaryServerInterceptor(serverID.Load)),
		grpc.StreamInterceptor(ServerIDValidationStreamServerInterceptor(serverID.Load)),
	)
	healthServer := health.NewServer()
	healthServer.SetServingStatus("", grpc_health_v1.HealthCheckResponse_SERVING)
	grpc_health_v1.RegisterHealthServer(server, healthServer)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient(listener.Addr().String(),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithUnaryInterceptor(ServerIDInjectionUnaryClientInterceptorWithGetter(clientID.Load)),
		grpc.WithStreamInterceptor(ServerIDInjectionStreamClientInterceptorWithGetter(clientID.Load)),
	)
	require.NoError(t, err)
	t.Cleanup(func() { assert.NoError(t, conn.Close()) })
	client := grpc_health_v1.NewHealthClient(conn)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	for _, id := range []int64{10, 11} {
		serverID.Store(id)
		clientID.Store(id)
		t.Run(fmt.Sprintf("unary/%d", id), func(t *testing.T) {
			response, err := client.Check(ctx, &grpc_health_v1.HealthCheckRequest{})
			require.NoError(t, err)
			assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, response.Status)
		})
		t.Run(fmt.Sprintf("stream/%d", id), func(t *testing.T) {
			streamCtx, streamCancel := context.WithCancel(ctx)
			defer streamCancel()
			stream, err := client.Watch(streamCtx, &grpc_health_v1.HealthCheckRequest{})
			require.NoError(t, err)
			response, err := stream.Recv()
			require.NoError(t, err)
			assert.Equal(t, grpc_health_v1.HealthCheckResponse_SERVING, response.Status)
		})
	}
}

func TestServerIDInterceptor(t *testing.T) {
	t.Run("test ServerIDInjectionUnaryClientInterceptor", func(t *testing.T) {
		method := "MockMethod"
		req := &milvuspb.InsertRequest{}
		serverID := int64(1)

		var incomingContext context.Context
		invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			incomingContext = ctx
			return nil
		}
		interceptor := ServerIDInjectionUnaryClientInterceptor(serverID)
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(make(map[string]string)))
		err := interceptor(ctx, method, req, nil, nil, invoker)
		assert.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(incomingContext)
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprint(serverID), md.Get(ServerIDKey)[0])
	})

	t.Run("test ServerIDInjectionStreamClientInterceptor", func(t *testing.T) {
		method := "MockMethod"
		serverID := int64(1)

		var incomingContext context.Context
		streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			incomingContext = ctx
			return nil, nil
		}
		interceptor := ServerIDInjectionStreamClientInterceptor(serverID)
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(make(map[string]string)))
		_, err := interceptor(ctx, nil, nil, method, streamer)
		assert.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(incomingContext)
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprint(serverID), md.Get(ServerIDKey)[0])
	})

	t.Run("test ServerIDValidationUnaryServerInterceptor", func(t *testing.T) {
		method := "MockMethod"
		req := &milvuspb.InsertRequest{}

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return nil, nil
		}
		serverInfo := &grpc.UnaryServerInfo{FullMethod: method}
		interceptor := ServerIDValidationUnaryServerInterceptor(paramtable.GetNodeID)

		// no md in context
		_, err := interceptor(context.Background(), req, serverInfo, handler)
		assert.NoError(t, err)

		// no ServerID in md
		ctx := metadata.NewIncomingContext(context.Background(), metadata.New(make(map[string]string)))
		_, err = interceptor(ctx, req, serverInfo, handler)
		assert.NoError(t, err)

		// with invalid ServerID
		md := metadata.Pairs(ServerIDKey, "@$#$%")
		ctx = metadata.NewIncomingContext(context.Background(), md)
		_, err = interceptor(ctx, req, serverInfo, handler)
		assert.NoError(t, err)

		// with mismatch ServerID
		md = metadata.Pairs(ServerIDKey, "1234")
		ctx = metadata.NewIncomingContext(context.Background(), md)
		_, err = interceptor(ctx, req, serverInfo, handler)
		assert.ErrorIs(t, err, merr.ErrNodeNotMatch)

		// with same ServerID
		md = metadata.Pairs(ServerIDKey, fmt.Sprint(paramtable.GetNodeID()))
		ctx = metadata.NewIncomingContext(context.Background(), md)
		_, err = interceptor(ctx, req, serverInfo, handler)
		assert.NoError(t, err)
	})

	t.Run("test ServerIDValidationUnaryServerInterceptor", func(t *testing.T) {
		handler := func(srv interface{}, stream grpc.ServerStream) error {
			return nil
		}
		interceptor := ServerIDValidationStreamServerInterceptor(paramtable.GetNodeID)

		// no md in context
		err := interceptor(nil, newMockSS(context.Background()), nil, handler)
		assert.NoError(t, err)

		// no ServerID in md
		ctx := metadata.NewIncomingContext(context.Background(), metadata.New(make(map[string]string)))
		err = interceptor(nil, newMockSS(ctx), nil, handler)
		assert.NoError(t, err)

		// with invalid ServerID
		md := metadata.Pairs(ServerIDKey, "@$#$%")
		ctx = metadata.NewIncomingContext(context.Background(), md)
		err = interceptor(nil, newMockSS(ctx), nil, handler)
		assert.NoError(t, err)

		// with mismatch ServerID
		md = metadata.Pairs(ServerIDKey, "1234")
		ctx = metadata.NewIncomingContext(context.Background(), md)
		err = interceptor(nil, newMockSS(ctx), nil, handler)
		assert.ErrorIs(t, err, merr.ErrNodeNotMatch)

		// with same ServerID
		md = metadata.Pairs(ServerIDKey, fmt.Sprint(paramtable.GetNodeID()))
		ctx = metadata.NewIncomingContext(context.Background(), md)
		err = interceptor(nil, newMockSS(ctx), nil, handler)
		assert.NoError(t, err)
	})
}
