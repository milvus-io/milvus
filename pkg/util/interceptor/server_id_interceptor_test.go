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
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/v2/util/mcontext"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

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
		interceptor := NewMilvusContextUnaryClientInterceptor(serverID)
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(make(map[string]string)))
		err := interceptor(ctx, method, req, nil, nil, invoker)
		assert.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(incomingContext)
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprint(serverID), md.Get(mcontext.DestinationServerIDKey)[0])
	})

	t.Run("test ServerIDInjectionStreamClientInterceptor", func(t *testing.T) {
		method := "MockMethod"
		serverID := int64(1)

		var incomingContext context.Context
		streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			incomingContext = ctx
			return nil, nil
		}
		interceptor := NewMilvusContextStreamClientInterceptor(serverID)
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(make(map[string]string)))
		_, err := interceptor(ctx, nil, nil, method, streamer)
		assert.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(incomingContext)
		assert.True(t, ok)
		assert.Equal(t, fmt.Sprint(serverID), md.Get(mcontext.DestinationServerIDKey)[0])
	})

	t.Run("test ServerIDNotInjectedWhenZeroUnary", func(t *testing.T) {
		method := "MockMethod"
		req := &milvuspb.InsertRequest{}

		var incomingContext context.Context
		invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			incomingContext = ctx
			return nil
		}
		interceptor := NewMilvusContextUnaryClientInterceptor(0)
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(make(map[string]string)))
		err := interceptor(ctx, method, req, nil, nil, invoker)
		assert.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(incomingContext)
		assert.True(t, ok)
		// ServerID should not be injected when dstNodeID is 0
		assert.Empty(t, md.Get(mcontext.DestinationServerIDKey))
	})

	t.Run("test ServerIDNotInjectedWhenZeroStream", func(t *testing.T) {
		method := "MockMethod"

		var incomingContext context.Context
		streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			incomingContext = ctx
			return nil, nil
		}
		interceptor := NewMilvusContextStreamClientInterceptor(0)
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(make(map[string]string)))
		_, err := interceptor(ctx, nil, nil, method, streamer)
		assert.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(incomingContext)
		assert.True(t, ok)
		// ServerID should not be injected when dstNodeID is 0
		assert.Empty(t, md.Get(mcontext.DestinationServerIDKey))
	})

	t.Run("test MilvusContextUnaryServerInterceptor", func(t *testing.T) {
		method := "MockMethod"
		req := &milvuspb.InsertRequest{}

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return nil, nil
		}
		serverInfo := &grpc.UnaryServerInfo{FullMethod: method}
		interceptor := NewMilvusContextUnaryServerInterceptor()

		// no md in context
		_, err := interceptor(context.Background(), req, serverInfo, handler)
		assert.NoError(t, err)

		// no ServerID in md
		ctx := metadata.NewIncomingContext(context.Background(), metadata.New(make(map[string]string)))
		_, err = interceptor(ctx, req, serverInfo, handler)
		assert.NoError(t, err)

		// with invalid ServerID
		md := metadata.Pairs(mcontext.DestinationServerIDKey, "@$#$%")
		ctx = metadata.NewIncomingContext(context.Background(), md)
		_, err = interceptor(ctx, req, serverInfo, handler)
		assert.NoError(t, err)

		// with mismatch ServerID
		md = metadata.Pairs(mcontext.DestinationServerIDKey, "1234")
		ctx = metadata.NewIncomingContext(context.Background(), md)
		_, err = interceptor(ctx, req, serverInfo, handler)
		assert.ErrorIs(t, err, merr.ErrNodeNotMatch)

		// with same ServerID
		md = metadata.Pairs(mcontext.DestinationServerIDKey, fmt.Sprint(paramtable.GetNodeID()))
		ctx = metadata.NewIncomingContext(context.Background(), md)
		_, err = interceptor(ctx, req, serverInfo, handler)
		assert.NoError(t, err)
	})

	t.Run("test MilvusContextStreamServerInterceptor", func(t *testing.T) {
		handler := func(srv interface{}, stream grpc.ServerStream) error {
			return nil
		}
		interceptor := NewMilvusContextStreamServerInterceptor()

		// no md in context
		err := interceptor(nil, newMockSS(context.Background()), nil, handler)
		assert.NoError(t, err)

		// no ServerID in md
		ctx := metadata.NewIncomingContext(context.Background(), metadata.New(make(map[string]string)))
		err = interceptor(nil, newMockSS(ctx), nil, handler)
		assert.NoError(t, err)

		// with invalid ServerID
		md := metadata.Pairs(mcontext.DestinationServerIDKey, "@$#$%")
		ctx = metadata.NewIncomingContext(context.Background(), md)
		err = interceptor(nil, newMockSS(ctx), nil, handler)
		assert.NoError(t, err)

		// with mismatch ServerID
		md = metadata.Pairs(mcontext.DestinationServerIDKey, "1234")
		ctx = metadata.NewIncomingContext(context.Background(), md)
		err = interceptor(nil, newMockSS(ctx), nil, handler)
		assert.ErrorIs(t, err, merr.ErrNodeNotMatch)

		// with same ServerID
		md = metadata.Pairs(mcontext.DestinationServerIDKey, fmt.Sprint(paramtable.GetNodeID()))
		ctx = metadata.NewIncomingContext(context.Background(), md)
		err = interceptor(nil, newMockSS(ctx), nil, handler)
		assert.NoError(t, err)
	})

	t.Run("test withRandomTraceIDIfNotStart", func(t *testing.T) {
		// Test when no trace ID is set
		ctx := context.Background()
		newCtx := withRandomTraceIDIfNotStart(ctx)
		// Should have added a trace ID
		assert.NotEqual(t, ctx, newCtx)

		// Test when trace ID is already set
		ctxWithTrace := newCtx
		newCtx2 := withRandomTraceIDIfNotStart(ctxWithTrace)
		// Should return the same context since trace ID is valid
		assert.Equal(t, ctxWithTrace, newCtx2)
	})
}
