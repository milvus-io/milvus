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
	"testing"

	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus/pkg/util/merr"
	"github.com/milvus-io/milvus/pkg/util/paramtable"
)

type mockSS struct {
	grpc.ServerStream
	ctx context.Context
}

func newMockSS(ctx context.Context) grpc.ServerStream {
	return &mockSS{
		ctx: ctx,
	}
}

func (m *mockSS) Context() context.Context {
	return m.ctx
}

func init() {
	paramtable.Get().Init()
}

func TestClusterInterceptor(t *testing.T) {
	t.Run("test ClusterInjectionUnaryClientInterceptor", func(t *testing.T) {
		method := "MockMethod"
		req := &milvuspb.InsertRequest{}

		var incomingContext context.Context
		invoker := func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			incomingContext = ctx
			return nil
		}
		interceptor := ClusterInjectionUnaryClientInterceptor()
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(make(map[string]string)))
		err := interceptor(ctx, method, req, nil, nil, invoker)
		assert.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(incomingContext)
		assert.True(t, ok)
		assert.Equal(t, paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), md.Get(ClusterKey)[0])
	})

	t.Run("test ClusterInjectionStreamClientInterceptor", func(t *testing.T) {
		method := "MockMethod"

		var incomingContext context.Context
		streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			incomingContext = ctx
			return nil, nil
		}
		interceptor := ClusterInjectionStreamClientInterceptor()
		ctx := metadata.NewOutgoingContext(context.Background(), metadata.New(make(map[string]string)))
		_, err := interceptor(ctx, nil, nil, method, streamer)
		assert.NoError(t, err)

		md, ok := metadata.FromOutgoingContext(incomingContext)
		assert.True(t, ok)
		assert.Equal(t, paramtable.Get().CommonCfg.ClusterPrefix.GetValue(), md.Get(ClusterKey)[0])
	})

	t.Run("test ClusterValidationUnaryServerInterceptor", func(t *testing.T) {
		method := "MockMethod"
		req := &milvuspb.InsertRequest{}

		handler := func(ctx context.Context, req interface{}) (interface{}, error) {
			return nil, nil
		}
		serverInfo := &grpc.UnaryServerInfo{FullMethod: method}
		interceptor := ClusterValidationUnaryServerInterceptor()

		// no md in context
		_, err := interceptor(context.Background(), req, serverInfo, handler)
		assert.NoError(t, err)

		// no cluster in md
		ctx := metadata.NewIncomingContext(context.Background(), metadata.New(make(map[string]string)))
		_, err = interceptor(ctx, req, serverInfo, handler)
		assert.NoError(t, err)

		// with cross-cluster
		md := metadata.Pairs(ClusterKey, "ins-1")
		ctx = metadata.NewIncomingContext(context.Background(), md)
		_, err = interceptor(ctx, req, serverInfo, handler)
		assert.ErrorIs(t, err, merr.ErrCrossClusterRouting)

		// with same cluster
		md = metadata.Pairs(ClusterKey, paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
		ctx = metadata.NewIncomingContext(context.Background(), md)
		_, err = interceptor(ctx, req, serverInfo, handler)
		assert.NoError(t, err)
	})

	t.Run("test ClusterValidationUnaryServerInterceptor", func(t *testing.T) {
		handler := func(srv interface{}, stream grpc.ServerStream) error {
			return nil
		}
		interceptor := ClusterValidationStreamServerInterceptor()

		// no md in context
		err := interceptor(nil, newMockSS(context.Background()), nil, handler)
		assert.NoError(t, err)

		// no cluster in md
		ctx := metadata.NewIncomingContext(context.Background(), metadata.New(make(map[string]string)))
		err = interceptor(nil, newMockSS(ctx), nil, handler)
		assert.NoError(t, err)

		// with cross-cluster
		md := metadata.Pairs(ClusterKey, "ins-1")
		ctx = metadata.NewIncomingContext(context.Background(), md)
		err = interceptor(nil, newMockSS(ctx), nil, handler)
		assert.ErrorIs(t, err, merr.ErrCrossClusterRouting)

		// with same cluster
		md = metadata.Pairs(ClusterKey, paramtable.Get().CommonCfg.ClusterPrefix.GetValue())
		ctx = metadata.NewIncomingContext(context.Background(), md)
		err = interceptor(nil, newMockSS(ctx), nil, handler)
		assert.NoError(t, err)
	})
}
