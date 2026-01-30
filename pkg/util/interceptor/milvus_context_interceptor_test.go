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
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"

	"github.com/milvus-io/milvus/pkg/v2/util/mcontext"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestValidateMilvusContext(t *testing.T) {
	paramtable.Init()

	t.Run("valid context with empty destination", func(t *testing.T) {
		mctx := &mcontext.MilvusContext{
			SourceNodeID:       123,
			DestinationNodeID:  0,
			DestinationCluster: "",
		}
		err := validateMilvusContext(mctx)
		assert.NoError(t, err)
	})

	t.Run("valid context with matching cluster", func(t *testing.T) {
		clusterPrefix := paramtable.Get().CommonCfg.ClusterPrefix.GetValue()
		mctx := &mcontext.MilvusContext{
			SourceNodeID:       123,
			DestinationNodeID:  0,
			DestinationCluster: clusterPrefix,
		}
		err := validateMilvusContext(mctx)
		assert.NoError(t, err)
	})

	t.Run("invalid cluster routing", func(t *testing.T) {
		mctx := &mcontext.MilvusContext{
			SourceNodeID:       123,
			DestinationNodeID:  0,
			DestinationCluster: "different-cluster",
		}
		err := validateMilvusContext(mctx)
		assert.Error(t, err)
		assert.True(t, merr.ErrServiceCrossClusterRouting.Is(err))
	})

	t.Run("valid context with matching node ID", func(t *testing.T) {
		nodeID := paramtable.GetNodeID()
		mctx := &mcontext.MilvusContext{
			SourceNodeID:       123,
			DestinationNodeID:  nodeID,
			DestinationCluster: "",
		}
		err := validateMilvusContext(mctx)
		assert.NoError(t, err)
	})

	t.Run("invalid node ID mismatch", func(t *testing.T) {
		nodeID := paramtable.GetNodeID()
		mctx := &mcontext.MilvusContext{
			SourceNodeID:       123,
			DestinationNodeID:  nodeID + 999,
			DestinationCluster: "",
		}
		err := validateMilvusContext(mctx)
		assert.Error(t, err)
		assert.True(t, merr.ErrNodeNotMatch.Is(err))
	})
}

func TestWithRandomTraceIDIfNotStart(t *testing.T) {
	t.Run("context without trace ID gets random ID", func(t *testing.T) {
		ctx := context.Background()
		newCtx := withRandomTraceIDIfNotStart(ctx)

		sc := trace.SpanFromContext(newCtx).SpanContext()
		assert.True(t, sc.IsValid())
		assert.NotEqual(t, trace.TraceID{}, sc.TraceID())
		assert.NotEqual(t, trace.SpanID{}, sc.SpanID())
		assert.True(t, sc.IsSampled())
	})

	t.Run("context with valid trace ID remains unchanged", func(t *testing.T) {
		traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
		spanID, _ := trace.SpanIDFromHex("0102030405060708")
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
		})
		ctx := trace.ContextWithSpanContext(context.Background(), sc)

		newCtx := withRandomTraceIDIfNotStart(ctx)

		newSc := trace.SpanFromContext(newCtx).SpanContext()
		assert.Equal(t, traceID, newSc.TraceID())
		assert.Equal(t, spanID, newSc.SpanID())
	})

	t.Run("generates different IDs each time", func(t *testing.T) {
		ctx1 := withRandomTraceIDIfNotStart(context.Background())
		ctx2 := withRandomTraceIDIfNotStart(context.Background())

		sc1 := trace.SpanFromContext(ctx1).SpanContext()
		sc2 := trace.SpanFromContext(ctx2).SpanContext()

		assert.NotEqual(t, sc1.TraceID(), sc2.TraceID())
		assert.NotEqual(t, sc1.SpanID(), sc2.SpanID())
	})
}

func TestNewMilvusContextUnaryServerInterceptor(t *testing.T) {
	paramtable.Init()

	t.Run("interceptor returns function", func(t *testing.T) {
		interceptor := NewMilvusContextUnaryServerInterceptor()
		assert.NotNil(t, interceptor)
	})

	t.Run("interceptor passes valid context", func(t *testing.T) {
		interceptor := NewMilvusContextUnaryServerInterceptor()

		called := false
		handler := func(ctx context.Context, req any) (any, error) {
			called = true
			mctx := mcontext.FromContext(ctx)
			assert.NotNil(t, mctx)
			return "response", nil
		}

		ctx := context.Background()
		resp, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/test/Method"}, handler)

		assert.NoError(t, err)
		assert.True(t, called)
		assert.Equal(t, "response", resp)
	})

	t.Run("interceptor rejects mismatched cluster", func(t *testing.T) {
		interceptor := NewMilvusContextUnaryServerInterceptor()

		handler := func(ctx context.Context, req any) (any, error) {
			return "response", nil
		}

		md := metadata.New(map[string]string{
			mcontext.DestinationClusterKey: "wrong-cluster",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/test/Method"}, handler)

		assert.Error(t, err)
		assert.True(t, merr.ErrServiceCrossClusterRouting.Is(err))
	})

	t.Run("interceptor rejects mismatched node ID", func(t *testing.T) {
		interceptor := NewMilvusContextUnaryServerInterceptor()

		handler := func(ctx context.Context, req any) (any, error) {
			return "response", nil
		}

		md := metadata.New(map[string]string{
			mcontext.DestinationServerIDKey: "999999",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		_, err := interceptor(ctx, nil, &grpc.UnaryServerInfo{FullMethod: "/test/Method"}, handler)

		assert.Error(t, err)
		assert.True(t, merr.ErrNodeNotMatch.Is(err))
	})
}

func TestNewMilvusContextStreamServerInterceptor(t *testing.T) {
	paramtable.Init()

	t.Run("interceptor returns function", func(t *testing.T) {
		interceptor := NewMilvusContextStreamServerInterceptor()
		assert.NotNil(t, interceptor)
	})

	t.Run("interceptor passes valid context", func(t *testing.T) {
		interceptor := NewMilvusContextStreamServerInterceptor()

		called := false
		handler := func(srv interface{}, stream grpc.ServerStream) error {
			called = true
			mctx := mcontext.FromContext(stream.Context())
			assert.NotNil(t, mctx)
			return nil
		}

		mockStream := &mockServerStream{ctx: context.Background()}
		err := interceptor(nil, mockStream, &grpc.StreamServerInfo{FullMethod: "/test/Method"}, handler)

		assert.NoError(t, err)
		assert.True(t, called)
	})

	t.Run("interceptor rejects mismatched cluster", func(t *testing.T) {
		interceptor := NewMilvusContextStreamServerInterceptor()

		handler := func(srv interface{}, stream grpc.ServerStream) error {
			return nil
		}

		md := metadata.New(map[string]string{
			mcontext.DestinationClusterKey: "wrong-cluster",
		})
		ctx := metadata.NewIncomingContext(context.Background(), md)
		mockStream := &mockServerStream{ctx: ctx}
		err := interceptor(nil, mockStream, &grpc.StreamServerInfo{FullMethod: "/test/Method"}, handler)

		assert.Error(t, err)
		assert.True(t, merr.ErrServiceCrossClusterRouting.Is(err))
	})
}

func TestNewMilvusContextUnaryClientInterceptor(t *testing.T) {
	paramtable.Init()

	t.Run("interceptor returns function", func(t *testing.T) {
		interceptor := NewMilvusContextUnaryClientInterceptor(0)
		assert.NotNil(t, interceptor)
	})

	t.Run("interceptor adds metadata without dest node ID", func(t *testing.T) {
		interceptor := NewMilvusContextUnaryClientInterceptor(0)

		var capturedCtx context.Context
		invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			capturedCtx = ctx
			return nil
		}

		ctx := context.Background()
		err := interceptor(ctx, "/test/Method", nil, nil, nil, invoker)

		assert.NoError(t, err)
		md, ok := metadata.FromOutgoingContext(capturedCtx)
		assert.True(t, ok)
		assert.NotEmpty(t, md.Get(mcontext.DestinationClusterKey))
		assert.Empty(t, md.Get(mcontext.DestinationServerIDKey))
	})

	t.Run("interceptor adds dest node ID when provided", func(t *testing.T) {
		interceptor := NewMilvusContextUnaryClientInterceptor(12345)

		var capturedCtx context.Context
		invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			capturedCtx = ctx
			return nil
		}

		ctx := context.Background()
		err := interceptor(ctx, "/test/Method", nil, nil, nil, invoker)

		assert.NoError(t, err)
		md, ok := metadata.FromOutgoingContext(capturedCtx)
		assert.True(t, ok)
		assert.Equal(t, "12345", md.Get(mcontext.DestinationServerIDKey)[0])
	})

	t.Run("interceptor adds trace ID if not present", func(t *testing.T) {
		interceptor := NewMilvusContextUnaryClientInterceptor(0)

		var capturedCtx context.Context
		invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			capturedCtx = ctx
			return nil
		}

		ctx := context.Background()
		err := interceptor(ctx, "/test/Method", nil, nil, nil, invoker)

		assert.NoError(t, err)
		sc := trace.SpanFromContext(capturedCtx).SpanContext()
		assert.True(t, sc.IsValid())
	})

	t.Run("interceptor preserves existing trace ID", func(t *testing.T) {
		interceptor := NewMilvusContextUnaryClientInterceptor(0)

		traceID, _ := trace.TraceIDFromHex("0102030405060708090a0b0c0d0e0f10")
		spanID, _ := trace.SpanIDFromHex("0102030405060708")
		sc := trace.NewSpanContext(trace.SpanContextConfig{
			TraceID:    traceID,
			SpanID:     spanID,
			TraceFlags: trace.FlagsSampled,
		})
		ctx := trace.ContextWithSpanContext(context.Background(), sc)

		var capturedCtx context.Context
		invoker := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, opts ...grpc.CallOption) error {
			capturedCtx = ctx
			return nil
		}

		err := interceptor(ctx, "/test/Method", nil, nil, nil, invoker)

		assert.NoError(t, err)
		capturedSc := trace.SpanFromContext(capturedCtx).SpanContext()
		assert.Equal(t, traceID, capturedSc.TraceID())
	})
}

func TestNewMilvusContextStreamClientInterceptor(t *testing.T) {
	paramtable.Init()

	t.Run("interceptor returns function", func(t *testing.T) {
		interceptor := NewMilvusContextStreamClientInterceptor(0)
		assert.NotNil(t, interceptor)
	})

	t.Run("interceptor adds metadata without dest node ID", func(t *testing.T) {
		interceptor := NewMilvusContextStreamClientInterceptor(0)

		var capturedCtx context.Context
		streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			capturedCtx = ctx
			return nil, nil
		}

		ctx := context.Background()
		_, err := interceptor(ctx, &grpc.StreamDesc{}, nil, "/test/Method", streamer)

		assert.NoError(t, err)
		md, ok := metadata.FromOutgoingContext(capturedCtx)
		assert.True(t, ok)
		assert.NotEmpty(t, md.Get(mcontext.DestinationClusterKey))
		assert.Empty(t, md.Get(mcontext.DestinationServerIDKey))
	})

	t.Run("interceptor adds dest node ID when provided", func(t *testing.T) {
		interceptor := NewMilvusContextStreamClientInterceptor(54321)

		var capturedCtx context.Context
		streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			capturedCtx = ctx
			return nil, nil
		}

		ctx := context.Background()
		_, err := interceptor(ctx, &grpc.StreamDesc{}, nil, "/test/Method", streamer)

		assert.NoError(t, err)
		md, ok := metadata.FromOutgoingContext(capturedCtx)
		assert.True(t, ok)
		assert.Equal(t, "54321", md.Get(mcontext.DestinationServerIDKey)[0])
	})

	t.Run("interceptor adds trace ID if not present", func(t *testing.T) {
		interceptor := NewMilvusContextStreamClientInterceptor(0)

		var capturedCtx context.Context
		streamer := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			capturedCtx = ctx
			return nil, nil
		}

		ctx := context.Background()
		_, err := interceptor(ctx, &grpc.StreamDesc{}, nil, "/test/Method", streamer)

		assert.NoError(t, err)
		sc := trace.SpanFromContext(capturedCtx).SpanContext()
		assert.True(t, sc.IsValid())
	})
}

// mockServerStream implements grpc.ServerStream for testing
type mockServerStream struct {
	grpc.ServerStream
	ctx context.Context
}

func (m *mockServerStream) Context() context.Context {
	return m.ctx
}
