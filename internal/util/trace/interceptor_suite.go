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

package trace

import (
	"context"
	"strings"

	grpc_middleware "github.com/grpc-ecosystem/go-grpc-middleware"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.12.0"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
	"google.golang.org/grpc/status"
)

// InterceptorSuite contains client option and server option
type InterceptorSuite struct {
	ClientOpts []grpc.DialOption
	ServerOpts []grpc.ServerOption
}

var (
	filterFunc = func(ctx context.Context, fullMethodName string) bool {
		if fullMethodName == `/milvus.proto.rootcoord.RootCoord/UpdateChannelTimeTick` ||
			fullMethodName == `/milvus.proto.rootcoord.RootCoord/AllocTimestamp` {
			return false
		}
		return true
	}
)

func UnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if !filterFunc(ctx, info.FullMethod) {
			return handler(ctx, req)
		}
		requestMeta, _ := metadata.FromIncomingContext(ctx)
		metadataCopy := requestMeta.Copy()

		bags, spanCtx := otelgrpc.Extract(ctx, &metadataCopy)
		ctx = baggage.ContextWithBaggage(ctx, bags)
		peerAddr := ""
		if p, ok := peer.FromContext(ctx); ok {
			peerAddr = p.Addr.String()
		}

		name, attrs := parseFullMethod(info.FullMethod)
		if peerAddr != "" {
			attrs = append(attrs, attribute.String("rpc.peer", peerAddr))
		}
		attrs = append(attrs, attribute.String("traceID", spanCtx.TraceID().String()))

		ctx, span := DefaultTracer().Start(
			trace.ContextWithRemoteSpanContext(ctx, spanCtx),
			name,
			trace.WithSpanKind(trace.SpanKindServer), trace.WithAttributes(attrs...),
		)
		defer span.End()

		resp, err := handler(ctx, req)
		if err != nil {
			s, _ := status.FromError(err)
			span.SetStatus(codes.Error, s.Message())
			span.SetAttributes(attribute.Int64("rpc.errcode", int64(s.Code())))
		} else {
			span.SetAttributes(attribute.Int64("rpc.errorcode", int64(grpc.Code(nil))))
		}

		return resp, err
	}
}

func StreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(
		srv interface{},
		ss grpc.ServerStream,
		info *grpc.StreamServerInfo,
		handler grpc.StreamHandler,
	) error {
		ctx := ss.Context()
		if !filterFunc(ctx, info.FullMethod) {
			return handler(srv, ss)
		}
		requestMeta, _ := metadata.FromIncomingContext(ctx)
		metadataCopy := requestMeta.Copy()

		bags, spanCtx := otelgrpc.Extract(ctx, &metadataCopy)
		ctx = baggage.ContextWithBaggage(ctx, bags)

		peerAddr := ""
		if p, ok := peer.FromContext(ctx); ok {
			peerAddr = p.Addr.String()
		}
		name, attrs := parseFullMethod(info.FullMethod)
		if peerAddr != "" {
			attrs = append(attrs, attribute.String("rpc.peer", peerAddr))
		}
		attrs = append(attrs, attribute.String("traceID", spanCtx.TraceID().String()))

		ctx, span := DefaultTracer().Start(
			trace.ContextWithRemoteSpanContext(ctx, spanCtx),
			name,
			trace.WithSpanKind(trace.SpanKindServer),
			trace.WithAttributes(attrs...),
		)
		defer span.End()
		wrappedStream := grpc_middleware.WrapServerStream(ss)
		wrappedStream.WrappedContext = ctx
		return handler(srv, wrappedStream)
	}
}

func parseFullMethod(fullMethod string) (string, []attribute.KeyValue) {
	name := strings.TrimLeft(fullMethod, "/")
	parts := strings.SplitN(name, "/", 2)
	if len(parts) != 2 {
		return name, []attribute.KeyValue(nil)
	}

	var attrs []attribute.KeyValue
	if service := parts[0]; service != "" {
		attrs = append(attrs, semconv.RPCServiceKey.String(service))
	}
	if method := parts[1]; method != "" {
		attrs = append(attrs, semconv.RPCMethodKey.String(method))
	}
	return name, attrs
}
