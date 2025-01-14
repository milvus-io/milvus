package interceptor

import (
	"context"
	"strings"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/proto/streamingpb"
)

// NewStreamingServiceUnaryClientInterceptor returns a new unary client interceptor for error handling.
func NewStreamingServiceUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if strings.HasPrefix(method, streamingpb.ServiceMethodPrefix) {
			st := status.ConvertStreamingError(method, err)
			return st
		}
		return err
	}
}

// NewStreamingServiceStreamClientInterceptor returns a new stream client interceptor for error handling.
func NewStreamingServiceStreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		clientStream, err := streamer(ctx, desc, cc, method, opts...)
		if strings.HasPrefix(method, streamingpb.ServiceMethodPrefix) {
			e := status.ConvertStreamingError(method, err)
			return status.NewClientStreamWrapper(method, clientStream), e
		}
		return clientStream, err
	}
}
