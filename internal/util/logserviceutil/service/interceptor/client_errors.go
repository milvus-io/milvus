package interceptor

import (
	"context"
	"strings"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"google.golang.org/grpc"
)

// NewLogServiceUnaryClientInterceptor returns a new unary client interceptor for error handling.
func NewLogServiceUnaryClientInterceptor() grpc.UnaryClientInterceptor {
	return func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		err := invoker(ctx, method, req, reply, cc, opts...)
		if strings.HasPrefix(method, logpb.ServiceMethodPrefix) {
			st := status.ConvertLogError(method, err)
			return st
		}
		return err
	}
}

// NewLogServiceStreamClientInterceptor returns a new stream client interceptor for error handling.
func NewLogServiceStreamClientInterceptor() grpc.StreamClientInterceptor {
	return func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		clientStream, err := streamer(ctx, desc, cc, method, opts...)
		if strings.HasPrefix(method, logpb.ServiceMethodPrefix) {
			e := status.ConvertLogError(method, err)
			return status.NewClientStreamWrapper(method, clientStream), e
		}
		return clientStream, err
	}
}
