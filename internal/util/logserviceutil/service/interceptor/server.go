package interceptor

import (
	"context"
	"strings"

	"github.com/milvus-io/milvus/internal/proto/logpb"
	"github.com/milvus-io/milvus/internal/util/logserviceutil/status"
	"google.golang.org/grpc"
)

// NewLogServiceUnaryServerInterceptor returns a new unary server interceptor for error handling, metric...
func NewLogServiceUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err == nil {
			return resp, err
		}
		// Log Service Method should be overwrite the response error code.
		if strings.HasPrefix(info.FullMethod, logpb.ServiceMethodPrefix) {
			err := status.AsLogError(err)
			if err == nil {
				// return no error if LogError is ok.
				return resp, nil
			}
			return resp, status.NewGRPCStatusFromLogError(err).Err()
		}
		return resp, err
	}
}

// NewLogServiceStreamServerInterceptor returns a new stream server interceptor for error handling, metric...
func NewLogServiceStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		if err == nil {
			return err
		}

		// Log Service Method should be overwrite the response error code.
		if strings.HasPrefix(info.FullMethod, logpb.ServiceMethodPrefix) {
			err := status.AsLogError(err)
			if err == nil {
				// return no error if LogError is ok.
				return nil
			}
			return status.NewGRPCStatusFromLogError(err).Err()
		}
		return err
	}
}
