package interceptor

import (
	"context"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v2/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

// NewStreamingServiceUnaryServerInterceptor returns a new unary server interceptor for error handling, metric...
func NewStreamingServiceUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		if err == nil {
			return resp, nil
		}
		// Streaming Service Method should be overwrite the response error code.
		if strings.HasPrefix(info.FullMethod, streamingpb.ServiceMethodPrefix) {
			err = convertMilvusErrorIntoStreamingError(err)
			err := status.AsStreamingError(err)
			if err == nil {
				// return no error if StreamingError is ok.
				return resp, nil
			}
			return resp, status.NewGRPCStatusFromStreamingError(err).Err()
		}
		return resp, err
	}
}

// NewStreamingServiceStreamServerInterceptor returns a new stream server interceptor for error handling, metric...
func NewStreamingServiceStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		err := handler(srv, ss)
		if err == nil {
			return nil
		}

		// Streaming Service Method should be overwrite the response error code.
		if strings.HasPrefix(info.FullMethod, streamingpb.ServiceMethodPrefix) {
			err = convertMilvusErrorIntoStreamingError(err)
			err := status.AsStreamingError(err)
			if err == nil {
				// return no error if StreamingError is ok.
				return nil
			}
			return status.NewGRPCStatusFromStreamingError(err).Err()
		}
		return err
	}
}

// convertMilvusErrorIntoStreamingError converts milvus error into streaming error.
func convertMilvusErrorIntoStreamingError(err error) error {
	// milvus global error code in interceptor make the streaming error handling complex.
	// convert it into streaming error code.
	if errors.IsAny(err, merr.ErrNodeNotMatch, merr.ErrServiceCrossClusterRouting) {
		return status.NewIgnoreOperation(err.Error())
	}
	return err
}
