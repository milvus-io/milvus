package interceptor

import (
	"context"
	"io"
	"strings"

	"github.com/cockroachdb/errors"
	"google.golang.org/grpc"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// NewServiceUnaryServerInterceptor encodes domain errors using the protocol of
// the invoked service, paired with NewServiceUnaryClientInterceptor.
func NewServiceUnaryServerInterceptor() grpc.UnaryServerInterceptor {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		resp, err := handler(ctx, req)
		return resp, convertServiceError(info.FullMethod, err)
	}
}

// NewServiceStreamServerInterceptor applies the same policy to terminal stream
// errors, paired with NewServiceStreamClientInterceptor.
func NewServiceStreamServerInterceptor() grpc.StreamServerInterceptor {
	return func(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		return convertServiceError(info.FullMethod, handler(srv, ss))
	}
}

func convertServiceError(method string, err error) error {
	if err == nil {
		return nil
	}
	switch {
	case strings.HasPrefix(method, streamingpb.ServiceMethodPrefix):
		err = convertMilvusErrorIntoStreamingError(err)
		return status.NewGRPCStatusFromStreamingError(status.AsStreamingError(err)).Err()
	case strings.HasPrefix(method, viewpb.ServiceMethodPrefix):
		return convertViewServiceError(err)
	default:
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

// convertViewServiceError owns the View RPC wire boundary. WAL errors may reach
// the handler from local dependencies, but StreamingError never crosses this
// boundary. The client therefore selects its wrapper solely by service path.
func convertViewServiceError(err error) error {
	if err == nil || errors.IsAny(err, context.Canceled, context.DeadlineExceeded, io.EOF) {
		return err
	}
	if errors.IsAny(err, merr.ErrNodeNotMatch, merr.ErrServiceCrossClusterRouting) {
		err = viewerror.NewNotPrimaryError(err.Error())
	}
	var streamingErr *status.StreamingError
	if errors.As(err, &streamingErr) {
		switch {
		case streamingErr.IsWrongStreamingNode(), streamingErr.IsFenced(), streamingErr.IsIgnoredOperation():
			err = viewerror.NewNotPrimaryError(streamingErr.Error())
		case streamingErr.IsOnShutdown():
			err = viewerror.NewOnShutdownError(streamingErr.Error())
		case streamingErr.IsRateLimitRejected():
			err = merr.Wrap(merr.ErrServiceRateLimit, streamingErr.Error())
		default:
			err = viewerror.NewUnknownError(streamingErr.Error())
		}
	}
	// Recv/Send can return a native gRPC transport error. Preserve its status.
	if _, ok := grpcstatus.FromError(err); ok {
		return err
	}
	return viewerror.NewGRPCStatusFromError(err).Err()
}
