package interceptor

import (
	"context"
	"fmt"
	"io"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestConvertMilvusErrorIntoStreamingError(t *testing.T) {
	err := merr.WrapErrNodeNotMatch(1, 2)
	err = convertMilvusErrorIntoStreamingError(err)
	s := status.AsStreamingError(err)
	assert.True(t, s.IsSkippedOperation())

	err = merr.WrapErrServiceCrossClusterRouting("1", "2")
	err = convertMilvusErrorIntoStreamingError(err)
	s = status.AsStreamingError(err)
	assert.True(t, s.IsSkippedOperation())
}

// The same domain error must be projected according to the invoked service,
// for both unary and streaming RPCs, before the corresponding client wraps it.
func TestPairedServiceInterceptors(t *testing.T) {
	for _, streaming := range []bool{false, true} {
		for _, tc := range []struct {
			name     string
			err      error
			viewCode viewpb.ViewCode
		}{
			{"missing WAL", status.NewChannelNotExist("p"), viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY},
			{"old term", status.NewUnmatchedChannelTerm("p", 1, 2), viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY},
			{"shutdown", status.NewOnShutdownError("closed"), viewpb.ViewCode_VIEW_CODE_ON_SHUTDOWN},
			{"internal WAL error", status.NewInner("broken"), viewpb.ViewCode_VIEW_CODE_UNKNOWN},
			{"node mismatch", merr.WrapErrNodeNotMatch(1, 2), viewpb.ViewCode_VIEW_CODE_NOT_PRIMARY},
		} {
			t.Run(fmt.Sprintf("%t/%s", streaming, tc.name), func(t *testing.T) {
				for _, method := range []string{viewpb.ViewSyncService_SyncQueryView_FullMethodName, streamingpb.StreamingNodeManagerService_CollectStatus_FullMethodName} {
					var wire error
					if streaming {
						wire = NewServiceStreamServerInterceptor()(nil, nil, &grpc.StreamServerInfo{FullMethod: method}, func(any, grpc.ServerStream) error { return tc.err })
					} else {
						_, wire = NewServiceUnaryServerInterceptor()(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: method}, func(context.Context, any) (any, error) { return nil, tc.err })
					}
					var got error
					if streaming {
						_, got = NewServiceStreamClientInterceptor()(t.Context(), nil, nil, method, func(context.Context, *grpc.StreamDesc, *grpc.ClientConn, string, ...grpc.CallOption) (grpc.ClientStream, error) {
							return nil, wire
						})
					} else {
						got = NewServiceUnaryClientInterceptor()(t.Context(), method, nil, nil, nil, func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error { return wire })
					}
					if strings.HasPrefix(method, viewpb.ServiceMethodPrefix) {
						require.Equal(t, tc.viewCode, viewerror.AsViewError(got).Code)
						for _, detail := range grpcstatus.Convert(got).Details() {
							_, isStreaming := detail.(*streamingpb.StreamingError)
							require.False(t, isStreaming)
						}
					} else {
						require.Equal(t, status.AsStreamingError(convertMilvusErrorIntoStreamingError(tc.err)).Code, status.AsStreamingError(got).Code)
					}
				}
			})
		}
	}
}

func TestViewServerInterceptorPreservesBusinessAndTransportErrors(t *testing.T) {
	for _, source := range []error{
		merr.SegcoreError(2042, "invalid ef"),
		merr.WrapErrParameterInvalidMsg("bad expression"),
		merr.WrapErrServiceUnavailable("retry"),
	} {
		wire := convertViewServiceError(source)
		var business *commonpb.Status
		for _, detail := range grpcstatus.Convert(wire).Details() {
			if st, ok := detail.(*commonpb.Status); ok {
				business = st
			}
		}
		require.NotNil(t, business)
		restored := merr.Error(business)
		require.Equal(t, merr.Code(source), merr.Code(restored))
		require.Equal(t, merr.GetErrorType(source), merr.GetErrorType(restored))
	}
	for _, err := range []error{nil, context.Canceled, context.DeadlineExceeded, io.EOF, grpcstatus.Error(codes.Unavailable, "disconnected")} {
		require.Equal(t, err, convertViewServiceError(err))
	}
	rateLimit := convertViewServiceError(status.NewRateLimitRejected("busy"))
	require.EqualValues(t, merr.Code(merr.ErrServiceRateLimit), merr.Code(viewerror.ConvertViewError("test", rateLimit)))
	for _, streaming := range []bool{false, true} {
		for _, source := range []error{nil, merr.WrapErrServiceInternalMsg("unrelated service")} {
			var got error
			if streaming {
				got = NewServiceStreamServerInterceptor()(nil, nil, &grpc.StreamServerInfo{FullMethod: "/other.Service/Call"}, func(any, grpc.ServerStream) error { return source })
			} else {
				_, got = NewServiceUnaryServerInterceptor()(t.Context(), nil, &grpc.UnaryServerInfo{FullMethod: "/other.Service/Call"}, func(context.Context, any) (any, error) { return nil, source })
			}
			require.Equal(t, source, got)
		}
	}
}
