package queryclient

import (
	"context"
	"io"
	"testing"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func requireBusinessError(t *testing.T, err error) {
	t.Helper()
	require.True(t, merr.IsMilvusError(err))
	var viewStatus *viewerror.ViewClientStatus
	var ve *viewerror.ViewError
	var streamingStatus *streamingstatus.StreamingClientStatus
	var se *streamingstatus.StreamingError
	require.False(t, errors.As(err, &viewStatus))
	require.False(t, errors.As(err, &ve))
	require.False(t, errors.As(err, &streamingStatus))
	require.False(t, errors.As(err, &se))
	_, isRPC := status.FromError(err)
	require.False(t, isRPC)
	require.Equal(t, merr.Status(err), merr.Status(toMilvusError(err)), "conversion must be idempotent")
}

func TestToMilvusErrorPreservesBusinessStatus(t *testing.T) {
	require.NoError(t, toMilvusError(nil))
	for _, source := range []error{
		merr.SegcoreError(int32(merr.CodeInvalidParameter), "invalid ef"),
		merr.Wrap(merr.WrapErrFieldNotFound(100), "query field"),
		merr.WrapErrCollectionNotLoaded(1),
		merr.WrapErrServiceNotReadyMsg("preparing"),
		merr.WrapErrServiceUnavailableMsg("retry later"),
	} {
		for _, wire := range []error{
			source,
			viewerror.NewGRPCStatusFromError(source).Err(),
			streamingstatus.ConvertStreamingError("GetQueryPlan", viewerror.NewGRPCStatusFromError(source).Err()),
			merr.Wrap(viewerror.ConvertViewError("SearchOnView", viewerror.NewGRPCStatusFromError(source).Err()), "phase 2"),
		} {
			got := toMilvusError(wire)
			requireBusinessError(t, got)
			require.Equal(t, merr.Status(source), merr.Status(got))
		}
	}
}

func TestToMilvusErrorProtocolMapping(t *testing.T) {
	for _, tc := range []struct {
		source *viewerror.ViewError
		want   error
	}{
		{viewerror.NewViewInvalidated("retired"), merr.ErrQueryViewNotReady},
		{viewerror.NewViewNotFound("missing"), merr.ErrQueryViewNotReady},
		{viewerror.NewOnShutdownError("stopping"), merr.ErrServiceUnavailable},
		{viewerror.NewNotPrimaryError("moved"), merr.ErrServiceUnavailable},
		{viewerror.NewUnknownError("unexpected"), merr.ErrServiceInternal},
	} {
		for _, source := range []error{tc.source, viewerror.ConvertViewError("QueryOnView", viewerror.NewGRPCStatusFromViewError(tc.source).Err())} {
			got := toMilvusError(merr.Wrap(source, "dispatch"))
			requireBusinessError(t, got)
			require.True(t, errors.Is(got, tc.want))
			require.Equal(t, merr.SystemError, merr.GetErrorType(got))
			require.Equal(t, merr.IsRetryableErr(tc.want), merr.IsRetryableErr(got))
			require.Contains(t, got.Error(), tc.source.Cause)
			require.NotContains(t, got.Error(), "VIEW_CODE_")
		}
	}
	for _, code := range []streamingpb.StreamingCode{
		streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_NOT_EXIST,
		streamingpb.StreamingCode_STREAMING_CODE_CHANNEL_FENCED,
		streamingpb.StreamingCode_STREAMING_CODE_UNMATCHED_CHANNEL_TERM,
		streamingpb.StreamingCode_STREAMING_CODE_ON_SHUTDOWN,
		streamingpb.StreamingCode_STREAMING_CODE_RATE_LIMIT_REJECTED,
		streamingpb.StreamingCode_STREAMING_CODE_INVAILD_ARGUMENT,
		streamingpb.StreamingCode_STREAMING_CODE_UNKNOWN,
	} {
		want := merr.ErrServiceUnavailable
		switch code {
		case streamingpb.StreamingCode_STREAMING_CODE_RATE_LIMIT_REJECTED:
			want = merr.ErrServiceRateLimit
		case streamingpb.StreamingCode_STREAMING_CODE_INVAILD_ARGUMENT, streamingpb.StreamingCode_STREAMING_CODE_UNKNOWN:
			want = merr.ErrServiceInternal
		}
		se := streamingstatus.New(code, "channel detail")
		for _, source := range []error{se, streamingstatus.ConvertStreamingError("Query", streamingstatus.NewGRPCStatusFromStreamingError(se).Err())} {
			got := toMilvusError(source)
			requireBusinessError(t, got)
			require.True(t, errors.Is(got, want))
			require.Equal(t, merr.SystemError, merr.GetErrorType(got))
			require.NotContains(t, got.Error(), "STREAMING_CODE_")
		}
	}
}

func TestToMilvusErrorCancellationAndTransport(t *testing.T) {
	for _, tc := range []struct {
		cause error
		grpc  codes.Code
		code  int32
	}{
		{context.Canceled, codes.Canceled, merr.CanceledCode},
		{context.DeadlineExceeded, codes.DeadlineExceeded, merr.TimeoutCode},
	} {
		businessWire := status.New(tc.grpc, "stopped")
		businessWire, err := businessWire.WithDetails(merr.Status(tc.cause))
		require.NoError(t, err)
		for _, source := range []error{
			tc.cause, merr.Wrap(tc.cause, "query"), status.Error(tc.grpc, "stopped"), businessWire.Err(),
			viewerror.ConvertViewError("QueryOnView", businessWire.Err()),
			streamingstatus.ConvertStreamingError("GetQueryPlan", businessWire.Err()),
		} {
			got := toMilvusError(source)
			requireBusinessError(t, got)
			require.True(t, errors.Is(got, tc.cause))
			require.Equal(t, tc.code, merr.Code(got))
			require.True(t, merr.IsCanceledOrTimeout(got))
		}
	}
	for _, tc := range []struct{ source, want error }{
		{status.Error(codes.Unavailable, "disconnected"), merr.ErrServiceUnavailable},
		{status.Error(codes.Unimplemented, "old server"), merr.ErrServiceUnimplemented},
		{status.Error(codes.InvalidArgument, "bad internal metadata"), merr.ErrServiceInternal},
		{status.Error(codes.NotFound, "unknown resource"), merr.ErrServiceInternal},
		{io.EOF, merr.ErrServiceUnavailable},
		{errors.New("untyped failure"), merr.ErrServiceInternal},
	} {
		got := toMilvusError(tc.source)
		requireBusinessError(t, got)
		require.True(t, errors.Is(got, tc.want))
	}
}

func TestPublicQueryEntrypointsNormalizeEarlyErrors(t *testing.T) {
	mockey.PatchConvey("resolver failures cross every public query boundary", t, func() {
		mockey.Mock((*legacyResolver).ResolveVChannels).Return(nil, viewerror.NewViewNotFound("no view")).Build()
		legacy := &legacyClient{shardResolver: &legacyResolver{}}
		generic := &viewQueryClientImpl{shardResolver: &legacyResolver{}}
		calls := []func() error{
			func() error {
				_, err := legacy.Search(t.Context(), &LegacySearchRequest{Req: &internalpb.SearchRequest{}})
				return err
			},
			func() error {
				_, err := legacy.Query(t.Context(), &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{}})
				return err
			},
			func() error { _, err := legacy.Statistics(t.Context(), 1, &viewpb.StatisticsRequest{}, 0); return err },
			func() error {
				_, err := generic.Search(t.Context(), &SearchRequest{Req: &internalpb.SearchRequest{}})
				return err
			},
			func() error {
				_, err := generic.Query(t.Context(), &QueryRequest{Req: &internalpb.RetrieveRequest{}})
				return err
			},
		}
		for _, call := range calls {
			err := call()
			requireBusinessError(t, err)
			require.True(t, errors.Is(err, merr.ErrQueryViewNotReady))
		}
	})
}

func TestToMilvusErrorFormatsRPCContext(t *testing.T) {
	for _, tc := range []struct{ method, role, prefix string }{
		{viewpb.ViewQueryService_SearchOnView_FullMethodName, "querynode", "fail to search on QueryNode 11: "},
		{viewpb.ViewQueryService_QueryOnView_FullMethodName, "querynode", "fail to query on QueryNode 11: "},
		{viewpb.ViewQueryService_StatisticsOnView_FullMethodName, "querynode", "fail to get statistics on QueryNode 11: "},
		{viewpb.ViewQueryService_SearchOnView_FullMethodName, "streamingnode", "fail to search on StreamingNode 11: "},
		{viewpb.QueryPlanService_GetQueryPlan_FullMethodName, "streamingnode", "fail to get query plan on StreamingNode 11: "},
		{viewpb.QueryPlanService_GetMVCCTimestamp_FullMethodName, "streamingnode", "fail to get MVCC timestamp on StreamingNode 11: "},
		{"unknown", "querynode", ""},
		{viewpb.ViewQueryService_SearchOnView_FullMethodName, "unknown", ""},
	} {
		source := merr.SegcoreError(2042, "invalid ef")
		wire := viewerror.ConvertViewError(tc.method, viewerror.NewGRPCStatusFromError(source).Err())
		info := &viewpb.RPCRequestContext{Method: tc.method}
		switch tc.role {
		case "querynode":
			info.Target = &viewpb.RPCRequestContext_QueryNode{QueryNode: &viewpb.QueryNodeRequestContext{NodeId: 11}}
		case "streamingnode":
			info.Target = &viewpb.RPCRequestContext_StreamingNode{StreamingNode: &viewpb.StreamingNodeRequestContext{NodeId: 11}}
		}
		detailed := viewerror.AttachRPCContext(wire, info)
		// Both local error chains and serialized RPC details retain the context.
		for _, input := range []error{detailed, status.FromProto(status.Convert(detailed).Proto()).Err()} {
			got := toMilvusError(input)
			requireBusinessError(t, got)
			require.Equal(t, tc.prefix+source.Error(), got.Error())
			require.Equal(t, merr.Code(source), merr.Code(got))
			require.Equal(t, merr.GetErrorType(source), merr.GetErrorType(got))
			require.Equal(t, merr.IsRetryableErr(source), merr.IsRetryableErr(got))
		}
	}
	for _, source := range []error{
		context.Canceled, context.DeadlineExceeded, io.EOF,
		status.Error(codes.Unavailable, "disconnected"),
		viewerror.NewViewInvalidated("retired"),
		merr.WrapErrParameterInvalidMsg("bad expression"),
		streamingstatus.NewGRPCStatusFromStreamingError(streamingstatus.NewOnShutdownError("closing")).Err(),
	} {
		got := toMilvusError(viewerror.AttachRPCContext(source, &viewpb.RPCRequestContext{Method: viewpb.ViewQueryService_SearchOnView_FullMethodName, Target: &viewpb.RPCRequestContext_QueryNode{QueryNode: &viewpb.QueryNodeRequestContext{NodeId: 11}}}))
		plain := toMilvusError(source)
		requireBusinessError(t, got)
		require.Equal(t, merr.Code(plain), merr.Code(got))
		require.Equal(t, merr.IsRetryableErr(plain), merr.IsRetryableErr(got))
		require.Equal(t, merr.IsCanceledOrTimeout(plain), merr.IsCanceledOrTimeout(got))
	}
}

func TestToMilvusErrorPreservesStreamingContext(t *testing.T) {
	info := &viewpb.RPCRequestContext{
		Method: viewpb.QueryPlanService_GetQueryPlan_FullMethodName,
		Target: &viewpb.RPCRequestContext_StreamingNode{
			StreamingNode: &viewpb.StreamingNodeRequestContext{NodeId: 11},
		},
	}
	for _, source := range []*streamingstatus.StreamingError{
		streamingstatus.NewOnShutdownError("closing"),
		streamingstatus.NewChannelFenced("p0"),
		streamingstatus.NewChannelNotExist("p0"),
		streamingstatus.NewUnmatchedChannelTerm("p0", 7, 8),
		streamingstatus.NewRateLimitRejected("busy"),
	} {
		t.Run(source.Code.String(), func(t *testing.T) {
			plain := toMilvusError(source)
			for _, input := range []error{
				source,
				merr.Wrap(source, "local provider"),
				streamingstatus.ConvertStreamingError(info.Method, streamingstatus.NewGRPCStatusFromStreamingError(source).Err()),
			} {
				attached := viewerror.AttachRPCContext(input, info)
				require.True(t, errors.Is(attached, input))
				for _, result := range []error{attached, status.FromProto(status.Convert(attached).Proto()).Err()} {
					got := toMilvusError(result)
					requireBusinessError(t, got)
					require.Equal(t, merr.Code(plain), merr.Code(got))
					require.Equal(t, merr.IsRetryableErr(plain), merr.IsRetryableErr(got))
					require.Equal(t, "fail to get query plan on StreamingNode 11: "+plain.Error(), got.Error())
				}
			}
		})
	}
}
