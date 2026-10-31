package viewerror

import (
	"context"
	"io"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestRPCContextPreservesOriginalError(t *testing.T) {
	info := &viewpb.RPCRequestContext{Method: viewpb.ViewQueryService_SearchOnView_FullMethodName, Target: &viewpb.RPCRequestContext_QueryNode{QueryNode: &viewpb.QueryNodeRequestContext{NodeId: 11}}}
	require.NoError(t, AttachRPCContext(nil, info))
	for _, source := range []error{context.Canceled, context.DeadlineExceeded, io.EOF, NewViewInvalidated("retired"), merr.WrapErrServiceInternalMsg("failed"), status.Error(codes.Unavailable, "disconnected"), errors.New("unexpected")} {
		got := AttachRPCContext(source, info)
		require.True(t, errors.Is(got, source))
		require.Equal(t, source.Error(), got.Error(), "inner client must not format context")
		require.True(t, proto.Equal(info, RPCContextFromDetails(status.Convert(got).Details())))
	}
	source := ConvertViewError("SearchOnView", NewGRPCStatusFromError(merr.SegcoreError(2042, "invalid ef")).Err())
	got := AttachRPCContext(source, info)
	require.EqualValues(t, 2042, merr.Code(got))
	require.Equal(t, merr.GetErrorType(source), merr.GetErrorType(got))
	require.True(t, AsViewError(AttachRPCContext(ConvertViewError("SearchOnView", NewGRPCStatusFromViewError(NewViewInvalidated("retired")).Err()), info)).IsRetryable())

	// Local context overrides peer context without changing the original details.
	next := proto.Clone(info).(*viewpb.RPCRequestContext)
	next.GetQueryNode().NodeId = 22
	nextErr := AttachRPCContext(got, next)
	require.True(t, proto.Equal(next, RPCContextFromDetails(status.Convert(nextErr).Details())))
	require.True(t, proto.Equal(info, RPCContextFromDetails(status.Convert(got).Details())))
}

func TestRPCContextIsIndependentWireDetail(t *testing.T) {
	info := &viewpb.RPCRequestContext{Method: viewpb.ViewQueryService_QueryOnView_FullMethodName, Target: &viewpb.RPCRequestContext_StreamingNode{StreamingNode: &viewpb.StreamingNodeRequestContext{NodeId: 11}}}
	for _, source := range []*status.Status{
		NewGRPCStatusFromViewError(NewViewInvalidated("retired")),
		NewGRPCStatusFromError(merr.SegcoreError(2042, "invalid ef")),
		streamingstatus.NewGRPCStatusFromStreamingError(streamingstatus.NewOnShutdownError("closing")),
		status.New(codes.Unavailable, "disconnected"),
	} {
		snapshot := proto.Clone(info).(*viewpb.RPCRequestContext)
		attached := status.Convert(AttachRPCContext(source.Err(), info))
		// Mutation after attachment cannot alter the serialized request context.
		info.GetStreamingNode().NodeId++
		encoded, err := proto.Marshal(attached.Proto())
		require.NoError(t, err)
		decoded := source.Proto()
		require.NoError(t, proto.Unmarshal(encoded, decoded))
		roundTrip := status.FromProto(decoded)
		require.Equal(t, source.Code(), roundTrip.Code())
		require.Equal(t, source.Message(), roundTrip.Message())
		require.Len(t, roundTrip.Details(), len(source.Details())+1)
		for i, detail := range source.Proto().Details {
			require.True(t, proto.Equal(detail, roundTrip.Proto().Details[i]))
		}
		require.True(t, proto.Equal(snapshot, RPCContextFromDetails(roundTrip.Details())))
	}
}

func TestRPCContextIgnoresUnrelatedDetails(t *testing.T) {
	for _, details := range [][]any{nil, {&viewpb.ViewError{}}, {errors.New("cannot decode unknown detail")}} {
		require.Nil(t, RPCContextFromDetails(details))
	}
}
