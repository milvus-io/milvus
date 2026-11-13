package interceptor

import (
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"

	"github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	pkginterceptor "github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

// Exercise the real ServerID validation origin and gRPC serialization, rather
// than creating an already-encoded routing error in the client test.
func TestQueryViewNodeNotMatchWire(t *testing.T) {
	serverCalls := atomic.Int32{}
	conn := newQueryViewTestConn(t, func(ctx context.Context, req any) (any, error) {
		serverCalls.Add(1)
		return &viewpb.GetQueryPlanResponse{}, nil
	})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	staleCtx := metadata.AppendToOutgoingContext(ctx, pkginterceptor.ServerIDKey, "1682")

	for _, method := range []string{
		viewpb.QueryPlanService_GetQueryPlan_FullMethodName,
		viewpb.QueryPlanService_GetMVCCTimestamp_FullMethodName,
		viewpb.ViewQueryService_SearchOnView_FullMethodName,
		viewpb.ViewQueryService_QueryOnView_FullMethodName,
	} {
		t.Run(method, func(t *testing.T) {
			err := conn.Invoke(staleCtx, method, &emptypb.Empty{}, &emptypb.Empty{})
			converted := viewerror.ConvertViewError(method, err)
			require.True(t, viewerror.AsViewError(converted).IsNotPrimary())
			require.Contains(t, converted.Error(), "expectedNodeID=1682")
			require.Contains(t, converted.Error(), "actualNodeID=1683")
		})
	}
	require.Zero(t, serverCalls.Load(), "validation must reject before executing a query")

	matchingCtx := metadata.AppendToOutgoingContext(ctx, pkginterceptor.ServerIDKey, "1683")
	_, err := viewpb.NewQueryPlanServiceClient(conn).GetQueryPlan(matchingCtx, &viewpb.GetQueryPlanRequest{})
	require.NoError(t, err)
	require.EqualValues(t, 1, serverCalls.Load())

	// Streaming RPCs retain their existing StreamingError dialect.
	err = conn.Invoke(staleCtx, streamingpb.StreamingNodeStateService_GetComponentStates_FullMethodName, &emptypb.Empty{}, &emptypb.Empty{})
	streamingErr := status.AsStreamingError(status.ConvertStreamingError("GetComponentStates", err))
	require.True(t, streamingErr.IsSkippedOperation())
}

func TestQueryViewErrorsKeepTheirWireSemantics(t *testing.T) {
	viewErr := viewerror.NewGRPCStatusFromViewError(viewerror.NewViewInvalidated("view changed")).Err()
	for _, test := range []struct {
		name string
		err  error
	}{
		{name: "view error", err: viewErr},
		{name: "unknown with matching text", err: grpcstatus.Error(codes.Unknown, merr.WrapErrNodeNotMatch(1682, 1683).Error())},
	} {
		t.Run(test.name, func(t *testing.T) {
			conn := newQueryViewTestConn(t, func(context.Context, any) (any, error) { return nil, test.err })
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_, err := viewpb.NewQueryPlanServiceClient(conn).GetQueryPlan(ctx, &viewpb.GetQueryPlanRequest{})
			require.Equal(t, grpcstatus.Convert(test.err).Proto(), grpcstatus.Convert(err).Proto())
			require.False(t, viewerror.AsViewError(viewerror.ConvertViewError("GetQueryPlan", err)).IsNotPrimary())
			if test.name == "view error" {
				require.True(t, viewerror.AsViewError(viewerror.ConvertViewError("GetQueryPlan", err)).IsViewInvalidated())
			}
		})
	}

	t.Run("cluster validation precedes node validation", func(t *testing.T) {
		paramtable.Init()
		conn := newQueryViewTestConn(t, func(context.Context, any) (any, error) {
			t.Error("cluster validation should reject the request")
			return &viewpb.GetQueryPlanResponse{}, nil
		})
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		ctx = metadata.AppendToOutgoingContext(ctx,
			pkginterceptor.ClusterKey, paramtable.Get().CommonCfg.ClusterPrefix.GetValue()+"-other",
			pkginterceptor.ServerIDKey, "1682")
		_, err := viewpb.NewQueryPlanServiceClient(conn).GetQueryPlan(ctx, &viewpb.GetQueryPlanRequest{})
		converted := viewerror.ConvertViewError("GetQueryPlan", err)
		require.True(t, viewerror.AsViewError(converted).IsNotPrimary())
		require.Contains(t, converted.Error(), merr.ErrServiceCrossClusterRouting.Error())
		require.NotContains(t, converted.Error(), "expectedNodeID")
	})
}

func newQueryViewTestConn(t *testing.T, handler grpc.UnaryHandler) *grpc.ClientConn {
	t.Helper()
	listener := bufconn.Listen(1024 * 1024)
	t.Cleanup(func() { _ = listener.Close() })
	server := grpc.NewServer(grpc.ChainUnaryInterceptor(
		NewServiceUnaryServerInterceptor(),
		pkginterceptor.ClusterValidationUnaryServerInterceptor(),
		pkginterceptor.ServerIDValidationUnaryServerInterceptor(func() int64 { return 1683 }),
		func(ctx context.Context, req any, _ *grpc.UnaryServerInfo, _ grpc.UnaryHandler) (any, error) {
			return handler(ctx, req)
		},
	))
	viewpb.RegisterQueryPlanServiceServer(server, &viewpb.UnimplementedQueryPlanServiceServer{})
	viewpb.RegisterViewQueryServiceServer(server, &viewpb.UnimplementedViewQueryServiceServer{})
	viewpb.RegisterViewSyncServiceServer(server, &viewpb.UnimplementedViewSyncServiceServer{})
	streamingpb.RegisterStreamingNodeStateServiceServer(server, &streamingpb.UnimplementedStreamingNodeStateServiceServer{})
	go server.Serve(listener)
	t.Cleanup(server.Stop)
	conn, err := grpc.NewClient("passthrough:///query-view-test",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}
