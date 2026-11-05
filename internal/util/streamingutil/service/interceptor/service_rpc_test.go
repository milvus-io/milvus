package interceptor

import (
	"context"
	"io"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	validation "github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Exercise the same ordering as the node registrations, with a real picker and
// wire serialization. In particular, validation failures must reach the outer
// service encoder even though the application handler was never invoked.
func TestViewRPCPickerAndValidation(t *testing.T) {
	paramtable.Init()
	for _, role := range []string{typeutil.QueryNodeRole, typeutil.StreamingNodeRole} {
		mockey.PatchConvey(role, t, func() {
			failure := merr.SegcoreError(2042, "invalid execution parameter")
			var unaryCalls, streamCalls atomic.Int64
			var successful atomic.Bool
			mockey.Mock(viewpb.UnimplementedViewQueryServiceServer.SearchOnView).To(func(viewpb.UnimplementedViewQueryServiceServer, context.Context, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
				unaryCalls.Add(1)
				if successful.Load() {
					return &viewpb.SearchOnViewResponse{}, nil
				}
				return nil, failure
			}).Build()
			mockey.Mock(viewpb.UnimplementedViewSyncServiceServer.SyncQueryView).To(func(_ viewpb.UnimplementedViewSyncServiceServer, stream viewpb.ViewSyncService_SyncQueryViewServer) error {
				streamCalls.Add(1)
				if _, err := stream.Recv(); err != nil {
					return err
				}
				if successful.Load() {
					return nil
				}
				return failure
			}).Build()
			var serverID atomic.Int64
			serverID.Store(47)
			listener := bufconn.Listen(1024 * 1024)
			server := grpc.NewServer(
				grpc.ChainUnaryInterceptor(NewServiceUnaryServerInterceptor(), validation.ClusterValidationUnaryServerInterceptor(), validation.ServerIDValidationUnaryServerInterceptor(serverID.Load)),
				grpc.ChainStreamInterceptor(NewServiceStreamServerInterceptor(), validation.ClusterValidationStreamServerInterceptor(), validation.ServerIDValidationStreamServerInterceptor(serverID.Load)))
			viewpb.RegisterViewQueryServiceServer(server, &viewpb.UnimplementedViewQueryServiceServer{})
			viewpb.RegisterViewSyncServiceServer(server, &viewpb.UnimplementedViewSyncServiceServer{})
			go func() { _ = server.Serve(listener) }()
			defer server.Stop()
			defer listener.Close()
			rb := manual.NewBuilderWithScheme("validation-test")
			rb.InitialState(resolver.State{Addresses: []resolver.Address{*pickedAddress(47)}})
			conn, err := grpc.NewClient("validation-test:///node", grpc.WithResolvers(rb),
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
				grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"`+picker.ServerIDPickerBalancerName+`":{}}]}`),
				grpc.WithUnaryInterceptor(NewServiceUnaryClientInterceptor(role)), grpc.WithStreamInterceptor(NewServiceStreamClientInterceptor(role)))
			require.NoError(t, err)
			defer conn.Close()
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			for _, scenario := range []string{"business", "cluster mismatch", "node mismatch", "EOF"} {
				callCtx := ctx
				if scenario == "cluster mismatch" {
					callCtx = metadata.AppendToOutgoingContext(ctx, validation.ClusterKey, "another-cluster")
				}
				if scenario == "node mismatch" {
					serverID.Store(48)
				}
				if scenario == "EOF" {
					serverID.Store(47)
					successful.Store(true)
				}
				_, unaryErr := viewpb.NewViewQueryServiceClient(conn).SearchOnView(callCtx, &viewpb.SearchOnViewRequest{})
				stream, err := viewpb.NewViewSyncServiceClient(conn).SyncQueryView(callCtx)
				require.NoError(t, err)
				// Send may race with a validation rejection. Recv obtains its status.
				_ = stream.Send(&viewpb.SyncRequest{})
				_, streamErr := stream.Recv()
				if scenario == "EOF" {
					require.NoError(t, unaryErr)
					require.Same(t, io.EOF, streamErr)
					continue
				}
				for _, err := range []error{unaryErr, streamErr} {
					require.Error(t, err)
					info := viewerror.RPCContextFromDetails(grpcstatus.Convert(err).Details())
					require.NotNil(t, info)
					if role == typeutil.QueryNodeRole {
						require.EqualValues(t, 47, info.GetQueryNode().GetNodeId())
					} else {
						require.EqualValues(t, 47, info.GetStreamingNode().GetNodeId())
					}
					if scenario == "business" {
						require.EqualValues(t, 2042, merr.Code(err))
						require.Equal(t, merr.InputError, merr.GetErrorType(err))
					} else {
						require.True(t, viewerror.AsViewError(err).IsNotPrimary())
						require.Equal(t, codes.FailedPrecondition, grpcstatus.Code(err))
					}
				}
			}
			require.EqualValues(t, 2, unaryCalls.Load())
			require.EqualValues(t, 2, streamCalls.Load())
		})
	}
}
