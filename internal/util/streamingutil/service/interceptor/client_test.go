package interceptor

import (
	"context"
	"fmt"
	"io"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	streamingstatus "github.com/milvus-io/milvus/internal/util/streamingutil/status"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func pickedAddress(id int64) *resolver.Address {
	return &resolver.Address{Addr: "localhost:1234", BalancerAttributes: attributes.WithServerID(new(attributes.Attributes), id)}
}

func TestUnaryRequestContextUsesObservedPick(t *testing.T) {
	for _, role := range []string{typeutil.StreamingNodeRole, typeutil.QueryNodeRole} {
		for _, method := range []string{viewpb.ViewQueryService_SearchOnView_FullMethodName, streamingpb.StreamingNodeManagerService_CollectStatus_FullMethodName} {
			t.Run(role+method, func(t *testing.T) {
				var source error
				if method == streamingpb.StreamingNodeManagerService_CollectStatus_FullMethodName {
					source = streamingstatus.NewGRPCStatusFromStreamingError(streamingstatus.NewRateLimitRejected("busy")).Err()
				} else {
					source = viewerror.NewGRPCStatusFromError(merr.SegcoreError(2042, "invalid ef")).Err()
				}
				ctx := contextutil.WithPickServerID(t.Context(), 1)
				invoke := func(ctx context.Context, _ string, _, _ interface{}, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
					contextutil.SetPickedServerID(ctx, 7)
					contextutil.SetPickedServerID(ctx, 9) // a later attempt
					return source
				}
				got := NewServiceUnaryClientInterceptor(role)(ctx, method, nil, nil, nil, invoke)
				require.Equal(t, grpcstatus.Code(source), grpcstatus.Code(got))
				if method == streamingpb.StreamingNodeManagerService_CollectStatus_FullMethodName {
					require.True(t, streamingstatus.AsStreamingError(got).IsRateLimitRejected())
				}
				info := viewerror.RPCContextFromDetails(grpcstatus.Convert(got).Details())
				require.NotNil(t, info)
				require.Equal(t, method, info.Method)
				if role == typeutil.StreamingNodeRole {
					require.EqualValues(t, 9, info.GetStreamingNode().GetNodeId())
					require.Nil(t, info.GetQueryNode())
				} else {
					require.EqualValues(t, 9, info.GetQueryNode().GetNodeId())
					require.Nil(t, info.GetStreamingNode())
					if method == viewpb.ViewQueryService_SearchOnView_FullMethodName {
						require.EqualValues(t, 2042, merr.Code(got))
					}
				}
			})
		}
	}
}

func TestRPCContextDoesNotInferRequestedNode(t *testing.T) {
	for _, mode := range []string{"no pick", "failed repick", "success"} {
		t.Run(mode, func(t *testing.T) {
			invoke := func(ctx context.Context, _ string, _, _ interface{}, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
				switch mode {
				case "failed repick":
					contextutil.SetPickedServerID(ctx, 7)
					contextutil.SetPickedServerID(ctx, contextutil.UnspecifiedServerID)
				case "success":
					contextutil.SetPickedServerID(ctx, 7)
					return nil
				}
				return grpcstatus.Error(codes.Unavailable, "not connected")
			}
			got := NewServiceUnaryClientInterceptor(typeutil.QueryNodeRole)(contextutil.WithPickServerID(t.Context(), 99), viewpb.ViewQueryService_QueryOnView_FullMethodName, nil, nil, nil, invoke)
			if mode == "success" {
				require.NoError(t, got)
			} else {
				require.Equal(t, codes.Unavailable, grpcstatus.Code(got))
				require.Nil(t, viewerror.RPCContextFromDetails(grpcstatus.Convert(got).Details()))
			}
		})
	}
}

func TestConcurrentRPCContextsAreIndependent(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		t.Run(fmt.Sprint(explicit), func(t *testing.T) {
			intercept := NewServiceUnaryClientInterceptor(typeutil.QueryNodeRole)
			parent := t.Context()
			const count = 16
			var wg sync.WaitGroup
			wg.Add(count)
			for id := int64(1); id <= count; id++ {
				go func() {
					defer wg.Done()
					ctx := parent
					if explicit {
						// Production callers create the route at dispatch, not on
						// the parent context shared by concurrent RPCs.
						ctx = contextutil.WithPickServerID(ctx, id)
					}
					err := intercept(ctx, viewpb.ViewQueryService_QueryOnView_FullMethodName, nil, nil, nil, func(rpcCtx context.Context, _ string, _, _ interface{}, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
						if explicit {
							require.Same(t, ctx, rpcCtx)
						}
						contextutil.SetPickedServerID(rpcCtx, id)
						return grpcstatus.Error(codes.Unavailable, "disconnected")
					})
					info := viewerror.RPCContextFromDetails(grpcstatus.Convert(err).Details())
					require.Equal(t, id, info.GetQueryNode().GetNodeId())
				}()
			}
			wg.Wait()
			require.Equal(t, contextutil.UnspecifiedServerID, contextutil.GetPickedServerID(parent))
		})
	}
}

func TestStreamRequestContextAndProtocolErrors(t *testing.T) {
	mockey.PatchConvey("stream failures retain the actual peer", t, func() {
		// Use the real gRPC stream type; mock only its I/O operations.
		var stream grpc.ClientStream
		listener := bufconn.Listen(1024 * 1024)
		server := grpc.NewServer(grpc.UnaryInterceptor(NewServiceUnaryServerInterceptor()), grpc.StreamInterceptor(NewServiceStreamServerInterceptor()))
		go func() { _ = server.Serve(listener) }()
		defer server.Stop()
		defer listener.Close()
		conn, err := grpc.NewClient("passthrough:///stream", grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }))
		require.NoError(t, err)
		defer conn.Close()
		ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
		defer cancel()
		stream, err = conn.NewStream(ctx, &grpc.StreamDesc{ServerStreams: true, ClientStreams: true}, "/test/stream", grpc.WaitForReady(true))
		require.NoError(t, err)
		failure := streamingstatus.NewGRPCStatusFromStreamingError(streamingstatus.NewOnShutdownError("closing")).Err()
		mockey.Mock(mockey.GetMethod(stream, "SendMsg")).Return(failure).Build()
		recv := mockey.Mock(mockey.GetMethod(stream, "RecvMsg")).Return(failure).Build()
		mockey.Mock(mockey.GetMethod(stream, "Header")).Return(metadata.MD{}, failure).Build()
		mockey.Mock(mockey.GetMethod(stream, "CloseSend")).Return(failure).Build()
		method := streamingpb.StreamingNodeHandlerService_Produce_FullMethodName
		pickCtx := contextutil.WithPickServerID(t.Context(), 33)
		wrapped, err := NewServiceStreamClientInterceptor(typeutil.StreamingNodeRole)(pickCtx, &grpc.StreamDesc{}, nil, method, func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			require.Same(t, pickCtx, ctx)
			contextutil.SetPickedServerID(ctx, 33)
			return stream, nil
		})
		require.NoError(t, err)
		_, headerErr := wrapped.Header()
		for _, err := range []error{wrapped.SendMsg(nil), wrapped.RecvMsg(nil), wrapped.CloseSend(), headerErr} {
			require.True(t, streamingstatus.AsStreamingError(err).IsOnShutdown())
			info := viewerror.RPCContextFromDetails(grpcstatus.Convert(err).Details())
			require.EqualValues(t, 33, info.GetStreamingNode().GetNodeId())
		}
		recv.UnPatch()
		mockey.Mock(mockey.GetMethod(stream, "RecvMsg")).Return(io.EOF).Build()
		require.True(t, errors.Is(wrapped.RecvMsg(nil), io.EOF))
	})
}

func TestActualGRPCPickerContextReachesInterceptor(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	server := grpc.NewServer(grpc.UnaryInterceptor(NewServiceUnaryServerInterceptor()), grpc.StreamInterceptor(NewServiceStreamServerInterceptor()))
	viewpb.RegisterViewQueryServiceServer(server, &viewpb.UnimplementedViewQueryServiceServer{})
	go func() { _ = server.Serve(listener) }()
	defer server.Stop()
	defer listener.Close()
	rb := manual.NewBuilderWithScheme("context-test")
	rb.InitialState(resolver.State{Addresses: []resolver.Address{*pickedAddress(47)}})
	conn, err := grpc.NewClient("context-test:///node", grpc.WithResolvers(rb),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"`+picker.ServerIDPickerBalancerName+`":{}}]}`),
		grpc.WithUnaryInterceptor(NewServiceUnaryClientInterceptor(typeutil.QueryNodeRole)))
	require.NoError(t, err)
	defer conn.Close()
	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()
	_, err = viewpb.NewViewQueryServiceClient(conn).QueryOnView(ctx, &viewpb.QueryOnViewRequest{})
	require.Equal(t, codes.Unimplemented, grpcstatus.Code(err))
	info := viewerror.RPCContextFromDetails(grpcstatus.Convert(err).Details())
	require.NotNil(t, info)
	require.EqualValues(t, 47, info.GetQueryNode().GetNodeId())

	// A routing target that does not exist must not be reported as an actual peer.
	_, err = viewpb.NewViewQueryServiceClient(conn).QueryOnView(contextutil.WithPickServerID(ctx, 99), &viewpb.QueryOnViewRequest{})
	require.Equal(t, codes.Unavailable, grpcstatus.Code(err))
	require.Nil(t, viewerror.RPCContextFromDetails(grpcstatus.Convert(err).Details()))
}

func TestViewStreamCreationPreservesViewError(t *testing.T) {
	failure := viewerror.NewGRPCStatusFromViewError(viewerror.NewNotPrimaryError("moved")).Err()
	stream, err := NewServiceStreamClientInterceptor(typeutil.StreamingNodeRole)(t.Context(), &grpc.StreamDesc{}, nil, viewpb.ViewSyncService_SyncQueryView_FullMethodName,
		func(ctx context.Context, _ *grpc.StreamDesc, _ *grpc.ClientConn, _ string, _ ...grpc.CallOption) (grpc.ClientStream, error) {
			contextutil.SetPickedServerID(ctx, 55)
			return nil, failure
		})
	require.Nil(t, stream)
	require.True(t, viewerror.AsViewError(err).IsNotPrimary())
	info := viewerror.RPCContextFromDetails(grpcstatus.Convert(err).Details())
	require.EqualValues(t, 55, info.GetStreamingNode().GetNodeId())
}

func TestInterceptorWithoutRoleStillInstallsPickState(t *testing.T) {
	ctx := contextutil.WithPickServerID(t.Context(), 7)
	invoke := func(rpcCtx context.Context, _ string, _, _ interface{}, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		id, ok := contextutil.GetPickServerID(rpcCtx)
		require.True(t, ok)
		require.EqualValues(t, 7, id)
		contextutil.SetPickedServerID(rpcCtx, id)
		return grpcstatus.Error(codes.Unavailable, "disconnected")
	}
	err := NewServiceUnaryClientInterceptor()(ctx, streamingpb.StreamingNodeManagerService_CollectStatus_FullMethodName, nil, nil, nil, invoke)
	require.Nil(t, viewerror.RPCContextFromDetails(grpcstatus.Convert(err).Details()))
	require.EqualValues(t, 7, contextutil.GetPickedServerID(ctx))
}

func TestInterceptorReusesUnspecifiedPickState(t *testing.T) {
	ctx := contextutil.WithPickServerID(t.Context(), contextutil.UnspecifiedServerID)
	invoke := func(rpcCtx context.Context, _ string, _, _ interface{}, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
		require.Same(t, ctx, rpcCtx)
		contextutil.SetPickedServerID(rpcCtx, 33)
		return grpcstatus.Error(codes.Unavailable, "disconnected")
	}
	err := NewServiceUnaryClientInterceptor(typeutil.QueryNodeRole)(ctx, viewpb.ViewQueryService_QueryOnView_FullMethodName, nil, nil, nil, invoke)
	info := viewerror.RPCContextFromDetails(grpcstatus.Convert(err).Details())
	require.EqualValues(t, 33, info.GetQueryNode().GetNodeId())
	require.EqualValues(t, 33, contextutil.GetPickedServerID(ctx))
}

func TestUnrelatedClientServicesPreserveError(t *testing.T) {
	failure := grpcstatus.Error(codes.Unavailable, "unrelated service")
	got := NewServiceUnaryClientInterceptor(typeutil.QueryNodeRole)(t.Context(), "/other.Service/Call", nil, nil, nil,
		func(ctx context.Context, _ string, _, _ interface{}, _ *grpc.ClientConn, _ ...grpc.CallOption) error {
			contextutil.SetPickedServerID(ctx, 7)
			return failure
		})
	require.Same(t, failure, got)
}
