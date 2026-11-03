package queryclient

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	serviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// Retained requery must cross the public error boundary without replanning its
// snapshot. Use the real gRPC picker and both interceptors, rather than feeding
// pre-encoded status details directly to the formatter.
func TestRetainedQueryPublicErrorOverGRPC(t *testing.T) {
	for _, role := range []string{typeutil.QueryNodeRole, typeutil.StreamingNodeRole} {
		for _, cause := range []error{
			merr.SegcoreError(2042, "invalid ef"), merr.WrapErrServiceUnavailableMsg("preparing"), viewerror.NewViewInvalidated("retired"),
			context.Canceled, context.DeadlineExceeded, status.Error(codes.Unavailable, "transport closed"),
		} {
			mockey.PatchConvey(role+cause.Error(), t, func() {
				listener := bufconn.Listen(1024 * 1024)
				server := grpc.NewServer(grpc.UnaryInterceptor(serviceinterceptor.NewServiceUnaryServerInterceptor()))
				viewpb.RegisterViewQueryServiceServer(server, &viewpb.UnimplementedViewQueryServiceServer{})
				defer server.Stop()
				defer listener.Close()
				rb := manual.NewBuilderWithScheme("public-error-test")
				rb.InitialState(resolver.State{Addresses: []resolver.Address{{Addr: "node", BalancerAttributes: attributes.WithServerID(new(attributes.Attributes), 19)}}})
				conn, err := grpc.NewClient("public-error-test:///node", grpc.WithResolvers(rb),
					grpc.WithTransportCredentials(insecure.NewCredentials()),
					grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
					grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"`+picker.ServerIDPickerBalancerName+`":{}}]}`),
					grpc.WithUnaryInterceptor(serviceinterceptor.NewServiceUnaryClientInterceptor(role)))
				require.NoError(t, err)
				defer conn.Close()
				rpc := viewpb.NewViewQueryServiceClient(conn)
				domainCalls := mockey.Mock(viewpb.UnimplementedViewQueryServiceServer.QueryOnView).To(func(_ viewpb.UnimplementedViewQueryServiceServer, ctx context.Context, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
					if req.GetLegacyReq().GetCollectionID() == 999 {
						forwarded := proto.Clone(req).(*viewpb.QueryOnViewRequest)
						forwarded.LegacyReq.CollectionID = 1
						return rpc.QueryOnView(ctx, forwarded)
					}
					return nil, cause
				}).Build()
				go func() { _ = server.Serve(listener) }()
				service := &fakeViewQueryServiceClient{}
				calls := mockey.Mock((*fakeViewQueryServiceClient).QueryOnView).To(func(_ *fakeViewQueryServiceClient, ctx context.Context, _ qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
					return rpc.QueryOnView(contextutil.WithPickServerID(ctx, 19), req)
				}).Build()
				client := &legacyClient{shardClient: &shardViewQueryClient{queryServiceClient: service}}
				plan := ShardPlan{ShardID: qviews.ShardID{VChannel: "p_1v0", ReplicaID: 2}, Version: &viewpb.QueryViewVersion{QueryVersion: 5}, Mvcc: &viewpb.QueryPlanMVCC{GrowingTimetick: 100, TransformingTimetick: 90}, WorkNodes: []qviews.WorkNode{qviews.NewQueryNode(19)}}
				ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
				defer cancel()
				_, err = client.Query(ctx, &LegacyQueryRequest{Req: &internalpb.RetrieveRequest{CollectionID: 999}, Plans: []ShardPlan{plan}})
				requireBusinessError(t, err)
				plain := toMilvusError(cause)
				require.Equal(t, merr.Code(plain), merr.Code(err))
				require.Equal(t, merr.GetErrorType(plain), merr.GetErrorType(err))
				require.Equal(t, merr.IsRetryableErr(plain), merr.IsRetryableErr(err))
				node := "QueryNode"
				if role == typeutil.StreamingNodeRole {
					node = "StreamingNode"
				}
				require.Equal(t, "fail to query on "+node+" 19: "+plain.Error(), err.Error())
				require.Equal(t, 1, calls.Times(), "retained requery must not retry another snapshot")
				require.Equal(t, 2, domainCalls.Times(), "business classification must survive two real RPC hops")
			})
		}
	}
}
