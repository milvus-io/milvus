package streamingquery

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/resolver"
	"google.golang.org/grpc/resolver/manual"
	"google.golang.org/grpc/status"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/attributes"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/balancer/picker"
	"github.com/milvus-io/milvus/internal/util/streamingutil/service/contextutil"
	serviceinterceptor "github.com/milvus-io/milvus/internal/util/streamingutil/service/interceptor"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/internal/views/worknode/handler"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/streamingpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	validation "github.com/milvus-io/milvus/pkg/v3/util/interceptor"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The endpoints must belong to an explicitly managed verification workspace.
// These failure paths need neither the full Proxy QV route nor WAL writes.
func TestQueryViewErrorRPCAtWorkNodes(t *testing.T) {
	qn, sn := os.Getenv("QV_RPC_TEST_QUERYNODE"), os.Getenv("QV_RPC_TEST_STREAMINGNODE")
	if qn == "" || sn == "" {
		t.Skip("requires explicitly managed QueryNode and StreamingNode endpoints")
	}
	for _, target := range []struct{ role, address string }{{typeutil.QueryNodeRole, qn}, {typeutil.StreamingNodeRole, sn}} {
		t.Run(target.role, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), 15*time.Second)
			defer cancel()
			probe, err := grpc.NewClient("passthrough:///"+target.address, grpc.WithTransportCredentials(insecure.NewCredentials()))
			require.NoError(t, err)
			defer probe.Close()
			var states *milvuspb.ComponentStates
			if target.role == typeutil.QueryNodeRole {
				states, err = querypb.NewQueryNodeClient(probe).GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
			} else {
				states, err = streamingpb.NewStreamingNodeStateServiceClient(probe).GetComponentStates(ctx, &milvuspb.GetComponentStatesRequest{})
			}
			require.NoError(t, err)
			nodeID := states.GetState().GetNodeID()
			require.Positive(t, nodeID)
			rb := manual.NewBuilderWithScheme("worknode-error-test")
			rb.InitialState(resolver.State{Addresses: []resolver.Address{{Addr: target.address, BalancerAttributes: attributes.WithServerID(new(attributes.Attributes), nodeID)}}})
			conn, err := grpc.NewClient("worknode-error-test:///node", grpc.WithResolvers(rb), grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithDefaultServiceConfig(`{"loadBalancingConfig":[{"`+picker.ServerIDPickerBalancerName+`":{}}]}`),
				grpc.WithUnaryInterceptor(serviceinterceptor.NewServiceUnaryClientInterceptor(target.role)),
				grpc.WithStreamInterceptor(serviceinterceptor.NewServiceStreamClientInterceptor(target.role)))
			require.NoError(t, err)
			defer conn.Close()
			assertTarget := func(err error) {
				t.Helper()
				info := viewerror.RPCContextFromDetails(status.Convert(err).Details())
				require.NotNil(t, info)
				if target.role == typeutil.QueryNodeRole {
					require.Equal(t, nodeID, info.GetQueryNode().GetNodeId())
				} else {
					require.Equal(t, nodeID, info.GetStreamingNode().GetNodeId())
				}
			}
			if target.role == typeutil.QueryNodeRole {
				_, err = viewpb.NewViewQueryServiceClient(conn).QueryOnView(ctx, &viewpb.QueryOnViewRequest{
					ShardId: &viewpb.ShardID{ReplicaId: 40451, Vchannel: "rpc_missing_999v0"},
					Version: &viewpb.QueryViewVersion{DataVersion: &viewpb.DataVersion{StreamingVersion: 1}, QueryVersion: 1},
					Mvcc:    &viewpb.QueryPlanMVCC{GrowingTimetick: 1, TransformingTimetick: 1}, LegacyReq: &internalpb.RetrieveRequest{CollectionID: 999},
				})
				require.True(t, viewerror.AsViewError(err).IsViewNotFound())
				assertTarget(err)
				t.Logf("missing QN view retains %s and actual node %d", viewerror.AsViewError(err).Code, nodeID)
			} else {
				rpcCtx := handler.EncodeQueryViewPChannelToOutgoingContext(ctx, types.PChannelInfo{Name: "rpc_missing", Term: 1, AccessMode: types.AccessModeRW})
				stream, err := viewpb.NewViewSyncServiceClient(conn).SyncQueryView(rpcCtx)
				require.NoError(t, err)
				_, err = stream.Recv()
				require.True(t, viewerror.AsViewError(err).IsNotPrimary())
				assertTarget(err)
				t.Logf("missing WAL on SyncQueryView retains NotPrimary and actual node %d", nodeID)
			}
			// The outer server interceptor must encode cluster validation failures.
			wrongCluster := metadata.AppendToOutgoingContext(ctx, validation.ClusterKey, "rpc-error-test-other-cluster")
			_, err = viewpb.NewViewQueryServiceClient(conn).SearchOnView(wrongCluster, &viewpb.SearchOnViewRequest{})
			require.True(t, viewerror.AsViewError(err).IsNotPrimary())
			assertTarget(err)
			stream, err := viewpb.NewViewSyncServiceClient(conn).SyncQueryView(wrongCluster)
			require.NoError(t, err)
			_, err = stream.Recv()
			require.True(t, viewerror.AsViewError(err).IsNotPrimary())
			assertTarget(err)
			// An unavailable routing input is never an observed target.
			missing := contextutil.WithFailFastPick(contextutil.WithPickServerID(ctx, nodeID+100000))
			_, err = viewpb.NewViewQueryServiceClient(conn).SearchOnView(missing, &viewpb.SearchOnViewRequest{})
			require.Equal(t, codes.Unavailable, status.Code(err))
			require.Nil(t, viewerror.RPCContextFromDetails(status.Convert(err).Details()))
		})
	}
}
