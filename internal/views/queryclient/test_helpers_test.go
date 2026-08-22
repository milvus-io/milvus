package queryclient

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/queryclient/resolver"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

// These types are patch targets for mockey. Each test patches every method it
// invokes; the bodies deliberately fail if a test misses a required mock.
type mockShardResolver struct{}

func (*mockShardResolver) ResolveVChannels(context.Context, int64) ([]string, error) {
	panic("ResolveVChannels must be mocked")
}

func (*mockShardResolver) ResolveShard(context.Context, int64, string) (*resolver.ShardReplicas, error) {
	panic("ResolveShard must be mocked")
}

type mockQueryPlanClient struct{}

func (*mockQueryPlanClient) GetQueryPlan(context.Context, qviews.ShardID, *viewpb.GetQueryPlanRequest) (*viewpb.GetQueryPlanResponse, error) {
	panic("GetQueryPlan must be mocked")
}

type mockReplicaPicker struct{}

func (*mockReplicaPicker) Pick(context.Context, *resolver.ShardReplicas) (qviews.ShardID, error) {
	panic("Pick must be mocked")
}

type mockViewQueryServiceClient struct{}

func (*mockViewQueryServiceClient) SearchOnView(context.Context, qviews.WorkNode, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	panic("SearchOnView must be mocked")
}

func (*mockViewQueryServiceClient) QueryOnView(context.Context, qviews.WorkNode, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	panic("QueryOnView must be mocked")
}

type mockStreamingNodeClient struct{}

func (*mockStreamingNodeClient) SearchOnView(context.Context, types.PChannelInfo, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	panic("SearchOnView must be mocked")
}

func (*mockStreamingNodeClient) QueryOnView(context.Context, types.PChannelInfo, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	panic("QueryOnView must be mocked")
}

type mockQueryNodeClient struct{}

func (*mockQueryNodeClient) SearchOnView(context.Context, int64, *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	panic("SearchOnView must be mocked")
}

func (*mockQueryNodeClient) QueryOnView(context.Context, int64, *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	panic("QueryOnView must be mocked")
}

func testShard(vchannel string) qviews.ShardID {
	return qviews.ShardID{ReplicaID: 1, VChannel: vchannel}
}

func queryWorkNode(nodeID int64) *viewpb.QueryPlanWorkNode {
	return &viewpb.QueryPlanWorkNode{Node: &viewpb.QueryPlanWorkNode_QueryNode{
		QueryNode: &viewpb.QueryWorkNode{NodeId: nodeID},
	}}
}

func streamingWorkNode(pchannel string) *viewpb.QueryPlanWorkNode {
	return &viewpb.QueryPlanWorkNode{Node: &viewpb.QueryPlanWorkNode_StreamingNode{
		StreamingNode: &viewpb.StreamingWorkNode{Pchannel: pchannel},
	}}
}
