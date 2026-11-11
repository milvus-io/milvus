package queryclient

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
)

type compositeViewQueryServiceClient struct {
	streamingNode StreamingNodeViewQueryServiceClient
	queryNode     QueryNodeViewQueryServiceClient
}

type StreamingNodeViewQueryServiceClient interface {
	PrewarmOnView(context.Context, types.PChannelInfoAssigned, *viewpb.PrewarmOnViewRequest) (*viewpb.PrewarmOnViewResponse, error)
	SearchOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	QueryOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
}

type QueryNodeViewQueryServiceClient interface {
	PrewarmOnView(context.Context, int64, *viewpb.PrewarmOnViewRequest) (*viewpb.PrewarmOnViewResponse, error)
	StatisticsOnView(ctx context.Context, nodeID int64, req *viewpb.StatisticsOnViewRequest) (*viewpb.StatisticsOnViewResponse, error)

	SearchOnView(ctx context.Context, nodeID int64, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	QueryOnView(ctx context.Context, nodeID int64, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
}

func NewCompositeViewQueryServiceClient(streamingNode StreamingNodeViewQueryServiceClient, queryNode QueryNodeViewQueryServiceClient) ViewQueryServiceClient {
	return &compositeViewQueryServiceClient{
		streamingNode: streamingNode,
		queryNode:     queryNode,
	}
}

func (c *compositeViewQueryServiceClient) SearchOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error) {
	switch typed := node.(type) {
	case qviews.StreamingNode:
		return c.streamingNode.SearchOnView(ctx, types.PChannelInfo{Name: typed.PChannel}, req)
	case qviews.QueryNode:
		return c.queryNode.SearchOnView(ctx, typed.ID, req)
	default:
		return nil, invalidWorkNodeError(node)
	}
}

func (c *compositeViewQueryServiceClient) QueryOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error) {
	switch typed := node.(type) {
	case qviews.StreamingNode:
		return c.streamingNode.QueryOnView(ctx, types.PChannelInfo{Name: typed.PChannel}, req)
	case qviews.QueryNode:
		return c.queryNode.QueryOnView(ctx, typed.ID, req)
	default:
		return nil, invalidWorkNodeError(node)
	}
}

func invalidWorkNodeError(node qviews.WorkNode) error {
	if node == nil {
		return viewerror.NewUnknownError("nil query view work node")
	}
	return viewerror.NewUnknownError("unknown query view work node %s", node.String())
}

func (c *compositeViewQueryServiceClient) StatisticsOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.StatisticsOnViewRequest) (*viewpb.StatisticsOnViewResponse, error) {
	qn, ok := node.(qviews.QueryNode)
	if !ok {
		return nil, invalidWorkNodeError(node)
	}
	return c.queryNode.StatisticsOnView(ctx, qn.ID, req)
}

func (c *compositeViewQueryServiceClient) PrewarmOnView(ctx context.Context, node qviews.WorkNode, plan *qviews.PrewarmPlan) error {
	req := &viewpb.PrewarmOnViewRequest{ShardId: plan.Plan.ShardId, Version: plan.Plan.Version}
	switch target := node.(type) {
	case qviews.StreamingNode:
		if target.PChannel != plan.StreamingNode.Channel.Name {
			return viewerror.NewUnknownError("prewarm owner does not match plan")
		}
		_, err := c.streamingNode.PrewarmOnView(ctx, plan.StreamingNode, req)
		return err
	case qviews.QueryNode:
		_, err := c.queryNode.PrewarmOnView(ctx, target.ID, req)
		return err
	default:
		return invalidWorkNodeError(node)
	}
}
