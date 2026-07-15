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
	SearchOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	QueryOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
	RequeryOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error)
}

type QueryNodeViewQueryServiceClient interface {
	SearchOnView(ctx context.Context, nodeID int64, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	QueryOnView(ctx context.Context, nodeID int64, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
	RequeryOnView(ctx context.Context, nodeID int64, req *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error)
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

func (c *compositeViewQueryServiceClient) RequeryOnView(ctx context.Context, node qviews.WorkNode, req *viewpb.RequeryOnViewRequest) (*viewpb.RequeryOnViewResponse, error) {
	switch typed := node.(type) {
	case qviews.StreamingNode:
		return c.streamingNode.RequeryOnView(ctx, types.PChannelInfo{Name: typed.PChannel}, req)
	case qviews.QueryNode:
		return c.queryNode.RequeryOnView(ctx, typed.ID, req)
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
