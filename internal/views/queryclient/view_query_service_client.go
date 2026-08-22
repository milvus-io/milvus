package queryclient

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/streaming/util/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// StreamingNodeViewQueryServiceClient is the injected StreamingNode subclient.
type StreamingNodeViewQueryServiceClient interface {
	SearchOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	QueryOnView(ctx context.Context, pchannel types.PChannelInfo, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
}

// QueryNodeViewQueryServiceClient is the injected QueryNode subclient.
type QueryNodeViewQueryServiceClient interface {
	SearchOnView(ctx context.Context, nodeID int64, req *viewpb.SearchOnViewRequest) (*viewpb.SearchOnViewResponse, error)
	QueryOnView(ctx context.Context, nodeID int64, req *viewpb.QueryOnViewRequest) (*viewpb.QueryOnViewResponse, error)
}

type compositeViewQueryServiceClient struct {
	streamingNode StreamingNodeViewQueryServiceClient
	queryNode     QueryNodeViewQueryServiceClient
}

// NewCompositeViewQueryServiceClient dispatches planned requests to injected
// node-domain clients without owning discovery or connections.
func NewCompositeViewQueryServiceClient(
	streamingNode StreamingNodeViewQueryServiceClient,
	queryNode QueryNodeViewQueryServiceClient,
) ViewQueryServiceClient {
	return &compositeViewQueryServiceClient{streamingNode: streamingNode, queryNode: queryNode}
}

func (c *compositeViewQueryServiceClient) SearchOnView(
	ctx context.Context,
	node qviews.WorkNode,
	request *viewpb.SearchOnViewRequest,
) (*viewpb.SearchOnViewResponse, error) {
	switch typed := node.(type) {
	case qviews.StreamingNode:
		if c.streamingNode == nil {
			return nil, merr.WrapErrServiceInternalMsg("streaming node query view client is not configured")
		}
		return c.streamingNode.SearchOnView(ctx, types.PChannelInfo{Name: typed.PChannel}, request)
	case qviews.QueryNode:
		if c.queryNode == nil {
			return nil, merr.WrapErrServiceInternalMsg("query node query view client is not configured")
		}
		return c.queryNode.SearchOnView(ctx, typed.ID, request)
	default:
		return nil, invalidWorkNodeError(node)
	}
}

func (c *compositeViewQueryServiceClient) QueryOnView(
	ctx context.Context,
	node qviews.WorkNode,
	request *viewpb.QueryOnViewRequest,
) (*viewpb.QueryOnViewResponse, error) {
	switch typed := node.(type) {
	case qviews.StreamingNode:
		if c.streamingNode == nil {
			return nil, merr.WrapErrServiceInternalMsg("streaming node query view client is not configured")
		}
		return c.streamingNode.QueryOnView(ctx, types.PChannelInfo{Name: typed.PChannel}, request)
	case qviews.QueryNode:
		if c.queryNode == nil {
			return nil, merr.WrapErrServiceInternalMsg("query node query view client is not configured")
		}
		return c.queryNode.QueryOnView(ctx, typed.ID, request)
	default:
		return nil, invalidWorkNodeError(node)
	}
}

func invalidWorkNodeError(node qviews.WorkNode) error {
	if node == nil {
		return merr.WrapErrServiceInternalMsg("query view work node is nil")
	}
	return merr.WrapErrServiceInternalMsg("unknown query view work node %s", node)
}
