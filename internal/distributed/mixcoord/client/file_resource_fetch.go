package grpcmixcoordclient

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

func (c *Client) FetchFileResources(ctx context.Context, req *rootcoordpb.FetchFileResourcesRequest, opts ...grpc.CallOption) (*rootcoordpb.FetchFileResourcesResponse, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*rootcoordpb.FetchFileResourcesResponse, error) {
		return client.FetchFileResources(ctx, req, opts...)
	})
}
