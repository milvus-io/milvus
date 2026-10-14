package grpcmixcoordclient

import (
	"context"

	"google.golang.org/grpc"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

func (c *Client) GetFileResourceSnapshot(ctx context.Context, req *milvuspb.ListFileResourcesRequest, opts ...grpc.CallOption) (*rootcoordpb.FileResourceSnapshot, error) {
	return wrapGrpcCall(ctx, c, func(client MixCoordClient) (*rootcoordpb.FileResourceSnapshot, error) {
		return client.GetFileResourceSnapshot(ctx, req, opts...)
	})
}
