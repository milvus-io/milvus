package grpcmixcoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

func (s *Server) FetchFileResources(ctx context.Context, req *rootcoordpb.FetchFileResourcesRequest) (*rootcoordpb.FetchFileResourcesResponse, error) {
	return s.mixCoord.FetchFileResources(ctx, req)
}
