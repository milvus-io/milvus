package coordinator

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

func (s *mixCoordImpl) FetchFileResources(ctx context.Context, req *rootcoordpb.FetchFileResourcesRequest) (*rootcoordpb.FetchFileResourcesResponse, error) {
	return s.rootcoordServer.FetchFileResources(ctx, req)
}
