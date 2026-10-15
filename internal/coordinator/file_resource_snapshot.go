package coordinator

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

func (s *mixCoordImpl) GetFileResourceSnapshot(ctx context.Context, req *milvuspb.ListFileResourcesRequest) (*rootcoordpb.FileResourceSnapshot, error) {
	return s.rootcoordServer.GetFileResourceSnapshot(ctx, req)
}
