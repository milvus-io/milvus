package grpcmixcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

func (s *Server) GetFileResourceSnapshot(ctx context.Context, req *milvuspb.ListFileResourcesRequest) (*rootcoordpb.FileResourceSnapshot, error) {
	return s.mixCoord.GetFileResourceSnapshot(ctx, req)
}
