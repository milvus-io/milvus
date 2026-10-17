package grpcmixcoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

func (s *Server) GetFileResourceSnapshot(ctx context.Context, req *rootcoordpb.GetFileResourceSnapshotRequest) (*rootcoordpb.FileResourceSnapshot, error) {
	return s.mixCoord.GetFileResourceSnapshot(ctx, req)
}
