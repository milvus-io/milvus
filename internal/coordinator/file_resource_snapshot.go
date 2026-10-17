package coordinator

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
)

func (s *mixCoordImpl) GetFileResourceSnapshot(ctx context.Context, req *rootcoordpb.GetFileResourceSnapshotRequest) (*rootcoordpb.FileResourceSnapshot, error) {
	return s.rootcoordServer.GetFileResourceSnapshot(ctx, req)
}
