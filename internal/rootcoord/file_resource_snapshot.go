package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (c *Core) GetFileResourceSnapshot(ctx context.Context, req *rootcoordpb.GetFileResourceSnapshotRequest) (*rootcoordpb.FileResourceSnapshot, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &rootcoordpb.FileResourceSnapshot{Status: merr.Status(err)}, nil
	}
	resources, version := c.meta.ListFileResourceIfChanged(ctx, req.GetKnownVersion())
	return &rootcoordpb.FileResourceSnapshot{Status: merr.Success(), Version: version, Resources: resources}, nil
}
