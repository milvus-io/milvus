package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (c *Core) GetFileResourceSnapshot(ctx context.Context, req *milvuspb.ListFileResourcesRequest) (*rootcoordpb.FileResourceSnapshot, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &rootcoordpb.FileResourceSnapshot{Status: merr.Status(err)}, nil
	}
	resources, version := c.meta.ListFileResource(ctx)
	return &rootcoordpb.FileResourceSnapshot{Status: merr.Success(), Version: version, Resources: resources}, nil
}
