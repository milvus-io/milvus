package rootcoord

import (
	"context"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (c *Core) FetchFileResources(ctx context.Context, req *rootcoordpb.FetchFileResourcesRequest) (*rootcoordpb.FetchFileResourcesResponse, error) {
	if err := merr.CheckHealthy(c.GetStateCode()); err != nil {
		return &rootcoordpb.FetchFileResourcesResponse{Status: merr.Status(err)}, nil
	}
	var resources []*internalpb.FileResourceInfo
	var err error
	switch {
	case len(req.GetResourceIds()) > 0 && len(req.GetResourceNames()) > 0:
		err = merr.WrapErrServiceInternalMsg("file resource fetch cannot combine IDs and names")
	case len(req.GetResourceIds()) > 0:
		resources, err = c.meta.GetFileResources(ctx, req.GetResourceIds()...)
	default:
		resources, err = c.meta.GetFileResourcesByNames(ctx, req.GetResourceNames()...)
	}
	if err != nil {
		return &rootcoordpb.FetchFileResourcesResponse{Status: merr.Status(err)}, nil
	}
	return &rootcoordpb.FetchFileResourcesResponse{Status: merr.Success(), Resources: resources}, nil
}
