package server

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/retry"
)

func fetchFileResources(ctx context.Context, ids []int64, names []string) ([]*internalpb.FileResourceInfo, error) {
	coord, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return nil, err
	}
	resp, err := coord.FetchFileResources(ctx, &rootcoordpb.FetchFileResourcesRequest{ResourceIds: ids, ResourceNames: names})
	if err != nil {
		return nil, err
	}
	if err := merr.CheckRPCCall(resp, nil); err != nil {
		if !resp.GetStatus().GetRetriable() {
			return nil, retry.Unrecoverable(err)
		}
		return nil, err
	}
	return resp.GetResources(), nil
}
