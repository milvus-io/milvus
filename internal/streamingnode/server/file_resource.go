package server

import (
	"context"

	"github.com/milvus-io/milvus/internal/streamingnode/server/resource"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/rootcoordpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func fetchFileResourceSnapshot(ctx context.Context, knownVersion uint64) ([]*internalpb.FileResourceInfo, uint64, error) {
	coord, err := resource.Resource().MixCoordClient().GetWithContext(ctx)
	if err != nil {
		return nil, 0, err
	}
	resp, err := coord.GetFileResourceSnapshot(ctx, &rootcoordpb.GetFileResourceSnapshotRequest{KnownVersion: knownVersion})
	if err := merr.CheckRPCCall(resp, err); err != nil {
		return nil, 0, err
	}
	return resp.GetResources(), resp.GetVersion(), nil
}
