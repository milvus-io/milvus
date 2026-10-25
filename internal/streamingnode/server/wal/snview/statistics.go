package snview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Statistics counts only growing segments not handed off at the chosen DataVersion.
// Its caller keeps the topology lease until the count and QN routing are returned.
func (h *SNQueryViewHandler) Statistics(ctx context.Context, req *viewpb.StatisticsOnViewRequest) (int64, error) {
	shardID := qviews.FromProtoShardID(req.GetShardId())
	version := qviews.FromProtoQueryViewVersion(req.GetVersion())
	lease, err := h.acquireUpView(ctx, shardID, version, false)
	if err != nil {
		return 0, err
	}
	defer lease.Release()
	runtime, err := h.queryRuntime(qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version})
	if err != nil {
		return 0, err
	}
	if err := runtime.WaitMVCCVisible(ctx, req.GetMvcc().GetGrowingTimetick(), req.GetMvcc().GetTransformingTimetick()); err != nil {
		return 0, err
	}
	partitions, err := runtime.ResolvePartitions(lease.Meta.GetLoadInfoVersion(), selectedPartitionIDs(req.GetPartitionIds()))
	if err != nil {
		return 0, err
	}
	handles, err := runtime.AcquireGrowingSegmentHandles(ctx, version.DataVersion, partitions)
	if err != nil {
		return 0, err
	}
	defer func() {
		for _, handle := range handles {
			handle.Release()
		}
	}()
	var rows int64
	for _, handle := range handles {
		if err := ctx.Err(); err != nil {
			return 0, err
		}
		rows += handle.Segment().RowNum()
	}
	return rows, nil
}
