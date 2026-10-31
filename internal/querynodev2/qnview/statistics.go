package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// Statistics samples physical rows on the exact ReadStates retained by the View.
// The MVCC is an application lower bound, not a historical count snapshot.
func (h *QNQueryViewHandler) Statistics(ctx context.Context, req *viewpb.StatisticsOnViewRequest) (int64, error) {
	shardID := qviews.FromProtoShardID(req.GetShardId())
	version := qviews.FromProtoQueryViewVersion(req.GetVersion())
	lease, err := h.AcquireReadyView(ctx, shardID, version)
	if err != nil {
		return 0, err
	}
	defer lease.Release()
	key := qviews.QueryViewKey{ShardID: shardID, QueryViewVersion: version}
	if err := h.segMgr.WaitTransformVisible(ctx, key, req.GetMvcc().GetTransformingTimetick()); err != nil {
		return 0, err
	}
	view := filterQueryNodeViewByPartitions(lease.View, req.GetPartitionIds())
	handles, err := h.segMgr.AcquireSealedSegmentHandles(ctx, key, view, req.GetMvcc().GetTransformingTimetick())
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
		rows += handle.Segment().(ReadableSealedSegment).QuerySegment().RowNum()
	}
	return rows, nil
}
