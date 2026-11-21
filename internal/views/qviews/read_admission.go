package qviews

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
)

// CheckReadAdmission checks the executor coordinates on the already selected View.
func CheckReadAdmission(ctx context.Context, meta *viewpb.QueryViewMeta, mvcc *viewpb.QueryPlanMVCC) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if mvcc == nil {
		return viewerror.NewUnknownError("missing query plan mvcc")
	}
	floor := meta.GetReadAdmissionFloor()
	if floor == nil {
		return viewerror.NewViewInvalidated("query view has no certified read history")
	}
	if mvcc.GetGrowingTimetick() < floor.GetGrowingTimetick() || mvcc.GetTransformingTimetick() < floor.GetTransformingTimetick() {
		return viewerror.NewViewInvalidated("requested read (%d,%d) precedes query view minimum (%d,%d)",
			mvcc.GetGrowingTimetick(), mvcc.GetTransformingTimetick(), floor.GetGrowingTimetick(), floor.GetTransformingTimetick())
	}
	return nil
}

// CheckBarrierAdmission preserves the logical iterator timestamp before its
// projection onto executor coordinates by the primary WAL.
func CheckBarrierAdmission(ctx context.Context, meta *viewpb.QueryViewMeta, barrier uint64) error {
	return CheckReadAdmission(ctx, meta, &viewpb.QueryPlanMVCC{GrowingTimetick: barrier, TransformingTimetick: barrier})
}
