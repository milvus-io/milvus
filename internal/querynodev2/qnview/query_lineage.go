package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// lineageVisibleHandle is implemented by handles that can wait for the lineage
// streams of their view on the exact physical segment they pin.
type lineageVisibleHandle interface {
	WaitLineageTransformVisible(ctx context.Context, ticks map[string]uint64) error
}

// acquireVisibleSealedHandles pins the selected segments once they cover the
// plan MVCC. For a view with lineage the plan must name the group's read point
// and one transform tick per lineage stream, and every pinned segment has then
// applied each stream through its tick. A view without lineage waits for its
// own stream only, and refuses a plan that carries a lineage read point.
func (h *QNQueryViewHandler) acquireVisibleSealedHandles(
	ctx context.Context,
	key qviews.QueryViewKey,
	meta *viewpb.QueryViewMeta,
	view *viewpb.QueryViewOfQueryNode,
	mvcc *viewpb.QueryPlanMVCC,
) ([]SealedSegmentHandle, error) {
	if reason := qviews.LineageMVCCMismatch(meta.GetLineage(), mvcc); reason != "" {
		// The client planned against another serving generation; let it replan.
		return nil, viewerror.NewViewInvalidated("query view %s cannot serve the plan: %s", key.String(), reason)
	}
	if err := h.segMgr.WaitTransformVisible(ctx, key, mvcc.GetTransformingTimetick()); err != nil {
		return nil, err
	}
	handles, err := h.segMgr.AcquireSealedSegmentHandles(ctx, key, view, mvcc.GetTransformingTimetick())
	if err != nil {
		return nil, err
	}
	if len(meta.GetLineage().GetVchannels()) == 0 {
		return handles, nil
	}
	for _, handle := range handles {
		waiter, ok := handle.(lineageVisibleHandle)
		if !ok {
			err = merr.WrapErrServiceInternalMsg("query view %s segment %d cannot wait for its lineage streams", key.String(), handle.ID())
		} else {
			err = waiter.WaitLineageTransformVisible(ctx, mvcc.GetLineageTransformingTimeticks())
		}
		if err != nil {
			for _, pinned := range handles {
				pinned.Release()
			}
			return nil, err
		}
	}
	return handles, nil
}
