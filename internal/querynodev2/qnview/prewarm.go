package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (h *QNQueryViewHandler) Prewarm(ctx context.Context, req *viewpb.PrewarmOnViewRequest) error {
	key := qviews.QueryViewKey{ShardID: qviews.FromProtoShardID(req.GetShardId()), QueryViewVersion: qviews.FromProtoQueryViewVersion(req.GetVersion())}
	lease, err := h.AcquireReadyView(ctx, key.ShardID, key.QueryViewVersion)
	if err != nil {
		return err
	}
	defer lease.Release()
	warmer, ok := h.segMgr.(interface {
		Prewarm(context.Context, qviews.QueryViewKey, *viewpb.QueryViewOfQueryNode) error
	})
	if !ok {
		return merr.Wrap(merr.ErrServiceUnimplemented, "segment manager does not support prewarm")
	}
	return warmer.Prewarm(ctx, key, lease.View)
}
