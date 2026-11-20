package snview

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func (h *SNQueryViewHandler) Prewarm(ctx context.Context, req *viewpb.PrewarmOnViewRequest) error {
	key := qviews.QueryViewKey{ShardID: qviews.FromProtoShardID(req.GetShardId()), QueryViewVersion: qviews.FromProtoQueryViewVersion(req.GetVersion())}
	lease, err := h.acquireUpView(ctx, key.ShardID, key.QueryViewVersion, false)
	if err != nil {
		return err
	}
	defer lease.Release()
	warmer, ok := h.resMgr.(interface {
		Prewarm(context.Context, qviews.QueryViewKey) error
	})
	if !ok {
		return merr.Wrap(merr.ErrServiceUnimplemented, "resource manager does not support prewarm")
	}
	return warmer.Prewarm(ctx, key)
}
