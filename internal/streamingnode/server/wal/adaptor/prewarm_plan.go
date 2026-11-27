package adaptor

import (
	"context"

	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/internal/views/viewerror"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/contextutil"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// GetPrewarmPlan borrows topology only. It does not wait for MVCC, optimize a
// query, or choose a replacement View if the advertised version has retired.
func (w *walAdaptorImpl) GetPrewarmPlan(ctx context.Context, req *viewpb.GetPrewarmPlanRequest) (*viewpb.GetPrewarmPlanResponse, error) {
	if !w.lifetime.Add(typeutil.LifetimeStateWorking) {
		return nil, viewerror.NewOnShutdownError("wal is on shutdown")
	}
	defer w.lifetime.Done()
	ctx, cancel := contextutil.MergeContext(ctx, w.availableCtx)
	defer cancel()
	if req.GetShardId() == nil || req.GetVersion() == nil || req.GetLoadInfoVersion() == 0 {
		return nil, viewerror.NewUnknownError("prewarm plan misses shard, version or load configuration")
	}
	if w.queryViewHandler == nil {
		return nil, viewerror.NewViewNotFound("query view handler is unavailable")
	}
	lease, err := w.queryViewHandler.AcquireUpView(ctx, qviews.FromProtoShardID(req.ShardId), qviews.FromProtoQueryViewVersion(req.Version))
	if err != nil {
		return nil, err
	}
	defer lease.Release()
	if lease.Meta.GetCollectionId() != req.CollectionId || lease.Meta.GetLoadInfoVersion() != req.LoadInfoVersion {
		return nil, viewerror.NewViewInvalidated("prewarm load configuration changed")
	}
	nodes, _ := buildQueryPlanWorkNodes(lease.View, queryPlanWorkNodeOptions{})
	lease.Renew()
	return &viewpb.GetPrewarmPlanResponse{Plan: &viewpb.QueryPlan{
		ShardId: qviews.NewShardIDFromQVMeta(lease.Meta).IntoProto(), Version: lease.Version.IntoProto(), WorkNodes: nodes,
	}}, nil
}
