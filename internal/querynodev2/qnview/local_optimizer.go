package qnview

import (
	"context"

	"github.com/milvus-io/milvus/internal/querynodev2/pkoracle"
	"github.com/milvus-io/milvus/internal/views/optimizer"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type localOptimizer struct {
	segMgr SegmentManager
}

func (o localOptimizer) OptimizeSearch(ctx context.Context, req *internalpb.SearchRequest, scope optimizer.LocalSegmentScope) (optimizer.LocalOptimization, error) {
	return optimizer.LocalOptimization{SegmentSelection: o.pruneByPK(ctx, scope, req.GetSerializedExprPlan(), req.GetPkFilter())}, nil
}

func (o localOptimizer) OptimizeRetrieve(ctx context.Context, req *internalpb.RetrieveRequest, scope optimizer.LocalSegmentScope) (optimizer.LocalOptimization, error) {
	return optimizer.LocalOptimization{SegmentSelection: o.pruneByPK(ctx, scope, req.GetSerializedExprPlan(), req.GetPkFilter())}, nil
}

func (o localOptimizer) pruneByPK(ctx context.Context, scope optimizer.LocalSegmentScope, plan []byte, hint int32) *viewpb.SegmentSelection {
	if !paramtable.Get().QueryNodeCfg.EnableSegmentFilter.GetAsBool() {
		return nil
	}
	expr := pkoracle.ParsePKFilterExpr(ctx, plan, hint)
	if expr == nil {
		return nil
	}
	candidates := o.segMgr.SealedPKCandidates(scope.Key, scope.Partitions)
	matches, all := pkoracle.CheckPKFilter(expr, candidates)
	if all || len(matches) == len(candidates) {
		return nil
	}
	var excluded []int64
	for _, candidate := range candidates {
		if !matches.Contain(candidate.ID()) {
			excluded = append(excluded, candidate.ID())
		}
	}
	return &viewpb.SegmentSelection{Mode: viewpb.SegmentSelection_Exclude, SegmentIds: excluded}
}
