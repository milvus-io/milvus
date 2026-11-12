package queryclient

import (
	"context"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/featureusage"
	"github.com/milvus-io/milvus/internal/util/searchutil/optimizers"
	"github.com/milvus-io/milvus/internal/views/qviews"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/proto/viewpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A new value is constructed for every shard attempt. Only successful ANN
// responses enter reduction; filter counts never enter the search reducer.
type twoStageSearch struct {
	options   *viewpb.SearchExecutionOptions
	filters   map[int64]*internalpb.SearchResults
	completed bool
}

func (s *shardViewQueryClient) prepareTwoStageSearch(ctx context.Context, response *viewpb.GetQueryPlanResponse, shardID qviews.ShardID, channelNum int32, stage *twoStageSearch) error {
	plan := response.GetPlan()
	info := response.GetClientOptimizeInfo().GetSearchOptimization()
	req := plan.GetLegacySearchRequest()
	if req.GetIsAdvanced() || info.GetSealedSegmentCount() == 0 ||
		!optimizers.ShouldUseTwoStageSearch(&querypb.SearchRequest{Req: req}, int(info.GetSealedSegmentCount())) {
		return nil
	}

	nodes := make([]qviews.WorkNode, 0, len(plan.GetWorkNodes()))
	for _, node := range workNodesFromPlan(plan) {
		if node.NodeType() == qviews.NodeTypeQueryNode {
			nodes = append(nodes, node)
		}
	}
	stage.filters = make(map[int64]*internalpb.SearchResults, len(nodes))
	var mu sync.Mutex
	err := s.fanOutToWorkNodes(ctx, nodes, plan, shardID, func(ctx context.Context, node qviews.WorkNode, plan *viewpb.QueryPlan, shardID qviews.ShardID) error {
		response, err := s.queryServiceClient.SearchOnView(ctx, node, &viewpb.SearchOnViewRequest{
			LegacyReq: legacySearchRequestForNode(plan, node),
			ShardId:   shardID.IntoProto(), Version: plan.Version, Mvcc: plan.GetMvcc(),
			SegmentSelection: qviews.WorkNodeSelection(plan, node).GetSegmentSelection(),
			ExecutionOptions: &viewpb.SearchExecutionOptions{FilterOnly: true, EnableExprCache: true},
		})
		if err != nil {
			return err
		}
		result := response.GetLegacyResults()
		if result == nil {
			return merr.WrapErrServiceInternalMsg("filter-only search returned no result")
		}
		if err := merr.Error(result.GetStatus()); err != nil {
			return err
		}
		mu.Lock()
		stage.filters[node.(qviews.QueryNode).ID] = result
		mu.Unlock()
		return nil
	})
	if err != nil {
		return err
	}

	counts := make([]int64, 0, info.GetSealedSegmentCount())
	seen := make(map[int64]struct{}, info.GetSealedSegmentCount())
	for _, result := range stage.filters {
		if len(result.GetFilterValidCounts()) != len(result.GetSealedSegmentIDsSearched()) {
			// Old executors may not supply statistics. Search normally rather
			// than tuning against partial cardinalities.
			return nil
		}
		for i, count := range result.GetFilterValidCounts() {
			if count < 0 {
				return merr.WrapErrServiceInternalMsg("filter-only search returned negative count %d", count)
			}
			id := result.GetSealedSegmentIDsSearched()[i]
			if _, duplicate := seen[id]; duplicate {
				return merr.WrapErrServiceInternalMsg("filter-only search returned duplicate segment %d", id)
			}
			seen[id] = struct{}{}
			counts = append(counts, count)
		}
	}
	if len(counts) != int(info.GetSealedSegmentCount()) {
		return nil
	}

	effective := optimizers.CalculateEffectiveSegmentNum(s.queryHook, counts, req.GetTopk())
	optimized, err := optimizers.OptimizeSearchParams(ctx, &querypb.SearchRequest{
		Req: proto.Clone(req).(*internalpb.SearchRequest), TotalChannelNum: max(1, channelNum),
	}, s.queryHook, effective, true, func(int64) int64 { return info.GetVectorDim() }, info.GetIndexType())
	if err != nil {
		return err
	}
	plan.Request = &viewpb.QueryPlan_LegacySearchRequest{LegacySearchRequest: optimized.Req}
	stage.options = &viewpb.SearchExecutionOptions{EnableExprCache: true}
	stage.completed = true
	return nil
}

func (s *twoStageSearch) mergeFeatures(node qviews.WorkNode, response *viewpb.SearchOnViewResponse) {
	result := response.GetLegacyResults()
	if result == nil {
		return
	}
	if qn, ok := node.(qviews.QueryNode); ok {
		if filter := s.filters[qn.ID]; filter != nil {
			result.FeatureBits |= filter.GetFeatureBits()
			result.ScannedRemoteBytes += filter.GetScannedRemoteBytes()
			result.ScannedTotalBytes += filter.GetScannedTotalBytes()
			if cost := filter.GetCostAggregation(); cost != nil {
				if result.CostAggregation == nil {
					result.CostAggregation = &internalpb.CostAggregation{}
				}
				result.CostAggregation.ServiceTime += cost.GetServiceTime()
			}
		}
	}
	if s.completed {
		bit, _ := featureusage.ExecBit(featureusage.FeatureTwoStageSearch)
		result.FeatureBits |= bit
	}
}
