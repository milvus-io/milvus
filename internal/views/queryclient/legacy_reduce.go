package queryclient

import (
	"context"

	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/internal/querynodev2/segments"
	"github.com/milvus-io/milvus/internal/util/reduce"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// Reduce runs after all shard attempts finish. A retry has already discarded
// that shard's partial results, so each bucket represents one successful plan.
func (c *legacyQueryCollector) Reduce(ctx context.Context, req *LegacyQueryRequest, plans []ShardPlan) ([]*internalpb.RetrieveResults, error) {
	results := make([]*internalpb.RetrieveResults, 0, len(plans))
	for _, plan := range plans {
		inputs := c.results[plan.ShardID.String()]
		if len(inputs) == 0 {
			continue
		}
		var result *internalpb.RetrieveResults
		if len(inputs) == 1 {
			result = inputs[0]
		} else {
			var err error
			result, err = segments.RunDelegatorQueryPipeline(ctx, &querypb.QueryRequest{Req: req.Req}, req.Schema, inputs)
			if err != nil {
				return nil, err
			}
			costs := make([]*internalpb.CostAggregation, 0, len(inputs))
			for _, input := range inputs {
				costs = append(costs, input.GetCostAggregation())
			}
			result.CostAggregation = mergeLegacyCosts(costs)
		}
		result.MvccTimestamp = max(plan.Mvcc.GetGrowingTimetick(), plan.Mvcc.GetTransformingTimetick())
		results = append(results, result)
	}
	return results, nil
}

func (c *legacySearchCollector) Reduce(ctx context.Context, req *internalpb.SearchRequest, plans []ShardPlan) ([]*internalpb.SearchResults, error) {
	results := make([]*internalpb.SearchResults, 0, len(plans))
	for _, plan := range plans {
		inputs := c.results[plan.ShardID.String()]
		if len(inputs) == 0 {
			continue
		}
		result, err := reduceLegacyShardSearch(ctx, req, inputs)
		if err != nil {
			return nil, err
		}
		result.ChannelsMvcc = map[string]uint64{plan.ShardID.VChannel: max(plan.Mvcc.GetGrowingTimetick(), plan.Mvcc.GetTransformingTimetick())}
		results = append(results, result)
	}
	return results, nil
}

func reduceLegacyShardSearch(ctx context.Context, req *internalpb.SearchRequest, inputs []*internalpb.SearchResults) (*internalpb.SearchResults, error) {
	if len(inputs) == 1 {
		return inputs[0], nil
	}
	costs := make([]*internalpb.CostAggregation, 0, len(inputs))
	var scannedRemote, scannedTotal int64
	var topkReduce, recallEvaluation bool
	for _, input := range inputs {
		costs = append(costs, input.GetCostAggregation())
		scannedRemote += input.GetScannedRemoteBytes()
		scannedTotal += input.GetScannedTotalBytes()
		topkReduce = topkReduce || input.GetIsTopkReduce()
		recallEvaluation = recallEvaluation || input.GetIsRecallEvaluation()
	}
	cost := mergeLegacyCosts(costs)
	info := reduce.NewReduceSearchResultInfo(req.GetNq(), req.GetTopk()).WithMetricType(req.GetMetricType()).WithGroupSize(req.GetGroupSize()).WithGroupByFieldIdsFromProto(req.GetGroupByFieldId(), req.GetGroupByFieldIds())
	var result *internalpb.SearchResults
	var err error
	// Advanced node responses already contain SubResults. Merge by ReqIndex
	// before assembling the shard response; do not wrap these envelopes again.
	if req.GetIsAdvanced() {
		reduced := make([]*internalpb.SearchResults, len(req.GetSubReqs()))
		buckets := make([][]*internalpb.SearchResults, len(reduced))
		for _, input := range inputs {
			for _, sub := range input.GetSubResults() {
				i := sub.GetReqIndex()
				if i < 0 || i >= int64(len(buckets)) {
					return nil, merr.WrapErrServiceInternalMsg("invalid sub-search result index %d", i)
				}
				buckets[i] = append(buckets[i], &internalpb.SearchResults{MetricType: sub.GetMetricType(), NumQueries: sub.GetNumQueries(), TopK: sub.GetTopK(), SlicedBlob: sub.GetSlicedBlob(), ResultData: sub.GetResultData(), SlicedNumCount: sub.GetSlicedNumCount(), SlicedOffset: sub.GetSlicedOffset()})
			}
		}
		for i, sub := range req.GetSubReqs() {
			subInfo := reduce.NewReduceSearchResultInfo(sub.GetNq(), sub.GetTopk()).WithMetricType(sub.GetMetricType()).WithGroupSize(sub.GetGroupSize()).WithGroupByFieldIdsFromProto(sub.GetGroupByFieldId(), req.GetGroupByFieldIds())
			reduced[i], err = segments.ReduceSearchResults(ctx, buckets[i], subInfo)
			if err != nil {
				return nil, err
			}
		}
		result, err = segments.ReduceAdvancedSearchResults(ctx, reduced)
	} else {
		result, err = segments.ReduceSearchOnQueryNode(ctx, inputs, info)
	}
	if err != nil {
		return nil, err
	}
	// Preserve accounting even for empty worker results discarded by reducers.
	result.ScannedRemoteBytes, result.ScannedTotalBytes = scannedRemote, scannedTotal
	result.IsTopkReduce, result.IsRecallEvaluation = topkReduce, recallEvaluation
	result.CostAggregation = cost
	result.Status = merr.Success()
	return result, nil
}

func mergeLegacyCosts(costs []*internalpb.CostAggregation) *internalpb.CostAggregation {
	result := &internalpb.CostAggregation{}
	var related int64
	selected := false
	for _, cost := range costs {
		related += cost.GetTotalRelatedDataSize()
		if cost != nil && (!selected || cost.GetResponseTime() > result.GetResponseTime()) {
			selected = true
			result = proto.Clone(cost).(*internalpb.CostAggregation)
		}
	}
	result.TotalRelatedDataSize = related
	return result
}
