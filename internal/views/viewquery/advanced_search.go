package viewquery

import (
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// BuildSubSearchRequest expands one advanced-search sub-request into a regular
// SearchRequest while preserving the execution context carried by its parent.
func BuildSubSearchRequest(parent *internalpb.SearchRequest, sub *internalpb.SubSearchRequest) (*internalpb.SearchRequest, error) {
	if parent == nil {
		return nil, merr.WrapErrServiceInternalMsg("advanced search request is nil")
	}
	if sub == nil {
		return nil, merr.WrapErrServiceInternalMsg("advanced search contains a nil sub-request")
	}

	req := proto.Clone(parent).(*internalpb.SearchRequest)
	req.PartitionIDs = append([]int64(nil), sub.GetPartitionIDs()...)
	req.Dsl = sub.GetDsl()
	req.PlaceholderGroup = append([]byte(nil), sub.GetPlaceholderGroup()...)
	req.DslType = sub.GetDslType()
	req.SerializedExprPlan = append([]byte(nil), sub.GetSerializedExprPlan()...)
	req.Nq = sub.GetNq()
	req.Topk = sub.GetTopk()
	req.Offset = sub.GetOffset()
	req.MetricType = sub.GetMetricType()
	req.IgnoreGrowing = sub.GetIgnoreGrowing()
	req.GroupByFieldId = sub.GetGroupByFieldId()
	req.GroupSize = sub.GetGroupSize()
	req.FieldId = sub.GetFieldId()
	req.AnalyzerName = sub.GetAnalyzerName()
	req.SearchType = sub.GetSearchType()
	req.PkFilter = common.PkFilterNoPkFilter
	req.SubReqs = nil
	req.IsAdvanced = false
	return req, nil
}

// UpdateSubSearchRequest writes optimizer-owned fields back to the advanced
// request so Phase 2 observes the exact regular request optimized in Phase 1.
func UpdateSubSearchRequest(sub *internalpb.SubSearchRequest, optimized *internalpb.SearchRequest, skip bool) error {
	if sub == nil || optimized == nil {
		return merr.WrapErrServiceInternalMsg("cannot update advanced sub-search from a nil request")
	}

	sub.Dsl = optimized.GetDsl()
	sub.PlaceholderGroup = append([]byte(nil), optimized.GetPlaceholderGroup()...)
	sub.DslType = optimized.GetDslType()
	sub.SerializedExprPlan = append([]byte(nil), optimized.GetSerializedExprPlan()...)
	sub.Nq = optimized.GetNq()
	sub.PartitionIDs = append([]int64(nil), optimized.GetPartitionIDs()...)
	sub.Topk = optimized.GetTopk()
	sub.Offset = optimized.GetOffset()
	sub.MetricType = optimized.GetMetricType()
	sub.GroupByFieldId = optimized.GetGroupByFieldId()
	sub.GroupSize = optimized.GetGroupSize()
	sub.FieldId = optimized.GetFieldId()
	sub.IgnoreGrowing = optimized.GetIgnoreGrowing()
	sub.AnalyzerName = optimized.GetAnalyzerName()
	sub.SearchType = optimized.GetSearchType()
	sub.Skip = skip
	return nil
}

// assembleAdvancedSearchResults preserves sub-search order and converts regular
// node-local results into the protocol consumed by Proxy's hybrid reducer.
func assembleAdvancedSearchResults(results []*internalpb.SearchResults) *internalpb.SearchResults {
	channelsMVCC := make(map[string]uint64)
	searchResults := &internalpb.SearchResults{
		Status:     merr.Success(),
		IsAdvanced: true,
	}
	var selectedCost *internalpb.CostAggregation
	var relatedDataSize int64

	for index, result := range results {
		if result.GetIsTopkReduce() {
			searchResults.IsTopkReduce = true
		}
		if result.GetIsRecallEvaluation() {
			searchResults.IsRecallEvaluation = true
		}
		relatedDataSize += result.GetCostAggregation().GetTotalRelatedDataSize()
		searchResults.ScannedRemoteBytes += result.GetScannedRemoteBytes()
		searchResults.ScannedTotalBytes += result.GetScannedTotalBytes()
		for channel, ts := range result.GetChannelsMvcc() {
			channelsMVCC[channel] = ts
		}
		if cost := result.GetCostAggregation(); cost != nil && (selectedCost == nil || selectedCost.GetResponseTime() < cost.GetResponseTime()) {
			selectedCost = cost
		}

		searchResults.NumQueries = result.GetNumQueries()
		searchResults.SubResults = append(searchResults.SubResults, &internalpb.SubSearchResults{
			MetricType:     result.GetMetricType(),
			NumQueries:     result.GetNumQueries(),
			TopK:           result.GetTopK(),
			SlicedBlob:     result.GetSlicedBlob(),
			ResultData:     result.GetResultData(),
			SlicedNumCount: result.GetSlicedNumCount(),
			SlicedOffset:   result.GetSlicedOffset(),
			ReqIndex:       int64(index),
		})
	}

	searchResults.ChannelsMvcc = channelsMVCC
	searchResults.CostAggregation = &internalpb.CostAggregation{}
	if selectedCost != nil {
		searchResults.CostAggregation = proto.Clone(selectedCost).(*internalpb.CostAggregation)
	}
	searchResults.CostAggregation.TotalRelatedDataSize = relatedDataSize
	return searchResults
}
