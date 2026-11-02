package viewquery

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func emptySearchResults(req *internalpb.SearchRequest) *internalpb.SearchResults {
	nq := req.GetNq()
	topK := req.GetTopk()
	return &internalpb.SearchResults{
		Status:             merr.Success(),
		MetricType:         req.GetMetricType(),
		NumQueries:         nq,
		TopK:               topK,
		IsTopkReduce:       req.GetIsTopkReduce(),
		IsRecallEvaluation: req.GetIsRecallEvaluation(),
		ResultData: &schemapb.SearchResultData{
			NumQueries: nq,
			TopK:       topK,
			Ids:        &schemapb.IDs{},
			Scores:     []float32{},
			Topks:      make([]int64, int(nq)),
			FieldsData: []*schemapb.FieldData{},
		},
		CostAggregation: &internalpb.CostAggregation{},
	}
}

func emptyQueryResults() *internalpb.RetrieveResults {
	return &internalpb.RetrieveResults{
		Status:          merr.Success(),
		CostAggregation: &internalpb.CostAggregation{},
	}
}

// EmptySearchResults preserves the regular/hybrid response shape for plans
// with no worker results, whether or not pruning was applied.
func EmptySearchResults(req *internalpb.SearchRequest) (*internalpb.SearchResults, error) {
	if !req.GetIsAdvanced() {
		return emptySearchResults(req), nil
	}
	requests, err := BuildSubSearchRequests(req)
	if err != nil {
		return nil, err
	}
	results := make([]*internalpb.SearchResults, len(requests))
	for i, request := range requests {
		results[i] = emptySearchResults(request)
	}
	return assembleAdvancedSearchResults(results), nil
}
