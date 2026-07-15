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
		Status:     merr.Success(),
		NumQueries: nq,
		TopK:       topK,
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
