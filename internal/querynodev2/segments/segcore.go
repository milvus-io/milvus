package segments

import "github.com/milvus-io/milvus/internal/util/segcore"

type (
	SearchRequest = segcore.SearchRequest
	SearchResult  = segcore.SearchResult
	SearchPlan    = segcore.SearchPlan
	RetrievePlan  = segcore.RetrievePlan
)

func DeleteSearchResults(results []*SearchResult) {
	if len(results) == 0 {
		return
	}
	for _, result := range results {
		if result != nil {
			result.Release()
		}
	}
}
