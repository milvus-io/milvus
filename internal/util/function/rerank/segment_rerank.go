package rerank

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// ApplyRerankOnSearchResultData applies a query-node reranker directly on already-reduced SearchResultData.
func ApplyRerankOnSearchResultData(
	ctx context.Context,
	reducedData *schemapb.SearchResultData,
	collSchema *schemapb.CollectionSchema,
	funcSchema *schemapb.FunctionSchema,
	metricType string,
	nq int64,
	topK int64,
) (*schemapb.SearchResultData, error) {
	if funcSchema == nil {
		return reducedData, nil
	}
	if !IsQueryNodeRanker(funcSchema) {
		return reducedData, nil
	}
	if reducedData == nil || reducedData.Ids == nil {
		return reducedData, nil
	}

	reranker, err := createFunction(collSchema, funcSchema)
	if err != nil {
		return nil, fmt.Errorf("failed to create segment-level reranker: %w", err)
	}

	searchParams := &SearchParams{
		nq:              nq,
		limit:           topK,
		offset:          0,
		roundDecimal:    -1,
		groupByFieldId:  -1,
		groupSize:       0,
		strictGroupSize: false,
		groupScore:      maxScorer,
		searchMetrics:   []string{metricType},
	}

	inputs, err := newRerankInputs([]*schemapb.SearchResultData{reducedData}, reranker.GetInputFieldIDs(), false)
	if err != nil {
		return nil, fmt.Errorf("failed to create rerank inputs: %w", err)
	}

	outputs, err := reranker.Process(ctx, searchParams, inputs)
	if err != nil {
		return nil, fmt.Errorf("failed to apply segment-level reranking: %w", err)
	}

	return outputs.searchResultData, nil
}
