package rerank

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/log"
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
	if reducedData == nil || reducedData.Ids == nil {
		return reducedData, nil
	}

	// Validate function type first: non-rerank types should error
	if funcSchema.GetType() != schemapb.FunctionType_Rerank {
		return nil, fmt.Errorf("%s is not rerank function.", funcSchema.GetType().String())
	}

	// Non-QueryNode rerankers are a no-op at segment-level
	if !IsQueryNodeRanker(funcSchema) {
		return reducedData, nil
	}

	rerankerName := GetRerankName(funcSchema)
	log.Ctx(ctx).Info("EXPR_RERANK: QueryNode creating segment-level reranker",
		zap.String("reranker_name", rerankerName),
		zap.String("function_name", funcSchema.GetName()),
		zap.Int32("function_type", int32(funcSchema.GetType())),
		zap.Strings("input_fields", funcSchema.GetInputFieldNames()),
	)
	reranker, err := createFunction(collSchema, funcSchema)
	if err != nil {
		log.Ctx(ctx).Error("EXPR_RERANK: Failed to create segment-level reranker", zap.Error(err))
		return nil, fmt.Errorf("failed to create segment-level reranker: %w", err)
	}
	log.Ctx(ctx).Info("EXPR_RERANK: Segment-level reranker created successfully",
		zap.Int("num_input_field_ids", len(reranker.GetInputFieldIDs())),
	)

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

	log.Ctx(ctx).Info("EXPR_RERANK: Creating rerank inputs for segment-level processing",
		zap.Int64("nq", nq),
		zap.Int64("topk", topK),
		zap.String("metric_type", metricType),
	)
	inputs, err := newRerankInputs([]*schemapb.SearchResultData{reducedData}, reranker.GetInputFieldIDs(), false)
	if err != nil {
		log.Ctx(ctx).Error("EXPR_RERANK: Failed to create rerank inputs", zap.Error(err))
		return nil, fmt.Errorf("failed to create rerank inputs: %w", err)
	}

	log.Ctx(ctx).Info("EXPR_RERANK: Processing segment-level reranking")
	outputs, err := reranker.Process(ctx, searchParams, inputs)
	if err != nil {
		log.Ctx(ctx).Error("EXPR_RERANK: Failed to apply segment-level reranking", zap.Error(err))
		return nil, fmt.Errorf("failed to apply segment-level reranking: %w", err)
	}

	log.Ctx(ctx).Info("EXPR_RERANK: Segment-level reranking process completed successfully")
	return outputs.searchResultData, nil
}
