package proxy

import (
	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// rerankMeta provides access to common rerank metadata.
// A nil rerankMeta means no reranking is configured.
type rerankMeta interface {
	GetInputFieldNames() []string
	GetInputFieldIDs() []int64
	GetInputPlan() *chain.DataFrameInputPlan
}

// funcScoreRerankMeta holds rerank configuration from a FunctionScore proto.
type funcScoreRerankMeta struct {
	inputFieldNames []string
	inputFieldIDs   []int64
	inputPlan       *chain.DataFrameInputPlan
	funcScore       *schemapb.FunctionScore
}

func (m *funcScoreRerankMeta) GetInputFieldNames() []string { return m.inputFieldNames }
func (m *funcScoreRerankMeta) GetInputFieldIDs() []int64    { return m.inputFieldIDs }
func (m *funcScoreRerankMeta) GetInputPlan() *chain.DataFrameInputPlan {
	return m.inputPlan
}

// legacyRerankMeta holds rerank configuration from legacy rank parameters.
type legacyRerankMeta struct {
	legacyParams []*commonpb.KeyValuePair
}

func (m *legacyRerankMeta) GetInputFieldNames() []string { return nil }
func (m *legacyRerankMeta) GetInputFieldIDs() []int64    { return nil }
func (m *legacyRerankMeta) GetInputPlan() *chain.DataFrameInputPlan {
	return nil
}

// newRerankMeta creates a rerankMeta from a FunctionScore proto.
// Returns nil if funcScore is nil, has no functions, or all functions are boost
// (boost is pushed down to QueryNode and doesn't need proxy-level reranking).
func newRerankMeta(collSchema *schemapb.CollectionSchema, funcScore *schemapb.FunctionScore) (rerankMeta, error) {
	if funcScore == nil || len(funcScore.Functions) == 0 {
		return nil, nil
	}
	// Boost ranker is executed at segment level in QueryNode, proxy doesn't handle it.
	// If all functions are boost, no proxy rerank is needed.
	allBoost := true
	for _, f := range funcScore.Functions {
		if rerank.GetRerankName(f) != rerank.BoostName {
			allBoost = false
			break
		}
	}
	if allBoost {
		return nil, nil
	}
	inputFieldNames := chain.GetInputFieldNamesFromFuncScore(funcScore)
	inputPlan, err := newDataFrameInputPlanFromFieldNames(collSchema, inputFieldNames)
	if err != nil {
		return nil, err
	}
	return &funcScoreRerankMeta{
		funcScore:       funcScore,
		inputFieldNames: inputFieldNames,
		inputFieldIDs:   inputPlan.PhysicalFieldIDs(),
		inputPlan:       inputPlan,
	}, nil
}

// newDataFrameInputPlanFromFieldNames adapts schema field names used by
// FunctionScore to the plan-based SearchResultData converter.
func newDataFrameInputPlanFromFieldNames(
	schema *schemapb.CollectionSchema,
	fieldNames []string,
) (*chain.DataFrameInputPlan, error) {
	plan := &chain.DataFrameInputPlan{Inputs: make([]chain.ResolvedChainInput, 0, len(fieldNames))}
	fieldsByName := make(map[string]*schemapb.FieldSchema, len(schema.GetFields()))
	for _, field := range schema.GetFields() {
		fieldsByName[field.GetName()] = field
	}
	for _, fieldName := range fieldNames {
		field := fieldsByName[fieldName]
		if field == nil {
			return nil, merr.WrapErrParameterInvalidMsg(
				"function score input field %q not found in collection schema", fieldName)
		}
		plan.Inputs = append(plan.Inputs, chain.ResolvedChainInput{
			LogicalName:   fieldName,
			SourceFieldID: field.GetFieldID(),
			FieldName:     field.GetName(),
			DataType:      field.GetDataType(),
			Nullable:      field.GetNullable(),
		})
	}
	return plan, nil
}

// newRerankMetaFromLegacy creates a rerankMeta from legacy search rank parameters.
func newRerankMetaFromLegacy(params []*commonpb.KeyValuePair) rerankMeta {
	return &legacyRerankMeta{
		legacyParams: params,
	}
}
