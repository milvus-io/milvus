package proxy

import (
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
)

// rerankMeta provides access to common rerank metadata.
// A nil rerankMeta means no reranking is configured.
type rerankMeta interface {
	GetInputFieldNames() []string
	GetInputFieldIDs() []int64
}

// funcScoreRerankMeta holds rerank configuration from a FunctionScore proto.
type funcScoreRerankMeta struct {
	inputFieldNames []string
	inputFieldIDs   []int64
	funcScore       *schemapb.FunctionScore
}

func (m *funcScoreRerankMeta) GetInputFieldNames() []string { return m.inputFieldNames }
func (m *funcScoreRerankMeta) GetInputFieldIDs() []int64    { return m.inputFieldIDs }

// legacyRerankMeta holds rerank configuration from legacy rank parameters.
type legacyRerankMeta struct {
	legacyParams []*commonpb.KeyValuePair
}

func (m *legacyRerankMeta) GetInputFieldNames() []string { return nil }
func (m *legacyRerankMeta) GetInputFieldIDs() []int64    { return nil }

// newRerankMeta creates a rerankMeta from a FunctionScore proto.
// Returns nil if funcScore is nil, has no functions, or all functions are boost
// (boost is pushed down to QueryNode and doesn't need proxy-level reranking).
func newRerankMeta(collSchema *schemapb.CollectionSchema, funcScore *schemapb.FunctionScore) rerankMeta {
	if funcScore == nil || len(funcScore.Functions) == 0 {
		return nil
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
		return nil
	}
	return &funcScoreRerankMeta{
		funcScore:       funcScore,
		inputFieldNames: chain.GetInputFieldNamesFromFuncScore(funcScore),
		inputFieldIDs:   chain.GetInputFieldIDsFromSchema(collSchema, funcScore),
	}
}

// newRerankMetaFromLegacy creates a rerankMeta from legacy search rank parameters.
func newRerankMetaFromLegacy(params []*commonpb.KeyValuePair) rerankMeta {
	return &legacyRerankMeta{
		legacyParams: params,
	}
}
