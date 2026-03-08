package milvusclient

import (
	"encoding/json"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
)

const (
	rerankType    = "strategy"
	rerankParams  = "params"
	rffParam      = "k"
	weightedParam = "weights"

	rrfRerankType      = `rrf`
	weightedRerankType = `weighted`
)

type Reranker interface {
	GetParams() []*commonpb.KeyValuePair
}

type rrfReranker struct {
	K float64 `json:"k,omitempty"`
}

func (r *rrfReranker) WithK(k float64) *rrfReranker {
	r.K = k
	return r
}

func (r *rrfReranker) GetParams() []*commonpb.KeyValuePair {
	bs, _ := json.Marshal(r)

	return []*commonpb.KeyValuePair{
		{Key: rerankType, Value: rrfRerankType},
		{Key: rerankParams, Value: string(bs)},
	}
}

func NewRRFReranker() *rrfReranker {
	return &rrfReranker{K: 60}
}

type weightedReranker struct {
	Weights []float64 `json:"weights,omitempty"`
}

func (r *weightedReranker) GetParams() []*commonpb.KeyValuePair {
	bs, _ := json.Marshal(r)

	return []*commonpb.KeyValuePair{
		{Key: rerankType, Value: weightedRerankType},
		{Key: rerankParams, Value: string(bs)},
	}
}

func NewWeightedReranker(weights []float64) *weightedReranker {
	return &weightedReranker{
		Weights: weights,
	}
}
