package milvusclient

import (
	"encoding/json"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
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
	K       float64 `json:"k,omitempty"`
	weights *[]float64
}

func (r *rrfReranker) WithK(k float64) *rrfReranker {
	r.K = k
	return r
}

// WithWeights sets optional reciprocal-rank coefficients in ANN request order.
// The server requires a non-empty slice, one value in [0, 1] per ANN request;
// nil and empty slices are serialized so the server can reject them.
func (r *rrfReranker) WithWeights(weights []float64) *rrfReranker {
	var copied []float64
	if weights != nil {
		copied = make([]float64, len(weights))
		copy(copied, weights)
	}
	r.weights = &copied
	return r
}

func (r *rrfReranker) GetParams() []*commonpb.KeyValuePair {
	params := struct {
		K       float64    `json:"k,omitempty"`
		Weights *[]float64 `json:"weights,omitempty"`
	}{
		K:       r.K,
		Weights: r.weights,
	}
	bs, _ := json.Marshal(params)

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
