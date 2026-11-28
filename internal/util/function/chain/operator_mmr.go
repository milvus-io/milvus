/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"fmt"
	"math"
	"sort"

	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func init() {
	MustRegisterOperator(types.OpTypeMMR, NewMMROpFromReprWithContext)
}

// MMRConfig configures one MMR operator.
type MMRConfig struct {
	// Field is the vector column used for candidate-to-candidate similarity.
	Field string
	// Lambda in [0, 1] weights relevance against redundancy.
	Lambda float64
	// CandidateSize is the fixed window: MMR orders the top CandidateSize rows
	// by $score, always counted from the best row.
	CandidateSize int64
	// SelectCount is the number of rows MMR emits per chunk, in selection
	// order. Zero or more than the window emits the whole window. Greedy
	// selection is prefix-stable, so emitting offset+limit rows equals the
	// first offset+limit positions of the full order.
	SelectCount int64
	// Similarity is the metric used between candidates (index metric or
	// user override).
	Similarity string
	// RelevanceMetric, when set, maps raw search scores with that metric onto
	// the same scale as Similarity. When empty, relevance is min-max
	// normalized inside the window (scores fused by another reranker).
	RelevanceMetric string
	// ScoreDescending is the direction of $score: larger is better when true.
	ScoreDescending bool
	// MaxSimilarityOps bounds the estimated work per request; zero uses the
	// configured default.
	MaxSimilarityOps int64
}

// MMROp diversifies each query chunk with Maximal Marginal Relevance:
//
//	next = argmax_c lambda*rel(c) - (1-lambda)*max_{s in selected} sim(c, s)
//
// It selects rows in MMR order and never drops a candidate: redundant rows are
// only moved later. Each result keeps its upstream $score.
type MMROp struct {
	BaseOp
	cfg MMRConfig
}

// NewMMROp creates an MMR operator. The configuration is validated.
func NewMMROp(cfg MMRConfig) (*MMROp, error) {
	if cfg.Field == "" {
		return nil, merr.WrapErrParameterInvalidMsg("mmr: vector field is required")
	}
	if math.IsNaN(cfg.Lambda) || cfg.Lambda < 0 || cfg.Lambda > 1 {
		return nil, merr.WrapErrParameterInvalidMsg("mmr: lambda must be in [0, 1], got %v", cfg.Lambda)
	}
	maxCandidates := paramtable.Get().FunctionCfg.MMRMaxCandidateSize.GetAsInt64()
	if cfg.CandidateSize <= 0 || cfg.CandidateSize > maxCandidates {
		return nil, merr.WrapErrParameterInvalidMsg(
			"mmr: candidate_size must be in [1, %d], got %d", maxCandidates, cfg.CandidateSize)
	}
	if cfg.SelectCount < 0 {
		return nil, merr.WrapErrParameterInvalidMsg("mmr: limit must be non-negative, got %d", cfg.SelectCount)
	}
	if cfg.Similarity == "" {
		return nil, merr.WrapErrParameterInvalidMsg("mmr: similarity metric is required")
	}
	return &MMROp{
		BaseOp: BaseOp{
			inputs:  []string{cfg.Field, types.ScoreFieldName},
			outputs: []string{},
		},
		cfg: cfg,
	}, nil
}

func (o *MMROp) Name() string { return "MMR" }

func (o *MMROp) String() string {
	return fmt.Sprintf("MMR(field=%s, lambda=%g, candidate_size=%d, limit=%d, similarity=%s)",
		o.cfg.Field, o.cfg.Lambda, o.cfg.CandidateSize, o.cfg.SelectCount, o.cfg.Similarity)
}

// Config returns a copy of the operator configuration.
func (o *MMROp) Config() MMRConfig { return o.cfg }

func (o *MMROp) Execute(ctx *types.FuncContext, input *DataFrame) (*OperatorResult, error) {
	vecCol := input.Column(o.cfg.Field)
	if vecCol == nil {
		return nil, merr.WrapErrServiceInternalMsg("mmr: vector column %q not found", o.cfg.Field)
	}
	scoreCol := input.Column(types.ScoreFieldName)
	if scoreCol == nil {
		return nil, merr.WrapErrServiceInternalMsg("mmr: column %q not found", types.ScoreFieldName)
	}
	meta, ok := input.fieldMetadata[o.cfg.Field]
	if !ok || meta.field == nil || !typeutil.IsVectorType(meta.field.GetDataType()) {
		return nil, merr.WrapErrParameterInvalidMsg("mmr: column %q is not a vector field", o.cfg.Field)
	}
	dim, err := typeutil.GetDim(meta.field)
	// Sparse vectors have no dim. A column with only null vectors may arrive
	// without one; nulls are never decoded, so the dim is not needed.
	if err != nil && !typeutil.IsSparseFloatVectorType(meta.field.GetDataType()) && vecCol.NullN() != vecCol.Len() {
		return nil, merr.Wrap(err, "mmr: vector column has no dim")
	}
	sim, err := newMMRSimilarity(meta.field.GetDataType(), meta.field.GetElementType(), dim, o.cfg.Similarity)
	if err != nil {
		return nil, err
	}
	// The budget is charged with the cost of ordering each whole window, so
	// every page of a paging session is accepted or rejected alike.
	budget := o.cfg.MaxSimilarityOps
	if budget <= 0 {
		budget = paramtable.Get().FunctionCfg.MMRMaxSimilarityOps.GetAsInt64()
	}

	selection := &RowSelection{Chunks: make([]ChunkSelection, input.NumChunks())}
	for chunkIdx := range input.NumChunks() {
		vecs, ok := vecCol.Chunk(chunkIdx).(*array.Binary)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("mmr: vector column %q is not binary", o.cfg.Field)
		}
		scores, ok := scoreCol.Chunk(chunkIdx).(*array.Float32)
		if !ok {
			return nil, merr.WrapErrServiceInternalMsg("mmr: %s is not float32", types.ScoreFieldName)
		}
		picked, err := o.selectChunk(vecs, scores, sim, &budget)
		if err != nil {
			return nil, err
		}
		selection.Chunks[chunkIdx] = RowIndices(picked)
	}
	return &OperatorResult{Selection: selection}, nil
}

// selectChunk returns input positions in MMR selection order.
func (o *MMROp) selectChunk(
	vecs *array.Binary,
	scores *array.Float32,
	sim *mmrSimilarity,
	budget *int64,
) ([]int, error) {
	n := scores.Len()
	if vecs.Len() != n {
		return nil, merr.WrapErrServiceInternalMsg("mmr: vector and score lengths differ (%d vs %d)", vecs.Len(), n)
	}

	// The window is the top CandidateSize rows by $score, counted from the
	// best row. Ties keep input order, which carries the upstream primary-key
	// tie breaker.
	order := make([]int, n)
	for i := range order {
		order[i] = i
	}
	better := func(a, b float32) bool {
		if o.cfg.ScoreDescending {
			return a > b
		}
		return a < b
	}
	sort.SliceStable(order, func(i, j int) bool {
		ai, bj := order[i], order[j]
		if scores.IsNull(ai) || scores.IsNull(bj) {
			return !scores.IsNull(ai) && scores.IsNull(bj)
		}
		return better(scores.Value(ai), scores.Value(bj))
	})
	w := min(int64(n), o.cfg.CandidateSize)
	window := order[:w]
	k := w
	if o.cfg.SelectCount > 0 {
		k = min(k, o.cfg.SelectCount)
	}
	if k == 0 {
		return []int{}, nil
	}

	cands := make([]mmrVector, w)
	for i, pos := range window {
		v, err := sim.decode(vecs.Value(pos), vecs.IsNull(pos))
		if err != nil {
			return nil, err
		}
		cands[i] = v
	}
	if *budget -= sim.windowOps(cands); *budget < 0 {
		return nil, merr.WrapErrParameterInvalidMsg(
			"mmr: request exceeds function.rerank.mmr.maxSimilarityOps; reduce candidate_size or nq")
	}
	rel := o.relevance(window, scores, sim.dim)

	lambda := o.cfg.Lambda
	maxSim := make([]float64, w)
	chosen := make([]bool, w)
	picked := make([]int, 0, k)
	for len(picked) < int(k) {
		best := -1
		bestScore := math.Inf(-1)
		for i := 0; i < int(w); i++ {
			if chosen[i] {
				continue
			}
			s := lambda * rel[i]
			if len(picked) > 0 {
				s -= (1 - lambda) * maxSim[i]
			}
			// Ties: higher relevance first, then the earlier window position.
			if best < 0 || s > bestScore || (s == bestScore && rel[i] > rel[best]) {
				best, bestScore = i, s
			}
		}
		chosen[best] = true
		picked = append(picked, window[best])
		if len(picked) == int(k) {
			break
		}
		for i := 0; i < int(w); i++ {
			if chosen[i] {
				continue
			}
			if s := sim.similarity(&cands[i], &cands[best]); s > maxSim[i] {
				maxSim[i] = s
			}
		}
	}
	return picked, nil
}

// relevance returns rel(c) in [0, 1] for every window row.
func (o *MMROp) relevance(window []int, scores *array.Float32, dim int64) []float64 {
	rel := make([]float64, len(window))
	if o.cfg.RelevanceMetric != "" {
		for i, pos := range window {
			if scores.IsNull(pos) {
				continue
			}
			rel[i] = mmrRelevance(o.cfg.RelevanceMetric, scores.Value(pos), dim)
		}
		return rel
	}
	lo, hi := math.Inf(1), math.Inf(-1)
	for _, pos := range window {
		if scores.IsNull(pos) {
			continue
		}
		v := float64(scores.Value(pos))
		lo, hi = math.Min(lo, v), math.Max(hi, v)
	}
	for i, pos := range window {
		if scores.IsNull(pos) {
			continue
		}
		v := float64(scores.Value(pos))
		switch {
		case hi <= lo:
			rel[i] = 1
		case o.cfg.ScoreDescending:
			rel[i] = (v - lo) / (hi - lo)
		default:
			rel[i] = (hi - v) / (hi - lo)
		}
	}
	return rel
}

// NewMMROpFromReprWithContext builds the FunctionChains "mmr" operator.
// The single input is the vector field. Similarity defaults to the search
// metric when the request has exactly one ANN search; otherwise it must be
// set explicitly. Relevance is min-max normalized inside the window.
func NewMMROpFromReprWithContext(repr *OperatorRepr, buildCtx types.FunctionBuildContext) (Operator, error) {
	if len(repr.Inputs) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg("mmr: expects exactly 1 input vector field, got %d", len(repr.Inputs))
	}
	reader := types.NewParamReader("mmr", repr.Params)
	lambda, err := reader.Float64(types.MMRParamLambda, false, types.MMRDefaultLambda)
	if err != nil {
		return nil, err
	}
	candidateSize, err := reader.Int64(types.MMRParamCandidateSize, false, types.MMRMinDefaultCandidateSize)
	if err != nil {
		return nil, err
	}
	limit, err := reader.Int64(types.MMRParamLimit, false, 0)
	if err != nil {
		return nil, err
	}
	descending, err := reader.Bool(types.MMRParamScoreDescending, false, true)
	if err != nil {
		return nil, err
	}
	similarity, err := reader.String(types.MMRParamSimilarity, false)
	if err != nil {
		return nil, err
	}
	if similarity == "" {
		if buildCtx.Search == nil || len(buildCtx.Search.MetricTypes) != 1 {
			return nil, merr.WrapErrParameterInvalidMsg(
				"mmr: %q is required when the request has more than one ANN search", types.MMRParamSimilarity)
		}
		similarity = buildCtx.Search.MetricTypes[0]
	}
	return NewMMROp(MMRConfig{
		Field:           repr.Inputs[0],
		Lambda:          lambda,
		CandidateSize:   candidateSize,
		SelectCount:     limit,
		Similarity:      similarity,
		ScoreDescending: descending,
	})
}

// mmrVectorFieldSchema resolves the MMR vector field, including struct-array
// vector sub-fields. Retrievability is checked when the request input plan is
// built (ValidateMMRInputField).
func mmrVectorFieldSchema(collSchema *schemapb.CollectionSchema, name string) (*schemapb.FieldSchema, error) {
	for _, f := range typeutil.GetAllFieldSchemas(collSchema) {
		if f.GetName() == name {
			if !typeutil.IsVectorType(f.GetDataType()) {
				return nil, merr.WrapErrParameterInvalidMsg("mmr: input field %q is not a vector field", name)
			}
			return f, nil
		}
	}
	return nil, merr.WrapErrParameterInvalidMsg("mmr: input field %q not found in collection schema", name)
}
