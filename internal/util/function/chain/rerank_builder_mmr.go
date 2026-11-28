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
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// mmrParams holds the parsed FunctionScore parameters of the MMR reranker.
type mmrParams struct {
	field         string
	lambda        float64
	candidateSize int64
	similarity    string
}

// DefaultMMRCandidateSize returns the default MMR window for a page size. It
// depends only on limit, never on offset, so every page of a paging session
// diversifies the same window.
func DefaultMMRCandidateSize(limit int64) int64 {
	maxCandidates := paramtable.Get().FunctionCfg.MMRMaxCandidateSize.GetAsInt64()
	return min(maxCandidates, max(int64(types.MMRMinDefaultCandidateSize), types.MMRDefaultCandidateSizeFactor*limit))
}

// FindMMRFunction returns the MMR function of a FunctionScore, or nil.
func FindMMRFunction(funcScore *schemapb.FunctionScore) *schemapb.FunctionSchema {
	for _, f := range funcScore.GetFunctions() {
		if rerank.GetRerankName(f) == MMRRerankerName {
			return f
		}
	}
	return nil
}

// MMRCandidateSize returns the window an MMR function will use for the given
// page size, applying the default when candidate_size is not set.
func MMRCandidateSize(mmrFunc *schemapb.FunctionSchema, limit int64) (int64, error) {
	p, err := parseMMRParams(mmrFunc, limit)
	if err != nil {
		return 0, err
	}
	return p.candidateSize, nil
}

// ValidateMMRWindow checks a candidate window against the configured maximum
// and the page size. Both rerank APIs use it before the window becomes the
// search top-k.
func ValidateMMRWindow(candidateSize, limit int64) error {
	maxCandidates := paramtable.Get().FunctionCfg.MMRMaxCandidateSize.GetAsInt64()
	if candidateSize <= 0 || candidateSize > maxCandidates {
		return merr.WrapErrParameterInvalidMsg(
			"mmr: candidate_size must be in [1, %d], got %d", maxCandidates, candidateSize)
	}
	if limit > candidateSize {
		return merr.WrapErrParameterInvalidMsg(
			"mmr: limit (%d) must not exceed candidate_size (%d)", limit, candidateSize)
	}
	return nil
}

func parseMMRParams(funcSchema *schemapb.FunctionSchema, limit int64) (*mmrParams, error) {
	p := &mmrParams{lambda: types.MMRDefaultLambda}
	candidateSet := false
	for _, kv := range funcSchema.GetParams() {
		switch strings.ToLower(kv.GetKey()) {
		case types.MMRParamLambda:
			v, err := strconv.ParseFloat(kv.GetValue(), 64)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("mmr: lambda must be a number, got %q", kv.GetValue())
			}
			p.lambda = v
		case types.MMRParamCandidateSize:
			v, err := strconv.ParseInt(kv.GetValue(), 10, 64)
			if err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("mmr: candidate_size must be an integer, got %q", kv.GetValue())
			}
			p.candidateSize = v
			candidateSet = true
		case types.MMRParamSimilarity:
			p.similarity = strings.ToUpper(strings.TrimSpace(kv.GetValue()))
		}
	}
	if !candidateSet {
		p.candidateSize = DefaultMMRCandidateSize(limit)
	}
	if p.lambda < 0 || p.lambda > 1 {
		return nil, merr.WrapErrParameterInvalidMsg("mmr: lambda must be in [0, 1], got %v", p.lambda)
	}
	if err := ValidateMMRWindow(p.candidateSize, limit); err != nil {
		return nil, err
	}
	if len(funcSchema.GetInputFieldNames()) != 1 {
		return nil, merr.WrapErrParameterInvalidMsg(
			"mmr: requires exactly 1 vector input field, got %d", len(funcSchema.GetInputFieldNames()))
	}
	p.field = funcSchema.GetInputFieldNames()[0]
	return p, nil
}

// buildMMRRerankChain builds:
//
//	[upstream body | Merge(max)] → MMR(window W, emit offset+limit)
//	→ Limit(limit, offset) → [RoundDecimal]
//
// W is the fixed candidate window. offset and limit slice the MMR order of
// that window; past the window the page is short or empty.
func buildMMRRerankChain(
	collSchema *schemapb.CollectionSchema,
	upstream *schemapb.FunctionSchema,
	mmrFunc *schemapb.FunctionSchema,
	searchMetrics []string,
	searchParams *SearchParams,
	alloc memory.Allocator,
) (*FuncChain, error) {
	if searchParams.HasGrouping() {
		return nil, merr.WrapErrParameterInvalidMsg("mmr: cannot be combined with group_by")
	}
	p, err := parseMMRParams(mmrFunc, searchParams.Limit)
	if err != nil {
		return nil, err
	}
	fieldSchema, err := mmrVectorFieldSchema(collSchema, p.field)
	if err != nil {
		return nil, err
	}

	// The similarity between candidates defaults to the metric of the ANN
	// search on the same field.
	routeIdx := -1
	for i, f := range searchParams.AnnsFields {
		if f == p.field && i < len(searchMetrics) {
			routeIdx = i
			break
		}
	}
	similarity := p.similarity
	if similarity == "" {
		if routeIdx < 0 {
			return nil, merr.WrapErrParameterInvalidMsg(
				"mmr: input field %q is not searched in this request; set the %q parameter",
				p.field, types.MMRParamSimilarity)
		}
		similarity = searchMetrics[routeIdx]
	}
	if err := validateMMRSimilarity(fieldSchema, similarity); err != nil {
		return nil, err
	}

	var fc *FuncChain
	var sortDescending bool
	if upstream != nil {
		fc, sortDescending, err = buildRerankBody(collSchema, upstream, searchMetrics, searchParams, alloc)
		if err != nil {
			return nil, err
		}
	} else {
		if alloc == nil {
			alloc = memory.DefaultAllocator
		}
		fc = NewFuncChainWithAllocator(alloc).
			SetStage(types.StageL2Rerank).
			SetName("rerank_chain")
		mergeOp := NewMergeOp(MergeStrategyMax,
			WithMetricTypes(searchMetrics),
			WithScoresRewritten(searchParams.ScoresRewritten))
		fc.Add(mergeOp)
		sortDescending = mergeOp.SortDescending()
	}

	// MMR directly on the search scores of the same field: relevance and
	// redundancy share the field's metric scale. Otherwise min-max inside the
	// window.
	relevanceMetric := ""
	if upstream == nil && len(searchMetrics) == 1 && routeIdx == 0 &&
		!isScoreRewritten(searchParams.ScoresRewritten, 0) {
		relevanceMetric = searchMetrics[0]
	}

	selectCount := searchParams.Offset + searchParams.Limit
	if searchParams.Limit <= 0 {
		selectCount = p.candidateSize
	}
	mmrOp, err := NewMMROp(MMRConfig{
		Field:           p.field,
		Lambda:          p.lambda,
		CandidateSize:   p.candidateSize,
		SelectCount:     min(selectCount, p.candidateSize),
		Similarity:      similarity,
		RelevanceMetric: relevanceMetric,
		ScoreDescending: sortDescending,
	})
	if err != nil {
		return nil, err
	}

	// MMR takes the top-W window by $score itself.
	fc.Add(mmrOp)
	if searchParams.Limit > 0 {
		fc.LimitWithOffset(searchParams.Limit, searchParams.Offset)
	}
	return appendRounding(fc, searchParams)
}

// validateMMRSimilarity rejects a similarity that does not fit the field type
// at build time, before any search work is done.
func validateMMRSimilarity(field *schemapb.FieldSchema, similarity string) error {
	// Sparse fields have no dim; the metric check does not need it.
	dim, _ := typeutil.GetDim(field)
	_, err := newMMRSimilarity(field.GetDataType(), field.GetElementType(), dim, similarity)
	return err
}
