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
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/expr"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// =============================================================================
// Constants
// =============================================================================

const (
	// Reranker names
	DecayRerankerName    = "decay"
	ModelRerankerName    = "model"
	RRFRerankerName      = "rrf"
	WeightedRerankerName = "weighted"

	// Parameter keys
	rerankerKey  = "reranker"
	rrfKKey      = "k"
	weightsKey   = "weights"
	normScoreKey = "norm_score"
	scoreModeKey = "score_mode"
	functionKey  = "function"
	originKey    = "origin"
	scaleKey     = "scale"
	offsetKey    = "offset"
	decayKey     = "decay"

	// Legacy parameter keys
	legacyRankTypeKey   = "strategy"
	legacyRankParamsKey = "params"

	// Model parameter keys
	queryKeyName = "queries"

	// Default values
	defaultRRFK      = 60.0
	defaultDecay     = 0.5
	defaultScoreMode = "max"
)

// =============================================================================
// SearchParams
// =============================================================================

// SearchParams contains search parameters for reranking.
type SearchParams struct {
	Nq           int64
	Limit        int64
	Offset       int64
	RoundDecimal int64

	// Grouping parameters
	GroupByField string      // Field to group by (empty means no grouping)
	GroupSize    int64       // Maximum rows per group
	GroupScorer  GroupScorer // How to compute group score ("max", "sum", "avg")

	// Model reranker extra info (can be nil for non-model rerankers)
	ModelExtraInfo *models.ModelExtraInfo
}

// NewSearchParams creates a new SearchParams.
func NewSearchParams(nq, limit, offset, roundDecimal int64) *SearchParams {
	return &SearchParams{
		Nq:           nq,
		Limit:        limit,
		Offset:       offset,
		RoundDecimal: roundDecimal,
	}
}

// NewSearchParamsWithGrouping creates a new SearchParams with grouping support.
func NewSearchParamsWithGrouping(nq, limit, offset, roundDecimal int64, groupByField string, groupSize int64) *SearchParams {
	return NewSearchParamsWithGroupingAndScorer(nq, limit, offset, roundDecimal, groupByField, groupSize, GroupScorerMax)
}

// NewSearchParamsWithGroupingAndScorer creates a new SearchParams with grouping and scorer support.
func NewSearchParamsWithGroupingAndScorer(nq, limit, offset, roundDecimal int64, groupByField string, groupSize int64, scorer GroupScorer) *SearchParams {
	return &SearchParams{
		Nq:           nq,
		Limit:        limit,
		Offset:       offset,
		RoundDecimal: roundDecimal,
		GroupByField: groupByField,
		GroupSize:    groupSize,
		GroupScorer:  scorer,
	}
}

// HasGrouping returns true if grouping is enabled.
func (sp *SearchParams) HasGrouping() bool {
	return sp.GroupByField != "" && sp.GroupSize > 0
}

// =============================================================================
// BuildRerankChain
// =============================================================================

// BuildRerankChain builds a FuncChain for reranking from FunctionScore schema.
func BuildRerankChain(
	collSchema *schemapb.CollectionSchema,
	funcScoreSchema *schemapb.FunctionScore,
	searchMetrics []string,
	searchParams *SearchParams,
	alloc memory.Allocator,
) (*FuncChain, error) {
	if funcScoreSchema == nil || len(funcScoreSchema.Functions) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("rerank_builder: no rerank functions specified")
	}

	// Filter out boost functions — boost is pushed down to QueryNode, proxy doesn't handle it
	var proxyFunctions []*schemapb.FunctionSchema
	for _, f := range funcScoreSchema.Functions {
		if rerank.GetRerankName(f) != rerank.BoostName {
			proxyFunctions = append(proxyFunctions, f)
		}
	}

	// All functions are boost, proxy has nothing to rerank
	if len(proxyFunctions) == 0 {
		return nil, nil
	}

	// Currently only support single proxy rerank function
	if len(proxyFunctions) > 1 {
		return nil, merr.WrapErrParameterInvalidMsg("rerank_builder: currently only supports one rerank function")
	}

	return buildRerankChainInternal(collSchema, proxyFunctions[0], searchMetrics, searchParams, alloc)
}

// BuildRerankChainWithLegacy builds a FuncChain from legacy rank parameters.
func BuildRerankChainWithLegacy(
	collSchema *schemapb.CollectionSchema,
	rankParams []*commonpb.KeyValuePair,
	searchMetrics []string,
	searchParams *SearchParams,
	alloc memory.Allocator,
) (*FuncChain, error) {
	funcSchema, err := convertLegacyParams(rankParams)
	if err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("rerank_builder: %v", err)
	}

	return buildRerankChainInternal(collSchema, funcSchema, searchMetrics, searchParams, alloc)
}

// convertLegacyParams converts legacy rank params to FunctionSchema.
func convertLegacyParams(rankParams []*commonpb.KeyValuePair) (*schemapb.FunctionSchema, error) {
	var params map[string]interface{}
	rankTypeStr := "rrf"

	for _, kv := range rankParams {
		switch strings.ToLower(kv.Key) {
		case legacyRankTypeKey:
			rankTypeStr = strings.ToLower(kv.Value)
		case legacyRankParamsKey:
			if err := json.Unmarshal([]byte(kv.Value), &params); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("parse rerank params failed: %v", err)
			}
		}
	}

	fSchema := &schemapb.FunctionSchema{
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{},
		OutputFieldNames: []string{},
		Params:           []*commonpb.KeyValuePair{},
	}

	switch rankTypeStr {
	case "rrf":
		fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: rerankerKey, Value: RRFRerankerName})
		if v, ok := params[rrfKKey]; ok {
			if k, ok := v.(float64); ok {
				fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: rrfKKey, Value: strconv.FormatFloat(k, 'f', -1, 64)})
			} else {
				return nil, merr.WrapErrParameterInvalidMsg("the type of rank param k should be float")
			}
		}
	case "weighted":
		fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: rerankerKey, Value: WeightedRerankerName})
		if v, ok := params[weightsKey]; ok {
			if d, err := json.Marshal(v); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("the weights param should be an array")
			} else {
				fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: weightsKey, Value: string(d)})
			}
		}
		if normScore, ok := params[normScoreKey]; ok {
			switch ns := normScore.(type) {
			case bool:
				fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: normScoreKey, Value: strconv.FormatBool(ns)})
			case string:
				if parsed, err := strconv.ParseBool(ns); err != nil {
					return nil, merr.WrapErrParameterInvalidMsg("the type of rank param norm_score should be bool")
				} else {
					fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: normScoreKey, Value: strconv.FormatBool(parsed)})
				}
			default:
				return nil, merr.WrapErrParameterInvalidMsg("the type of rank param norm_score should be bool")
			}
		}
	default:
		return nil, merr.WrapErrParameterInvalidMsg("unsupported rank type %s", rankTypeStr)
	}

	return fSchema, nil
}

// buildRerankChainInternal builds a FuncChain from FunctionSchema.
//
// It produces 4 kinds of chains depending on the reranker, sharing a common tail:
//
//  1. RRF:
//     Merge(RRF) → Sort/GroupBy → [RoundDecimal] → Select
//
//  2. Weighted:
//     Merge(Weighted) → Sort/GroupBy → [RoundDecimal] → Select
//
//  3. Decay:
//     Merge(Max|Sum|Avg) → Map(DecayExpr→_decay_score) → Map(ScoreCombine: $score*_decay_score→$score) → Sort/GroupBy → [RoundDecimal] → Select
//
//  4. Model:
//     Merge(Max) → Map(RerankModelExpr) → Sort/GroupBy → [RoundDecimal] → Select
//
// Common tail behavior:
//   - Without grouping: Sort($score, DESC) → Limit(limit, offset)
//   - With grouping:    GroupBy(field, groupSize, limit, offset, scorer)
//   - RoundDecimal >= 0: Map(RoundDecimalExpr) rounds $score after ordering
//   - Select: keeps only $id, $score (plus groupByField, $group_score if grouping)
func buildRerankChainInternal(
	collSchema *schemapb.CollectionSchema,
	funcSchema *schemapb.FunctionSchema,
	searchMetrics []string,
	searchParams *SearchParams,
	alloc memory.Allocator,
) (*FuncChain, error) {
	rerankerName := rerank.GetRerankName(funcSchema)
	if rerankerName == "" {
		return nil, merr.WrapErrParameterInvalidMsg("rerank_builder: reranker name not specified")
	}

	if alloc == nil {
		alloc = memory.DefaultAllocator
	}

	fc := NewFuncChainWithAllocator(alloc).
		SetStage(types.StageL2Rerank).
		SetName("rerank_chain")

	sortDescending := true // default: larger score = better match

	switch rerankerName {
	case RRFRerankerName:
		if err := buildRRFChain(fc, funcSchema, searchMetrics); err != nil {
			return nil, err
		}

	case WeightedRerankerName:
		var err error
		sortDescending, err = buildWeightedChain(fc, funcSchema, searchMetrics)
		if err != nil {
			return nil, err
		}

	case DecayRerankerName:
		if err := buildDecayChain(fc, collSchema, funcSchema, searchMetrics); err != nil {
			return nil, err
		}

	case ModelRerankerName:
		if err := buildModelChain(fc, collSchema, funcSchema, searchMetrics, searchParams); err != nil {
			return nil, err
		}

	default:
		return nil, merr.WrapErrParameterInvalidMsg("rerank_builder: unsupported reranker %s", rerankerName)
	}

	// Add grouping or sort+limit
	if searchParams.HasGrouping() {
		// Use GroupBy for grouping search
		scorer := searchParams.GroupScorer
		if scorer == "" {
			scorer = GroupScorerMax
		}
		// Construct the GroupByOp directly so we can propagate the sort
		// direction from the merge stage. fc.GroupByWithScorer would default
		// to DESC, which is wrong for distance metrics + no normalization
		// (e.g., weighted reranker on raw L2 distances).
		groupByOp := NewGroupByOpWithScorer(
			searchParams.GroupByField,
			searchParams.GroupSize,
			searchParams.Limit,
			searchParams.Offset,
			scorer,
		).SetSortDescending(sortDescending)
		fc.Add(groupByOp)
	} else {
		// Non-grouping: sort by score and apply limit
		fc.Sort(types.ScoreFieldName, sortDescending)
		if searchParams.Limit > 0 {
			fc.LimitWithOffset(searchParams.Limit, searchParams.Offset)
		}
	}

	// Apply score rounding after sort/limit (rounding should not affect ordering)
	if searchParams.RoundDecimal >= 0 {
		roundExpr, err := expr.NewRoundDecimalExpr(searchParams.RoundDecimal)
		if err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("rerank_builder: %v", err)
		}
		fc.Map(roundExpr,
			[]string{types.ScoreFieldName},
			[]string{types.ScoreFieldName})
	}

	// Only keep $id and $score (plus group-by and $group_score if present) in the output.
	// Other field columns are no longer needed — downstream pipeline stages
	// (organize/requery) re-fetch fields from segments.
	selectCols := []string{types.IDFieldName, types.ScoreFieldName}
	if searchParams.HasGrouping() {
		selectCols = append(selectCols, searchParams.GroupByField, GroupScoreFieldName)
	}
	fc.Select(selectCols...)

	return fc, nil
}

// =============================================================================
// RRF Builder
// =============================================================================

func buildRRFChain(fc *FuncChain, funcSchema *schemapb.FunctionSchema, searchMetrics []string) error {
	rrfK, err := parseRRFK(funcSchema)
	if err != nil {
		return err
	}

	// RRF scores are computed purely from rank position, not from original scores,
	// so metricTypes and normalize are not needed.
	fc.Merge(MergeStrategyRRF,
		WithRRFK(rrfK))

	return nil
}

func parseRRFK(funcSchema *schemapb.FunctionSchema) (float64, error) {
	for _, param := range funcSchema.Params {
		if strings.ToLower(param.Key) == rrfKKey {
			k, err := strconv.ParseFloat(param.Value, 64)
			if err != nil {
				return 0, merr.WrapErrParameterInvalidMsg("%s is not a number", param.Value)
			}
			if k <= 0 || k >= 16384 {
				return 0, merr.WrapErrParameterInvalidMsg("The rank params k should be in range (0, %d)", 16384)
			}
			return k, nil
		}
	}
	return defaultRRFK, nil
}

// =============================================================================
// Weighted Builder
// =============================================================================

func buildWeightedChain(fc *FuncChain, funcSchema *schemapb.FunctionSchema, searchMetrics []string) (sortDescending bool, err error) {
	weights, normalize, err := parseWeightedParams(funcSchema)
	if err != nil {
		return true, err
	}

	if len(weights) == 0 {
		return true, merr.WrapErrParameterInvalidMsg("rerank_builder: weighted reranker requires weights parameter")
	}

	if len(weights) != len(searchMetrics) {
		return true, merr.WrapErrParameterInvalid(fmt.Sprint(len(searchMetrics)), fmt.Sprint(len(weights)), "the length of weights param mismatch with ann search requests")
	}

	mergeOp := NewMergeOp(MergeStrategyWeighted,
		WithWeights(weights),
		WithMetricTypes(searchMetrics),
		WithNormalize(normalize))

	fc.Add(mergeOp)

	return mergeOp.SortDescending(), nil
}

func parseWeightedParams(funcSchema *schemapb.FunctionSchema) ([]float64, bool, error) {
	var weights []float64
	normalize := false

	for _, param := range funcSchema.Params {
		switch strings.ToLower(param.Key) {
		case weightsKey:
			var ws []float64
			if err := json.Unmarshal([]byte(param.Value), &ws); err != nil {
				return nil, false, merr.WrapErrParameterInvalidMsg("failed to parse weights: %v", err)
			}
			for _, w := range ws {
				if w < 0 || w > 1 {
					return nil, false, merr.WrapErrParameterInvalidMsg("rank param weight should be in range [0, 1]")
				}
			}
			weights = ws
		case normScoreKey:
			if ns, err := strconv.ParseBool(param.Value); err != nil {
				return nil, false, merr.WrapErrParameterInvalidMsg("failed to parse norm_score: %v", err)
			} else {
				normalize = ns
			}
		}
	}

	return weights, normalize, nil
}

// =============================================================================
// Decay Builder
// =============================================================================

func buildDecayChain(fc *FuncChain, collSchema *schemapb.CollectionSchema, funcSchema *schemapb.FunctionSchema, searchMetrics []string) error {
	// Parse decay parameters
	strategy, normalize, decayParams, err := parseDecayParams(funcSchema)
	if err != nil {
		return err
	}

	// Get input field name
	if len(funcSchema.InputFieldNames) != 1 {
		return merr.WrapErrParameterInvalidMsg("rerank_builder: decay reranker requires exactly 1 input field, got %d", len(funcSchema.InputFieldNames))
	}
	inputField := funcSchema.InputFieldNames[0]

	// Validate input field exists in collection schema
	if err := validateInputField(collSchema, inputField); err != nil {
		return err
	}

	// Add MergeOp with score_mode strategy.
	//
	// Decay multiplies $score by a decay factor in [0, 1] and assumes
	// "higher = more relevant" semantics. WithForceDescending(true) ensures
	// distance metrics like L2 are flipped via 1 - 2·atan(d)/π before the
	// multiplication, even when norm_score=false. Without this, raw L2
	// distances would be combined with the decay factor and the worst
	// vector match would rank first after the outer DESC sort.
	//
	// WithNormalize(true) (full range normalization) is still honored when
	// the user opts in via norm_score=true; the two flags compose without
	// overlap because the normalize branch returns first.
	fc.Merge(strategy,
		WithMetricTypes(searchMetrics),
		WithNormalize(normalize),
		WithForceDescending(true))

	// DecayExpr only computes the decay factor (0~1) into an intermediate column,
	// then ScoreCombineExpr multiplies it with $score.
	decayScoreCol := "_decay_score"

	decayExpr, err := expr.NewDecayExpr(
		decayParams.function,
		decayParams.origin,
		decayParams.scale,
		decayParams.offset,
		decayParams.decay,
	)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("rerank_builder: %v", err)
	}

	fc.Map(decayExpr,
		[]string{inputField},
		[]string{decayScoreCol})

	combineExpr, err := expr.NewScoreCombineExpr(expr.ModeMultiply, nil)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("rerank_builder: %v", err)
	}

	fc.Map(combineExpr,
		[]string{types.ScoreFieldName, decayScoreCol},
		[]string{types.ScoreFieldName})

	return nil
}

type decayParams struct {
	function string
	origin   float64
	scale    float64
	offset   float64
	decay    float64
}

func parseDecayParams(funcSchema *schemapb.FunctionSchema) (MergeStrategy, bool, *decayParams, error) {
	params := &decayParams{
		offset: 0,
		decay:  defaultDecay,
	}
	scoreMode := defaultScoreMode
	normalize := false

	originSet := false
	scaleSet := false
	functionSet := false

	for _, param := range funcSchema.Params {
		switch strings.ToLower(param.Key) {
		case functionKey:
			params.function = strings.ToLower(param.Value)
			functionSet = true
		case originKey:
			if v, err := strconv.ParseFloat(param.Value, 64); err == nil {
				params.origin = v
				originSet = true
			} else {
				return "", false, nil, merr.WrapErrParameterInvalidMsg("decay param origin: %s is not a number", param.Value)
			}
		case scaleKey:
			if v, err := strconv.ParseFloat(param.Value, 64); err == nil {
				params.scale = v
				scaleSet = true
			} else {
				return "", false, nil, merr.WrapErrParameterInvalidMsg("decay param scale: %s is not a number", param.Value)
			}
		case offsetKey:
			if v, err := strconv.ParseFloat(param.Value, 64); err == nil {
				params.offset = v
			} else {
				return "", false, nil, merr.WrapErrParameterInvalidMsg("decay param offset: %s is not a number", param.Value)
			}
		case decayKey:
			if v, err := strconv.ParseFloat(param.Value, 64); err == nil {
				params.decay = v
			} else {
				return "", false, nil, merr.WrapErrParameterInvalidMsg("decay param decay: %s is not a number", param.Value)
			}
		case normScoreKey:
			if ns, err := strconv.ParseBool(param.Value); err == nil {
				normalize = ns
			}
		case scoreModeKey:
			scoreMode = strings.ToLower(param.Value)
		}
	}

	if !functionSet {
		return "", false, nil, merr.WrapErrParameterInvalidMsg("decay function not specified")
	}
	if !originSet {
		return "", false, nil, merr.WrapErrParameterInvalidMsg("decay origin not specified")
	}
	if !scaleSet {
		return "", false, nil, merr.WrapErrParameterInvalidMsg("decay scale not specified")
	}

	// Convert score_mode to MergeStrategy
	var strategy MergeStrategy
	switch scoreMode {
	case "max":
		strategy = MergeStrategyMax
	case "sum":
		strategy = MergeStrategySum
	case "avg":
		strategy = MergeStrategyAvg
	default:
		return "", false, nil, merr.WrapErrParameterInvalidMsg("unsupported score_mode: %s, only supports [max, sum, avg]", scoreMode)
	}

	return strategy, normalize, params, nil
}

// =============================================================================
// Model Builder
// =============================================================================

func buildModelChain(fc *FuncChain, collSchema *schemapb.CollectionSchema, funcSchema *schemapb.FunctionSchema, searchMetrics []string, searchParams *SearchParams) error {
	// Validate input field
	if len(funcSchema.InputFieldNames) != 1 {
		return merr.WrapErrParameterInvalidMsg("rerank_builder: model reranker requires exactly 1 input field, got %d", len(funcSchema.InputFieldNames))
	}
	inputField := funcSchema.InputFieldNames[0]

	if err := validateVarcharInputField(collSchema, inputField); err != nil {
		return err
	}

	// Parse queries from params
	queries, err := parseModelQueries(funcSchema)
	if err != nil {
		return err
	}

	// Create provider
	extraInfo := searchParams.ModelExtraInfo
	if extraInfo == nil {
		extraInfo = &models.ModelExtraInfo{}
	}
	provider, err := rerank.NewModelProvider(funcSchema.Params, extraInfo)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("rerank_builder: %v", err)
	}

	// Add Merge to combine and deduplicate results from multiple search paths.
	// No metricTypes or normalization needed: the model will overwrite all scores,
	// so any score processing here (normalization, direction conversion) is wasted work.
	fc.Merge(MergeStrategyMax)

	// Create and add RerankModelExpr
	modelExpr, err := expr.NewRerankModelExpr(provider, queries)
	if err != nil {
		return merr.WrapErrParameterInvalidMsg("rerank_builder: %v", err)
	}

	fc.Map(modelExpr,
		[]string{inputField},
		[]string{types.ScoreFieldName})

	return nil
}

func parseModelQueries(funcSchema *schemapb.FunctionSchema) ([]string, error) {
	for _, param := range funcSchema.Params {
		if param.Key == queryKeyName {
			var queries []string
			if err := json.Unmarshal([]byte(param.Value), &queries); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("rerank_builder: parse rerank params [queries] failed: %v", err)
			}
			if len(queries) == 0 {
				return nil, merr.WrapErrParameterInvalidMsg("rerank_builder: rerank queries must not be empty")
			}
			return queries, nil
		}
	}
	return nil, merr.WrapErrParameterInvalidMsg("rerank_builder: rerank function missing required param: queries")
}

func validateVarcharInputField(collSchema *schemapb.CollectionSchema, fieldName string) error {
	for _, field := range collSchema.Fields {
		if field.Name == fieldName {
			if field.DataType == schemapb.DataType_VarChar {
				return nil
			}
			return merr.WrapErrParameterInvalidMsg("rerank_builder: model input field %s must be VarChar, got %s", fieldName, field.DataType.String())
		}
	}
	return merr.WrapErrParameterInvalidMsg("rerank_builder: input field %s not found in collection schema", fieldName)
}

// GetInputFieldNamesFromFuncScore returns input field names from a FunctionScore schema.
// RRF/Weighted have no input fields; Decay has input fields from InputFieldNames.
func GetInputFieldNamesFromFuncScore(funcScore *schemapb.FunctionScore) []string {
	if funcScore == nil || len(funcScore.Functions) == 0 {
		return nil
	}
	funcSchema := funcScore.Functions[0]
	return funcSchema.GetInputFieldNames()
}

// GetInputFieldIDsFromSchema resolves input field IDs from the collection schema using the function score's input field names.
func GetInputFieldIDsFromSchema(collSchema *schemapb.CollectionSchema, funcScore *schemapb.FunctionScore) []int64 {
	names := GetInputFieldNamesFromFuncScore(funcScore)
	if len(names) == 0 {
		return nil
	}
	nameToID := make(map[string]int64, len(collSchema.GetFields()))
	for _, field := range collSchema.GetFields() {
		nameToID[field.GetName()] = field.GetFieldID()
	}
	ids := make([]int64, 0, len(names))
	for _, name := range names {
		if id, ok := nameToID[name]; ok {
			ids = append(ids, id)
		}
	}
	return ids
}

// GetRerankNameFromFuncScore returns the reranker name from a FunctionScore schema.
func GetRerankNameFromFuncScore(funcScore *schemapb.FunctionScore) string {
	if funcScore == nil || len(funcScore.Functions) == 0 {
		return ""
	}
	return rerank.GetRerankName(funcScore.Functions[0])
}

func validateInputField(collSchema *schemapb.CollectionSchema, fieldName string) error {
	for _, field := range collSchema.Fields {
		if field.Name == fieldName {
			// Check if field type is numeric.
			//
			// Timestamptz is intentionally excluded: although the legacy
			// rerank/decay code listed it in its type-dispatch switch, no
			// production path or test ever exercised it end-to-end (the
			// Arrow converter and GetNumericValue have no Timestamptz
			// support). Reject it here so the failure is reported at
			// validation time with a clear message rather than at runtime.
			switch field.DataType {
			case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
				schemapb.DataType_Int64, schemapb.DataType_Float, schemapb.DataType_Double:
				return nil
			default:
				return merr.WrapErrParameterInvalidMsg("rerank_builder: decay input field %s must be numeric, got %s", fieldName, field.DataType.String())
			}
		}
	}
	return merr.WrapErrParameterInvalidMsg("rerank_builder: input field %s not found in collection schema", fieldName)
}
