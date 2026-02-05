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
	"reflect"
	"strconv"
	"strings"

	"github.com/apache/arrow/go/v17/arrow/memory"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/expr"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
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
	rerankerKey     = "reranker"
	rrfKKey         = "k"
	weightsKey      = "weights"
	normScoreKey    = "norm_score"
	scoreModeKey    = "score_mode"
	functionKey     = "function"
	originKey       = "origin"
	scaleKey        = "scale"
	offsetKey       = "offset"
	decayKey        = "decay"

	// Legacy parameter keys
	legacyRankTypeKey   = "strategy"
	legacyRankParamsKey = "params"

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
		return nil, fmt.Errorf("rerank_builder: no rerank functions specified")
	}

	// Currently only support single rerank function
	if len(funcScoreSchema.Functions) > 1 {
		return nil, fmt.Errorf("rerank_builder: currently only supports one rerank function")
	}

	funcSchema := funcScoreSchema.Functions[0]
	return buildRerankChainInternal(collSchema, funcSchema, searchMetrics, searchParams, alloc)
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
		return nil, fmt.Errorf("rerank_builder: %w", err)
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
				return nil, fmt.Errorf("parse rerank params failed: %w", err)
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
			if reflect.ValueOf(v).CanFloat() {
				k := reflect.ValueOf(v).Float()
				fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: rrfKKey, Value: strconv.FormatFloat(k, 'f', -1, 64)})
			} else {
				return nil, fmt.Errorf("the type of rank param k should be float")
			}
		}
	case "weighted":
		fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: rerankerKey, Value: WeightedRerankerName})
		if v, ok := params[weightsKey]; ok {
			if d, err := json.Marshal(v); err != nil {
				return nil, fmt.Errorf("the weights param should be an array")
			} else {
				fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: weightsKey, Value: string(d)})
			}
		}
		if normScore, ok := params[normScoreKey]; ok {
			if ns, ok := normScore.(bool); ok {
				fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: normScoreKey, Value: strconv.FormatBool(ns)})
			}
		}
	default:
		return nil, fmt.Errorf("unsupported rank type %s", rankTypeStr)
	}

	return fSchema, nil
}

// buildRerankChainInternal builds a FuncChain from FunctionSchema.
func buildRerankChainInternal(
	collSchema *schemapb.CollectionSchema,
	funcSchema *schemapb.FunctionSchema,
	searchMetrics []string,
	searchParams *SearchParams,
	alloc memory.Allocator,
) (*FuncChain, error) {
	rerankerName := GetRerankName(funcSchema)
	if rerankerName == "" {
		return nil, fmt.Errorf("rerank_builder: reranker name not specified")
	}

	if alloc == nil {
		alloc = memory.DefaultAllocator
	}

	fc := NewFuncChainWithAllocator(alloc).
		SetStage(types.StageL2Rerank).
		SetName("rerank_chain")

	switch rerankerName {
	case RRFRerankerName:
		if err := buildRRFChain(fc, funcSchema, searchMetrics); err != nil {
			return nil, err
		}

	case WeightedRerankerName:
		if err := buildWeightedChain(fc, funcSchema, searchMetrics); err != nil {
			return nil, err
		}

	case DecayRerankerName:
		if err := buildDecayChain(fc, collSchema, funcSchema, searchMetrics); err != nil {
			return nil, err
		}

	case ModelRerankerName:
		return nil, fmt.Errorf("rerank_builder: model reranker not yet implemented")

	default:
		return nil, fmt.Errorf("rerank_builder: unsupported reranker %s", rerankerName)
	}

	// Add sort and limit
	fc.Sort(types.ScoreFieldName, true)
	if searchParams.Limit > 0 {
		fc.LimitWithOffset(searchParams.Limit, searchParams.Offset)
	}

	return fc, nil
}

// GetRerankName extracts the reranker name from FunctionSchema.
func GetRerankName(funcSchema *schemapb.FunctionSchema) string {
	for _, param := range funcSchema.Params {
		if strings.ToLower(param.Key) == rerankerKey {
			return strings.ToLower(param.Value)
		}
	}
	return ""
}

// =============================================================================
// RRF Builder
// =============================================================================

func buildRRFChain(fc *FuncChain, funcSchema *schemapb.FunctionSchema, searchMetrics []string) error {
	rrfK := parseRRFK(funcSchema)

	fc.Merge(MergeStrategyRRF,
		WithRRFK(rrfK),
		WithMetricTypes(searchMetrics),
		WithNormalize(true))

	return nil
}

func parseRRFK(funcSchema *schemapb.FunctionSchema) float64 {
	for _, param := range funcSchema.Params {
		if strings.ToLower(param.Key) == rrfKKey {
			if k, err := strconv.ParseFloat(param.Value, 64); err == nil {
				if k > 0 && k < 16384 {
					return k
				}
			}
		}
	}
	return defaultRRFK
}

// =============================================================================
// Weighted Builder
// =============================================================================

func buildWeightedChain(fc *FuncChain, funcSchema *schemapb.FunctionSchema, searchMetrics []string) error {
	weights, normalize := parseWeightedParams(funcSchema)

	if len(weights) == 0 {
		return fmt.Errorf("rerank_builder: weighted reranker requires weights parameter")
	}

	if len(weights) != len(searchMetrics) {
		return fmt.Errorf("rerank_builder: weights count %d != search count %d", len(weights), len(searchMetrics))
	}

	fc.Merge(MergeStrategyWeighted,
		WithWeights(weights),
		WithMetricTypes(searchMetrics),
		WithNormalize(normalize))

	return nil
}

func parseWeightedParams(funcSchema *schemapb.FunctionSchema) ([]float64, bool) {
	var weights []float64
	normalize := false

	for _, param := range funcSchema.Params {
		switch strings.ToLower(param.Key) {
		case weightsKey:
			var ws []float64
			if err := json.Unmarshal([]byte(param.Value), &ws); err == nil {
				weights = ws
			}
		case normScoreKey:
			if ns, err := strconv.ParseBool(param.Value); err == nil {
				normalize = ns
			}
		}
	}

	return weights, normalize
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
		return fmt.Errorf("rerank_builder: decay reranker requires exactly 1 input field, got %d", len(funcSchema.InputFieldNames))
	}
	inputField := funcSchema.InputFieldNames[0]

	// Validate input field exists in collection schema
	if err := validateInputField(collSchema, inputField); err != nil {
		return err
	}

	// Add MergeOp with score_mode strategy
	fc.Merge(strategy,
		WithMetricTypes(searchMetrics),
		WithNormalize(normalize))

	// Create and add DecayExpr
	decayExpr, err := expr.NewDecayExpr(
		decayParams.function,
		decayParams.origin,
		decayParams.scale,
		decayParams.offset,
		decayParams.decay,
	)
	if err != nil {
		return fmt.Errorf("rerank_builder: %w", err)
	}

	fc.Map(decayExpr,
		[]string{inputField, types.ScoreFieldName},
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
				return "", false, nil, fmt.Errorf("decay param origin: %s is not a number", param.Value)
			}
		case scaleKey:
			if v, err := strconv.ParseFloat(param.Value, 64); err == nil {
				params.scale = v
				scaleSet = true
			} else {
				return "", false, nil, fmt.Errorf("decay param scale: %s is not a number", param.Value)
			}
		case offsetKey:
			if v, err := strconv.ParseFloat(param.Value, 64); err == nil {
				params.offset = v
			} else {
				return "", false, nil, fmt.Errorf("decay param offset: %s is not a number", param.Value)
			}
		case decayKey:
			if v, err := strconv.ParseFloat(param.Value, 64); err == nil {
				params.decay = v
			} else {
				return "", false, nil, fmt.Errorf("decay param decay: %s is not a number", param.Value)
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
		return "", false, nil, fmt.Errorf("decay function not specified")
	}
	if !originSet {
		return "", false, nil, fmt.Errorf("decay origin not specified")
	}
	if !scaleSet {
		return "", false, nil, fmt.Errorf("decay scale not specified")
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
		return "", false, nil, fmt.Errorf("unsupported score_mode: %s, only supports [max, sum, avg]", scoreMode)
	}

	return strategy, normalize, params, nil
}

func validateInputField(collSchema *schemapb.CollectionSchema, fieldName string) error {
	for _, field := range collSchema.Fields {
		if field.Name == fieldName {
			// Check if field type is numeric
			switch field.DataType {
			case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32,
				schemapb.DataType_Int64, schemapb.DataType_Float, schemapb.DataType_Double,
				schemapb.DataType_Timestamptz:
				return nil
			default:
				return fmt.Errorf("rerank_builder: decay input field %s must be numeric, got %s", fieldName, field.DataType.String())
			}
		}
	}
	return fmt.Errorf("rerank_builder: input field %s not found in collection schema", fieldName)
}
