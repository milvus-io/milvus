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

package rerank

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

const (
	WeightsParamsKey string = "weights"
	NormScoreKey     string = "norm_score"
)

type WeightedFunction[T PKType] struct {
	RerankBase

	weight   []float32
	needNorm bool
}

func newWeightedFunction(collSchema *schemapb.CollectionSchema, funcSchema *schemapb.FunctionSchema) (Reranker, error) {
	base, err := newRerankBase(collSchema, funcSchema, WeightedName, true)
	if err != nil {
		return nil, err
	}

	if len(base.GetInputFieldNames()) != 0 {
		return nil, fmt.Errorf("The weighted function does not support input parameters, but got %s", base.GetInputFieldNames())
	}

	var weights []float32
	needNorm := false
	for _, param := range funcSchema.Params {
		switch strings.ToLower(param.Key) {
		case WeightsParamsKey:
			if err := json.Unmarshal([]byte(param.Value), &weights); err != nil {
				return nil, fmt.Errorf("Parse %s param failed, weight should be []float, bug got: %s", WeightsParamsKey, param.Value)
			}
			for _, weight := range weights {
				if weight < 0 || weight > 1 {
					return nil, fmt.Errorf("rank param weight should be in range [0, 1]")
				}
			}
		case NormScoreKey:
			if needNorm, err = strconv.ParseBool(param.Value); err != nil {
				return nil, fmt.Errorf("%s params must be true/false, bug got %s", NormScoreKey, param.Value)
			}
		}
	}
	if len(weights) == 0 {
		return nil, fmt.Errorf(WeightsParamsKey + " not found")
	}
	if base.pkType == schemapb.DataType_Int64 {
		return &WeightedFunction[int64]{RerankBase: *base, weight: weights, needNorm: needNorm}, nil
	} else {
		return &WeightedFunction[string]{RerankBase: *base, weight: weights, needNorm: needNorm}, nil
	}
}

func (weighted *WeightedFunction[T]) processOneSearchData(ctx context.Context, searchParams *SearchParams, cols []*columns, idGroup map[any]any) (*IDScores[T], error) {
	if len(cols) != len(weighted.weight) {
		return nil, merr.WrapErrParameterInvalid(fmt.Sprint(len(cols)), fmt.Sprint(len(weighted.weight)), "the length of weights param mismatch with ann search requests")
	}
	weightedScores := map[T]float32{}
	isMixd, descendingOrder := classifyMetricsOrder(searchParams.searchMetrics)
	idLocations := make(map[T]IDLoc)
	for i, col := range cols {
		if col.size == 0 {
			continue
		}
		// If it is a mixed metric (L2 + IP), with both large to small sorting and small to large sorting,
		// force the small to large sorting scores to be converted to large to small sorting
		normFunc := getNormalizeFunc(weighted.needNorm, searchParams.searchMetrics[i], isMixd)
		ids := col.ids.([]T)
		for j, id := range ids {
			if score, ok := weightedScores[id]; !ok {
				idLocations[id] = IDLoc{batchIdx: i, offset: j + int(col.nqOffset)}
				weightedScores[id] = weighted.weight[i] * normFunc(col.scores[j])
			} else {
				weightedScores[id] = score + weighted.weight[i]*normFunc(col.scores[j])
			}
		}
	}
	if searchParams.isGrouping() {
		return newGroupingIDScores(weightedScores, idLocations, searchParams, idGroup)
	}
	// If normlize is set, the final result is sorted from largest to smallest, otherwise it depends on descendingOrder
	return newIDScores(weightedScores, idLocations, searchParams, weighted.needNorm || descendingOrder), nil
}

func (weighted *WeightedFunction[T]) Process(ctx context.Context, searchParams *SearchParams, inputs *rerankInputs) (*rerankOutputs, error) {
	outputs := newRerankOutputs(inputs, searchParams)
	for _, cols := range inputs.data {
		idScore, err := weighted.processOneSearchData(ctx, searchParams, cols, inputs.idGroupValue)
		if err != nil {
			return nil, err
		}
		appendResult(inputs, outputs, idScore)
	}
	return outputs, nil
}
