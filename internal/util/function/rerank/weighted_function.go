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
	"math"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
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
	for i, col := range cols {
		if col.size == 0 {
			continue
		}
		normFunc := getNormalizeFunc(weighted.needNorm, searchParams.searchMetrics[i])
		ids := col.ids.([]T)
		for j, id := range ids {
			if score, ok := weightedScores[id]; !ok {
				weightedScores[id] = weighted.weight[i] * normFunc(col.scores[j])
			} else {
				weightedScores[id] = score + weighted.weight[i]*normFunc(col.scores[j])
			}
		}
	}
	if searchParams.isGrouping() {
		return newGroupingIDScores(weightedScores, searchParams, idGroup)
	}
	return newIDScores(weightedScores, searchParams), nil
}

func (weighted *WeightedFunction[T]) Process(ctx context.Context, searchParams *SearchParams, inputs *rerankInputs) (*rerankOutputs, error) {
	outputs := newRerankOutputs(searchParams)
	for _, cols := range inputs.data {
		for i, col := range cols {
			metricType := searchParams.searchMetrics[i]
			for j, score := range col.scores {
				col.scores[j] = toGreaterScore(score, metricType)
			}
		}
		idScore, err := weighted.processOneSearchData(ctx, searchParams, cols, inputs.idGroupValue)
		if err != nil {
			return nil, err
		}
		appendResult(outputs, idScore.ids, idScore.scores)
	}
	return outputs, nil
}

type normalizeFunc func(float32) float32

func getNormalizeFunc(normScore bool, metrics string) normalizeFunc {
	if !normScore {
		return func(distance float32) float32 {
			return distance
		}
	}
	switch metrics {
	case metric.COSINE:
		return func(distance float32) float32 {
			return (1 + distance) * 0.5
		}
	case metric.IP:
		return func(distance float32) float32 {
			return 0.5 + float32(math.Atan(float64(distance)))/math.Pi
		}
	case metric.BM25:
		return func(distance float32) float32 {
			return 2 * float32(math.Atan(float64(distance))) / math.Pi
		}
	default:
		return func(distance float32) float32 {
			return 1.0 - 2*float32(math.Atan(float64(distance)))/math.Pi
		}
	}
}
