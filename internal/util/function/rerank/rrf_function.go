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
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

const (
	RRFParamsKey string = "k"

	defaultRRFParamsValue float64 = 60
)

type RRFFunction[T PKType] struct {
	RerankBase

	k          float32
	weights    []float64
	weightsSet bool
}

func newRRFFunction(collSchema *schemapb.CollectionSchema, funcSchema *schemapb.FunctionSchema, pkTypeOverride ...schemapb.DataType) (Reranker, error) {
	base, err := newRerankBase(collSchema, funcSchema, RRFName, true, pkTypeOverride...)
	if err != nil {
		return nil, err
	}

	if len(base.GetInputFieldNames()) != 0 {
		return nil, merr.WrapErrParameterInvalidMsg("the rrf function does not support input parameters, but got %s", base.GetInputFieldNames())
	}

	k := float64(defaultRRFParamsValue)
	var weights []float64
	weightsSet := false
	for _, param := range funcSchema.Params {
		switch strings.ToLower(param.Key) {
		case RRFParamsKey:
			if k, err = strconv.ParseFloat(param.Value, 64); err != nil {
				return nil, merr.WrapErrParameterInvalidMsg("param k:%s is not a number", param.Value)
			}
		case WeightsParamsKey:
			weights, err = parseRRFWeights(param.Value)
			if err != nil {
				return nil, err
			}
			weightsSet = true
		}
	}
	if k <= 0 || k >= 16384 {
		return nil, merr.WrapErrParameterInvalidMsg("the rank params k should be in range (0, %d)", 16384)
	}
	if base.pkType == schemapb.DataType_Int64 {
		return &RRFFunction[int64]{RerankBase: *base, k: float32(k), weights: weights, weightsSet: weightsSet}, nil
	} else {
		return &RRFFunction[string]{RerankBase: *base, k: float32(k), weights: weights, weightsSet: weightsSet}, nil
	}
}

func parseRRFWeights(value string) ([]float64, error) {
	var rawWeights []json.RawMessage
	if err := json.Unmarshal([]byte(value), &rawWeights); err != nil {
		return nil, merr.WrapErrParameterInvalidMsg("failed to parse weights: %v", err)
	}
	if len(rawWeights) == 0 {
		return nil, merr.WrapErrParameterInvalidMsg("rrf weights parameter must be a non-empty array")
	}

	weights := make([]float64, len(rawWeights))
	for index, rawWeight := range rawWeights {
		if string(rawWeight) == "null" {
			return nil, merr.WrapErrParameterInvalidMsg("failed to parse weights: weight at index %d must be a number", index)
		}
		if err := json.Unmarshal(rawWeight, &weights[index]); err != nil {
			return nil, merr.WrapErrParameterInvalidMsg("failed to parse weights: weight at index %d must be a number", index)
		}
		if weights[index] < 0 || weights[index] > 1 {
			return nil, merr.WrapErrParameterInvalidMsg("rank param weight should be in range [0, 1]")
		}
	}
	return weights, nil
}

func (rrf *RRFFunction[T]) processOneSearchData(ctx context.Context, searchParams *SearchParams, cols []*columns, idGroup map[any]any) (*IDScores[T], error) {
	if rrf.weightsSet && len(rrf.weights) != len(cols) {
		return nil, merr.WrapErrParameterInvalidMsg(
			"the length of weights param mismatch with ann search requests: got %d, want %d",
			len(rrf.weights), len(cols))
	}

	rrfScores := map[T]float32{}
	idLocations := make(map[T]IDLoc)
	for i, col := range cols {
		if col.size == 0 {
			continue
		}
		ids := col.ids.([]T)
		pathWeight := 1.0
		if rrf.weightsSet {
			pathWeight = rrf.weights[i]
		}
		for idx, id := range ids {
			rrfScore := 1 / (rrf.k + float32(idx+1))
			if rrf.weightsSet {
				rrfScore = float32(pathWeight / float64(rrf.k+float32(idx+1)))
			}
			if score, ok := rrfScores[id]; !ok {
				idLocations[id] = IDLoc{batchIdx: i, offset: idx + int(col.nqOffset)}
				rrfScores[id] = rrfScore
			} else {
				rrfScores[id] = score + rrfScore
			}
		}
	}
	if searchParams.isGrouping() {
		return newGroupingIDScores(rrfScores, idLocations, searchParams, idGroup)
	}
	return newIDScores(rrfScores, idLocations, searchParams, true), nil
}

func (rrf *RRFFunction[T]) Process(ctx context.Context, searchParams *SearchParams, inputs *rerankInputs) (*rerankOutputs, error) {
	outputs := newRerankOutputs(inputs, searchParams)
	for _, cols := range inputs.data {
		idScore, err := rrf.processOneSearchData(ctx, searchParams, cols, inputs.idGroupValue)
		if err != nil {
			return nil, err
		}
		appendResult(inputs, outputs, idScore)
	}
	return outputs, nil
}
