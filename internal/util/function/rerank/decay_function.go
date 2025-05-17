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
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/metric"
)

const (
	originKey   string = "origin"
	scaleKey    string = "scale"
	offsetKey   string = "offset"
	decayKey    string = "decay"
	functionKey string = "function"
)

const (
	gaussFunction  string = "gauss"
	linearFunction string = "linear"
	expFunction    string = "exp"
)

type DecayFunction[T PKType, R int32 | int64 | float32 | float64] struct {
	RerankBase

	functionName string
	origin       float64
	scale        float64
	offset       float64
	decay        float64
	reScorer     decayReScorer
}

func newDecayFunction(collSchema *schemapb.CollectionSchema, funcSchema *schemapb.FunctionSchema) (Reranker, error) {
	pkType := schemapb.DataType_None
	for _, field := range collSchema.Fields {
		if field.IsPrimaryKey {
			pkType = field.DataType
		}
	}

	if pkType == schemapb.DataType_None {
		return nil, fmt.Errorf("Collection %s can not found pk field", collSchema.Name)
	}

	base, err := newRerankBase(collSchema, funcSchema, decayFunctionName, false, pkType)
	if err != nil {
		return nil, err
	}

	if len(base.GetInputFieldNames()) != 1 {
		return nil, fmt.Errorf("Decay function only supoorts single input, but gets [%s] input", base.GetInputFieldNames())
	}

	var inputType schemapb.DataType
	for _, field := range collSchema.Fields {
		if field.Name == base.GetInputFieldNames()[0] {
			inputType = field.DataType
		}
	}

	if pkType == schemapb.DataType_Int64 {
		switch inputType {
		case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
			return newFunction[int64, int32](base, funcSchema)
		case schemapb.DataType_Int64:
			return newFunction[int64, int64](base, funcSchema)
		case schemapb.DataType_Float:
			return newFunction[int64, float32](base, funcSchema)
		case schemapb.DataType_Double:
			return newFunction[int64, float64](base, funcSchema)
		default:
			return nil, fmt.Errorf("Decay rerank: unsupported input field type:%s, only support numberic field", inputType.String())
		}
	} else {
		switch inputType {
		case schemapb.DataType_Int8, schemapb.DataType_Int16, schemapb.DataType_Int32:
			return newFunction[string, int32](base, funcSchema)
		case schemapb.DataType_Int64:
			return newFunction[string, int64](base, funcSchema)
		case schemapb.DataType_Float:
			return newFunction[string, float32](base, funcSchema)
		case schemapb.DataType_Double:
			return newFunction[string, float64](base, funcSchema)
		default:
			return nil, fmt.Errorf("Decay rerank: unsupported input field type:%s, only support numberic field", inputType.String())
		}
	}
}

// T: PK Type, R: field type
func newFunction[T PKType, R int32 | int64 | float32 | float64](base *RerankBase, funcSchema *schemapb.FunctionSchema) (Reranker, error) {
	var err error
	decayFunc := &DecayFunction[T, R]{RerankBase: *base, offset: 0, decay: 0.5}
	orginInit := false
	scaleInit := false
	for _, param := range funcSchema.Params {
		switch strings.ToLower(param.Key) {
		case functionKey:
			decayFunc.functionName = param.Value
		case originKey:
			if decayFunc.origin, err = strconv.ParseFloat(param.Value, 64); err != nil {
				return nil, fmt.Errorf("Param origin:%s is not a number", param.Value)
			}
			orginInit = true
		case scaleKey:
			if decayFunc.scale, err = strconv.ParseFloat(param.Value, 64); err != nil {
				return nil, fmt.Errorf("Param scale:%s is not a number", param.Value)
			}
			scaleInit = true
		case offsetKey:
			if decayFunc.offset, err = strconv.ParseFloat(param.Value, 64); err != nil {
				return nil, fmt.Errorf("Param offset:%s is not a number", param.Value)
			}
		case decayKey:
			if decayFunc.decay, err = strconv.ParseFloat(param.Value, 64); err != nil {
				return nil, fmt.Errorf("Param decay:%s is not a number", param.Value)
			}
		default:
		}
	}

	if !orginInit {
		return nil, fmt.Errorf("Decay function lost param: origin")
	}

	if !scaleInit {
		return nil, fmt.Errorf("Decay function lost param: scale")
	}

	if decayFunc.scale <= 0 {
		return nil, fmt.Errorf("Decay function param: scale must > 0, but got %f", decayFunc.scale)
	}

	if decayFunc.offset < 0 {
		return nil, fmt.Errorf("Decay function param: offset must >= 0, but got %f", decayFunc.offset)
	}

	if decayFunc.decay <= 0 || decayFunc.decay >= 1 {
		return nil, fmt.Errorf("Decay function param: decay must 0 < decay < 1, but got %f", decayFunc.offset)
	}

	switch decayFunc.functionName {
	case gaussFunction:
		decayFunc.reScorer = gaussianDecay
	case expFunction:
		decayFunc.reScorer = expDecay
	case linearFunction:
		decayFunc.reScorer = linearDecay
	default:
		return nil, fmt.Errorf("Invaild decay function: %s, only support [%s,%s,%s]", decayFunctionName, gaussFunction, linearFunction, expFunction)
	}
	return decayFunc, nil
}

func toGreaterScore(score float32, metricType string) float32 {
	switch strings.ToUpper(metricType) {
	case metric.COSINE, metric.IP, metric.BM25:
		return score
	default:
		return 1.0 - 2*float32(math.Atan(float64(score)))/math.Pi
	}
}

func (decay *DecayFunction[T, R]) processOneSearchData(ctx context.Context, searchParams *SearchParams, cols []*columns) *IDScores[T] {
	srcScores := maxMerge[T](cols)
	decayScores := map[T]float32{}
	for _, col := range cols {
		if col.size == 0 {
			continue
		}
		nums := col.data[0].([]R)
		ids := col.ids.([]T)
		for idx, id := range ids {
			if _, ok := decayScores[id]; !ok {
				decayScores[id] = float32(decay.reScorer(decay.origin, decay.scale, decay.decay, decay.offset, float64(nums[idx])))
			}
		}
	}
	for id := range decayScores {
		decayScores[id] = decayScores[id] * srcScores[id]
	}
	return newIDScores(decayScores, searchParams)
}

func (decay *DecayFunction[T, R]) Process(ctx context.Context, searchParams *SearchParams, inputs *rerankInputs) (*rerankOutputs, error) {
	outputs := newRerankOutputs(searchParams)
	for _, cols := range inputs.data {
		for i, col := range cols {
			metricType := searchParams.searchMetrics[i]
			for j, score := range col.scores {
				col.scores[j] = toGreaterScore(score, metricType)
			}
		}
		idScore := decay.processOneSearchData(ctx, searchParams, cols)
		appendResult(outputs, idScore.ids, idScore.scores)
	}
	return outputs, nil
}

type decayReScorer func(float64, float64, float64, float64, float64) float64

func gaussianDecay(origin, scale, decay, offset, distance float64) float64 {
	adjustedDist := math.Max(0, math.Abs(distance-origin)-offset)
	sigmaSquare := math.Pow(scale, 2.0) / math.Log(decay)
	exponent := math.Pow(adjustedDist, 2.0) / sigmaSquare
	return math.Exp(exponent)
}

func expDecay(origin, scale, decay, offset, distance float64) float64 {
	adjustedDist := math.Max(0, math.Abs(distance-origin)-offset)
	lambda := math.Log(decay) / scale
	return math.Exp(lambda * adjustedDist)
}

func linearDecay(origin, scale, decay, offset, distance float64) float64 {
	adjustedDist := math.Max(0, math.Abs(distance-origin)-offset)
	slope := (1 - decay) / scale
	return math.Max(decay, 1-slope*adjustedDist)
}
