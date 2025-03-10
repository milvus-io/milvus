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
	"sort"
	"strconv"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	originKey   string = "origin"
	scaleKey    string = "scale"
	offsetKey   string = "offset"
	decayKey    string = "decay"
	functionKey string = "function"
)

const (
	gaussFunction string = "gauss"
	linerFunction string = "liner"
	expFunction   string = "exp"
)

type DecayFunction[T int64 | string, R int32 | int64 | float32 | float64] struct {
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
func newFunction[T int64 | string, R int32 | int64 | float32 | float64](base *RerankBase, funcSchema *schemapb.FunctionSchema) (Reranker, error) {
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
		return nil, fmt.Errorf("Decay function param: offset must => 0, but got %f", decayFunc.offset)
	}

	if decayFunc.decay <= 0 || decayFunc.decay >= 1 {
		return nil, fmt.Errorf("Decay function param: decay must 0 < decay < 1 0, but got %f", decayFunc.offset)
	}

	switch decayFunc.functionName {
	case gaussFunction:
		decayFunc.reScorer = gaussianDecay
	case expFunction:
		decayFunc.reScorer = expDecay
	case linerFunction:
		decayFunc.reScorer = linearDecay
	default:
		return nil, fmt.Errorf("Invaild decay function: %s, only support [%s,%s,%s]", decayFunctionName, gaussFunction, linerFunction, expFunction)
	}

	return decayFunc, nil
}

func (decay *DecayFunction[T, R]) reScore(multipSearchResultData []*schemapb.SearchResultData) (*idSocres[T], error) {
	newScores := newIdScores[T]()
	for _, data := range multipSearchResultData {
		var inputField *schemapb.FieldData
		for _, field := range data.FieldsData {
			if field.FieldId == decay.GetInputFieldIDs()[0] {
				inputField = field
			}
		}
		if inputField == nil {
			return nil, fmt.Errorf("Rerank decay function can not find input field, name: %s", decay.GetInputFieldNames()[0])
		}
		var inputValues *numberField[R]
		if tmp, err := getNumberic(inputField); err != nil {
			return nil, err
		} else {
			inputValues = tmp.(*numberField[R])
		}

		ids := newMilvusIDs(data.Ids, decay.pkType).(milvusIDs[T])
		for idx, id := range ids.data {
			if !newScores.exist(id) {
				if v, err := inputValues.GetFloat64(idx); err != nil {
					return nil, err
				} else {
					newScores.set(id, float32(decay.reScorer(decay.origin, decay.scale, decay.decay, decay.offset, v)))
				}
			}
		}
	}
	return newScores, nil
}

func (decay *DecayFunction[T, R]) orgnizeNqScores(searchParams *SearchParams, multipSearchResultData []*schemapb.SearchResultData, idScoreData *idSocres[T]) []map[T]float32 {
	nqScores := make([]map[T]float32, searchParams.nq)
	for i := int64(0); i < searchParams.nq; i++ {
		nqScores[i] = make(map[T]float32)
	}

	for _, data := range multipSearchResultData {
		start := int64(0)
		for nqIdx := int64(0); nqIdx < searchParams.nq; nqIdx++ {
			realTopk := data.Topks[nqIdx]
			for j := start; j < start+realTopk; j++ {
				id := typeutil.GetPK(data.GetIds(), j).(T)
				if _, exists := nqScores[nqIdx][id]; !exists {
					nqScores[nqIdx][id] = idScoreData.get(id)
				}
			}
			start += realTopk
		}
	}
	return nqScores
}

func (decay *DecayFunction[T, R]) Process(ctx context.Context, searchParams *SearchParams, multipSearchResultData []*schemapb.SearchResultData) (*schemapb.SearchResultData, error) {
	ret := &schemapb.SearchResultData{
		NumQueries: searchParams.nq,
		TopK:       searchParams.limit,
		FieldsData: make([]*schemapb.FieldData, 0),
		Scores:     []float32{},
		Ids:        &schemapb.IDs{},
		Topks:      []int64{},
	}
	multipSearchResultData = lo.Filter(multipSearchResultData, func(searchResult *schemapb.SearchResultData, i int) bool {
		return len(searchResult.FieldsData) != 0
	})

	if len(multipSearchResultData) == 0 {
		return ret, nil
	}
	idScoreData, err := decay.reScore(multipSearchResultData)
	if err != nil {
		return nil, err
	}

	nqScores := decay.orgnizeNqScores(searchParams, multipSearchResultData, idScoreData)
	topk := searchParams.limit + searchParams.offset
	for i := int64(0); i < searchParams.nq; i++ {
		idScoreMap := nqScores[i]
		ids := make([]T, 0)
		for id := range idScoreMap {
			ids = append(ids, id)
		}

		big := func(i, j int) bool {
			if idScoreMap[ids[i]] == idScoreMap[ids[j]] {
				return ids[i] < ids[j]
			}
			return idScoreMap[ids[i]] > idScoreMap[ids[j]]
		}
		sort.Slice(ids, big)

		if int64(len(ids)) > topk {
			ids = ids[:topk]
		}

		// set real topk
		ret.Topks = append(ret.Topks, max(0, int64(len(ids))-searchParams.offset))
		// append id and score
		for index := searchParams.offset; index < int64(len(ids)); index++ {
			typeutil.AppendPKs(ret.Ids, ids[index])
			score := idScoreMap[ids[index]]
			if searchParams.roundDecimal != -1 {
				multiplier := math.Pow(10.0, float64(searchParams.roundDecimal))
				score = float32(math.Floor(float64(score)*multiplier+0.5) / multiplier)
			}
			ret.Scores = append(ret.Scores, score)
		}
	}

	return ret, nil
}

type decayReScorer func(float64, float64, float64, float64, float64) float64

func gaussianDecay(origin, scale, decay, offset, distance float64) float64 {
	adjustedDist := math.Max(0, math.Abs(distance-origin)-offset)
	sigmaSquare := 0.5 * math.Pow(scale, 2.0) / math.Log(decay)
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
