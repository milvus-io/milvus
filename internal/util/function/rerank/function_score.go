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
	"reflect"
	"strconv"
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

const (
	DecayFunctionName string = "decay"
	ModelFunctionName string = "model"
	RRFName           string = "rrf"
	WeightedName      string = "weighted"
)

const (
	maxScorer string = "max"
	sumScorer string = "sum"
	avgScorer string = "avg"
)

// legacy rrf/weighted rerank configs

const (
	legacyRankTypeKey   = "strategy"
	legacyRankParamsKey = "params"
)

type rankType int

const (
	invalidRankType  rankType = iota // invalidRankType   = 0
	rrfRankType                      // rrfRankType = 1
	weightedRankType                 // weightedRankType = 2
)

// segment scorer configs
const (
	BoostName = "boost"
	FilterKey = "filter"
	WeightKey = "weight"
)

var rankTypeMap = map[string]rankType{
	"invalid":  invalidRankType,
	"rrf":      rrfRankType,
	"weighted": weightedRankType,
}

type SearchParams struct {
	nq           int64
	limit        int64
	offset       int64
	roundDecimal int64

	groupByFieldId  int64
	groupSize       int64
	strictGroupSize bool
	groupScore      string

	searchMetrics []string
}

func (s *SearchParams) isGrouping() bool {
	return s.groupByFieldId > 0
}

func NewSearchParams(nq, limit, offset, roundDecimal, groupByFieldId, groupSize int64, strictGroupSize bool, groupScore string, searchMetrics []string) *SearchParams {
	if groupScore == "" {
		groupScore = maxScorer
	}
	return &SearchParams{
		nq, limit, offset, roundDecimal, groupByFieldId, groupSize, strictGroupSize, groupScore, searchMetrics,
	}
}

type Reranker interface {
	Process(ctx context.Context, searchParams *SearchParams, inputs *rerankInputs) (*rerankOutputs, error)
	IsSupportGroup() bool
	GetInputFieldNames() []string
	GetInputFieldIDs() []int64
	GetRankName() string
}

func GetRerankName(funcSchema *schemapb.FunctionSchema) string {
	for _, param := range funcSchema.Params {
		switch strings.ToLower(param.Key) {
		case reranker:
			return strings.ToLower(param.Value)
		default:
		}
	}
	return ""
}

// Currently only supports single rerank
type FunctionScore struct {
	reranker Reranker
}

func createFunction(collSchema *schemapb.CollectionSchema, funcSchema *schemapb.FunctionSchema) (Reranker, error) {
	if funcSchema.GetType() != schemapb.FunctionType_Rerank {
		return nil, fmt.Errorf("%s is not rerank function.", funcSchema.GetType().String())
	}
	if len(funcSchema.GetOutputFieldNames()) != 0 {
		return nil, fmt.Errorf("Rerank function should not have output field, but now is %d", len(funcSchema.GetOutputFieldNames()))
	}

	rerankerName := GetRerankName(funcSchema)
	var rerankFunc Reranker
	var newRerankErr error
	switch rerankerName {
	case DecayFunctionName:
		rerankFunc, newRerankErr = newDecayFunction(collSchema, funcSchema)
	case ModelFunctionName:
		rerankFunc, newRerankErr = newModelFunction(collSchema, funcSchema)
	case RRFName:
		rerankFunc, newRerankErr = newRRFFunction(collSchema, funcSchema)
	case WeightedName:
		rerankFunc, newRerankErr = newWeightedFunction(collSchema, funcSchema)
	case BoostName:
		return nil, nil
	default:
		return nil, fmt.Errorf("Unsupported rerank function: [%s] , list of supported [%s,%s,%s,%s]", rerankerName, DecayFunctionName, ModelFunctionName, RRFName, WeightedName)
	}

	if newRerankErr != nil {
		return nil, newRerankErr
	}
	return rerankFunc, nil
}

func NewFunctionScore(collSchema *schemapb.CollectionSchema, funcScoreSchema *schemapb.FunctionScore) (*FunctionScore, error) {
	funcScore := &FunctionScore{}

	for _, function := range funcScoreSchema.Functions {
		reranker, err := createFunction(collSchema, function)
		if err != nil {
			return nil, err
		}

		if reranker != nil {
			if funcScore.reranker == nil {
				funcScore.reranker = reranker
			} else {
				// now only support only use one proxy rerank
				return nil, fmt.Errorf("Currently only supports one rerank")
			}
		}
	}

	if funcScore.reranker != nil {
		return funcScore, nil
	}
	return nil, nil
}

func NewFunctionScoreWithlegacy(collSchema *schemapb.CollectionSchema, rankParams []*commonpb.KeyValuePair) (*FunctionScore, error) {
	var params map[string]interface{}
	rankTypeStr, err := funcutil.GetAttrByKeyFromRepeatedKV(legacyRankTypeKey, rankParams)
	if err != nil {
		rankTypeStr = "rrf"
		params = make(map[string]interface{}, 0)
	} else {
		if _, ok := rankTypeMap[rankTypeStr]; !ok {
			return nil, fmt.Errorf("unsupported rank type %s", rankTypeStr)
		}
		paramStr, err := funcutil.GetAttrByKeyFromRepeatedKV(legacyRankParamsKey, rankParams)
		if err != nil {
			return nil, fmt.Errorf("params" + " not found in rank_params")
		}
		err = json.Unmarshal([]byte(paramStr), &params)
		if err != nil {
			return nil, fmt.Errorf("Parse rerank params failed, err: %s", err)
		}
	}
	fSchema := schemapb.FunctionSchema{
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{},
		OutputFieldNames: []string{},
		Params:           []*commonpb.KeyValuePair{},
	}
	switch rankTypeMap[rankTypeStr] {
	case rrfRankType:
		fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: reranker, Value: RRFName})
		if v, ok := params[RRFParamsKey]; ok {
			if reflect.ValueOf(params[RRFParamsKey]).CanFloat() {
				k := reflect.ValueOf(v).Float()
				fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: RRFParamsKey, Value: strconv.FormatFloat(k, 'f', -1, 64)})
			} else {
				return nil, fmt.Errorf("The type of rank param k should be float")
			}
		}
	case weightedRankType:
		fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: reranker, Value: WeightedName})
		if v, ok := params[WeightsParamsKey]; ok {
			if d, err := json.Marshal(v); err != nil {
				return nil, fmt.Errorf("The weights param should be an array")
			} else {
				fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: WeightsParamsKey, Value: string(d)})
			}
		}
		if normScore, ok := params[NormScoreKey]; ok {
			if ns, ok := normScore.(bool); ok {
				fSchema.Params = append(fSchema.Params, &commonpb.KeyValuePair{Key: NormScoreKey, Value: strconv.FormatBool(ns)})
			} else {
				return nil, fmt.Errorf("Weighted rerank err, norm_score should been bool type, but [norm_score:%s]'s type is %T", normScore, normScore)
			}
		}
	default:
		return nil, fmt.Errorf("unsupported rank type %s", rankTypeStr)
	}
	funcScore := &FunctionScore{}
	if funcScore.reranker, err = createFunction(collSchema, &fSchema); err != nil {
		return nil, err
	}
	return funcScore, nil
}

func (fScore *FunctionScore) Process(ctx context.Context, searchParams *SearchParams, multipleMilvusResults []*milvuspb.SearchResults) (*milvuspb.SearchResults, error) {
	if len(multipleMilvusResults) == 0 {
		return &milvuspb.SearchResults{
			Status: merr.Success(),
			Results: &schemapb.SearchResultData{
				NumQueries: searchParams.nq,
				TopK:       searchParams.limit,
				FieldsData: make([]*schemapb.FieldData, 0),
				Scores:     []float32{},
				Ids:        &schemapb.IDs{},
				Topks:      make([]int64, searchParams.nq),
			},
		}, nil
	}

	allSearchResultData := lo.FilterMap(multipleMilvusResults, func(m *milvuspb.SearchResults, _ int) (*schemapb.SearchResultData, bool) {
		return m.Results, true
	})

	// rankResult only has scores
	inputs, err := newRerankInputs(allSearchResultData, fScore.reranker.GetInputFieldIDs(), searchParams.isGrouping())
	if err != nil {
		return nil, err
	}
	rankResult, err := fScore.reranker.Process(ctx, searchParams, inputs)
	if err != nil {
		return nil, err
	}

	ret := &milvuspb.SearchResults{
		Status:  merr.Success(),
		Results: rankResult.searchResultData,
	}
	return ret, nil
}

func (fScore *FunctionScore) GetAllInputFieldNames() []string {
	if fScore == nil {
		return []string{}
	}
	return fScore.reranker.GetInputFieldNames()
}

func (fScore *FunctionScore) GetAllInputFieldIDs() []int64 {
	if fScore == nil {
		return []int64{}
	}
	return fScore.reranker.GetInputFieldIDs()
}

func (fScore *FunctionScore) IsSupportGroup() bool {
	if fScore == nil {
		return true
	}
	return fScore.reranker.IsSupportGroup()
}

func (fScore *FunctionScore) RerankName() string {
	if fScore == nil {
		return ""
	}
	return fScore.reranker.GetRankName()
}
