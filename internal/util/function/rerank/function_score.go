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
	"strings"

	"github.com/samber/lo"

	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

const (
	decayFunctionName string = "decay"
)

type SearchParams struct {
	nq           int64
	limit        int64
	offset       int64
	roundDecimal int64

	// TODO: supports group search
	groupByFieldId  int64
	groupSize       int64
	strictGroupSize bool

	searchMetrics []string
}

func NewSearchParams(nq, limit, offset, roundDecimal, groupByFieldId, groupSize int64, strictGroupSize bool, searchMetrics []string) *SearchParams {
	return &SearchParams{
		nq, limit, offset, roundDecimal, groupByFieldId, groupSize, strictGroupSize, searchMetrics,
	}
}

type Reranker interface {
	Process(ctx context.Context, searchParams *SearchParams, inputs *rerankInputs) (*rerankOutputs, error)
	IsSupportGroup() bool
	GetInputFieldNames() []string
	GetInputFieldIDs() []int64
	GetRankName() string
}

func getRerankName(funcSchema *schemapb.FunctionSchema) string {
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

	rerankerName := getRerankName(funcSchema)
	var rerankFunc Reranker
	var newRerankErr error
	switch rerankerName {
	case decayFunctionName:
		rerankFunc, newRerankErr = newDecayFunction(collSchema, funcSchema)
	default:
		return nil, fmt.Errorf("Unsupported rerank function: [%s] , list of supported [%s]", rerankerName, decayFunctionName)
	}

	if newRerankErr != nil {
		return nil, newRerankErr
	}
	return rerankFunc, nil
}

func NewFunctionScore(collSchema *schemapb.CollectionSchema, funcScoreSchema *schemapb.FunctionScore) (*FunctionScore, error) {
	if len(funcScoreSchema.Functions) > 1 || len(funcScoreSchema.Functions) == 0 {
		return nil, fmt.Errorf("Currently only supports one rerank, but got %d", len(funcScoreSchema.Functions))
	}
	funcScore := &FunctionScore{}
	var err error
	if funcScore.reranker, err = createFunction(collSchema, funcScoreSchema.Functions[0]); err != nil {
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
				Topks:      []int64{},
			},
		}, nil
	}

	allSearchResultData := lo.FilterMap(multipleMilvusResults, func(m *milvuspb.SearchResults, _ int) (*schemapb.SearchResultData, bool) {
		return m.Results, true
	})

	// rankResult only has scores
	inputs, err := newRerankInputs(allSearchResultData, fScore.reranker.GetInputFieldIDs())
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
