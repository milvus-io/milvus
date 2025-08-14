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
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

const (
	RRFParamsKey string = "k"

	defaultRRFParamsValue float64 = 60
)

type RRFFunction[T PKType] struct {
	RerankBase

	k float32
}

func newRRFFunction(collSchema *schemapb.CollectionSchema, funcSchema *schemapb.FunctionSchema) (Reranker, error) {
	base, err := newRerankBase(collSchema, funcSchema, RRFName, true)
	if err != nil {
		return nil, err
	}

	if len(base.GetInputFieldNames()) != 0 {
		return nil, fmt.Errorf("The rrf function does not support input parameters, but got %s", base.GetInputFieldNames())
	}

	k := float64(defaultRRFParamsValue)
	for _, param := range funcSchema.Params {
		if strings.ToLower(param.Key) == RRFParamsKey {
			if k, err = strconv.ParseFloat(param.Value, 64); err != nil {
				return nil, fmt.Errorf("Param k:%s is not a number", param.Value)
			}
		}
	}
	if k <= 0 || k >= 16384 {
		return nil, fmt.Errorf("The rank params k should be in range (0, %d)", 16384)
	}
	if base.pkType == schemapb.DataType_Int64 {
		return &RRFFunction[int64]{RerankBase: *base, k: float32(k)}, nil
	} else {
		return &RRFFunction[string]{RerankBase: *base, k: float32(k)}, nil
	}
}

func (rrf *RRFFunction[T]) processOneSearchData(ctx context.Context, searchParams *SearchParams, cols []*columns, idGroup map[any]any) (*IDScores[T], error) {
	rrfScores := map[T]float32{}
	for _, col := range cols {
		if col.size == 0 {
			continue
		}
		ids := col.ids.([]T)
		for idx, id := range ids {
			if score, ok := rrfScores[id]; !ok {
				rrfScores[id] = 1 / (rrf.k + float32(idx+1))
			} else {
				rrfScores[id] = score + 1/(rrf.k+float32(idx+1))
			}
		}
	}
	if searchParams.isGrouping() {
		return newGroupingIDScores(rrfScores, searchParams, idGroup)
	}
	return newIDScores(rrfScores, searchParams), nil
}

func (rrf *RRFFunction[T]) Process(ctx context.Context, searchParams *SearchParams, inputs *rerankInputs) (*rerankOutputs, error) {
	outputs := newRerankOutputs(searchParams)
	for _, cols := range inputs.data {
		idScore, err := rrf.processOneSearchData(ctx, searchParams, cols, inputs.idGroupValue)
		if err != nil {
			return nil, err
		}
		appendResult(outputs, idScore.ids, idScore.scores)
	}
	return outputs, nil
}
