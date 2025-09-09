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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	providerParamName       string = "provider"
	vllmProviderName        string = "vllm"
	teiProviderName         string = "tei"
	siliconflowProviderName string = "siliconflow"
	cohereProviderName      string = "cohere"
	voyageaiProviderName    string = "voyageai"

	queryKeyName string = "queries"
)

func parseMaxBatch(maxBatch string) (int, error) {
	if batch, err := strconv.Atoi(maxBatch); err != nil {
		return -1, fmt.Errorf("[%s param's value: %s] is not a valid number", models.MaxClientBatchSizeParamKey, maxBatch)
	} else {
		return batch, nil
	}
}

type modelProvider interface {
	rerank(context.Context, string, []string) ([]float32, error)
	maxBatch() int
}

type baseProvider struct {
	batchSize int
}

func (provider *baseProvider) maxBatch() int {
	return provider.batchSize
}

func newProvider(params []*commonpb.KeyValuePair) (modelProvider, error) {
	for _, param := range params {
		if strings.ToLower(param.Key) == providerParamName {
			provider := strings.ToLower(param.Value)
			conf := paramtable.Get().FunctionCfg.GetRerankModelProviders(provider)
			if !models.IsEnable(conf) {
				return nil, fmt.Errorf("Rerank provider: [%s] is disabled", provider)
			}
			credentials := credentials.NewCredentials(paramtable.Get().CredentialCfg.GetCredentials())
			switch provider {
			case vllmProviderName:
				return newVllmProvider(params, conf, credentials)
			case teiProviderName:
				return newTeiProvider(params, conf, credentials)
			case siliconflowProviderName:
				return newSiliconflowProvider(params, conf, credentials)
			case cohereProviderName:
				return newCohereProvider(params, conf, credentials)
			case voyageaiProviderName:
				return newVoyageaiProvider(params, conf, credentials)
			default:
				return nil, fmt.Errorf("Unknow rerank model provider:%s", param.Value)
			}
		}
	}
	return nil, fmt.Errorf("Lost rerank params:%s ", providerParamName)
}

type ModelFunction[T PKType] struct {
	RerankBase

	provider modelProvider
	queries  []string
}

func newModelFunction(collSchema *schemapb.CollectionSchema, funcSchema *schemapb.FunctionSchema) (Reranker, error) {
	base, err := newRerankBase(collSchema, funcSchema, DecayFunctionName, true)
	if err != nil {
		return nil, err
	}

	if len(base.GetInputFieldNames()) != 1 {
		return nil, fmt.Errorf("Rerank model only supports single input, but gets [%s] input", base.GetInputFieldNames())
	}

	if base.GetInputFieldTypes()[0] != schemapb.DataType_VarChar {
		return nil, fmt.Errorf("Rerank model only support varchar, bug got [%s]", base.GetInputFieldTypes()[0].String())
	}

	provider, err := newProvider(funcSchema.Params)
	if err != nil {
		return nil, err
	}

	queries := []string{}
	for _, param := range funcSchema.Params {
		if param.Key == queryKeyName {
			if err := json.Unmarshal([]byte(param.Value), &queries); err != nil {
				return nil, fmt.Errorf("Parse rerank params [queries] failed, err: %v", err)
			}
		}
	}
	if len(queries) == 0 {
		return nil, fmt.Errorf("Rerank function lost params queries")
	}

	if base.pkType == schemapb.DataType_Int64 {
		return &ModelFunction[int64]{RerankBase: *base, provider: provider, queries: queries}, nil
	} else {
		return &ModelFunction[string]{RerankBase: *base, provider: provider, queries: queries}, nil
	}
}

func (model *ModelFunction[T]) processOneSearchData(ctx context.Context, searchParams *SearchParams, query string, cols []*columns, idGroup map[any]any) (*IDScores[T], error) {
	uniqueData := make(map[T]string)
	for _, col := range cols {
		if col.size == 0 {
			continue
		}
		texts := col.data[0].([]string)
		ids := col.ids.([]T)
		for idx, id := range ids {
			if _, ok := uniqueData[id]; !ok {
				uniqueData[id] = texts[idx]
			}
		}
	}
	ids := make([]T, 0, len(uniqueData))
	texts := make([]string, 0, len(uniqueData))
	for id, text := range uniqueData {
		ids = append(ids, id)
		texts = append(texts, text)
	}
	scores := make([]float32, 0, len(texts))
	for i := 0; i < len(texts); i += model.provider.maxBatch() {
		end := i + model.provider.maxBatch()
		if end > len(texts) {
			end = len(texts)
		}
		newScores, err := model.provider.rerank(ctx, query, texts[i:end])
		if err != nil {
			return nil, err
		}
		if len(newScores) != end-i {
			return nil, fmt.Errorf("Call Rerank service failed, %d docs but got %d scores", end-i, len(newScores))
		}
		scores = append(scores, newScores...)
	}

	rerankScores := map[T]float32{}
	for idx, id := range ids {
		rerankScores[id] = scores[idx]
	}
	if searchParams.isGrouping() {
		return newGroupingIDScores(rerankScores, searchParams, idGroup)
	}
	return newIDScores(rerankScores, searchParams), nil
}

func (model *ModelFunction[T]) Process(ctx context.Context, searchParams *SearchParams, inputs *rerankInputs) (*rerankOutputs, error) {
	if len(model.queries) != int(searchParams.nq) {
		return nil, fmt.Errorf("nq must equal to queries size, but got nq [%d], queries size [%d], queries: [%v]", searchParams.nq, len(model.queries), model.queries)
	}
	outputs := newRerankOutputs(searchParams)
	for idx, cols := range inputs.data {
		idScore, err := model.processOneSearchData(ctx, searchParams, model.queries[idx], cols, inputs.idGroupValue)
		if err != nil {
			return nil, err
		}
		appendResult(outputs, idScore.ids, idScore.scores)
	}
	return outputs, nil
}
