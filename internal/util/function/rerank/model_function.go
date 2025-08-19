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
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/internal/util/function/models/utils"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

const (
	providerParamName string = "provider"
	vllmProviderName  string = "vllm"
	teiProviderName   string = "tei"

	queryKeyName    string = "queries"
	maxBatchKeyName string = "max_batch"

	vllmTruncateParamName           string = "truncate_prompt_tokens"
	tieTruncateParamName            string = "truncate"
	teiTruncationDirectionParamName string = "truncation_direction"
)

type modelProvider interface {
	rerank(context.Context, string, []string) ([]float32, error)
	getURL() string
}

type baseModel struct {
	url      string
	maxBatch int

	queryKey string
	docKey   string

	truncateParams map[string]any

	parseScores func([]byte) ([]float32, error)
}

func (base *baseModel) getURL() string {
	return base.url
}

func (base *baseModel) rerank(ctx context.Context, query string, docs []string) ([]float32, error) {
	requestBodies, err := genRerankRequestBody(query, docs, base.maxBatch, base.queryKey, base.docKey, base.truncateParams)
	if err != nil {
		return nil, err
	}
	scores := []float32{}
	for _, requestBody := range requestBodies {
		rerankResp, err := base.callService(ctx, requestBody, 30)
		if err != nil {
			return nil, fmt.Errorf("Call rerank model failed: %v\n", err)
		}
		scores = append(scores, rerankResp...)
	}

	if len(scores) != len(docs) {
		return nil, fmt.Errorf("Call Rerank service failed, %d docs but got %d scores", len(docs), len(scores))
	}
	return scores, nil
}

func (base *baseModel) callService(ctx context.Context, requestBody []byte, timeoutSec int64) ([]float32, error) {
	ctx, cancel := context.WithTimeout(ctx, time.Duration(timeoutSec)*time.Second)
	defer cancel()
	headers := map[string]string{
		"Content-Type": "application/json",
	}
	body, err := utils.RetrySend(ctx, requestBody, http.MethodPost, base.url, headers, 3)
	if err != nil {
		return nil, err
	}
	return base.parseScores(body)
}

type vllmRerankRequest struct {
	Query     string   `json:"query"`
	Documents []string `json:"documents"`
}

type vllmRerankResponse struct {
	ID      string       `json:"id"`
	Model   string       `json:"model"`
	Usage   vllmUsage    `json:"usage"`
	Results []vllmResult `json:"results"`
}

type vllmUsage struct {
	TotalTokens int `json:"total_tokens"`
}

type vllmResult struct {
	Index          int          `json:"index"`
	Document       vllmDocument `json:"document"`
	RelevanceScore float32      `json:"relevance_score"`
}

type vllmDocument struct {
	Text string `json:"text"`
}

type vllmProvider struct {
	baseModel
}

func newVllmProvider(params []*commonpb.KeyValuePair, conf map[string]string) (modelProvider, error) {
	if !isEnable(conf, function.EnableVllmEnvStr) {
		return nil, fmt.Errorf("Vllm rerank is disabled")
	}
	endpoint, maxBatch, truncateParams, err := parseParams(params)
	if err != nil {
		return nil, err
	}

	base, _ := url.Parse(endpoint)
	base.Path = "/v2/rerank"
	model := baseModel{
		url:            base.String(),
		maxBatch:       maxBatch,
		queryKey:       "query",
		docKey:         "documents",
		truncateParams: truncateParams,
		parseScores: func(body []byte) ([]float32, error) {
			var rerankResp vllmRerankResponse
			if err := json.Unmarshal(body, &rerankResp); err != nil {
				return nil, fmt.Errorf("Rerank error, parsing vllm response failed: %v", err)
			}

			sort.Slice(rerankResp.Results, func(i, j int) bool {
				return rerankResp.Results[i].Index < rerankResp.Results[j].Index
			})

			scores := make([]float32, 0, len(rerankResp.Results))
			for _, result := range rerankResp.Results {
				scores = append(scores, result.RelevanceScore)
			}

			return scores, nil
		},
	}
	return &vllmProvider{baseModel: model}, nil
}

type teiProvider struct {
	baseModel
}

type TEIResponse struct {
	Index int     `json:"index"`
	Score float32 `json:"score"`
}

func newTeiProvider(params []*commonpb.KeyValuePair, conf map[string]string) (modelProvider, error) {
	if !isEnable(conf, function.EnableTeiEnvStr) {
		return nil, fmt.Errorf("TEI rerank is disabled")
	}
	endpoint, maxBatch, truncateParams, err := parseParams(params)
	if err != nil {
		return nil, err
	}
	base, _ := url.Parse(endpoint)
	base.Path = "/rerank"
	model := baseModel{
		url:            base.String(),
		maxBatch:       maxBatch,
		queryKey:       "query",
		docKey:         "texts",
		truncateParams: truncateParams,
		parseScores: func(body []byte) ([]float32, error) {
			var results []TEIResponse
			if err := json.Unmarshal(body, &results); err != nil {
				return nil, fmt.Errorf("Rerank error, parsing TEI response failed: %v", err)
			}
			sort.Slice(results, func(i, j int) bool {
				return results[i].Index < results[j].Index
			})
			scores := make([]float32, len(results))
			for i, result := range results {
				scores[i] = result.Score
			}
			return scores, nil
		},
	}
	return &teiProvider{baseModel: model}, nil
}

func isEnable(conf map[string]string, envKey string) bool {
	// milvus.yaml > env
	value, exists := conf["enable"]
	if exists {
		return strings.ToLower(value) == "true"
	} else {
		return !(strings.ToLower(os.Getenv(envKey)) == "false")
	}
}

func parseParams(params []*commonpb.KeyValuePair) (string, int, map[string]any, error) {
	endpoint := ""
	maxBatch := 32
	truncateParams := map[string]any{}
	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case function.EndpointParamKey:
			base, err := url.Parse(param.Value)
			if err != nil {
				return "", 0, nil, err
			}
			if base.Scheme != "http" && base.Scheme != "https" {
				return "", 0, nil, fmt.Errorf("Rerank endpoint: [%s] is not a valid http/https link", param.Value)
			}
			if base.Host == "" {
				return "", 0, nil, fmt.Errorf("Rerank endpoint: [%s] is not a valid http/https link", param.Value)
			}
			endpoint = base.String()
		case maxBatchKeyName:
			if batch, err := strconv.ParseInt(param.Value, 10, 64); err != nil {
				return "", 0, nil, fmt.Errorf("Rerank params error, maxBatch: %s is not a number", param.Value)
			} else {
				maxBatch = int(batch)
			}
		case vllmTruncateParamName:
			if vllmTrun, err := strconv.ParseInt(param.Value, 10, 64); err != nil {
				return "", 0, nil, fmt.Errorf("Rerank params error, %s: %s is not a number", vllmTruncateParamName, param.Value)
			} else {
				truncateParams[vllmTruncateParamName] = vllmTrun
			}
		case teiTruncationDirectionParamName:
			truncateParams[teiTruncationDirectionParamName] = param.Value
		case tieTruncateParamName:
			if teiTrun, err := strconv.ParseBool(param.Value); err != nil {
				return "", 0, nil, fmt.Errorf("Rerank params error, %s: %s is not bool type", tieTruncateParamName, param.Value)
			} else {
				truncateParams[tieTruncateParamName] = teiTrun
			}
		}
	}
	if endpoint == "" {
		return "", 0, nil, fmt.Errorf("Rerank function lost params endpoint")
	}
	if maxBatch <= 0 {
		return "", 0, nil, fmt.Errorf("Rerank function params max_batch must > 0, but got %d", maxBatch)
	}
	return endpoint, maxBatch, truncateParams, nil
}

func genRerankRequestBody(query string, documents []string, maxSize int, queryKey string, docKey string, truncateParams map[string]any) ([][]byte, error) {
	requestBodies := [][]byte{}
	for i := 0; i < len(documents); i += maxSize {
		end := i + maxSize
		if end > len(documents) {
			end = len(documents)
		}
		requestBody := map[string]interface{}{
			queryKey: query,
			docKey:   documents[i:end],
		}
		for k, v := range truncateParams {
			requestBody[k] = v
		}
		jsonData, err := json.Marshal(requestBody)
		if err != nil {
			return nil, fmt.Errorf("Create model rerank request failed, err: %s", err)
		}
		requestBodies = append(requestBodies, jsonData)
	}
	return requestBodies, nil
}

func newProvider(params []*commonpb.KeyValuePair) (modelProvider, error) {
	for _, param := range params {
		if strings.ToLower(param.Key) == providerParamName {
			provider := strings.ToLower(param.Value)
			conf := paramtable.Get().FunctionCfg.GetRerankModelProviders(provider)
			switch provider {
			case vllmProviderName:
				return newVllmProvider(params, conf)
			case teiProviderName:
				return newTeiProvider(params, conf)
			default:
				return nil, fmt.Errorf("Unknow rerank provider:%s", param.Value)
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
	scores, err := model.provider.rerank(ctx, query, texts)
	if err != nil {
		return nil, err
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
