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
	"github.com/milvus-io/milvus/internal/util/function/models/cohere"
	"github.com/milvus-io/milvus/internal/util/function/models/siliconflow"
	"github.com/milvus-io/milvus/internal/util/function/models/tei"
	"github.com/milvus-io/milvus/internal/util/function/models/vllm"
	"github.com/milvus-io/milvus/internal/util/function/models/voyageai"
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

type vllmProvider struct {
	baseProvider
	client         *vllm.VLLMClient
	truncateParams map[string]any
}

func newVllmProvider(params []*commonpb.KeyValuePair, conf map[string]string, credentials *credentials.Credentials) (modelProvider, error) {
	var endpoint string
	var err error
	maxBatch := 32
	truncateParams := map[string]any{}

	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case models.EndpointParamKey:
			endpoint = param.Value
		case models.MaxClientBatchSizeParamKey:
			if maxBatch, err = parseMaxBatch(param.Value); err != nil {
				return nil, err
			}
		case models.VllmTruncateParamName:
			if vllmTrun, err := strconv.ParseInt(param.Value, 10, 64); err != nil {
				return nil, fmt.Errorf("Rerank params error, %s: %s is not a number", models.VllmTruncateParamName, param.Value)
			} else {
				truncateParams[models.VllmTruncateParamName] = vllmTrun
			}
		default:
		}
	}
	if endpoint == "" {
		return nil, fmt.Errorf("Rerank function lost params endpoint")
	}
	if maxBatch < 0 {
		return nil, fmt.Errorf("Rerank function params max_batch must > 0")
	}

	apiKey, _, err := models.ParseAKAndURL(credentials, params, conf, "")
	if err != nil {
		return nil, err
	}

	client, err := vllm.NewVLLMClient(apiKey, endpoint, conf["enable"])
	if err != nil {
		return nil, err
	}

	provider := vllmProvider{
		baseProvider:   baseProvider{batchSize: maxBatch},
		client:         client,
		truncateParams: truncateParams,
	}
	return &provider, nil
}

func (provider *vllmProvider) rerank(ctx context.Context, query string, docs []string) ([]float32, error) {
	rerankResp, err := provider.client.Rerank(query, docs, provider.truncateParams, 30)
	if err != nil {
		return nil, err
	}
	scores := make([]float32, len(docs))
	for i, result := range rerankResp.Results {
		scores[i] = result.RelevanceScore
	}
	return scores, nil
}

type teiProvider struct {
	baseProvider
	client *tei.TEIClient
	params map[string]any
}

func newTeiProvider(params []*commonpb.KeyValuePair, conf map[string]string, credentials *credentials.Credentials) (modelProvider, error) {
	apiKey, _, err := models.ParseAKAndURL(credentials, params, conf, "")
	if err != nil {
		return nil, err
	}

	var endpoint string
	// TEI default client batch size
	maxBatch := 32
	truncateParams := map[string]any{}

	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case models.EndpointParamKey:
			endpoint = param.Value
		case models.MaxClientBatchSizeParamKey:
			if maxBatch, err = parseMaxBatch(param.Value); err != nil {
				return nil, err
			}
		case models.TruncateParamKey:
			if teiTrun, err := strconv.ParseBool(param.Value); err != nil {
				return nil, fmt.Errorf("Rerank params error, %s: %s is not bool type", models.TruncateParamKey, param.Value)
			} else {
				truncateParams[models.TruncateParamKey] = teiTrun
			}
		case models.TruncationDirectionParamKey:
			if truncationDirection := param.Value; truncationDirection != "Left" && truncationDirection != "Right" {
				return nil, fmt.Errorf("[%s param's value: %s] is not invalid, only supports [Left/Right]", models.TruncationDirectionParamKey, param.Value)
			} else {
				truncateParams[models.TruncationDirectionParamKey] = truncationDirection
			}
		default:
		}
	}

	client, err := tei.NewTEIClient(apiKey, endpoint, conf["enable"])
	if err != nil {
		return nil, err
	}

	provider := teiProvider{
		baseProvider: baseProvider{batchSize: maxBatch},
		client:       client,
		params:       truncateParams,
	}
	return &provider, nil
}

func (provider *teiProvider) rerank(ctx context.Context, query string, docs []string) ([]float32, error) {
	rerankResp, err := provider.client.Rerank(query, docs, provider.params, 30)
	if err != nil {
		return nil, err
	}
	scores := make([]float32, len(docs))
	for i, result := range *rerankResp {
		scores[i] = result.Score
	}
	return scores, nil
}

type siliconflowProvider struct {
	baseProvider
	siliconflowClient *siliconflow.SiliconflowClient
	url               string
	modelName         string
	params            map[string]any
}

func newSiliconflowProvider(params []*commonpb.KeyValuePair, conf map[string]string, credentials *credentials.Credentials) (modelProvider, error) {
	apiKey, url, err := models.ParseAKAndURL(credentials, params, conf, models.SiliconflowAKEnvStr)
	if err != nil {
		return nil, err
	}
	if url == "" {
		url = "https://api.siliconflow.cn/v1/rerank"
	}

	siliconflowClient, err := siliconflow.NewSiliconflowClient(apiKey)
	if err != nil {
		return nil, err
	}

	var modelName string
	maxBatch := 128 // The document does not clearly state what the maximum batch size is.
	rankParams := map[string]any{}

	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		case models.MaxClientBatchSizeParamKey:
			if maxBatch, err = parseMaxBatch(param.Value); err != nil {
				return nil, err
			}
		case models.MaxChunksPerDocParamKey:
			maxChunksPerDoc, err := strconv.Atoi(param.Value)
			if err != nil {
				return nil, fmt.Errorf("[%s param's value: %s] is not a valid number", models.MaxChunksPerDocParamKey, param.Value)
			}
			rankParams[models.MaxChunksPerDocParamKey] = maxChunksPerDoc
		case models.OverlapTokensParamKey:
			overlapTokens, err := strconv.Atoi(param.Value)
			if err != nil {
				return nil, fmt.Errorf("[%s param's value: %s] is not a valid number", models.OverlapTokensParamKey, param.Value)
			}
			rankParams[models.OverlapTokensParamKey] = overlapTokens
		default:
		}
	}
	if modelName == "" {
		return nil, fmt.Errorf("siliconflow rerank model name is required")
	}

	provider := siliconflowProvider{
		baseProvider:      baseProvider{batchSize: maxBatch},
		siliconflowClient: siliconflowClient,
		url:               url,
		modelName:         modelName,
		params:            rankParams,
	}
	return &provider, nil
}

func (provider *siliconflowProvider) rerank(ctx context.Context, query string, docs []string) ([]float32, error) {
	rerankResp, err := provider.siliconflowClient.Rerank(provider.url, provider.modelName, query, docs, nil, 30)
	if err != nil {
		return nil, err
	}
	scores := make([]float32, len(docs))
	for i, result := range rerankResp.Results {
		scores[i] = result.RelevanceScore
	}
	return scores, nil
}

type cohereProvider struct {
	baseProvider
	cohereClient *cohere.CohereClient
	url          string
	modelName    string
	params       map[string]any
}

func newCohereProvider(params []*commonpb.KeyValuePair, conf map[string]string, credentials *credentials.Credentials) (modelProvider, error) {
	apiKey, url, err := models.ParseAKAndURL(credentials, params, conf, models.CohereAIAKEnvStr)
	if err != nil {
		return nil, err
	}
	if url == "" {
		url = "https://api.cohere.com/v2/rerank"
	}
	cohereClient, err := cohere.NewCohereClient(apiKey)
	if err != nil {
		return nil, err
	}

	var modelName string
	maxTokensPerDoc := -1
	maxBatch := 128
	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		case models.MaxClientBatchSizeParamKey:
			if maxBatch, err = parseMaxBatch(param.Value); err != nil {
				return nil, err
			}
		case models.MaxTKsPerDocParamKey:
			maxTokensPerDoc, err = strconv.Atoi(param.Value)
			if err != nil {
				return nil, fmt.Errorf("[%s param's value: %s] is not a valid number", models.MaxTKsPerDocParamKey, param.Value)
			}
		default:
		}
	}
	if modelName == "" {
		return nil, fmt.Errorf("cohere rerank model name is required")
	}
	provider := cohereProvider{
		baseProvider: baseProvider{batchSize: maxBatch},
		cohereClient: cohereClient,
		url:          url,
		modelName:    modelName,
		params:       nil,
	}
	if maxTokensPerDoc > 0 {
		provider.params = map[string]any{models.MaxTKsPerDocParamKey: maxTokensPerDoc}
	}
	return &provider, nil
}

func (provider *cohereProvider) rerank(ctx context.Context, query string, docs []string) ([]float32, error) {
	rerankResp, err := provider.cohereClient.Rerank(provider.url, provider.modelName, query, docs, nil, 30)
	if err != nil {
		return nil, err
	}
	scores := make([]float32, len(docs))
	for i, result := range rerankResp.Results {
		scores[i] = result.RelevanceScore
	}
	return scores, nil
}

type voyageaiProvider struct {
	baseProvider
	voyageaiClient *voyageai.VoyageAIClient
	url            string
	modelName      string
	params         map[string]any
}

func newVoyageaiProvider(params []*commonpb.KeyValuePair, conf map[string]string, credentials *credentials.Credentials) (modelProvider, error) {
	apiKey, url, err := models.ParseAKAndURL(credentials, params, conf, models.VoyageAIAKEnvStr)
	if err != nil {
		return nil, err
	}
	if url == "" {
		url = "https://api.voyageai.com/v1/rerank"
	}
	voyageaiClient, err := voyageai.NewVoyageAIClient(apiKey)
	if err != nil {
		return nil, err
	}
	var modelName string
	truncate := false
	maxBatch := 128
	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		case models.MaxClientBatchSizeParamKey:
			if maxBatch, err = parseMaxBatch(param.Value); err != nil {
				return nil, err
			}
		case models.TruncationParamKey:
			if truncate, err = strconv.ParseBool(param.Value); err != nil {
				return nil, fmt.Errorf("[%s param's value: %s] is invalid, only supports: [true/false]", models.TruncationParamKey, param.Value)
			}
		default:
		}
	}
	if modelName == "" {
		return nil, fmt.Errorf("voyageai rerank model name is required")
	}
	provider := voyageaiProvider{
		baseProvider:   baseProvider{batchSize: maxBatch},
		voyageaiClient: voyageaiClient,
		url:            url,
		modelName:      modelName,
		params:         map[string]any{models.TruncationParamKey: truncate},
	}
	return &provider, nil
}

func (provider *voyageaiProvider) rerank(ctx context.Context, query string, docs []string) ([]float32, error) {
	rerankResp, err := provider.voyageaiClient.Rerank(provider.url, provider.modelName, query, docs, nil, 30)
	if err != nil {
		return nil, err
	}
	scores := make([]float32, len(docs))
	for i, result := range rerankResp.Data {
		scores[i] = result.RelevanceScore
	}
	return scores, nil
}

func newProvider(params []*commonpb.KeyValuePair) (modelProvider, error) {
	for _, param := range params {
		if strings.ToLower(param.Key) == providerParamName {
			provider := strings.ToLower(param.Value)
			conf := paramtable.Get().FunctionCfg.GetRerankModelProviders(provider)
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
	base, err := newRerankBase(collSchema, funcSchema, decayFunctionName, true)
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
