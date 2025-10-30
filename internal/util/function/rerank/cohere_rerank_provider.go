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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/cohere"
)

type cohereProvider struct {
	baseProvider
	cohereClient *cohere.CohereClient
	url          string
	modelName    string
	params       map[string]any
}

func newCohereProvider(params []*commonpb.KeyValuePair, conf map[string]string, credentials *credentials.Credentials, extraInfo *models.ModelExtraInfo) (modelProvider, error) {
	apiKey, url, err := models.ParseAKAndURL(credentials, params, conf, models.CohereAIAKEnvStr, extraInfo)
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
	modelParams := map[string]any{}
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
			maxTokensPerDoc, err := strconv.Atoi(param.Value)
			if err != nil {
				return nil, fmt.Errorf("[%s param's value: %s] is not a valid number", models.MaxTKsPerDocParamKey, param.Value)
			} else {
				modelParams[models.MaxTKsPerDocParamKey] = maxTokensPerDoc
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
