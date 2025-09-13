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
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/ali"
)

type aliProvider struct {
	baseProvider
	client    *ali.AliDashScopeRerank
	modelName string
	params    map[string]any
}

func newAliProvider(params []*commonpb.KeyValuePair, conf map[string]string, credentials *credentials.Credentials) (modelProvider, error) {
	apiKey, _, err := models.ParseAKAndURL(credentials, params, conf, "")
	if err != nil {
		return nil, err
	}

	maxBatch := 32
	truncateParams := map[string]any{}

	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case models.MaxClientBatchSizeParamKey:
			if maxBatch, err = parseMaxBatch(param.Value); err != nil {
				return nil, err
			}
		default:
		}
	}

	url := "https://dashscope.aliyuncs.com/api/v1/services/rerank/text-rerank/text-rerank"
	client := ali.NewAliDashScopeRerank(apiKey, url)

	provider := aliProvider{
		baseProvider: baseProvider{batchSize: maxBatch},
		client:       client,
		params:       truncateParams,
	}
	return &provider, nil
}

func (provider *aliProvider) rerank(ctx context.Context, query string, docs []string) ([]float32, error) {
	rerankResp, err := provider.client.Rerank(provider.modelName, query, docs, provider.params, 30)
	if err != nil {
		return nil, err
	}
	scores := make([]float32, len(docs))
	for i, rerankResult := range rerankResp.Output.Results {
		scores[i] = rerankResult.RelevanceScore
	}
	return scores, nil
}
