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
	"github.com/milvus-io/milvus/internal/util/function/models/vllm"
)

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

	apiKey, _, err := models.ParseAKAndURL(credentials, params, conf, "", &models.ModelExtraInfo{})
	if err != nil {
		return nil, err
	}

	client, err := vllm.NewVLLMClient(apiKey, endpoint)
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
