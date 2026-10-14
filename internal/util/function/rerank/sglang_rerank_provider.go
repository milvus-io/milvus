/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rerank

import (
	"context"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/sglang"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type sglangProvider struct {
	baseProvider
	client    *sglang.Client
	modelName string
	timeoutMs int64
}

func newSGLangProvider(params []*commonpb.KeyValuePair, conf map[string]string, credentials *credentials.Credentials) (ModelProvider, error) {
	apiKey, _, err := models.ParseAKAndURL(credentials, params, conf, "", &models.ModelExtraInfo{})
	if err != nil {
		return nil, err
	}

	var endpoint, modelName string
	maxBatch := 32
	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case models.EndpointParamKey:
			endpoint = param.Value
		case models.ModelNameParamKey:
			modelName = param.Value
		case models.MaxClientBatchSizeParamKey:
			if maxBatch, err = parseMaxBatch(param.Value); err != nil {
				return nil, err
			}
		}
	}
	if endpoint == "" {
		return nil, merr.WrapErrParameterMissingMsg("sglang rerank endpoint is required")
	}
	if modelName == "" {
		return nil, merr.WrapErrParameterMissingMsg("sglang rerank model name is required")
	}

	return &sglangProvider{
		baseProvider: baseProvider{batchSize: maxBatch},
		client:       sglang.NewClient(apiKey, endpoint),
		modelName:    modelName,
		timeoutMs:    models.ResolveTimeoutMs(params),
	}, nil
}

func (provider *sglangProvider) Rerank(ctx context.Context, query string, docs []string) ([]float32, error) {
	results, err := provider.client.Rerank(ctx, provider.modelName, query, docs, provider.timeoutMs)
	if err != nil {
		return nil, err
	}
	for _, result := range results {
		if result.Index == nil || result.Score == nil {
			return nil, merr.WrapErrFunctionFailedMsg("get rerank scores failed, the sglang service returned a result without index or score")
		}
	}
	return rerankScoresByIndex(len(docs), len(results), func(i int) (int, float32) {
		return *results[i].Index, *results[i].Score
	})
}
