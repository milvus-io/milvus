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
	"github.com/milvus-io/milvus/internal/util/function/models/huggingface"
	"github.com/milvus-io/milvus/pkg/v2/util/merr"
)

type huggingFaceProvider struct {
	baseProvider
	client     *huggingface.Client
	modelName  string
	hfProvider string
	timeoutSec int64
}

func newHuggingFaceProvider(params []*commonpb.KeyValuePair, conf map[string]string, credentials *credentials.Credentials, extraInfo *models.ModelExtraInfo) (modelProvider, error) {
	apiKey, url, err := models.ParseAKAndURL(credentials, params, conf, models.HuggingFaceAKEnvStr, extraInfo)
	if err != nil {
		return nil, err
	}
	client, err := huggingface.NewClient(apiKey, url)
	if err != nil {
		return nil, err
	}

	var modelName string
	hfProvider := huggingface.DefaultHFProvider
	maxBatch := 32
	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		case models.HuggingFaceProviderParamKey:
			hfProvider = param.Value
		case models.MaxClientBatchSizeParamKey:
			if maxBatch, err = parseMaxBatch(param.Value); err != nil {
				return nil, err
			}
		default:
		}
	}
	if modelName == "" {
		return nil, merr.WrapErrParameterMissingMsg("huggingface rerank model name is required")
	}
	if hfProvider == "" {
		return nil, merr.WrapErrParameterInvalidMsg("huggingface rerank hf_provider cannot be empty")
	}

	provider := huggingFaceProvider{
		baseProvider: baseProvider{batchSize: maxBatch},
		client:       client,
		modelName:    modelName,
		hfProvider:   hfProvider,
		timeoutSec:   30,
	}
	return &provider, nil
}

func (provider *huggingFaceProvider) rerank(_ context.Context, query string, docs []string) ([]float32, error) {
	scores := make([]float32, 0, len(docs))
	for i := 0; i < len(docs); i += provider.batchSize {
		end := i + provider.batchSize
		if end > len(docs) {
			end = len(docs)
		}
		resp, err := provider.client.SentenceSimilarity(provider.hfProvider, provider.modelName, query, docs[i:end], provider.timeoutSec)
		if err != nil {
			return nil, err
		}
		if len(*resp) != end-i {
			return nil, merr.WrapErrFunctionFailedMsg("get rerank scores failed, the number of docs and scores does not match docs:[%d], scores:[%d]", end-i, len(*resp))
		}
		scores = append(scores, (*resp)...)
	}
	return scores, nil
}
