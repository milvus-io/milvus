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

package embedding

import (
	"context"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/openai"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// OrcaRouter is an OpenAI-compatible AI gateway, so it speaks the same
// /v1/embeddings protocol as OpenAI. The provider reuses the OpenAI client
// and only swaps the endpoint and credential source.
type OrcaRouterEmbeddingProvider struct {
	fieldDim int64

	client        openai.OpenAIEmbeddingInterface
	modelName     string
	embedDimParam int64
	user          string

	maxBatch  int
	timeoutMs int64
	extraInfo *models.ModelExtraInfo
}

func NewOrcaRouterEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.Credentials, extraInfo *models.ModelExtraInfo) (*OrcaRouterEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}

	apiKey, url, err := models.ParseAKAndURL(credentials, functionSchema.Params, params, models.OrcaRouterAKEnvStr, extraInfo)
	if err != nil {
		return nil, err
	}

	if apiKey == "" {
		return nil, merr.WrapErrParameterInvalidMsg("missing credentials config or configure the %s environment variable in the Milvus service", models.OrcaRouterAKEnvStr)
	}

	var modelName, user string
	var dim int64
	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		case models.DimParamKey:
			dim, err = models.ParseAndCheckFieldDim(param.Value, fieldDim, fieldSchema.Name)
			if err != nil {
				return nil, err
			}
		case models.UserParamKey:
			user = param.Value
		default:
		}
	}

	if url == "" {
		url = "https://api.orcarouter.ai/v1/embeddings"
	}

	c := openai.NewOpenAIEmbeddingClient(apiKey, url)
	timeoutMs := models.ResolveTimeoutMs(functionSchema.Params)

	provider := OrcaRouterEmbeddingProvider{
		client:        c,
		fieldDim:      fieldDim,
		modelName:     modelName,
		user:          user,
		embedDimParam: dim,
		maxBatch:      128,
		timeoutMs:     timeoutMs,
		extraInfo:     extraInfo,
	}
	return &provider, nil
}

func (provider *OrcaRouterEmbeddingProvider) MaxBatch() int {
	return provider.extraInfo.BatchFactor * provider.maxBatch
}

func (provider *OrcaRouterEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *OrcaRouterEmbeddingProvider) CallEmbedding(ctx context.Context, texts []string, _ models.TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(provider.modelName, texts[i:end], int(provider.embedDimParam), provider.user, provider.timeoutMs)
		if err != nil {
			return nil, err
		}
		if end-i != len(resp.Data) {
			return nil, merr.WrapErrFunctionFailedMsg("get embedding failed, the number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp.Data))
		}
		for _, item := range resp.Data {
			if len(item.Embedding) != int(provider.fieldDim) {
				return nil, merr.WrapErrFunctionFailedMsg("the required embedding dim is [%d], but the embedding obtained from the model is [%d]",
					provider.fieldDim, len(item.Embedding))
			}
			data = append(data, item.Embedding)
		}
	}
	return data, nil
}
