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
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	defaultYCTextEmbeddingURL = "https://llm.api.cloud.yandex.net/foundationModels/v1/textEmbedding"
)

type YCEmbeddingRequest struct {
	ModelURI string   `json:"modelUri"`
	Text     string   `json:"text,omitempty"`
	Texts    []string `json:"texts,omitempty"`
}

type YCEmbeddingResponse struct {
	Embedding  []float32   `json:"embedding"`
	Embeddings [][]float32 `json:"embeddings"`
}

type YCEmbeddingProvider struct {
	fieldDim int64

	url       string
	apiKey    string
	modelName string

	maxBatch   int
	timeoutSec int64
	extraInfo  *models.ModelExtraInfo
}

func NewYCEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.Credentials, extraInfo *models.ModelExtraInfo) (*YCEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}

	var modelName string
	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		case models.DimParamKey:
			if _, err = models.ParseAndCheckFieldDim(param.Value, fieldDim, fieldSchema.Name); err != nil {
				return nil, err
			}
		default:
		}
	}
	if modelName == "" {
		return nil, fmt.Errorf("yc embedding model name is required")
	}

	apiKey, url, err := models.ParseAKAndURL(credentials, functionSchema.Params, params, models.YandexCloudAKEnvStr, extraInfo)
	if err != nil {
		return nil, err
	}
	if apiKey == "" {
		return nil, fmt.Errorf("missing credentials config or configure the %s environment variable in the Milvus service", models.YandexCloudAKEnvStr)
	}
	if url == "" {
		url = defaultYCTextEmbeddingURL
	}

	provider := YCEmbeddingProvider{
		fieldDim:   fieldDim,
		url:        url,
		apiKey:     apiKey,
		modelName:  modelName,
		maxBatch:   128,
		timeoutSec: 30,
		extraInfo:  extraInfo,
	}
	return &provider, nil
}

func (provider *YCEmbeddingProvider) MaxBatch() int {
	return provider.extraInfo.BatchFactor * provider.maxBatch
}

func (provider *YCEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *YCEmbeddingProvider) headers() map[string]string {
	return map[string]string{
		"Content-Type":  "application/json",
		"Authorization": fmt.Sprintf("Api-Key %s", provider.apiKey),
	}
}

func (provider *YCEmbeddingProvider) CallEmbedding(ctx context.Context, texts []string, _ models.TextEmbeddingMode) (any, error) {
	_ = ctx
	numRows := len(texts)
	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}

		req := YCEmbeddingRequest{
			ModelURI: provider.modelName,
			Texts:    texts[i:end],
		}
		if end-i == 1 {
			req.Text = texts[i]
			req.Texts = nil
		}

		resp, err := models.PostRequest[YCEmbeddingResponse](req, provider.url, provider.headers(), provider.timeoutSec)
		if err != nil {
			return nil, err
		}

		embeddings := extractYCEmbeddings(resp)
		if end-i != len(embeddings) {
			return nil, fmt.Errorf("get embedding failed, the number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(embeddings))
		}
		for _, emb := range embeddings {
			if len(emb) != int(provider.fieldDim) {
				return nil, fmt.Errorf("the required embedding dim is [%d], but the embedding obtained from the model is [%d]",
					provider.fieldDim, len(emb))
			}
			data = append(data, emb)
		}
	}
	return data, nil
}

func extractYCEmbeddings(resp *YCEmbeddingResponse) [][]float32 {
	if len(resp.Embeddings) > 0 {
		return resp.Embeddings
	}
	if len(resp.Embedding) > 0 {
		return [][]float32{resp.Embedding}
	}
	return nil
}
