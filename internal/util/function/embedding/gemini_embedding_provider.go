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
	"github.com/milvus-io/milvus/internal/util/function/models/gemini"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type GeminiEmbeddingProvider struct {
	fieldDim int64

	client        *gemini.GeminiClient
	url           string
	modelName     string
	embedDimParam int64
	taskType      string

	maxBatch   int
	timeoutSec int64
	extraInfo  *models.ModelExtraInfo
}

func NewGeminiEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.Credentials, extraInfo *models.ModelExtraInfo) (*GeminiEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}

	if fieldSchema.DataType != schemapb.DataType_FloatVector {
		return nil, fmt.Errorf("Gemini embedding only supports FloatVector field, got %s", schemapb.DataType_name[int32(fieldSchema.DataType)])
	}

	apiKey, url, err := models.ParseAKAndURL(credentials, functionSchema.Params, params, models.GeminiAKEnvStr, extraInfo)
	if err != nil {
		return nil, err
	}

	var modelName string
	dim := int64(0)
	taskType := ""

	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		case models.DimParamKey:
			dim, err = models.ParseAndCheckFieldDim(param.Value, fieldDim, fieldSchema.Name)
			if err != nil {
				return nil, err
			}
		case models.TaskTypeParamKey:
			taskType = param.Value
		default:
		}
	}

	if modelName == "" {
		return nil, fmt.Errorf("model_name is required for Gemini embedding provider")
	}
	modelName = strings.TrimPrefix(modelName, "models/")

	c, err := gemini.NewGeminiClient(apiKey)
	if err != nil {
		return nil, err
	}

	if url == "" {
		url = fmt.Sprintf("https://generativelanguage.googleapis.com/v1beta/models/%s:batchEmbedContents", modelName)
	}

	provider := GeminiEmbeddingProvider{
		client:        c,
		url:           url,
		fieldDim:      fieldDim,
		modelName:     modelName,
		embedDimParam: dim,
		taskType:      taskType,
		maxBatch:      32,
		timeoutSec:    30,
		extraInfo:     extraInfo,
	}
	return &provider, nil
}

func (provider *GeminiEmbeddingProvider) MaxBatch() int {
	return provider.extraInfo.BatchFactor * provider.maxBatch
}

func (provider *GeminiEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *GeminiEmbeddingProvider) getTaskType(mode models.TextEmbeddingMode) string {
	if provider.taskType != "" {
		return provider.taskType
	}
	if mode == models.InsertMode {
		return "RETRIEVAL_DOCUMENT"
	}
	return "RETRIEVAL_QUERY"
}

func (provider *GeminiEmbeddingProvider) CallEmbedding(ctx context.Context, texts []string, mode models.TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	taskType := provider.getTaskType(mode)

	embRet := models.NewEmbdResult(numRows, models.Float32Embd)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(provider.url, provider.modelName, texts[i:end], int(provider.embedDimParam), taskType, provider.timeoutSec)
		if err != nil {
			return nil, err
		}
		if end-i != len(resp.Embeddings) {
			return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp.Embeddings))
		}

		for _, item := range resp.Embeddings {
			if len(item.Values) != int(provider.fieldDim) {
				return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
					provider.fieldDim, len(item.Values))
			}
			embRet.Append(item.Values)
		}
	}
	return embRet.FloatEmbds, nil
}
