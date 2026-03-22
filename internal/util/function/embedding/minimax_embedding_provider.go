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
	"github.com/milvus-io/milvus/internal/util/function/models/minimax"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type MiniMaxEmbeddingProvider struct {
	fieldDim int64

	client    *minimax.MiniMaxClient
	url       string
	modelName string

	maxBatch   int
	timeoutSec int64
	extraInfo  *models.ModelExtraInfo
}

func NewMiniMaxEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.Credentials, extraInfo *models.ModelExtraInfo) (*MiniMaxEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}

	if fieldSchema.DataType != schemapb.DataType_FloatVector {
		return nil, fmt.Errorf("MiniMax embedding only supports FloatVector output, but got %s", schemapb.DataType_name[int32(fieldSchema.DataType)])
	}

	apiKey, url, err := models.ParseAKAndURL(credentials, functionSchema.Params, params, models.MiniMaxAKEnvStr, extraInfo)
	if err != nil {
		return nil, err
	}

	var modelName string
	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		case models.DimParamKey:
			_, err = models.ParseAndCheckFieldDim(param.Value, fieldDim, fieldSchema.Name)
			if err != nil {
				return nil, err
			}
		default:
		}
	}

	c, err := minimax.NewMiniMaxClient(apiKey)
	if err != nil {
		return nil, err
	}

	if url == "" {
		url = "https://api.minimax.io/v1/embeddings"
	}

	provider := MiniMaxEmbeddingProvider{
		client:     c,
		url:        url,
		fieldDim:   fieldDim,
		modelName:  modelName,
		maxBatch:   128,
		timeoutSec: 30,
		extraInfo:  extraInfo,
	}
	return &provider, nil
}

func (provider *MiniMaxEmbeddingProvider) MaxBatch() int {
	return provider.extraInfo.BatchFactor * provider.maxBatch
}

func (provider *MiniMaxEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *MiniMaxEmbeddingProvider) CallEmbedding(ctx context.Context, texts []string, mode models.TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	var textType string
	if mode == models.InsertMode {
		textType = "db"
	} else {
		textType = "query"
	}

	embRet := models.NewEmbdResult(numRows, models.Float32Embd)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(provider.url, provider.modelName, texts[i:end], textType, provider.timeoutSec)
		if err != nil {
			return nil, err
		}
		if resp.BaseResp.StatusCode != 0 {
			return nil, fmt.Errorf("MiniMax embedding API error: %s (code: %d)", resp.BaseResp.StatusMsg, resp.BaseResp.StatusCode)
		}
		if end-i != len(resp.Vectors) {
			return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp.Vectors))
		}
		for _, vec := range resp.Vectors {
			if len(vec) != int(provider.fieldDim) {
				return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
					provider.fieldDim, len(vec))
			}
			embRet.Append(vec)
		}
	}
	return embRet.FloatEmbds, nil
}
