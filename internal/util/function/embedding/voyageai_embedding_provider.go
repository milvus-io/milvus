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
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/voyageai"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type VoyageAIEmbeddingProvider struct {
	fieldDim int64

	client        *voyageai.VoyageAIClient
	url           string
	modelName     string
	embedDimParam int64
	truncate      bool
	embdType      models.EmbeddingType
	outputType    string

	maxBatch   int
	timeoutSec int64
	extraInfo  *models.ModelExtraInfo
}

func NewVoyageAIEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.Credentials, extraInfo *models.ModelExtraInfo) (*VoyageAIEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	apiKey, url, err := models.ParseAKAndURL(credentials, functionSchema.Params, params, models.VoyageAIAKEnvStr, extraInfo)
	if err != nil {
		return nil, err
	}
	var modelName string
	dim := int64(0)
	truncate := false

	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		case models.DimParamKey:
			// Only voyage-3-large and voyage-code-3 support dim param: 1024 (default), 256, 512, 2048
			dim, err = models.ParseAndCheckFieldDim(param.Value, fieldDim, fieldSchema.Name)
			if err != nil {
				return nil, err
			}
		case models.TruncationParamKey:
			if truncate, err = strconv.ParseBool(param.Value); err != nil {
				return nil, fmt.Errorf("[%s param's value: %s] is invalid, only supports: [true/false]", models.TruncationParamKey, param.Value)
			}
		default:
		}
	}

	c, err := voyageai.NewVoyageAIClient(apiKey)
	if err != nil {
		return nil, err
	}

	if url == "" {
		url = "https://api.voyageai.com/v1/embeddings"
	}

	embdType := models.GetEmbdType(fieldSchema.DataType)
	if embdType == models.UnsupportEmbd {
		return nil, fmt.Errorf("Unsupport output type: %s", fieldSchema.DataType)
	}

	outputType := func() string {
		if embdType == models.Float32Embd {
			return "float"
		}
		return "int8"
	}()

	provider := VoyageAIEmbeddingProvider{
		client:        c,
		url:           url,
		fieldDim:      fieldDim,
		modelName:     modelName,
		truncate:      truncate,
		embedDimParam: dim,
		embdType:      embdType,
		outputType:    outputType,
		maxBatch:      128,
		timeoutSec:    30,
		extraInfo:     extraInfo,
	}
	return &provider, nil
}

func (provider *VoyageAIEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *VoyageAIEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *VoyageAIEmbeddingProvider) CallEmbedding(ctx context.Context, texts []string, mode models.TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	var textType string
	if mode == models.InsertMode {
		textType = "document"
	} else {
		textType = "query"
	}

	embRet := models.NewEmbdResult(numRows, provider.embdType)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		r, err := provider.client.Embedding(provider.url, provider.modelName, texts[i:end], int(provider.embedDimParam), textType, provider.outputType, provider.truncate, provider.timeoutSec)
		if err != nil {
			return nil, err
		}
		if provider.embdType == models.Float32Embd {
			resp := r.(*voyageai.EmbeddingResponse[float32])
			if end-i != len(resp.Data) {
				return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp.Data))
			}

			for _, item := range resp.Data {
				if len(item.Embedding) != int(provider.fieldDim) {
					return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
						provider.fieldDim, len(item.Embedding))
				}
				embRet.Append(item.Embedding)
			}
		} else {
			resp := r.(*voyageai.EmbeddingResponse[int8])
			if end-i != len(resp.Data) {
				return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp.Data))
			}

			for _, item := range resp.Data {
				if len(item.Embedding) != int(provider.fieldDim) {
					return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
						provider.fieldDim, len(item.Embedding))
				}
				embRet.Append(item.Embedding)
			}
		}
	}
	if embRet.EmbdType == models.Float32Embd {
		return embRet.FloatEmbds, nil
	}
	return embRet.Int8Embds, nil
}
