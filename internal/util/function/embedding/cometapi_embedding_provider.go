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
	"fmt"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/cometapi"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type CometAPIEmbeddingProvider struct {
	fieldDim int64

	client     *cometapi.CometAPIClient
	url        string
	modelName  string
	embdType   models.EmbeddingType
	outputType string

	maxBatch   int
	timeoutSec int64
}

func NewCometAPIEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.Credentials) (*CometAPIEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	apiKey, url, err := models.ParseAKAndURL(credentials, functionSchema.Params, params, models.CometAPIAKEnvStr)
	if err != nil {
		return nil, err
	}
	var modelName string
	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		default:
		}
	}

	c, err := cometapi.NewCometAPIClient(apiKey)
	if err != nil {
		return nil, err
	}

	if url == "" {
		url = "https://api.cometapi.com/v1/embeddings"
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

	provider := CometAPIEmbeddingProvider{
		client:     c,
		url:        url,
		fieldDim:   fieldDim,
		modelName:  modelName,
		embdType:   embdType,
		outputType: outputType,
		maxBatch:   128,
		timeoutSec: 30,
	}
	return &provider, nil
}

func (provider *CometAPIEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *CometAPIEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *CometAPIEmbeddingProvider) CallEmbedding(texts []string, _ models.TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	embRet := models.NewEmbdResult(numRows, provider.embdType)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}

		resp, err := provider.client.Embedding(provider.url, provider.modelName, texts[i:end], provider.timeoutSec)
		if err != nil {
			return nil, err
		}
		if end-i != len(resp.Data) {
			return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp.Data))
		}

		if provider.embdType == models.Float32Embd {
			floatEmbeddings := make([][]float32, 0, len(resp.Data))
			for _, item := range resp.Data {
				if len(item.Embedding) != int(provider.fieldDim) {
					return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
						provider.fieldDim, len(item.Embedding))
				}
				floatEmbeddings = append(floatEmbeddings, item.Embedding)
			}
			embRet.Append(floatEmbeddings)
		} else {
			// For int8 embeddings, we need to convert float32 to int8
			int8Embeddings := make([][]int8, 0, len(resp.Data))
			for _, item := range resp.Data {
				if len(item.Embedding) != int(provider.fieldDim) {
					return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
						provider.fieldDim, len(item.Embedding))
				}
				// Convert float32 to int8
				int8Embedding := make([]int8, len(item.Embedding))
				for j, val := range item.Embedding {
					// Scale and clamp to int8 range [-128, 127]
					scaled := int(val * 127)
					if scaled > 127 {
						scaled = 127
					} else if scaled < -128 {
						scaled = -128
					}
					int8Embedding[j] = int8(scaled)
				}
				int8Embeddings = append(int8Embeddings, int8Embedding)
			}
			embRet.Append(int8Embeddings)
		}
	}

	if embRet.EmbdType == models.Float32Embd {
		return embRet.FloatEmbds, nil
	}
	return embRet.Int8Embds, nil
}
