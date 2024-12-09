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

package function

import (
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/models/ali"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type AliEmbeddingProvider struct {
	fieldDim int64

	client        *ali.AliDashScopeEmbedding
	modelName     string
	embedDimParam int64

	maxBatch   int
	timeoutSec int
}

func createAliEmbeddingClient(apiKey string, url string) (*ali.AliDashScopeEmbedding, error) {
	if apiKey == "" {
		apiKey = os.Getenv("DASHSCOPE_API_KEY")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials. Please pass `api_key`, or configure the DASHSCOPE_API_KEY environment variable in the Milvus service.")
	}

	if url == "" {
		url = "https://dashscope.aliyuncs.com/api/v1/services/embeddings/text-embedding/text-embedding"
	}
	if url == "" {
		return nil, fmt.Errorf("Must provide `url` arguments or configure the DASHSCOPE_ENDPOINT environment variable in the Milvus service")
	}

	c := ali.NewAliDashScopeEmbeddingClient(apiKey, url)
	return c, nil
}

func NewAliDashScopeEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema) (*AliEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	var apiKey, url, modelName string
	var dim int64

	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case modelNameParamKey:
			modelName = param.Value
		case dimParamKey:
			dim, err = strconv.ParseInt(param.Value, 10, 64)
			if err != nil {
				return nil, fmt.Errorf("dim [%s] is not int", param.Value)
			}

			if dim != 0 && dim != fieldDim {
				return nil, fmt.Errorf("Field %s's dim is [%d], but embeding's dim is [%d]", functionSchema.Name, fieldDim, dim)
			}
		case apiKeyParamKey:
			apiKey = param.Value
		case embeddingUrlParamKey:
			url = param.Value
		default:
		}
	}

	if modelName != TextEmbeddingV1 && modelName != TextEmbeddingV2 && modelName != TextEmbeddingV3 {
		return nil, fmt.Errorf("Unsupported model: %s, only support [%s, %s, %s]",
			modelName, TextEmbeddingV1, TextEmbeddingV2, TextEmbeddingV3)
	}
	c, err := createAliEmbeddingClient(apiKey, url)
	if err != nil {
		return nil, err
	}
	provider := AliEmbeddingProvider{
		client:        c,
		fieldDim:      fieldDim,
		modelName:     modelName,
		embedDimParam: dim,
		maxBatch:      25,
		timeoutSec:    30,
	}
	return &provider, nil
}

func (provider *AliEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *AliEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *AliEmbeddingProvider) CallEmbedding(texts []string, batchLimit bool) ([][]float32, error) {
	numRows := len(texts)
	if batchLimit && numRows > provider.MaxBatch() {
		return nil, fmt.Errorf("Ali text embedding supports up to [%d] pieces of data at a time, got [%d]", provider.MaxBatch(), numRows)
	}

	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(provider.modelName, texts[i:end], int(provider.embedDimParam), "query", "dense", time.Duration(provider.timeoutSec))
		if err != nil {
			return nil, err
		}
		if end-i != len(resp.Output.Embeddings) {
			return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp.Output.Embeddings))
		}
		for _, item := range resp.Output.Embeddings {
			if len(item.Embedding) != int(provider.fieldDim) {
				return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
					provider.fieldDim, len(item.Embedding))
			}
			data = append(data, item.Embedding)
		}
	}
	return data, nil
}
