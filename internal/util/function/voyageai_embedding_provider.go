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
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/models/voyageai"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type VoyageAIEmbeddingProvider struct {
	fieldDim int64

	client        *voyageai.VoyageAIEmbedding
	modelName     string
	embedDimParam int64

	maxBatch   int
	timeoutSec int64
}

func createVoyageAIEmbeddingClient(apiKey string, url string) (*voyageai.VoyageAIEmbedding, error) {
	if apiKey == "" {
		apiKey = os.Getenv(voyageAIAKEnvStr)
	}
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials. Please pass `api_key`, or configure the %s environment variable in the Milvus service.", voyageAIAKEnvStr)
	}

	if url == "" {
		url = "https://api.voyageai.com/v1/embeddings"
	}

	c := voyageai.NewVoyageAIEmbeddingClient(apiKey, url)
	return c, nil
}

func NewVoyageAIEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema) (*VoyageAIEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	var apiKey, url, modelName string
	dim := int64(0)

	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case modelNameParamKey:
			modelName = param.Value
		case dimParamKey:
			// Only voyage-3-large and voyage-code-3 support dim param: 1024 (default), 256, 512, 2048
			dim, err = parseAndCheckFieldDim(param.Value, fieldDim, fieldSchema.Name)
			if err != nil {
				return nil, err
			}
		case apiKeyParamKey:
			apiKey = param.Value
		case embeddingURLParamKey:
			url = param.Value
		default:
		}
	}

	if modelName != voyage3Large && modelName != voyage3 && modelName != voyage3Lite && modelName != voyageCode3 && modelName != voyageFinance2 && modelName != voyageLaw2 && modelName != voyageCode2 {
		return nil, fmt.Errorf("Unsupported model: %s, only support [%s, %s, %s, %s, %s, %s, %s]",
			modelName, voyage3Large, voyage3, voyage3Lite, voyageCode3, voyageFinance2, voyageLaw2, voyageCode2)
	}

	if dim != 0 {
		if modelName != voyage3Large && modelName != voyageCode3 {
			return nil, fmt.Errorf("VoyageAI text embedding model: [%s] doesn't supports dim parameter, only [%s, %s] support it.", modelName, voyage3, voyageCode3)
		}
		if dim != 1024 && dim != 256 && dim != 512 && dim != 2048 {
			return nil, fmt.Errorf("VoyageAI text embedding model's dim only supports 2048, 1024 (default), 512, and 256.")
		}
	}
	c, err := createVoyageAIEmbeddingClient(apiKey, url)
	if err != nil {
		return nil, err
	}

	provider := VoyageAIEmbeddingProvider{
		client:        c,
		fieldDim:      fieldDim,
		modelName:     modelName,
		embedDimParam: dim,
		maxBatch:      128,
		timeoutSec:    30,
	}
	return &provider, nil
}

func (provider *VoyageAIEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *VoyageAIEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *VoyageAIEmbeddingProvider) CallEmbedding(texts []string, mode TextEmbeddingMode) ([][]float32, error) {
	numRows := len(texts)
	var textType string
	if mode == InsertMode {
		textType = "document"
	} else {
		textType = "query"
	}

	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(provider.modelName, texts[i:end], int(provider.embedDimParam), textType, "float", provider.timeoutSec)
		if err != nil {
			return nil, err
		}
		if end-i != len(resp.Data) {
			return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp.Data))
		}
		for _, item := range resp.Data {
			if len(item.Embedding) != int(provider.fieldDim) {
				return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
					provider.fieldDim, len(item.Embedding))
			}
			data = append(data, item.Embedding)
		}
	}
	return data, nil
}
