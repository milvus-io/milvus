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
	"github.com/milvus-io/milvus/internal/models/openai"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

type OpenAIEmbeddingProvider struct {
	fieldDim int64

	client        openai.OpenAIEmbeddingInterface
	modelName     string
	embedDimParam int64
	user          string

	maxBatch   int
	timeoutSec int
}

func createOpenAIEmbeddingClient(apiKey string, url string) (*openai.OpenAIEmbeddingClient, error) {
	if apiKey == "" {
		apiKey = os.Getenv("OPENAI_API_KEY")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials. Please pass `api_key`, or configure the OPENAI_API_KEY environment variable in the Milvus service.")
	}

	if url == "" {
		url = "https://api.openai.com/v1/embeddings"
	}
	if url == "" {
		return nil, fmt.Errorf("Must provide `url` arguments or configure the OPENAI_ENDPOINT environment variable in the Milvus service")
	}

	c := openai.NewOpenAIEmbeddingClient(apiKey, url)
	return c, nil
}

func createAzureOpenAIEmbeddingClient(apiKey string, url string) (*openai.AzureOpenAIEmbeddingClient, error) {
	if apiKey == "" {
		apiKey = os.Getenv("AZURE_OPENAI_API_KEY")
	}
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials. Please pass `api_key`, or configure the AZURE_OPENAI_API_KEY environment variable in the Milvus service")
	}

	if url == "" {
		url = os.Getenv("AZURE_OPENAI_ENDPOINT")
	}
	if url == "" {
		return nil, fmt.Errorf("Must provide `url` arguments or configure the AZURE_OPENAI_ENDPOINT environment variable in the Milvus service")
	}
	c := openai.NewAzureOpenAIEmbeddingClient(apiKey, url)
	return c, nil
}

func newOpenAIEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, isAzure bool) (*OpenAIEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	var apiKey, url, modelName, user string
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
				return nil, fmt.Errorf("Field %s's dim is [%d], but embeding's dim is [%d]", fieldSchema.Name, fieldDim, dim)
			}
		case userParamKey:
			user = param.Value
		case apiKeyParamKey:
			apiKey = param.Value
		case embeddingUrlParamKey:
			url = param.Value
		default:
		}
	}

	var c openai.OpenAIEmbeddingInterface
	if !isAzure {
		if modelName != TextEmbeddingAda002 && modelName != TextEmbedding3Small && modelName != TextEmbedding3Large {
			return nil, fmt.Errorf("Unsupported model: %s, only support [%s, %s, %s]",
				modelName, TextEmbeddingAda002, TextEmbedding3Small, TextEmbedding3Large)
		}

		c, err = createOpenAIEmbeddingClient(apiKey, url)
		if err != nil {
			return nil, err
		}
	} else {
		c, err = createAzureOpenAIEmbeddingClient(apiKey, url)
		if err != nil {
			return nil, err
		}
	}

	provider := OpenAIEmbeddingProvider{
		client:        c,
		fieldDim:      fieldDim,
		modelName:     modelName,
		user:          user,
		embedDimParam: dim,
		maxBatch:      128,
		timeoutSec:    30,
	}
	return &provider, nil
}

func NewOpenAIEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema) (*OpenAIEmbeddingProvider, error) {
	return newOpenAIEmbeddingProvider(fieldSchema, functionSchema, false)
}

func NewAzureOpenAIEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema) (*OpenAIEmbeddingProvider, error) {
	return newOpenAIEmbeddingProvider(fieldSchema, functionSchema, true)
}

func (provider *OpenAIEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *OpenAIEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *OpenAIEmbeddingProvider) CallEmbedding(texts []string, batchLimit bool) ([][]float32, error) {
	numRows := len(texts)
	if batchLimit && numRows > provider.MaxBatch() {
		return nil, fmt.Errorf("OpenAI embedding supports up to [%d] pieces of data at a time, got [%d]", provider.MaxBatch(), numRows)
	}

	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(provider.modelName, texts[i:end], int(provider.embedDimParam), provider.user, time.Duration(provider.timeoutSec))
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
