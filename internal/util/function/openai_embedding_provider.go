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
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models/openai"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type OpenAIEmbeddingProvider struct {
	fieldDim int64

	client        openai.OpenAIEmbeddingInterface
	modelName     string
	embedDimParam int64
	user          string

	maxBatch   int
	timeoutSec int64
}

func createOpenAIEmbeddingClient(apiKey string, url string) (*openai.OpenAIEmbeddingClient, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials config or configure the %s environment variable in the Milvus service.", openaiAKEnvStr)
	}

	if url == "" {
		url = "https://api.openai.com/v1/embeddings"
	}

	c := openai.NewOpenAIEmbeddingClient(apiKey, url)
	return c, nil
}

func createAzureOpenAIEmbeddingClient(apiKey string, url string, resourceName string) (*openai.AzureOpenAIEmbeddingClient, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials config or configure the %s environment variable in the Milvus service", azureOpenaiAKEnvStr)
	}

	if url == "" {
		if resourceName == "" {
			resourceName = os.Getenv(azureOpenaiResourceName)
		}
		if resourceName != "" {
			url = fmt.Sprintf("https://%s.openai.azure.com", resourceName)
		}
	}
	if url == "" {
		return nil, fmt.Errorf("Must configure the %s environment variable in the Milvus service", azureOpenaiResourceName)
	}
	c := openai.NewAzureOpenAIEmbeddingClient(apiKey, url)
	return c, nil
}

func newOpenAIEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, isAzure bool, credentials *credentials.Credentials) (*OpenAIEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}

	var modelName, user string
	var dim int64
	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case modelNameParamKey:
			modelName = param.Value
		case dimParamKey:
			dim, err = parseAndCheckFieldDim(param.Value, fieldDim, fieldSchema.Name)
			if err != nil {
				return nil, err
			}
		case userParamKey:
			user = param.Value
		default:
		}
	}

	var c openai.OpenAIEmbeddingInterface
	if !isAzure {
		apiKey, url, err := parseAKAndURL(credentials, functionSchema.Params, params, openaiAKEnvStr)
		if err != nil {
			return nil, err
		}
		c, err = createOpenAIEmbeddingClient(apiKey, url)
		if err != nil {
			return nil, err
		}
	} else {
		apiKey, url, err := parseAKAndURL(credentials, functionSchema.Params, params, azureOpenaiAKEnvStr)
		if err != nil {
			return nil, err
		}
		resourceName := params["resource_name"]
		c, err = createAzureOpenAIEmbeddingClient(apiKey, url, resourceName)
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

func NewOpenAIEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.Credentials) (*OpenAIEmbeddingProvider, error) {
	return newOpenAIEmbeddingProvider(fieldSchema, functionSchema, params, false, credentials)
}

func NewAzureOpenAIEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.Credentials) (*OpenAIEmbeddingProvider, error) {
	return newOpenAIEmbeddingProvider(fieldSchema, functionSchema, params, true, credentials)
}

func (provider *OpenAIEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *OpenAIEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *OpenAIEmbeddingProvider) CallEmbedding(texts []string, _ TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(provider.modelName, texts[i:end], int(provider.embedDimParam), provider.user, provider.timeoutSec)
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
