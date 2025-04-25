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
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models/ali"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type AliEmbeddingProvider struct {
	fieldDim int64

	client        *ali.AliDashScopeEmbedding
	modelName     string
	embedDimParam int64
	outputType    string

	maxBatch   int
	timeoutSec int64
}

func createAliEmbeddingClient(apiKey string, url string) (*ali.AliDashScopeEmbedding, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials config or configure the %s environment variable in the Milvus service.", dashscopeAKEnvStr)
	}

	if url == "" {
		url = "https://dashscope.aliyuncs.com/api/v1/services/embeddings/text-embedding/text-embedding"
	}
	c := ali.NewAliDashScopeEmbeddingClient(apiKey, url)
	return c, nil
}

func NewAliDashScopeEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.CredentialsManager) (*AliEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	apiKey, url, err := parseAKAndURL(credentials, functionSchema.Params, params, dashscopeAKEnvStr)
	if err != nil {
		return nil, err
	}
	var modelName string
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
		default:
		}
	}

	c, err := createAliEmbeddingClient(apiKey, url)
	if err != nil {
		return nil, err
	}

	maxBatch := 25
	if modelName == "text-embedding-v3" {
		maxBatch = 6
	}

	provider := AliEmbeddingProvider{
		client:        c,
		fieldDim:      fieldDim,
		modelName:     modelName,
		embedDimParam: dim,
		// TextEmbedding only supports dense embedding
		outputType: "dense",
		maxBatch:   maxBatch,
		timeoutSec: 30,
	}
	return &provider, nil
}

func (provider *AliEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *AliEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *AliEmbeddingProvider) CallEmbedding(texts []string, mode TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	var textType string
	if mode == SearchMode {
		textType = "query"
	} else {
		textType = "document"
	}
	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(provider.modelName, texts[i:end], int(provider.embedDimParam), textType, provider.outputType, provider.timeoutSec)
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
