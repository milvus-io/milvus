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
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models/voyageai"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type VoyageAIEmbeddingProvider struct {
	fieldDim int64

	client        *voyageai.VoyageAIEmbedding
	modelName     string
	embedDimParam int64
	truncate      bool
	embdType      embeddingType
	outputType    string

	maxBatch   int
	timeoutSec int64
}

func createVoyageAIEmbeddingClient(apiKey string, url string) (*voyageai.VoyageAIEmbedding, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials config or configure the %s environment variable in the Milvus service.", voyageAIAKEnvStr)
	}

	if url == "" {
		url = "https://api.voyageai.com/v1/embeddings"
	}

	c := voyageai.NewVoyageAIEmbeddingClient(apiKey, url)
	return c, nil
}

func NewVoyageAIEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.CredentialsManager) (*VoyageAIEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	apiKey, url, err := parseAKAndURL(credentials, functionSchema.Params, params, voyageAIAKEnvStr)
	if err != nil {
		return nil, err
	}
	var modelName string
	dim := int64(0)
	truncate := false

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
		case truncationParamKey:
			if truncate, err = strconv.ParseBool(param.Value); err != nil {
				return nil, fmt.Errorf("[%s param's value: %s] is invalid, only supports: [true/false]", truncationParamKey, param.Value)
			}
		default:
		}
	}

	c, err := createVoyageAIEmbeddingClient(apiKey, url)
	if err != nil {
		return nil, err
	}

	embdType := getEmbdType(fieldSchema.DataType)
	if embdType == unsupportEmbd {
		return nil, fmt.Errorf("Unsupport output type: %s", fieldSchema.DataType)
	}

	outputType := func() string {
		if embdType == float32Embd {
			return "float"
		}
		return "int8"
	}()

	provider := VoyageAIEmbeddingProvider{
		client:        c,
		fieldDim:      fieldDim,
		modelName:     modelName,
		truncate:      truncate,
		embedDimParam: dim,
		embdType:      embdType,
		outputType:    outputType,
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

func (provider *VoyageAIEmbeddingProvider) CallEmbedding(texts []string, mode TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	var textType string
	if mode == InsertMode {
		textType = "document"
	} else {
		textType = "query"
	}

	embRet := newEmbdResult(numRows, provider.embdType)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		r, err := provider.client.Embedding(provider.modelName, texts[i:end], int(provider.embedDimParam), textType, provider.outputType, provider.truncate, provider.timeoutSec)
		if err != nil {
			return nil, err
		}
		if provider.embdType == float32Embd {
			resp := r.(*voyageai.EmbeddingResponse[float32])
			if end-i != len(resp.Data) {
				return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp.Data))
			}

			for _, item := range resp.Data {
				if len(item.Embedding) != int(provider.fieldDim) {
					return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
						provider.fieldDim, len(item.Embedding))
				}
				embRet.append(item.Embedding)
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
				embRet.append(item.Embedding)
			}
		}
	}
	if embRet.eType == float32Embd {
		return embRet.floatEmbds, nil
	}
	return embRet.int8Embds, nil
}
