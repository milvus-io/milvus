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
	"github.com/milvus-io/milvus/internal/util/function/models/cohere"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type CohereEmbeddingProvider struct {
	fieldDim int64

	client     *cohere.CohereEmbedding
	modelName  string
	truncate   string
	embdType   embeddingType
	outputType string

	maxBatch   int
	timeoutSec int64
}

func createCohereEmbeddingClient(apiKey string, url string) (*cohere.CohereEmbedding, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials. Please pass `api_key`, or configure the %s environment variable in the Milvus service.", cohereAIAKEnvStr)
	}

	if url == "" {
		url = "https://api.cohere.com/v2/embed"
	}

	c := cohere.NewCohereEmbeddingClient(apiKey, url)
	return c, nil
}

func NewCohereEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string) (*CohereEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	apiKey, url := parseAKAndURL(functionSchema.Params, params, cohereAIAKEnvStr)
	var modelName string
	truncate := "END"
	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case modelNameParamKey:
			modelName = param.Value
		case truncateParamKey:
			if param.Value != "NONE" && param.Value != "START" && param.Value != "END" {
				return nil, fmt.Errorf("Illegal parameters, %s only supports [NONE, START, END]", truncateParamKey)
			}
			truncate = param.Value
		default:
		}
	}

	c, err := createCohereEmbeddingClient(apiKey, url)
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

	provider := CohereEmbeddingProvider{
		client:     c,
		fieldDim:   fieldDim,
		modelName:  modelName,
		truncate:   truncate,
		embdType:   embdType,
		outputType: outputType,
		maxBatch:   96,
		timeoutSec: 30,
	}
	return &provider, nil
}

func (provider *CohereEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *CohereEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

// Specifies the type of input passed to the model. Required for embedding models v3 and higher.
func (provider *CohereEmbeddingProvider) getInputType(mode TextEmbeddingMode) string {
	// v2 models not support instructor
	if strings.HasSuffix(provider.modelName, "v2.0") {
		return ""
	}
	if mode == InsertMode {
		return "search_document" // Used for embeddings stored in a vector database for search use-cases.
	}
	return "search_query" // Used for embeddings of search queries run against a vector DB to find relevant documents.
}

func (provider *CohereEmbeddingProvider) CallEmbedding(texts []string, mode TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	inputType := provider.getInputType(mode)
	embRet := newEmbdResult(numRows, provider.embdType)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}

		resp, err := provider.client.Embedding(provider.modelName, texts[i:end], inputType, provider.outputType, provider.truncate, provider.timeoutSec)
		if err != nil {
			return nil, err
		}
		if provider.embdType == float32Embd {
			if end-i != len(resp.Embeddings.Float) {
				return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp.Embeddings.Float))
			}
			for _, item := range resp.Embeddings.Float {
				if len(item) != int(provider.fieldDim) {
					return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
						provider.fieldDim, len(item))
				}
			}
			embRet.append(resp.Embeddings.Float)
		} else {
			if end-i != len(resp.Embeddings.Int8) {
				return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp.Embeddings.Int8))
			}
			for _, item := range resp.Embeddings.Int8 {
				if len(item) != int(provider.fieldDim) {
					return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
						provider.fieldDim, len(item))
				}
			}
			embRet.append(resp.Embeddings.Int8)
		}
	}

	if embRet.eType == float32Embd {
		return embRet.floatEmbds, nil
	}
	return embRet.int8Embds, nil
}
