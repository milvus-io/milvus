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
	"github.com/milvus-io/milvus/internal/util/function/models/cohere"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type CohereEmbeddingProvider struct {
	fieldDim int64

	client     *cohere.CohereClient
	url        string
	modelName  string
	truncate   string
	embdType   models.EmbeddingType
	outputType string

	maxBatch   int
	timeoutSec int64
	extraInfo  *models.ModelExtraInfo
}

func NewCohereEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.Credentials, extraInfo *models.ModelExtraInfo) (*CohereEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	apiKey, url, err := models.ParseAKAndURL(credentials, functionSchema.Params, params, models.CohereAIAKEnvStr, extraInfo)
	if err != nil {
		return nil, err
	}
	var modelName string
	truncate := "END"
	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		case models.TruncateParamKey:
			if param.Value != "NONE" && param.Value != "START" && param.Value != "END" {
				return nil, fmt.Errorf("Illegal parameters, %s only supports [NONE, START, END]", models.TruncateParamKey)
			}
			truncate = param.Value
		default:
		}
	}

	c, err := cohere.NewCohereClient(apiKey)
	if err != nil {
		return nil, err
	}

	if url == "" {
		url = "https://api.cohere.com/v2/embed"
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

	provider := CohereEmbeddingProvider{
		client:     c,
		url:        url,
		fieldDim:   fieldDim,
		modelName:  modelName,
		truncate:   truncate,
		embdType:   embdType,
		outputType: outputType,
		maxBatch:   96,
		timeoutSec: 30,
		extraInfo:  extraInfo,
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
func (provider *CohereEmbeddingProvider) getInputType(mode models.TextEmbeddingMode) string {
	// v2 models not support instructor
	if strings.HasSuffix(provider.modelName, "v2.0") {
		return ""
	}
	if mode == models.InsertMode {
		return "search_document" // Used for embeddings stored in a vector database for search use-cases.
	}
	return "search_query" // Used for embeddings of search queries run against a vector DB to find relevant documents.
}

func (provider *CohereEmbeddingProvider) CallEmbedding(ctx context.Context, texts []string, mode models.TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	inputType := provider.getInputType(mode)
	embRet := models.NewEmbdResult(numRows, provider.embdType)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}

		resp, err := provider.client.Embedding(provider.url, provider.modelName, texts[i:end], inputType, provider.outputType, provider.truncate, provider.timeoutSec)
		if err != nil {
			return nil, err
		}
		if provider.embdType == models.Float32Embd {
			if end-i != len(resp.Embeddings.Float) {
				return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp.Embeddings.Float))
			}
			for _, item := range resp.Embeddings.Float {
				if len(item) != int(provider.fieldDim) {
					return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
						provider.fieldDim, len(item))
				}
			}
			embRet.Append(resp.Embeddings.Float)
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
			embRet.Append(resp.Embeddings.Int8)
		}
	}

	if embRet.EmbdType == models.Float32Embd {
		return embRet.FloatEmbds, nil
	}
	return embRet.Int8Embds, nil
}
