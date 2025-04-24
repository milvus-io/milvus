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

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models/tei"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type TeiEmbeddingProvider struct {
	fieldDim int64

	client *tei.TEIEmbedding

	ingestionPrompt     string
	searchPrompt        string
	truncate            bool
	truncationDirection string

	maxBatch   int
	timeoutSec int64
}

func createTEIEmbeddingClient(apiKey string, endpoint string) (*tei.TEIEmbedding, error) {
	enable := os.Getenv(enableTeiEnvStr)
	if strings.ToLower(enable) == "false" {
		return nil, errors.New("TEI model serving is not enabled")
	}

	return tei.NewTEIEmbeddingClient(apiKey, endpoint)
}

func NewTEIEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.CredentialsManager) (*TeiEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	var endpoint, ingestionPrompt, searchPrompt string
	// TEI default client batch size
	maxBatch := 32
	truncate := false
	// TEI default is right
	truncationDirection := ""

	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case endpointParamKey:
			endpoint = param.Value
		case ingestionPromptParamKey:
			ingestionPrompt = param.Value
		case searchPromptParamKey:
			searchPrompt = param.Value
		case maxClientBatchSizeParamKey:
			if maxBatch, err = strconv.Atoi(param.Value); err != nil {
				return nil, fmt.Errorf("[%s param's value: %s] is not a valid number", maxClientBatchSizeParamKey, param.Value)
			}
		case truncationDirectionParamKey:
			if truncationDirection = param.Value; truncationDirection != "Left" && truncationDirection != "Right" {
				return nil, fmt.Errorf("[%s param's value: %s] is not invalid, only supports [Left/Right]", truncationDirectionParamKey, param.Value)
			}
		case truncateParamKey:
			if truncate, err = strconv.ParseBool(param.Value); err != nil {
				return nil, fmt.Errorf("[%s param's value: %s] is invalid, only supports: [true/false]", truncateParamKey, param.Value)
			}
		default:
		}
	}

	apiKey, _, err := parseAKAndURL(credentials, functionSchema.Params, params, "")
	if err != nil {
		return nil, err
	}
	c, err := createTEIEmbeddingClient(apiKey, endpoint)
	if err != nil {
		return nil, err
	}

	provider := TeiEmbeddingProvider{
		client:   c,
		fieldDim: fieldDim,

		ingestionPrompt:     ingestionPrompt,
		searchPrompt:        searchPrompt,
		truncationDirection: truncationDirection,
		maxBatch:            maxBatch,
		truncate:            truncate,
		timeoutSec:          30,
	}
	return &provider, nil
}

func (provider *TeiEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *TeiEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *TeiEmbeddingProvider) CallEmbedding(texts []string, mode TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	data := make([][]float32, 0, numRows)
	var prompt string
	if mode == InsertMode {
		prompt = provider.ingestionPrompt
	} else {
		prompt = provider.searchPrompt
	}

	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(texts[i:end], provider.truncate, provider.truncationDirection, prompt, provider.timeoutSec)
		if err != nil {
			return nil, err
		}
		if end-i != len(resp) {
			return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp))
		}
		for _, item := range resp {
			if len(item) != int(provider.fieldDim) {
				return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
					provider.fieldDim, len(item))
			}
			data = append(data, item)
		}
	}
	return data, nil
}
