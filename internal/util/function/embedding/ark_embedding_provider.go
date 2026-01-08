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
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/ark"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

const (
	arkDefaultURL           = "https://ark.cn-beijing.volces.com/api/v3"
	arkDefaultBatchSize     = 128
	arkMultimodalBatchSize  = 1
)

// ArkEmbeddingProvider implements the text embedding provider for Volcengine Ark/Doubao models.
//
// Supported function schema parameters:
//   - model_name: (required) The Ark model endpoint ID (e.g.,  "ep-20250101012345-uvxyz").
//   - user: (optional) A unique identifier for the end-user (only works for "text" model_type).
//   - dim: (optional) The output embedding dimension. If not set, uses the model's default.
//   - model_type: (optional) Set to "multimodal" to use the multimodal embedding API.
//     When set to "multimodal", batch size is limited to 1 due to SDK limitations.
//   - url: (optional) Custom API endpoint URL. Defaults to "https://ark.cn-beijing.volces.com/api/v3".
//   - api_key: (optional) API key for authentication. Can also be set via MILVUS_ARK_API_KEY env var.
//
// Example usage in function schema:
//
//	params: [
//	  {key: "provider", value: "ark"},
//	  {key: "model_name", value: "ep-20250101012345-uvxyz"},
//	  {key: "model_type", value: "text"},
//	]
type ArkEmbeddingProvider struct {
	fieldDim int64

	client       *ark.ArkClient
	modelName    string
	user         string
	dim          int
	isMultimodal bool

	maxBatch  int
	extraInfo *models.ModelExtraInfo
}

// NewArkEmbeddingProvider creates a new ArkEmbeddingProvider instance.
// It parses the function schema parameters and initializes the Ark SDK client.
func NewArkEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.Credentials, extraInfo *models.ModelExtraInfo) (*ArkEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	apiKey, url, err := models.ParseAKAndURL(credentials, functionSchema.Params, params, models.ArkAKEnvStr, extraInfo)
	if err != nil {
		return nil, err
	}
	var modelName string
	var user string
	var dim int
	var isMultimodal bool

	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		case "user":
			user = param.Value
		case "dim":
			var err error
			dim, err = strconv.Atoi(param.Value)
			if err != nil {
				return nil, fmt.Errorf("invalid 'dim' parameter value %q: expected integer: %w", param.Value, err)
			}
			if dim < 0 {
				return nil, fmt.Errorf("invalid 'dim' parameter value %d: must be non-negative", dim)
			}
		case "model_type":
			if strings.ToLower(param.Value) == "multimodal" {
				isMultimodal = true
			}
		default:
		}
	}

	if url == "" {
		url = arkDefaultURL
	}

	c, err := ark.NewArkClient(apiKey, url)
	if err != nil {
		return nil, err
	}

	maxBatch := arkDefaultBatchSize
	if isMultimodal {
		maxBatch = arkMultimodalBatchSize
	}

	provider := ArkEmbeddingProvider{
		client:       c,
		fieldDim:     fieldDim,
		modelName:    modelName,
		user:         user,
		dim:          dim,
		maxBatch:     maxBatch,
		extraInfo:    extraInfo,
		isMultimodal: isMultimodal,
	}
	return &provider, nil
}

func (provider *ArkEmbeddingProvider) MaxBatch() int {
	return provider.extraInfo.BatchFactor * provider.maxBatch
}

func (provider *ArkEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *ArkEmbeddingProvider) CallEmbedding(ctx context.Context, texts []string, _ models.TextEmbeddingMode) (any, error) {
	if provider.maxBatch <= 0 {
		return nil, fmt.Errorf("internal error: invalid maxBatch value %d, must be positive", provider.maxBatch)
	}

	numRows := len(texts)
	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(ctx, provider.modelName, texts[i:end], provider.dim, provider.user, provider.isMultimodal)
		if err != nil {
			return nil, err
		}
		if end-i != len(resp) {
			return nil, fmt.Errorf("Get embedding failed. The number of texts and embeddings does not match text:[%d], embedding:[%d]", end-i, len(resp))
		}
		for _, item := range resp {
			if len(item.Embedding) != int(provider.fieldDim) {
				return nil, fmt.Errorf("The required embedding dim is [%d], but the embedding obtained from the model is [%d]",
					provider.fieldDim, len(item.Embedding))
			}
			data = append(data, item.Embedding)
		}
	}
	return data, nil
}
