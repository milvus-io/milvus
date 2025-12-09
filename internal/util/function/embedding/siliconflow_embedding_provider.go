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
	"github.com/milvus-io/milvus/internal/util/function/models/siliconflow"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type SiliconflowEmbeddingProvider struct {
	fieldDim int64

	client        *siliconflow.SiliconflowClient
	url           string
	modelName     string
	embedDimParam int64

	maxBatch   int
	timeoutSec int64
	extraInfo  *models.ModelExtraInfo
}

func NewSiliconflowEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, credentials *credentials.Credentials, extraInfo *models.ModelExtraInfo) (*SiliconflowEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}
	apiKey, url, err := models.ParseAKAndURL(credentials, functionSchema.Params, params, models.SiliconflowAKEnvStr, extraInfo)
	if err != nil {
		return nil, err
	}
	var modelName string

	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case models.ModelNameParamKey:
			modelName = param.Value
		default:
		}
	}

	c, err := siliconflow.NewSiliconflowClient(apiKey)
	if err != nil {
		return nil, err
	}

	if url == "" {
		url = "https://api.siliconflow.cn/v1/embeddings"
	}

	provider := SiliconflowEmbeddingProvider{
		client:     c,
		url:        url,
		fieldDim:   fieldDim,
		modelName:  modelName,
		maxBatch:   32,
		timeoutSec: 30,
		extraInfo:  extraInfo,
	}
	return &provider, nil
}

func (provider *SiliconflowEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *SiliconflowEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *SiliconflowEmbeddingProvider) CallEmbedding(ctx context.Context, texts []string, _ models.TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		resp, err := provider.client.Embedding(provider.url, provider.modelName, texts[i:end], "float", provider.timeoutSec)
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
