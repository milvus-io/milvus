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
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/zilliz"
	"github.com/milvus-io/milvus/pkg/v2/util/typeutil"
)

type ZillizEmbeddingProvider struct {
	fieldDim int64

	client    *zilliz.ZillizClient
	modelName string

	maxBatch    int
	modelParams map[string]string
	extraInfo   *models.ModelExtraInfo
}

func NewZillizEmbeddingProvider(fieldSchema *schemapb.FieldSchema, functionSchema *schemapb.FunctionSchema, params map[string]string, extraInfo *models.ModelExtraInfo) (*ZillizEmbeddingProvider, error) {
	fieldDim, err := typeutil.GetDim(fieldSchema)
	if err != nil {
		return nil, err
	}

	var modelDeploymentID string
	modelParams := map[string]string{}
	for _, param := range functionSchema.Params {
		switch strings.ToLower(param.Key) {
		case models.ModelDeploymentIDKey:
			modelDeploymentID = param.Value
		default:
			modelParams[param.Key] = param.Value
		}
	}

	c, err := zilliz.NewZilliClient(modelDeploymentID, extraInfo.ClusterID, extraInfo.DBName, params)
	if err != nil {
		return nil, err
	}

	maxBatch := 64
	provider := ZillizEmbeddingProvider{
		client:      c,
		fieldDim:    fieldDim,
		maxBatch:    maxBatch,
		modelParams: modelParams,
		extraInfo:   extraInfo,
	}
	return &provider, nil
}

func (provider *ZillizEmbeddingProvider) MaxBatch() int {
	return 5 * provider.maxBatch
}

func (provider *ZillizEmbeddingProvider) FieldDim() int64 {
	return provider.fieldDim
}

func (provider *ZillizEmbeddingProvider) CallEmbedding(ctx context.Context, texts []string, mode models.TextEmbeddingMode) (any, error) {
	numRows := len(texts)
	if mode == models.SearchMode {
		provider.modelParams["input_type"] = "query"
	} else {
		provider.modelParams["input_type"] = "document"
	}

	data := make([][]float32, 0, numRows)
	for i := 0; i < numRows; i += provider.maxBatch {
		end := i + provider.maxBatch
		if end > numRows {
			end = numRows
		}
		embds, err := provider.client.Embedding(ctx, texts[i:end], provider.modelParams)
		if err != nil {
			return nil, err
		}
		data = append(data, embds...)
	}
	return data, nil
}
