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

package highlight

import (
	"context"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/zilliz"
)

type zillizHighlightProvider struct {
	baseSemanticHighlightProvider
	client    *zilliz.ZillizClient
	modelName string
	queries   []string

	modelParams map[string]string
}

func newZillizHighlightProvider(params []*commonpb.KeyValuePair, conf map[string]string, extraInfo *models.ModelExtraInfo) (semanticHighlightProvider, error) {
	var modelDeploymentID string
	var err error
	maxBatch := 64
	modelParams := map[string]string{}
	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case models.ModelDeploymentIDKey:
			modelDeploymentID = param.Value
		case models.MaxClientBatchSizeParamKey:
			if maxBatch, err = strconv.Atoi(param.Value); err != nil {
				return nil, err
			}

		default:
			modelParams[param.Key] = param.Value
		}
	}

	c, err := zilliz.NewZilliClient(modelDeploymentID, extraInfo.ClusterID, extraInfo.DBName, conf)
	if err != nil {
		return nil, err
	}

	provider := zillizHighlightProvider{
		baseSemanticHighlightProvider: baseSemanticHighlightProvider{batchSize: maxBatch},
		client:                        c,
		modelParams:                   modelParams,
	}
	return &provider, nil
}

func (h *zillizHighlightProvider) highlight(ctx context.Context, query string, texts []string) ([][]string, error) {
	highlights, err := h.client.Highlight(ctx, query, texts, h.modelParams)
	if err != nil {
		return nil, err
	}
	return highlights, nil
}
