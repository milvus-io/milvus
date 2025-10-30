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

package rerank

import (
	"context"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/zilliz"
)

type zillzProvider struct {
	baseProvider
	client *zilliz.ZillizClient
	params map[string]string
}

func newZillizProvider(params []*commonpb.KeyValuePair, conf map[string]string, extraInfo *models.ModelExtraInfo) (modelProvider, error) {
	var modelDeploymentID string
	var err error
	maxBatch := 64
	modelParams := map[string]string{}
	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case models.ModelDeploymentIDKey:
			modelDeploymentID = param.Value
		case models.MaxClientBatchSizeParamKey:
			if maxBatch, err = parseMaxBatch(param.Value); err != nil {
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

	provider := zillzProvider{
		baseProvider: baseProvider{batchSize: maxBatch},
		client:       c,
		params:       modelParams,
	}
	return &provider, nil
}

func (provider *zillzProvider) rerank(ctx context.Context, query string, docs []string) ([]float32, error) {
	return provider.client.Rerank(ctx, query, docs, provider.params)
}
