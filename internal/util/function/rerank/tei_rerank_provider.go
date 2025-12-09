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
	"fmt"
	"strconv"
	"strings"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/tei"
)

type teiProvider struct {
	baseProvider
	client *tei.TEIClient
	params map[string]any
}

func newTeiProvider(params []*commonpb.KeyValuePair, conf map[string]string, credentials *credentials.Credentials) (modelProvider, error) {
	apiKey, _, err := models.ParseAKAndURL(credentials, params, conf, "", &models.ModelExtraInfo{})
	if err != nil {
		return nil, err
	}

	var endpoint string
	// TEI default client batch size
	maxBatch := 32
	truncateParams := map[string]any{}

	for _, param := range params {
		switch strings.ToLower(param.Key) {
		case models.EndpointParamKey:
			endpoint = param.Value
		case models.MaxClientBatchSizeParamKey:
			if maxBatch, err = parseMaxBatch(param.Value); err != nil {
				return nil, err
			}
		case models.TruncateParamKey:
			if teiTrun, err := strconv.ParseBool(param.Value); err != nil {
				return nil, fmt.Errorf("Rerank params error, %s: %s is not bool type", models.TruncateParamKey, param.Value)
			} else {
				truncateParams[models.TruncateParamKey] = teiTrun
			}
		case models.TruncationDirectionParamKey:
			if truncationDirection := param.Value; truncationDirection != "Left" && truncationDirection != "Right" {
				return nil, fmt.Errorf("[%s param's value: %s] is not invalid, only supports [Left/Right]", models.TruncationDirectionParamKey, param.Value)
			} else {
				truncateParams[models.TruncationDirectionParamKey] = truncationDirection
			}
		default:
		}
	}

	client, err := tei.NewTEIClient(apiKey, endpoint)
	if err != nil {
		return nil, err
	}

	provider := teiProvider{
		baseProvider: baseProvider{batchSize: maxBatch},
		client:       client,
		params:       truncateParams,
	}
	return &provider, nil
}

func (provider *teiProvider) rerank(ctx context.Context, query string, docs []string) ([]float32, error) {
	rerankResp, err := provider.client.Rerank(query, docs, provider.params, 30)
	if err != nil {
		return nil, err
	}
	scores := make([]float32, len(docs))
	for i, result := range *rerankResp {
		scores[i] = result.Score
	}
	return scores, nil
}
