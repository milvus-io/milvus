// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package sglang

import (
	"context"
	"fmt"

	"github.com/milvus-io/milvus/internal/util/function/models"
)

type Client struct {
	apiKey   string
	endpoint string
}

func NewClient(apiKey string, endpoint string) *Client {
	return &Client{apiKey: apiKey, endpoint: endpoint}
}

type RerankResult struct {
	Index *int     `json:"index"`
	Score *float32 `json:"score"`
}

func (c *Client) Rerank(ctx context.Context, model string, query string, documents []string, timeoutMs int64) ([]RerankResult, error) {
	base, err := models.NewBaseURL(c.endpoint)
	if err != nil {
		return nil, err
	}
	base.Path = "/v1/rerank"

	headers := map[string]string{"Content-Type": "application/json"}
	if c.apiKey != "" {
		headers["Authorization"] = fmt.Sprintf("Bearer %s", c.apiKey)
	}
	request := map[string]any{
		"model":     model,
		"query":     query,
		"documents": documents,
	}
	response, err := models.PostRequestWithContext[[]RerankResult](ctx, request, base.String(), headers, timeoutMs)
	if err != nil {
		return nil, err
	}
	return *response, nil
}
