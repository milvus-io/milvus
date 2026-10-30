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

// Client implements SGLang's /v1/rerank protocol.
type Client struct {
	apiKey string
	url    string
}

func NewClient(apiKey, endpoint string) (*Client, error) {
	base, err := models.NewBaseURL(endpoint)
	if err != nil {
		return nil, err
	}
	base.Path = "/v1/rerank"
	return &Client{apiKey: apiKey, url: base.String()}, nil
}

type rerankRequest struct {
	Query           string   `json:"query"`
	Documents       []string `json:"documents"`
	ReturnDocuments bool     `json:"return_documents"`
}

type RerankResult struct {
	Index int     `json:"index"`
	Score float32 `json:"score"`
}

// RerankResponse is the ordered array returned by SGLang's /v1/rerank endpoint.
type RerankResponse []RerankResult

func (c *Client) headers() map[string]string {
	headers := map[string]string{"Content-Type": "application/json"}
	if c.apiKey != "" {
		headers["Authorization"] = fmt.Sprintf("Bearer %s", c.apiKey)
	}
	return headers
}

func (c *Client) Rerank(ctx context.Context, query string, documents []string, timeoutMs int64) (*RerankResponse, error) {
	return models.PostRequestWithContext[RerankResponse](ctx, rerankRequest{
		Query:           query,
		Documents:       documents,
		ReturnDocuments: false,
	}, c.url, c.headers(), timeoutMs)
}
