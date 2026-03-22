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

package minimax

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/util/function/models"
)

type MiniMaxClient struct {
	apiKey string
}

func NewMiniMaxClient(apiKey string) (*MiniMaxClient, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials config or configure the %s environment variable in the Milvus service.", models.MiniMaxAKEnvStr)
	}

	return &MiniMaxClient{
		apiKey: apiKey,
	}, nil
}

func (c *MiniMaxClient) headers() map[string]string {
	return map[string]string{
		"Content-Type":  "application/json",
		"Authorization": fmt.Sprintf("Bearer %s", c.apiKey),
	}
}

func (c *MiniMaxClient) Embedding(url string, modelName string, texts []string, embType string, timeoutSec int64) (*EmbeddingResponse, error) {
	embClient := newMiniMaxEmbedding(c.apiKey, url)
	return embClient.embedding(modelName, texts, embType, c.headers(), timeoutSec)
}

// EmbeddingRequest represents a MiniMax embedding API request.
// MiniMax uses "texts" instead of "input" and requires a "type" field.
type EmbeddingRequest struct {
	// Model ID to use (e.g., "embo-01").
	Model string `json:"model"`

	// Input texts to embed.
	Texts []string `json:"texts"`

	// Embedding type: "db" for storage/indexing, "query" for search queries.
	Type string `json:"type"`
}

// BaseResp contains the MiniMax API status information.
type BaseResp struct {
	StatusCode int    `json:"status_code"`
	StatusMsg  string `json:"status_msg"`
}

// EmbeddingResponse represents a MiniMax embedding API response.
// MiniMax returns embeddings in a "vectors" field as a 2D array.
type EmbeddingResponse struct {
	// Embedding vectors, one per input text.
	Vectors [][]float32 `json:"vectors"`

	// Total tokens consumed.
	TotalTokens int `json:"total_tokens"`

	// API status information.
	BaseResp BaseResp `json:"base_resp"`
}

type miniMaxEmbedding struct {
	apiKey string
	url    string
}

func newMiniMaxEmbedding(apiKey string, url string) *miniMaxEmbedding {
	return &miniMaxEmbedding{
		apiKey: apiKey,
		url:    url,
	}
}

func (c *miniMaxEmbedding) embedding(modelName string, texts []string, embType string, headers map[string]string, timeoutSec int64) (*EmbeddingResponse, error) {
	var r EmbeddingRequest
	r.Model = modelName
	r.Texts = texts
	r.Type = embType

	res, err := models.PostRequest[EmbeddingResponse](r, c.url, headers, timeoutSec)
	if err != nil {
		return nil, err
	}

	if res.BaseResp.StatusCode != 0 {
		return nil, fmt.Errorf("MiniMax embedding API error: status_code=%d, status_msg=%s", res.BaseResp.StatusCode, res.BaseResp.StatusMsg)
	}

	return res, nil
}
