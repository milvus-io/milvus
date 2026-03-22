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

func (c *MiniMaxClient) Embedding(url string, modelName string, texts []string, textType string, timeoutSec int64) (*EmbeddingResponse, error) {
	r := EmbeddingRequest{
		Model: modelName,
		Texts: texts,
		Type:  textType,
	}
	return models.PostRequest[EmbeddingResponse](r, url, c.headers(), timeoutSec)
}

// EmbeddingRequest represents a MiniMax embedding API request.
// MiniMax uses "texts" (not "input") and "type" (not "input_type").
type EmbeddingRequest struct {
	// ID of the model to use.
	Model string `json:"model"`

	// Input texts to embed.
	Texts []string `json:"texts"`

	// Type of the text: "db" for storage, "query" for search.
	Type string `json:"type"`
}

// EmbeddingResponse represents a MiniMax embedding API response.
type EmbeddingResponse struct {
	// The embedding vectors, one per input text.
	Vectors [][]float32 `json:"vectors"`

	// Total tokens used.
	TotalTokens int `json:"total_tokens"`

	// Base response with status info.
	BaseResp BaseResp `json:"base_resp"`
}

// BaseResp contains the status of the API response.
type BaseResp struct {
	StatusCode int    `json:"status_code"`
	StatusMsg  string `json:"status_msg"`
}
