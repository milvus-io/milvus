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

package gemini

import (
	"fmt"
	"strings"

	"github.com/milvus-io/milvus/internal/util/function/models"
)

type GeminiClient struct {
	apiKey string
}

func NewGeminiClient(apiKey string) (*GeminiClient, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("missing credentials config or configure the %s environment variable in the Milvus service", models.GeminiAKEnvStr)
	}
	return &GeminiClient{
		apiKey: apiKey,
	}, nil
}

func (c *GeminiClient) headers() map[string]string {
	return map[string]string{
		"Content-Type":   "application/json",
		"x-goog-api-key": c.apiKey,
	}
}

func (c *GeminiClient) Embedding(url string, modelName string, texts []string, dim int, taskType string, timeoutSec int64) (*EmbeddingResponse, error) {
	modelName = strings.TrimPrefix(modelName, "models/")
	requests := make([]BatchEmbedRequest, 0, len(texts))
	for _, text := range texts {
		req := BatchEmbedRequest{
			Model: "models/" + modelName,
			Content: Content{
				Parts: []Part{{Text: text}},
			},
		}
		if taskType != "" {
			req.TaskType = taskType
		}
		if dim > 0 {
			req.OutputDimensionality = dim
		}
		requests = append(requests, req)
	}

	batchReq := BatchEmbeddingRequest{
		Requests: requests,
	}

	res, err := models.PostRequest[EmbeddingResponse](batchReq, url, c.headers(), timeoutSec)
	if err != nil {
		return nil, err
	}
	return res, nil
}

// Request types

type Part struct {
	Text string `json:"text"`
}

type Content struct {
	Parts []Part `json:"parts"`
}

type BatchEmbedRequest struct {
	Model                string  `json:"model"`
	Content              Content `json:"content"`
	TaskType             string  `json:"taskType,omitempty"`
	OutputDimensionality int     `json:"outputDimensionality,omitempty"`
}

type BatchEmbeddingRequest struct {
	Requests []BatchEmbedRequest `json:"requests"`
}

// Response types

type EmbeddingValues struct {
	Values []float32 `json:"values"`
}

type EmbeddingResponse struct {
	Embeddings []EmbeddingValues `json:"embeddings"`
}
