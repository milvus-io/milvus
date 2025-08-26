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

package vllm

import (
	"fmt"
	"maps"
	"net/url"
	"sort"

	"github.com/milvus-io/milvus/internal/util/function/models"
)

func NewBaseURL(endpoint string) (*url.URL, error) {
	base, err := url.Parse(endpoint)
	if err != nil {
		return nil, err
	}
	if base.Scheme != "http" && base.Scheme != "https" {
		return nil, fmt.Errorf("endpoint: [%s] is not a valid http/https link", endpoint)
	}
	if base.Host == "" {
		return nil, fmt.Errorf("endpoint: [%s] is not a valid http/https link", endpoint)
	}
	return base, nil
}

type VLLMClient struct {
	apiKey   string
	endpoint string
}

func NewVLLMClient(apiKey string, endpoint string) (*VLLMClient, error) {
	return &VLLMClient{
		apiKey:   apiKey,
		endpoint: endpoint,
	}, nil
}

func (c *VLLMClient) headers() map[string]string {
	headers := map[string]string{
		"Content-Type": "application/json",
	}
	if c.apiKey != "" {
		headers["Authorization"] = fmt.Sprintf("Bearer %s", c.apiKey)
	}
	return headers
}

func (c *VLLMClient) Embedding(texts []string, params map[string]any, timeoutSec int64) (*EmbeddingResponse, error) {
	embClient, err := newVLLMEmbeddingClient(c.apiKey, c.endpoint)
	if err != nil {
		return nil, err
	}
	return embClient.Embedding(texts, params, c.headers(), timeoutSec)
}

func (c *VLLMClient) Rerank(query string, texts []string, params map[string]any, timeoutSec int64) (*RerankResponse, error) {
	rerankClient, err := newVLLMRerankClient(c.apiKey, c.endpoint)
	if err != nil {
		return nil, err
	}
	return rerankClient.Rerank(query, texts, params, c.headers(), timeoutSec)
}

/*
requests:
{
  "model": "string",
  "input": [
    0
  ],
  "encoding_format": "float",
  "dimensions": 0,
  "user": "string",
  "truncate_prompt_tokens": 1,
  "additional_data": "string",
  "add_special_tokens": true,
  "priority": 0,
  "additionalProp1": {}
}

*/

/*
responses:
{
  "id": "embd-22dfa7cd179d4bf3bac367a1db90fe62",
  "object": "list",
  "created": 1752044194,
  "model": "BAAI/bge-base-en-v1.5",
  "data": [
    {
      "index": 0,
      "object": "embedding",
      "embedding": []
    }
  ],
  "usage": {
    "prompt_tokens": 3,
    "total_tokens": 3,
    "completion_tokens": 0,
    "prompt_tokens_details": null
  }
}
*/

type EmbeddingResponse struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int    `json:"created"`
	Model   string `json:"model"`
	Data    []struct {
		Index     int       `json:"index"`
		Object    string    `json:"object"`
		Embedding []float32 `json:"embedding"`
	} `json:"data"`
	Usage struct {
		PromptTokens        int `json:"prompt_tokens"`
		TotalTokens         int `json:"total_tokens"`
		CompletionTokens    int `json:"completion_tokens"`
		PromptTokensDetails any `json:"prompt_tokens_details"`
	} `json:"usage"`
}

type VLLMEmbedding struct {
	apiKey string
	url    string
}

func newVLLMEmbeddingClient(apiKey string, endpoint string) (*VLLMEmbedding, error) {
	base, err := models.NewBaseURL(endpoint)
	if err != nil {
		return nil, err
	}

	base.Path = "/v1/embeddings"

	return &VLLMEmbedding{
		apiKey: apiKey,
		url:    base.String(),
	}, nil
}

func (c *VLLMEmbedding) Embedding(texts []string, params map[string]any, headers map[string]string, timeoutSec int64) (*EmbeddingResponse, error) {
	r := map[string]any{
		"input":           texts,
		"encoding_format": "float",
	}

	if params != nil {
		maps.Copy(r, params)
	}

	res, err := models.PostRequest[EmbeddingResponse](r, c.url, headers, timeoutSec)
	if err != nil {
		return nil, err
	}
	return res, nil
}

/*
{
  "id": "rerank-17b879c53eb547ceadd89ad9d402a461",
  "model": "BAAI/bge-base-en-v1.5",
  "usage": {
    "total_tokens": 7
  },
  "results": [
    {
      "index": 0,
      "relevance_score": 1.000000238418579
    }
  ]
}

*/

type RerankResponse struct {
	ID    string `json:"id"`
	Model string `json:"model"`
	Usage struct {
		TotalTokens int `json:"total_tokens"`
	} `json:"usage"`
	Results []struct {
		Index          int     `json:"index"`
		RelevanceScore float32 `json:"relevance_score"`
	} `json:"results"`
}

type VLLMRerank struct {
	apiKey string
	url    string
}

func newVLLMRerankClient(apiKey string, endpoint string) (*VLLMRerank, error) {
	base, err := NewBaseURL(endpoint)
	if err != nil {
		return nil, err
	}
	base.Path = "/rerank"
	return &VLLMRerank{
		apiKey: apiKey,
		url:    base.String(),
	}, nil
}

func (c *VLLMRerank) Rerank(query string, texts []string, params map[string]any, headers map[string]string, timeoutSec int64) (*RerankResponse, error) {
	r := map[string]any{
		"query":     query,
		"documents": texts,
	}
	if params != nil {
		maps.Copy(r, params)
	}

	res, err := models.PostRequest[RerankResponse](r, c.url, headers, timeoutSec)
	if err != nil {
		return nil, err
	}
	sort.Slice(res.Results, func(i, j int) bool {
		return res.Results[i].Index < res.Results[j].Index
	})
	return res, nil
}
