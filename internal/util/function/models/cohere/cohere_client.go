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

package cohere

import (
	"fmt"
	"sort"

	"github.com/milvus-io/milvus/internal/util/function/models"
)

type CohereClient struct {
	apiKey string
}

func NewCohereClient(apiKey string) (*CohereClient, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials config or configure the %s environment variable in the Milvus service.", models.CohereAIAKEnvStr)
	}
	return &CohereClient{
		apiKey: apiKey,
	}, nil
}

func (c *CohereClient) headers() map[string]string {
	return map[string]string{
		"accept":        "application/json",
		"Content-Type":  "application/json",
		"Authorization": fmt.Sprintf("bearer %s", c.apiKey),
	}
}

func (c *CohereClient) Embedding(url string, modelName string, texts []string, inputType string, outputType string, truncate string, timeoutSec int64) (*EmbeddingResponse, error) {
	embClient := newCohereEmbedding(c.apiKey, url)
	return embClient.embedding(modelName, texts, inputType, outputType, truncate, c.headers(), timeoutSec)
}

func (c *CohereClient) Rerank(url string, modelName string, query string, texts []string, params map[string]any, timeoutSec int64) (*RerankResponse, error) {
	rerankClient := newCohereRerankClient(c.apiKey, url)
	return rerankClient.rerank(modelName, query, texts, c.headers(), params, timeoutSec)
}

type EmbeddingRequest struct {
	// ID of the model to use.
	Model string `json:"model"`

	Texts []string `json:"texts"`

	InputType string `json:"input_type,omitempty"`

	EmbeddingTypes []string `json:"embedding_types"`

	// Allowed values: NONE,START, END (default)
	// Passing START will discard the start of the input.
	// END will discard the end of the input. In both cases, input is discarded until the remaining input is
	// exactly the maximum input token length for the model.
	// If NONE is selected, when the input exceeds the maximum input token length an error will be returned.
	Truncate string `json:"truncate,omitempty"`
}

// Currently only float32/int8 is supported
type Embeddings struct {
	Float [][]float32 `json:"float"`
	Int8  [][]int8    `json:"int8"`
}

type EmbeddingResponse struct {
	Id         string     `json:"id"`
	Embeddings Embeddings `json:"embeddings"`
}

type cohereEmbedding struct {
	apiKey string
	url    string
}

func newCohereEmbedding(apiKey string, url string) *cohereEmbedding {
	return &cohereEmbedding{
		apiKey: apiKey,
		url:    url,
	}
}

func (c *cohereEmbedding) embedding(modelName string, texts []string, inputType string, outputType string, truncate string, headers map[string]string, timeoutSec int64) (*EmbeddingResponse, error) {
	var r EmbeddingRequest
	r.Model = modelName
	r.Texts = texts
	if inputType != "" {
		r.InputType = inputType
	}
	r.EmbeddingTypes = []string{outputType}
	r.Truncate = truncate

	res, err := models.PostRequest[EmbeddingResponse](r, c.url, headers, timeoutSec)
	if err != nil {
		return nil, err
	}
	return res, err
}

/*
{
  "results": [
    {
      "index": 3,
      "relevance_score": 0.999071
    },
    {
      "index": 4,
      "relevance_score": 0.7867867
    },
    {
      "index": 0,
      "relevance_score": 0.32713068
    }
  ],
  "id": "07734bd2-2473-4f07-94e1-0d9f0e6843cf",
  "meta": {
    "api_version": {
      "version": "2",
      "is_experimental": false
    },
    "billed_units": {
      "search_units": 1
    }
  }
}
*/

type RerankResponse struct {
	Results []RerankResult `json:"results"`
	Id      string         `json:"id"`
	Meta    Meta           `json:"meta"`
}

type Meta struct {
	APIVersion struct {
		Version        string `json:"version"`
		IsExperimental bool   `json:"is_experimental"`
	} `json:"api_version"`
	BilledUnits struct {
		SearchUnits int `json:"search_units"`
	} `json:"billed_units"`
}

type RerankResult struct {
	Index          int     `json:"index"`
	RelevanceScore float32 `json:"relevance_score"`
}

type cohereRerank struct {
	apiKey string
	url    string
}

func newCohereRerankClient(apiKey string, url string) *cohereRerank {
	return &cohereRerank{
		apiKey: apiKey,
		url:    url,
	}
}

func (c *cohereRerank) rerank(modelName string, query string, texts []string, headers map[string]string, params map[string]any, timeoutSec int64) (*RerankResponse, error) {
	r := map[string]any{
		"model":     modelName,
		"query":     query,
		"documents": texts,
	}

	for k, v := range params {
		r[k] = v
	}

	res, err := models.PostRequest[RerankResponse](r, c.url, headers, timeoutSec)
	if err != nil {
		return nil, err
	}

	sort.Slice(res.Results, func(i, j int) bool {
		return res.Results[i].Index < res.Results[j].Index
	})
	return res, err
}
