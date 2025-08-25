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

package siliconflow

import (
	"fmt"
	"sort"

	"github.com/milvus-io/milvus/internal/util/function/models"
)

type SiliconflowClient struct {
	apiKey string
}

func NewSiliconflowClient(apiKey string) (*SiliconflowClient, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials conifg or configure the %s environment variable in the Milvus service.", models.SiliconflowAKEnvStr)
	}

	return &SiliconflowClient{
		apiKey: apiKey,
	}, nil
}

func (c *SiliconflowClient) headers() map[string]string {
	return map[string]string{
		"Content-Type":  "application/json",
		"Authorization": fmt.Sprintf("Bearer %s", c.apiKey),
	}
}

func (c *SiliconflowClient) Embedding(url string, modelName string, texts []string, encodingFormat string, timeoutSec int64) (*EmbeddingResponse, error) {
	embClient := newSiliconflowEmbedding(c.apiKey, url)
	return embClient.embedding(modelName, texts, encodingFormat, c.headers(), timeoutSec)
}

func (c *SiliconflowClient) Rerank(url string, modelName string, query string, texts []string, params map[string]any, timeoutSec int64) (*RerankResponse, error) {
	rerankClient := newSiliconflowRerank(c.apiKey, url)
	return rerankClient.rerank(modelName, query, texts, c.headers(), params, timeoutSec)
}

type EmbeddingRequest struct {
	// ID of the model to use.
	Model string `json:"model"`

	// Input text to embed, encoded as a string.
	Input []string `json:"input"`

	EncodingFormat string `json:"encoding_format,omitempty"`
}

type Usage struct {
	// The total number of tokens used by the request.
	TotalTokens int `json:"total_tokens"`

	PromptTokens int `json:"prompt_tokens"`

	CompletionTokens int `json:"completion_tokens"`
}

type EmbeddingData struct {
	Object string `json:"object"`

	Embedding []float32 `json:"embedding"`

	Index int `json:"index"`
}

type EmbeddingResponse struct {
	Object string `json:"object"`

	Data []EmbeddingData `json:"data"`

	Usage Usage `json:"usage"`
}

type siliconflowEmbedding struct {
	apiKey string
	url    string
}

func newSiliconflowEmbedding(apiKey string, url string) *siliconflowEmbedding {
	return &siliconflowEmbedding{
		apiKey: apiKey,
		url:    url,
	}
}

func (c *siliconflowEmbedding) embedding(modelName string, texts []string, encodingFormat string, headers map[string]string, timeoutSec int64) (*EmbeddingResponse, error) {
	var r EmbeddingRequest
	r.Model = modelName
	r.Input = texts
	r.EncodingFormat = encodingFormat

	res, err := models.PostRequest[EmbeddingResponse](r, c.url, headers, timeoutSec)
	if err != nil {
		return nil, err
	}
	sort.Slice(res.Data, func(i, j int) bool {
		return res.Data[i].Index < res.Data[j].Index
	})
	return res, nil
}

/*
	{
	  "id": "xxx",
	  "results": [
	    {
	      "index": 0,
	      "relevance_score": 0.99184376
	    },
	    {
	      "index": 1,
	      "relevance_score": 0.0034564096
	    },
	    {
	      "index": 2,
	      "relevance_score": 0.0003473453
	    },
	    {
	      "index": 3,
	      "relevance_score": 0.000019525885
	    }
	  ],
	  "meta": {
	    "billed_units": {
	      "input_tokens": 25,
	      "output_tokens": 0,
	      "search_units": 0,
	      "classifications": 0
	    },
	    "tokens": {
	      "input_tokens": 25,
	      "output_tokens": 0
	    }
	  }
	}
*/
type RerankResponse struct {
	ID string `json:"id"`

	Results []RerankResult `json:"results"`
	Meta    struct {
		BilledUnits struct {
			InputTokens     int `json:"input_tokens"`
			OutputTokens    int `json:"output_tokens"`
			SearchUnits     int `json:"search_units"`
			Classifications int `json:"classifications"`
		} `json:"billed_units"`
		Tokens struct {
			InputTokens  int `json:"input_tokens"`
			OutputTokens int `json:"output_tokens"`
		} `json:"tokens"`
	} `json:"meta"`
}

type RerankResult struct {
	Index          int     `json:"index"`
	RelevanceScore float32 `json:"relevance_score"`
}

type siliconflowRerank struct {
	apiKey string
	url    string
}

func newSiliconflowRerank(apiKey string, url string) *siliconflowRerank {
	return &siliconflowRerank{
		apiKey: apiKey,
		url:    url,
	}
}

func (c *siliconflowRerank) rerank(modelName string, query string, texts []string, headers map[string]string, params map[string]any, timeoutSec int64) (*RerankResponse, error) {
	requestBody := map[string]interface{}{
		"model":     modelName,
		"query":     query,
		"documents": texts,
	}
	for k, v := range params {
		requestBody[k] = v
	}
	res, err := models.PostRequest[RerankResponse](requestBody, c.url, headers, timeoutSec)
	if err != nil {
		return nil, err
	}

	// sort by index
	sort.Slice(res.Results, func(i, j int) bool {
		return res.Results[i].Index < res.Results[j].Index
	})
	return res, nil
}
