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

package voyageai

import (
	"fmt"
	"maps"
	"sort"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/util/function/models"
)

type VoyageAIClient struct {
	apiKey string
}

func NewVoyageAIClient(apiKey string) (*VoyageAIClient, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials config or configure the %s environment variable in the Milvus service.", models.VoyageAIAKEnvStr)
	}
	return &VoyageAIClient{
		apiKey: apiKey,
	}, nil
}

func (c *VoyageAIClient) headers() map[string]string {
	return map[string]string{
		"Content-Type":  "application/json",
		"Authorization": fmt.Sprintf("Bearer %s", c.apiKey),
	}
}

func (c *VoyageAIClient) Embedding(url string, modelName string, texts []string, dim int, textType string, outputType string, truncation bool, timeoutSec int64) (any, error) {
	embClient := newVoyageAIEmbedding(c.apiKey, url)
	return embClient.embedding(modelName, texts, dim, textType, outputType, truncation, c.headers(), timeoutSec)
}

func (c *VoyageAIClient) Rerank(url string, modelName string, query string, texts []string, params map[string]any, timeoutSec int64) (*RerankResponse, error) {
	rerankClient := newVoyageAIRerank(c.apiKey, url)
	return rerankClient.rerank(modelName, query, texts, c.headers(), params, timeoutSec)
}

type EmbeddingRequest struct {
	// ID of the model to use.
	Model string `json:"model"`

	// Input text to embed, encoded as a string.
	Input []string `json:"input"`

	InputType string `json:"input_type,omitempty"`

	Truncation bool `json:"truncation,omitempty"`

	OutputDimension int64 `json:"output_dimension,omitempty"`

	OutputDtype string `json:"output_dtype,omitempty"`

	EncodingFormat string `json:"encoding_format,omitempty"`
}

type Usage struct {
	// The total number of tokens used by the request.
	TotalTokens int `json:"total_tokens"`
}

type EmbeddingData[T int8 | float32] struct {
	Object string `json:"object"`

	Embedding []T `json:"embedding"`

	Index int `json:"index"`
}

type EmbeddingResponse[T int8 | float32] struct {
	Object string             `json:"object"`
	Model  string             `json:"model"`
	Usage  Usage              `json:"usage"`
	Data   []EmbeddingData[T] `json:"data"`
}

type ErrorInfo struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

type voyageAIEmbedding struct {
	apiKey string
	url    string
}

func newVoyageAIEmbedding(apiKey string, url string) *voyageAIEmbedding {
	return &voyageAIEmbedding{
		apiKey: apiKey,
		url:    url,
	}
}

func (c *voyageAIEmbedding) Check() error {
	if c.apiKey == "" {
		return errors.New("api key is empty")
	}

	if c.url == "" {
		return errors.New("url is empty")
	}
	return nil
}

func (c *voyageAIEmbedding) embedding(modelName string, texts []string, dim int, textType string, outputType string, truncation bool, headers map[string]string, timeoutSec int64) (any, error) {
	if outputType != "float" && outputType != "int8" {
		return nil, fmt.Errorf("Voyageai: unsupport output type: [%s], only support float and int8", outputType)
	}
	r := EmbeddingRequest{
		Model:      modelName,
		Input:      texts,
		InputType:  textType,
		Truncation: truncation,
	}
	if dim != 0 {
		r.OutputDimension = int64(dim)
	}

	if outputType == "float" {
		res, err := models.PostRequest[EmbeddingResponse[float32]](r, c.url, headers, timeoutSec)
		if err != nil {
			return nil, err
		}
		sort.Slice(res.Data, func(i, j int) bool {
			return res.Data[i].Index < res.Data[j].Index
		})
		return res, nil
	} else {
		res, err := models.PostRequest[EmbeddingResponse[int8]](r, c.url, headers, timeoutSec)
		if err != nil {
			return nil, err
		}
		sort.Slice(res.Data, func(i, j int) bool {
			return res.Data[i].Index < res.Data[j].Index
		})
		return res, nil
	}
}

/*
{
  "object": "list",
  "data": [
    {
      "relevance_score": 0.4375,
      "index": 0
    },
    {
      "relevance_score": 0.421875,
      "index": 1
    }
  ],
  "model": "rerank-lite-1",
  "usage": {
    "total_tokens": 26
  }
}
*/

type RerankResponse struct {
	Object string         `json:"object"`
	Data   []RerankResult `json:"data"`
	Model  string         `json:"model"`
	Usage  Usage          `json:"usage"`
}

type RerankResult struct {
	Index          int     `json:"index"`
	RelevanceScore float32 `json:"relevance_score"`
}

type voyageAIRerank struct {
	apiKey string
	url    string
}

func newVoyageAIRerank(apiKey string, url string) *voyageAIRerank {
	return &voyageAIRerank{
		apiKey: apiKey,
		url:    url,
	}
}

func (c *voyageAIRerank) rerank(modelName string, query string, texts []string, headers map[string]string, params map[string]any, timeoutSec int64) (*RerankResponse, error) {
	r := map[string]any{
		"model":     modelName,
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
	sort.Slice(res.Data, func(i, j int) bool {
		return res.Data[i].Index < res.Data[j].Index
	})
	return res, nil
}
