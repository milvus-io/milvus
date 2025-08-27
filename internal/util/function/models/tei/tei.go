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

package tei

import (
	"fmt"
	"maps"
	"sort"

	"github.com/milvus-io/milvus/internal/util/function/models"
)

type TEIClient struct {
	apiKey   string
	endpoint string
}

func NewTEIClient(apiKey string, endpoint string) (*TEIClient, error) {
	return &TEIClient{
		apiKey:   apiKey,
		endpoint: endpoint,
	}, nil
}

func (c *TEIClient) headers() map[string]string {
	headers := map[string]string{
		"Content-Type": "application/json",
	}
	if c.apiKey != "" {
		headers["Authorization"] = fmt.Sprintf("Bearer %s", c.apiKey)
	}
	return headers
}

func (c *TEIClient) Embedding(texts []string, truncate bool, truncationDirection string, prompt string, timeoutSec int64) (*EmbeddingResponse, error) {
	embClient, err := newTEIEmbedding(c.apiKey, c.endpoint)
	if err != nil {
		return nil, err
	}
	return embClient.embedding(texts, truncate, truncationDirection, prompt, c.headers(), timeoutSec)
}

func (c *TEIClient) Rerank(query string, texts []string, params map[string]any, timeoutSec int64) (*RerankResponse, error) {
	rerankClient, err := newTEIRerank(c.apiKey, c.endpoint)
	if err != nil {
		return nil, err
	}
	return rerankClient.rerank(query, texts, params, c.headers(), timeoutSec)
}

type EmbeddingRequest struct {
	Inputs              []string `json:"inputs"`
	Truncate            bool     `json:"truncate,omitempty"`
	TruncationDirection string   `json:"truncation_direction,omitempty"`
	PromptName          string   `json:"prompt_name,omitempty"`
}

type EmbeddingResponse [][]float32

type teiEmbedding struct {
	apiKey string
	url    string
}

func newTEIEmbedding(apiKey string, endpoint string) (*teiEmbedding, error) {
	base, err := models.NewBaseURL(endpoint)
	if err != nil {
		return nil, err
	}
	base.Path = "/embed"

	return &teiEmbedding{
		apiKey: apiKey,
		url:    base.String(),
	}, nil
}

func (c *teiEmbedding) embedding(texts []string, truncate bool, truncationDirection string, prompt string, headers map[string]string, timeoutSec int64) (*EmbeddingResponse, error) {
	var r EmbeddingRequest
	if prompt != "" {
		var newTexts []string
		for _, text := range texts {
			newTexts = append(newTexts, prompt+text)
		}
		r.Inputs = newTexts
	} else {
		r.Inputs = texts
	}

	r.Truncate = truncate
	if truncationDirection != "" {
		r.TruncationDirection = truncationDirection
	}

	res, err := models.PostRequest[EmbeddingResponse](r, c.url, headers, timeoutSec)
	if err != nil {
		return nil, err
	}
	return res, nil
}

/*[{"index":0,"score":0.0},{"index":1,"score":0.2}, {"index":2,"score":0.1}]*/
type RerankResponseItem struct {
	Index int     `json:"index"`
	Score float32 `json:"score"`
}

type RerankResponse []RerankResponseItem

type teiRerank struct {
	apiKey string
	url    string
}

func newTEIRerank(apiKey string, endpoint string) (*teiRerank, error) {
	base, err := models.NewBaseURL(endpoint)
	if err != nil {
		return nil, err
	}
	base.Path = "/rerank"
	return &teiRerank{
		apiKey: apiKey,
		url:    base.String(),
	}, nil
}

func (c *teiRerank) rerank(query string, texts []string, params map[string]any, headers map[string]string, timeoutSec int64) (*RerankResponse, error) {
	r := map[string]any{
		"query": query,
		"texts": texts,
	}
	maps.Copy(r, params)

	res, err := models.PostRequest[RerankResponse](r, c.url, headers, timeoutSec)
	if err != nil {
		return nil, err
	}
	sort.Slice(*res, func(i, j int) bool {
		return (*res)[i].Index < (*res)[j].Index
	})
	return res, nil
}
