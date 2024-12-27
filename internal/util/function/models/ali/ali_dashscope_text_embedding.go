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

package ali

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/milvus-io/milvus/internal/util/function/models/utils"
)

type Input struct {
	Texts []string `json:"texts"`
}

type Parameters struct {
	TextType   string `json:"text_type,omitempty"`
	Dimension  int    `json:"dimension,omitempty"`
	OutputType string `json:"output_type,omitempty"`
}

type EmbeddingRequest struct {
	// ID of the model to use.
	Model string `json:"model"`

	// Input text to embed, encoded as a string.
	Input Input `json:"input"`

	Parameters Parameters `json:"parameters,omitempty"`
}

type Usage struct {
	// The total number of tokens used by the request.
	TotalTokens int `json:"total_tokens"`
}

type SparseEmbedding struct {
	Index int     `json:"index"`
	Value float32 `json:"value"`
	Token string  `json:"token"`
}

type Embeddings struct {
	TextIndex       int               `json:"text_index"`
	Embedding       []float32         `json:"embedding,omitempty"`
	SparseEmbedding []SparseEmbedding `json:"sparse_embedding,omitempty"`
}

type Output struct {
	Embeddings []Embeddings `json:"embeddings"`
}

type EmbeddingResponse struct {
	Output    Output `json:"output"`
	Usage     Usage  `json:"usage"`
	RequestID string `json:"request_id"`
}

type ByIndex struct {
	resp *EmbeddingResponse
}

func (eb *ByIndex) Len() int { return len(eb.resp.Output.Embeddings) }
func (eb *ByIndex) Swap(i, j int) {
	eb.resp.Output.Embeddings[i], eb.resp.Output.Embeddings[j] = eb.resp.Output.Embeddings[j], eb.resp.Output.Embeddings[i]
}

func (eb *ByIndex) Less(i, j int) bool {
	return eb.resp.Output.Embeddings[i].TextIndex < eb.resp.Output.Embeddings[j].TextIndex
}

type ErrorInfo struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

type AliDashScopeEmbedding struct {
	apiKey string
	url    string
}

func NewAliDashScopeEmbeddingClient(apiKey string, url string) *AliDashScopeEmbedding {
	return &AliDashScopeEmbedding{
		apiKey: apiKey,
		url:    url,
	}
}

func (c *AliDashScopeEmbedding) Check() error {
	if c.apiKey == "" {
		return fmt.Errorf("api key is empty")
	}

	if c.url == "" {
		return fmt.Errorf("url is empty")
	}
	return nil
}

func (c *AliDashScopeEmbedding) Embedding(modelName string, texts []string, dim int, textType string, outputType string, timeoutSec int64) (*EmbeddingResponse, error) {
	var r EmbeddingRequest
	r.Model = modelName
	r.Input = Input{texts}
	r.Parameters.Dimension = dim
	r.Parameters.TextType = textType
	r.Parameters.OutputType = outputType
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}

	if timeoutSec <= 0 {
		timeoutSec = utils.DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.url, bytes.NewBuffer(data))
	if err != nil {
		return nil, err
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", c.apiKey))
	body, err := utils.RetrySend(req, 3)
	if err != nil {
		return nil, err
	}
	var res EmbeddingResponse
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}
	sort.Sort(&ByIndex{&res})
	return &res, err
}
