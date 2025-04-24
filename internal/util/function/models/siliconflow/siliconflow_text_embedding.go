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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/util/function/models/utils"
)

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

type ByIndex struct {
	resp *EmbeddingResponse
}

func (eb *ByIndex) Len() int { return len(eb.resp.Data) }
func (eb *ByIndex) Swap(i, j int) {
	eb.resp.Data[i], eb.resp.Data[j] = eb.resp.Data[j], eb.resp.Data[i]
}

func (eb *ByIndex) Less(i, j int) bool {
	return eb.resp.Data[i].Index < eb.resp.Data[j].Index
}

type SiliconflowEmbedding struct {
	apiKey string
	url    string
}

func NewSiliconflowEmbeddingClient(apiKey string, url string) *SiliconflowEmbedding {
	return &SiliconflowEmbedding{
		apiKey: apiKey,
		url:    url,
	}
}

func (c *SiliconflowEmbedding) Check() error {
	if c.apiKey == "" {
		return errors.New("api key is empty")
	}

	if c.url == "" {
		return errors.New("url is empty")
	}
	return nil
}

func (c *SiliconflowEmbedding) Embedding(modelName string, texts []string, encodingFormat string, timeoutSec int64) (*EmbeddingResponse, error) {
	var r EmbeddingRequest
	r.Model = modelName
	r.Input = texts
	r.EncodingFormat = encodingFormat
	data, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}

	if timeoutSec <= 0 {
		timeoutSec = utils.DefaultTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutSec)*time.Second)
	defer cancel()

	headers := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": fmt.Sprintf("Bearer %s", c.apiKey),
	}
	body, err := utils.RetrySend(ctx, data, http.MethodPost, c.url, headers, 3, 1)
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
