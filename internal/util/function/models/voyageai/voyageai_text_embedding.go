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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sort"
	"time"

	"github.com/milvus-io/milvus/internal/util/function/models/utils"
)

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

type EmbeddingData struct {
	Object string `json:"object"`

	Embedding []float32 `json:"embedding"`

	Index int `json:"index"`
}

type EmbeddingResponse struct {
	Object string `json:"object"`

	Data []EmbeddingData `json:"data"`

	Model string `json:"model"`

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

type ErrorInfo struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

type VoyageAIEmbedding struct {
	apiKey string
	url    string
}

func NewVoyageAIEmbeddingClient(apiKey string, url string) *VoyageAIEmbedding {
	return &VoyageAIEmbedding{
		apiKey: apiKey,
		url:    url,
	}
}

func (c *VoyageAIEmbedding) Check() error {
	if c.apiKey == "" {
		return fmt.Errorf("api key is empty")
	}

	if c.url == "" {
		return fmt.Errorf("url is empty")
	}
	return nil
}

func (c *VoyageAIEmbedding) Embedding(modelName string, texts []string, dim int, textType string, outputType string, timeoutSec int64) (*EmbeddingResponse, error) {
	var r EmbeddingRequest
	r.Model = modelName
	r.Input = texts
	r.InputType = textType
	r.OutputDtype = outputType
	if dim != 0 {
		r.OutputDimension = int64(dim)
	}
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
