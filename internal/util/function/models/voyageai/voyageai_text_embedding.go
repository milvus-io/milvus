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

type ByIndex[T int8 | float32] struct {
	resp *EmbeddingResponse[T]
}

func (eb *ByIndex[T]) Len() int { return len(eb.resp.Data) }
func (eb *ByIndex[T]) Swap(i, j int) {
	eb.resp.Data[i], eb.resp.Data[j] = eb.resp.Data[j], eb.resp.Data[i]
}

func (eb *ByIndex[T]) Less(i, j int) bool {
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
		return errors.New("api key is empty")
	}

	if c.url == "" {
		return errors.New("url is empty")
	}
	return nil
}

func (c *VoyageAIEmbedding) Embedding(modelName string, texts []string, dim int, textType string, outputType string, truncation bool, timeoutSec int64) (any, error) {
	if outputType != "float" && outputType != "int8" {
		return nil, fmt.Errorf("Voyageai: unsupport output type: [%s], only support float and int8", outputType)
	}
	var r EmbeddingRequest
	r.Model = modelName
	r.Input = texts
	r.InputType = textType
	r.OutputDtype = outputType
	r.Truncation = truncation
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
	headers := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": fmt.Sprintf("Bearer %s", c.apiKey),
	}
	body, err := utils.RetrySend(ctx, data, http.MethodPost, c.url, headers, 3, 1)
	if err != nil {
		return nil, err
	}
	if outputType == "float" {
		var res EmbeddingResponse[float32]
		err = json.Unmarshal(body, &res)
		if err != nil {
			return nil, err
		}
		sort.Sort(&ByIndex[float32]{&res})
		return &res, err
	}
	var res EmbeddingResponse[int8]
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}
	sort.Sort(&ByIndex[int8]{&res})
	return &res, err
}
