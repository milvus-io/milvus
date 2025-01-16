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
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/milvus-io/milvus/internal/util/function/models/utils"
)

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

// Currently only float type is supported
type Embeddings struct {
	Float [][]float32 `json:"float"`
}

type EmbeddingResponse struct {
	Id         string     `json:"id"`
	Embeddings Embeddings `json:"embeddings"`
}

type CohereEmbedding struct {
	apiKey string
	url    string
}

func NewCohereEmbeddingClient(apiKey string, url string) *CohereEmbedding {
	return &CohereEmbedding{
		apiKey: apiKey,
		url:    url,
	}
}

func (c *CohereEmbedding) Check() error {
	if c.apiKey == "" {
		return fmt.Errorf("api key is empty")
	}

	if c.url == "" {
		return fmt.Errorf("url is empty")
	}
	return nil
}

func (c *CohereEmbedding) Embedding(modelName string, texts []string, inputType string, outputType string, truncate string, timeoutSec int64) (*EmbeddingResponse, error) {
	var r EmbeddingRequest
	r.Model = modelName
	r.Texts = texts
	if inputType != "" {
		r.InputType = inputType
	}
	r.EmbeddingTypes = []string{outputType}
	r.Truncate = truncate

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

	req.Header.Set("accept", "application/json")
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("bearer %s", c.apiKey))
	body, err := utils.RetrySend(req, 3)
	if err != nil {
		return nil, err
	}
	var res EmbeddingResponse
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}
	return &res, err
}
