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
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"time"

	"github.com/milvus-io/milvus/internal/util/function/models/utils"
)

type EmbeddingRequest struct {
	Inputs              []string `json:"inputs"`
	Truncate            bool     `json:"truncate,omitempty"`
	TruncationDirection string   `json:"truncation_direction,omitempty"`
	PromptName          string   `json:"prompt_name,omitempty"`
}

type TEIEmbedding struct {
	apiKey string
	url    string
}

func NewTEIEmbeddingClient(apiKey string, endpoint string) (*TEIEmbedding, error) {
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

	base.Path = "/embed"

	return &TEIEmbedding{
		apiKey: apiKey,
		url:    base.String(),
	}, nil
}

func (c *TEIEmbedding) Embedding(texts []string, truncate bool, truncationDirection string, prompt string, timeoutSec int64) ([][]float32, error) {
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
		"Content-Type": "application/json",
	}
	if c.apiKey != "" {
		headers["Authorization"] = fmt.Sprintf("Bearer %s", c.apiKey)
	}
	body, err := utils.RetrySend(ctx, data, http.MethodPost, c.url, headers, 3, 1)
	if err != nil {
		return nil, err
	}
	var res [][]float32
	err = json.Unmarshal(body, &res)
	if err != nil {
		return nil, err
	}
	return res, err
}
