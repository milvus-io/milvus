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

package qianfan

import (
	"fmt"
	"sort"

	"github.com/cockroachdb/errors"

	"github.com/milvus-io/milvus/internal/util/function/models"
)

type EmbeddingRequest struct {
	Model string   `json:"model"`
	Input []string `json:"input"`
}

type Usage struct {
	PromptTokens int `json:"prompt_tokens"`
	TotalTokens  int `json:"total_tokens"`
}

type EmbeddingData struct {
	Object    string    `json:"object"`
	Embedding []float32 `json:"embedding"`
	Index     int       `json:"index"`
}

type EmbeddingResponse struct {
	Object string          `json:"object"`
	Data   []EmbeddingData `json:"data"`
	Model  string          `json:"model"`
	Usage  Usage           `json:"usage"`
}

type QianfanEmbeddingInterface interface {
	Check() error
	Embedding(modelName string, texts []string, dim int, timeoutSec int64) (*EmbeddingResponse, error)
}

type QianfanEmbeddingClient struct {
	apiKey string
	url    string
}

func NewQianfanEmbeddingClient(apiKey string, url string) *QianfanEmbeddingClient {
	return &QianfanEmbeddingClient{
		apiKey: apiKey,
		url:    url,
	}
}

func (c *QianfanEmbeddingClient) Check() error {
	if c.apiKey == "" {
		return errors.New("api key is empty")
	}

	if c.url == "" {
		return errors.New("url is empty")
	}
	return nil
}

func (c *QianfanEmbeddingClient) Embedding(modelName string, texts []string, _ int, timeoutSec int64) (*EmbeddingResponse, error) {
	req := EmbeddingRequest{
		Model: modelName,
		Input: texts,
	}
	headers := map[string]string{
		"Content-Type":  "application/json",
		"Authorization": fmt.Sprintf("Bearer %s", c.apiKey),
	}
	res, err := models.PostRequest[EmbeddingResponse](req, c.url, headers, timeoutSec)
	if err != nil {
		return nil, err
	}
	sort.Slice(res.Data, func(i, j int) bool {
		return res.Data[i].Index < res.Data[j].Index
	})
	return res, nil
}
