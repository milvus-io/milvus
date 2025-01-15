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

package vertexai

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"golang.org/x/oauth2/google"

	"github.com/milvus-io/milvus/internal/util/function/models/utils"
)

type Instance struct {
	TaskType string `json:"task_type,omitempty"`
	Content  string `json:"content"`
}

type Parameters struct {
	OutputDimensionality int64 `json:"outputDimensionality,omitempty"`
}

type EmbeddingRequest struct {
	Instances  []Instance `json:"instances"`
	Parameters Parameters `json:"parameters,omitempty"`
}

type Statistics struct {
	Truncated  bool `json:"truncated"`
	TokenCount int  `json:"token_count"`
}

type Embeddings struct {
	Statistics Statistics `json:"statistics"`
	Values     []float32  `json:"values"`
}

type Prediction struct {
	Embeddings Embeddings `json:"embeddings"`
}

type Metadata struct {
	BillableCharacterCount int `json:"billableCharacterCount"`
}

type EmbeddingResponse struct {
	Predictions []Prediction `json:"predictions"`
	Metadata    Metadata     `json:"metadata"`
}

type ErrorInfo struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	RequestID string `json:"request_id"`
}

type VertexAIEmbedding struct {
	url     string
	jsonKey []byte
	scopes  string
	token   string
}

func NewVertexAIEmbedding(url string, jsonKey []byte, scopes string, token string) *VertexAIEmbedding {
	return &VertexAIEmbedding{
		url:     url,
		jsonKey: jsonKey,
		scopes:  scopes,
		token:   token,
	}
}

func (c *VertexAIEmbedding) Check() error {
	if c.url == "" {
		return fmt.Errorf("VertexAI embedding url is empty")
	}
	if len(c.jsonKey) == 0 {
		return fmt.Errorf("jsonKey is empty")
	}
	if c.scopes == "" {
		return fmt.Errorf("Scopes param is empty")
	}
	return nil
}

func (c *VertexAIEmbedding) getAccessToken() (string, error) {
	ctx := context.Background()
	creds, err := google.CredentialsFromJSON(ctx, c.jsonKey, c.scopes)
	if err != nil {
		return "", fmt.Errorf("Failed to find credentials: %v", err)
	}
	token, err := creds.TokenSource.Token()
	if err != nil {
		return "", fmt.Errorf("Failed to get token: %v", err)
	}
	return token.AccessToken, nil
}

func (c *VertexAIEmbedding) Embedding(modelName string, texts []string, dim int64, taskType string, timeoutSec int64) (*EmbeddingResponse, error) {
	var r EmbeddingRequest
	for _, text := range texts {
		r.Instances = append(r.Instances, Instance{TaskType: taskType, Content: text})
	}
	if dim != 0 {
		r.Parameters.OutputDimensionality = dim
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
	var token string
	if c.token != "" {
		token = c.token
	} else {
		token, err = c.getAccessToken()
		if err != nil {
			return nil, err
		}
	}

	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
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
