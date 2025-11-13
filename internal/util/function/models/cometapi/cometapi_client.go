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

package cometapi

import (
	"fmt"

	"github.com/milvus-io/milvus/internal/util/function/models"
)

type CometAPIClient struct {
	apiKey string
}

func NewCometAPIClient(apiKey string) (*CometAPIClient, error) {
	if apiKey == "" {
		return nil, fmt.Errorf("Missing credentials config or configure the %s environment variable in the Milvus service.", models.CometAPIAKEnvStr)
	}
	return &CometAPIClient{
		apiKey: apiKey,
	}, nil
}

func (c *CometAPIClient) headers() map[string]string {
	return map[string]string{
		"accept":        "application/json",
		"Content-Type":  "application/json",
		"Authorization": fmt.Sprintf("Bearer %s", c.apiKey),
	}
}

func (c *CometAPIClient) Embedding(url string, modelName string, texts []string, timeoutSec int64) (*EmbeddingResponse, error) {
	embClient := newCometAPIEmbedding(c.apiKey, url)
	return embClient.embedding(modelName, texts, c.headers(), timeoutSec)
}
