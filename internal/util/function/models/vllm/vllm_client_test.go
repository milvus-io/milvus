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

package vllm

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	_, err := NewVLLMClient("mock_key", "http://localhost")
	assert.True(t, err == nil)
}

func TestEmbeddingOK(t *testing.T) {
	repStr := `{
  "id": "embd-22dfa7cd179d4bf3bac367a1db90fe62",
  "object": "list",
  "created": 1752044194,
  "model": "BAAI/bge-base-en-v1.5",
  "data": [
    {
      "index": 0,
      "object": "embedding",
      "embedding": [0.1, 0.2]
    }
  ],
  "usage": {
    "prompt_tokens": 3,
    "total_tokens": 3,
    "completion_tokens": 0,
    "prompt_tokens_details": null
  }
}`
	var res EmbeddingResponse
	err := json.Unmarshal([]byte(repStr), &res)
	assert.NoError(t, err)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	defer ts.Close()
	url := ts.URL

	{
		c, err := NewVLLMClient("mock_key", url)
		assert.NoError(t, err)
		r, err := c.Embedding([]string{"sentence"}, nil, 0)
		assert.NoError(t, err)
		assert.Equal(t, r.Data[0].Embedding, res.Data[0].Embedding)
	}
}

func TestEmbeddingFailed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer ts.Close()
	url := ts.URL

	{
		c, err := NewVLLMClient("mock_key", url)
		assert.NoError(t, err)
		_, err = c.Embedding([]string{"sentence"}, nil, 0)
		assert.Error(t, err)
	}
}

func TestRerankOK(t *testing.T) {
	repStr := `{
  "id": "rerank-17b879c53eb547ceadd89ad9d402a461",
  "model": "BAAI/bge-base-en-v1.5",
  "usage": {
    "total_tokens": 7
  },
  "results": [
    {
      "index": 0,
      "document": {
        "text": "string"
      },
      "relevance_score": 0.0
    },
	{
      "index": 2,
      "document": {
        "text": "string"
      },
      "relevance_score": 2.0
    },
	{
      "index": 1,
      "document": {
        "text": "string"
      },
      "relevance_score": 1.0
    }
  ]
}
`
	var res RerankResponse
	err := json.Unmarshal([]byte(repStr), &res)
	assert.NoError(t, err)
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	defer ts.Close()
	url := ts.URL

	{
		c, err := NewVLLMClient("mock_key", url)
		assert.NoError(t, err)
		r, err := c.Rerank("query", []string{"t1", "t2", "t3"}, nil, 0)
		assert.NoError(t, err)
		assert.Equal(t, r.Results[0].Index, 0)
		assert.Equal(t, r.Results[0].RelevanceScore, float32(0.0))
		assert.Equal(t, r.Results[1].Index, 1)
		assert.Equal(t, r.Results[1].RelevanceScore, float32(1.0))
		assert.Equal(t, r.Results[2].Index, 2)
		assert.Equal(t, r.Results[2].RelevanceScore, float32(2.0))
	}
}

func TestRerankFailed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))
	defer ts.Close()
	url := ts.URL

	{
		c, err := NewVLLMClient("mock_key", url)
		assert.NoError(t, err)
		_, err = c.Rerank("query", []string{"t1", "t2", "t3"}, nil, 0)
		assert.Error(t, err)
	}
}
