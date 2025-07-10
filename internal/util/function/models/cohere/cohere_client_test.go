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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEmbeddingOK(t *testing.T) {
	var res EmbeddingResponse
	repStr := `{
  "id": "5807ee2e-0cda-445a-9ec8-864c60a06606",
  "embeddings": {
    "float": [
      [
        0.0, 0.1
      ],
      [
        1.0, 1.1
      ]
    ]
  },
  "texts": [
    "hello",
    "goodbye"
  ],
  "meta": {
    "api_version": {
      "version": "2",
      "is_experimental": true
    },
    "billed_units": {
      "input_tokens": 2
    },
    "warnings": [
      "You are using an experimental version, for more information please refer to https://docs.cohere.com/versioning-reference"
    ]
  }
}`
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
		c, _ := NewCohereClient("mock_key")
		ret, err := c.Embedding(url, "cohere-3", []string{"sentence"}, "search_document", "float", "END", 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret.Embeddings.Float[0], []float32{0.0, 0.1})
		assert.Equal(t, ret.Embeddings.Float[1], []float32{1.0, 1.1})
	}
}

func TestEmbeddingFailed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))

	defer ts.Close()
	url := ts.URL

	{
		c, _ := NewCohereClient("mock_key")
		_, err := c.Embedding(url, "cohere-3", []string{"sentence"}, "search_document", "float", "END", 0)
		assert.True(t, err != nil)
	}
}

func TestRerankOK(t *testing.T) {
	var res RerankResponse
	repStr := `{
  "results": [
    {
      "index": 3,
      "relevance_score": 0.999071
    },
    {
      "index": 4,
      "relevance_score": 0.7867867
    },
    {
      "index": 0,
      "relevance_score": 0.32713068
    }
  ],
  "id": "07734bd2-2473-4f07-94e1-0d9f0e6843cf",
  "meta": {
    "api_version": {
      "version": "2",
      "is_experimental": false
    },
    "billed_units": {
      "search_units": 1
    }
  }
}`
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
		c, _ := NewCohereClient("mock_key")
		ret, err := c.Rerank(url, "rerank-lite-1", "query", []string{"text1", "text2", "text3"}, map[string]any{}, 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret.Results[0].Index, 0)
		assert.Equal(t, ret.Results[1].Index, 3)
		assert.Equal(t, ret.Results[2].Index, 4)
		assert.Equal(t, ret.Results[0].RelevanceScore, float32(0.32713068))
		assert.Equal(t, ret.Results[1].RelevanceScore, float32(0.999071))
		assert.Equal(t, ret.Results[2].RelevanceScore, float32(0.7867867))
		assert.Equal(t, ret.Id, "07734bd2-2473-4f07-94e1-0d9f0e6843cf")
		assert.Equal(t, ret.Meta.BilledUnits.SearchUnits, 1)
	}
}

func TestRerankFailed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))

	defer ts.Close()
	url := ts.URL

	{
		c, _ := NewCohereClient("mock_key")
		_, err := c.Rerank(url, "rerank-lite-1", "query", []string{"text1", "text2", "text3"}, map[string]any{}, 0)
		assert.True(t, err != nil)
	}
}
