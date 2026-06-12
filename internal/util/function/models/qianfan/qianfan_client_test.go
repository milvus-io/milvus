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
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQianfanEmbeddingClientCheck(t *testing.T) {
	{
		c := NewQianfanEmbeddingClient("", "mock_uri")
		err := c.Check()
		assert.Error(t, err)
	}

	{
		c := NewQianfanEmbeddingClient("mock_key", "")
		err := c.Check()
		assert.Error(t, err)
	}

	{
		c := NewQianfanEmbeddingClient("mock_key", "mock_uri")
		err := c.Check()
		assert.NoError(t, err)
	}
}

func TestQianfanEmbeddingSortsByIndexAndSendsRequest(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		assert.Equal(t, "application/json", r.Header.Get("Content-Type"))
		assert.Equal(t, "Bearer mock_key", r.Header.Get("Authorization"))

		var req EmbeddingRequest
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		assert.Equal(t, "embedding-v1", req.Model)
		assert.Equal(t, []string{"sentence 1", "sentence 2"}, req.Input)

		res := EmbeddingResponse{
			Object: "list",
			Model:  "embedding-v1",
			Data: []EmbeddingData{
				{
					Object:    "embedding",
					Embedding: []float32{1.0, 2.0, 3.0, 4.0},
					Index:     1,
				},
				{
					Object:    "embedding",
					Embedding: []float32{0.0, 1.0, 2.0, 3.0},
					Index:     0,
				},
			},
			Usage: Usage{
				PromptTokens: 2,
				TotalTokens:  2,
			},
		}
		w.WriteHeader(http.StatusOK)
		err = json.NewEncoder(w).Encode(res)
		require.NoError(t, err)
	}))
	defer ts.Close()

	c := NewQianfanEmbeddingClient("mock_key", ts.URL)
	ret, err := c.Embedding("embedding-v1", []string{"sentence 1", "sentence 2"}, 4, 30)
	require.NoError(t, err)
	assert.Equal(t, []float32{0.0, 1.0, 2.0, 3.0}, ret.Data[0].Embedding)
	assert.Equal(t, []float32{1.0, 2.0, 3.0, 4.0}, ret.Data[1].Embedding)
}

func TestQianfanEmbeddingFailed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = fmt.Fprint(w, "unauthorized")
	}))
	defer ts.Close()

	c := NewQianfanEmbeddingClient("mock_key", ts.URL)
	err := c.Check()
	assert.NoError(t, err)
	_, err = c.Embedding("embedding-v1", []string{"sentence"}, 0, 30)
	assert.Error(t, err)
}
