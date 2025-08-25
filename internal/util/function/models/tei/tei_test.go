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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewClient(t *testing.T) {
	_, err := NewTEIClient("mock_key", "http://localhost")
	assert.True(t, err == nil)
}

func TestEmbeddingOK(t *testing.T) {
	repStr := `[[0.0, 0.1],[1.0, 1.1], [2.0, 2.1]]`
	var res [][]float32
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
		c, _ := NewTEIClient("mock_key", url)
		ret, err := c.Embedding([]string{"sentence"}, true, "left", "query", 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret, &EmbeddingResponse{{0.0, 0.1}, {1.0, 1.1}, {2.0, 2.1}})
	}

	{
		c, _ := NewTEIClient("mock_key", url)
		ret, err := c.Embedding([]string{"sentence"}, false, "", "", 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret, &EmbeddingResponse{{0.0, 0.1}, {1.0, 1.1}, {2.0, 2.1}})
	}
}

func TestEmbeddingFailed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))

	defer ts.Close()
	url := ts.URL

	{
		c, _ := NewTEIClient("mock_key", url)
		_, err := c.Embedding([]string{"sentence"}, true, "left", "query", 0)
		assert.True(t, err != nil)
	}
}

func TestRerankOK(t *testing.T) {
	repStr := `[{"index":0,"score":0.0},{"index":2,"score":0.2}, {"index":1,"score":0.1}]`
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
		c, _ := NewTEIClient("", url)
		ret, err := c.Rerank("query", []string{"t1", "t2", "t3"}, nil, 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret, &RerankResponse{RerankResponseItem{Index: 0, Score: 0.0}, RerankResponseItem{Index: 1, Score: 0.1}, RerankResponseItem{Index: 2, Score: 0.2}})
	}
}

func TestRerankFailed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`not json`))
	}))
	defer ts.Close()
	url := ts.URL

	{
		c, _ := NewTEIClient("mock_key", url)
		_, err := c.Rerank("query", []string{"t1", "t2", "t3"}, nil, 0)
		assert.True(t, err != nil)
	}
}
