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

package minimax

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
  "vectors": [
    [0.0, 0.1],
    [1.0, 1.1],
    [2.0, 2.1]
  ],
  "total_tokens": 10,
  "base_resp": {
    "status_code": 0,
    "status_msg": "success"
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
		c, _ := NewMiniMaxClient("mock_key")
		ret, err := c.Embedding(url, "embo-01", []string{"sentence1", "sentence2", "sentence3"}, "db", 0)
		assert.NoError(t, err)
		assert.Equal(t, 3, len(ret.Vectors))
		assert.Equal(t, []float32{0.0, 0.1}, ret.Vectors[0])
		assert.Equal(t, []float32{1.0, 1.1}, ret.Vectors[1])
		assert.Equal(t, []float32{2.0, 2.1}, ret.Vectors[2])
		assert.Equal(t, 10, ret.TotalTokens)
		assert.Equal(t, 0, ret.BaseResp.StatusCode)
	}
}

func TestEmbeddingFailed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))

	defer ts.Close()
	url := ts.URL

	{
		c, _ := NewMiniMaxClient("mock_key")
		_, err := c.Embedding(url, "embo-01", []string{"sentence"}, "db", 0)
		assert.Error(t, err)
	}
}

func TestEmbeddingAPIError(t *testing.T) {
	res := EmbeddingResponse{
		Vectors:     nil,
		TotalTokens: 0,
		BaseResp: BaseResp{
			StatusCode: 1004,
			StatusMsg:  "invalid api key",
		},
	}
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	url := ts.URL

	{
		c, _ := NewMiniMaxClient("mock_key")
		_, err := c.Embedding(url, "embo-01", []string{"sentence"}, "db", 0)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "invalid api key")
	}
}

func TestNewMiniMaxClientEmptyKey(t *testing.T) {
	_, err := NewMiniMaxClient("")
	assert.Error(t, err)
}

func TestEmbeddingQueryType(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req EmbeddingRequest
		decoder := json.NewDecoder(r.Body)
		decoder.Decode(&req)
		assert.Equal(t, "query", req.Type)
		assert.Equal(t, "embo-01", req.Model)

		res := EmbeddingResponse{
			Vectors:     [][]float32{{0.1, 0.2}},
			TotalTokens: 5,
			BaseResp: BaseResp{
				StatusCode: 0,
				StatusMsg:  "success",
			},
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()

	c, _ := NewMiniMaxClient("mock_key")
	ret, err := c.Embedding(ts.URL, "embo-01", []string{"search query"}, "query", 0)
	assert.NoError(t, err)
	assert.Equal(t, 1, len(ret.Vectors))
}
