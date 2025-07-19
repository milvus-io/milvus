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

package ali

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestEmbeddingClientCheck(t *testing.T) {
	{
		c := NewAliDashScopeEmbeddingClient("", "mock_uri")
		err := c.Check()
		assert.True(t, err != nil)
		fmt.Println(err)
	}

	{
		c := NewAliDashScopeEmbeddingClient("mock_key", "")
		err := c.Check()
		assert.True(t, err != nil)
		fmt.Println(err)
	}

	{
		c := NewAliDashScopeEmbeddingClient("mock_key", "mock_uri")
		err := c.Check()
		assert.True(t, err == nil)
	}
}

func TestEmbeddingOK(t *testing.T) {
	var res EmbeddingResponse
	repStr := `{
"output": {
"embeddings": [
{
"text_index": 1,
"embedding": [0.1]
},
{
"text_index": 0,
"embedding": [0.0]
},
{
"text_index": 2,
"embedding": [0.2]
}
]
},
"usage": {
"total_tokens": 100
},
"request_id": "0000000000000"
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
		c := NewAliDashScopeEmbeddingClient("mock_key", url)
		err := c.Check()
		assert.True(t, err == nil)
		ret, err := c.Embedding("text-embedding-v2", []string{"sentence"}, 0, "query", "dense", 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret.Output.Embeddings[0].TextIndex, 0)
		assert.Equal(t, ret.Output.Embeddings[1].TextIndex, 1)
		assert.Equal(t, ret.Output.Embeddings[2].TextIndex, 2)
		assert.Equal(t, ret.Output.Embeddings[0].Embedding, []float32{0.0})
		assert.Equal(t, ret.Output.Embeddings[1].Embedding, []float32{0.1})
		assert.Equal(t, ret.Output.Embeddings[2].Embedding, []float32{0.2})
	}
}

func TestEmbeddingFailed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))

	defer ts.Close()
	url := ts.URL

	{
		c := NewAliDashScopeEmbeddingClient("mock_key", url)
		err := c.Check()
		assert.True(t, err == nil)
		_, err = c.Embedding("text-embedding-v2", []string{"sentence"}, 0, "query", "dense", 0)
		assert.True(t, err != nil)
	}
}
