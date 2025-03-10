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

package voyageai

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
		c := NewVoyageAIEmbeddingClient("", "mock_uri")
		err := c.Check()
		assert.True(t, err != nil)
		fmt.Println(err)
	}

	{
		c := NewVoyageAIEmbeddingClient("mock_key", "")
		err := c.Check()
		assert.True(t, err != nil)
		fmt.Println(err)
	}

	{
		c := NewVoyageAIEmbeddingClient("mock_key", "mock_uri")
		err := c.Check()
		assert.True(t, err == nil)
	}
}

func TestEmbeddingOK(t *testing.T) {
	var res EmbeddingResponse[float32]
	repStr := `{
  "object": "list",
  "data": [
    {
      "object": "embedding",
      "embedding": [
        0.0,
        0.1
      ],
      "index": 0
    },
    {
      "object": "embedding",
      "embedding": [
        2.0,
        2.1
      ],
      "index": 2
    },
    {
      "object": "embedding",
      "embedding": [
        1.0,
        1.1
      ],
      "index": 1
    }
  ],
  "model": "voyage-large-2",
  "usage": {
    "total_tokens": 10
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
		c := NewVoyageAIEmbeddingClient("mock_key", url)
		err := c.Check()
		assert.True(t, err == nil)
		r, err := c.Embedding("voyage-3", []string{"sentence"}, 0, "query", "float", true, 0)
		ret := r.(*EmbeddingResponse[float32])
		assert.True(t, err == nil)
		assert.Equal(t, ret.Data[0].Index, 0)
		assert.Equal(t, ret.Data[1].Index, 1)
		assert.Equal(t, ret.Data[2].Index, 2)

		assert.Equal(t, ret.Data[0].Embedding, []float32{0.0, 0.1})
		assert.Equal(t, ret.Data[1].Embedding, []float32{1.0, 1.1})
		assert.Equal(t, ret.Data[2].Embedding, []float32{2.0, 2.1})
	}
}

func TestEmbeddingInt8Embed(t *testing.T) {
	var res EmbeddingResponse[int8]
	repStr := `{
  "object": "list",
  "data": [
    {
      "object": "embedding",
      "embedding": [
        1,2
      ],
      "index": 0
    },
    {
      "object": "embedding",
      "embedding": [
        5,6
      ],
      "index": 2
    },
    {
      "object": "embedding",
      "embedding": [
        3,4
      ],
      "index": 1
    }
  ],
  "model": "voyage-large-2",
  "usage": {
    "total_tokens": 10
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
		c := NewVoyageAIEmbeddingClient("mock_key", url)
		err := c.Check()
		assert.True(t, err == nil)
		r, err := c.Embedding("voyage-3", []string{"sentence"}, 0, "query", "int8", false, 0)
		ret := r.(*EmbeddingResponse[int8])
		assert.True(t, err == nil)
		assert.Equal(t, ret.Data[0].Index, 0)
		assert.Equal(t, ret.Data[1].Index, 1)
		assert.Equal(t, ret.Data[2].Index, 2)

		assert.Equal(t, ret.Data[0].Embedding, []int8{1, 2})
		assert.Equal(t, ret.Data[1].Embedding, []int8{3, 4})
		assert.Equal(t, ret.Data[2].Embedding, []int8{5, 6})

		_, err = c.Embedding("voyage-3", []string{"sentence"}, 0, "query", "unknow", true, 0)
		assert.Error(t, err)
	}
}

func TestEmbeddingFailed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))

	defer ts.Close()
	url := ts.URL

	{
		c := NewVoyageAIEmbeddingClient("mock_key", url)
		err := c.Check()
		assert.True(t, err == nil)
		_, err = c.Embedding("voyage-3", []string{"sentence"}, 0, "query", "float", false, 0)
		assert.True(t, err != nil)
	}
}
