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

package openai

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEmbeddingClientCheck(t *testing.T) {
	{
		c := NewOpenAIEmbeddingClient("", "mock_uri")
		err := c.Check()
		assert.True(t, err != nil)
		fmt.Println(err)
	}

	{
		c := NewOpenAIEmbeddingClient("mock_key", "")
		err := c.Check()
		assert.True(t, err != nil)
		fmt.Println(err)
	}

	{
		c := NewOpenAIEmbeddingClient("mock_key", "mock_uri")
		err := c.Check()
		assert.True(t, err == nil)
	}
}

func TestEmbeddingOK(t *testing.T) {
	var res EmbeddingResponse
	res.Object = "list"
	res.Model = "text-embedding-3-small"
	res.Data = []EmbeddingData{
		{
			Object:    "embedding",
			Embedding: []float32{1.1, 2.2, 3.3, 4.4},
			Index:     1,
		},
		{
			Object:    "embedding",
			Embedding: []float32{1.1, 2.2, 3.3, 4.4},
			Index:     0,
		},
	}
	res.Usage = Usage{
		PromptTokens: 1,
		TotalTokens:  100,
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/" {
			if r.Header["Authorization"][0] != "" {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusBadRequest)
			}
		} else {
			if r.Header["Api-Key"][0] != "" {
				w.WriteHeader(http.StatusOK)
			} else {
				w.WriteHeader(http.StatusBadRequest)
			}
		}

		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	url := ts.URL

	{
		c := NewOpenAIEmbeddingClient("mock_key", url)
		err := c.Check()
		assert.True(t, err == nil)
		ret, err := c.Embedding("text-embedding-3-small", []string{"sentence"}, 0, "", 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret.Data[0].Index, 0)
		assert.Equal(t, ret.Data[1].Index, 1)
	}
	{
		c := NewAzureOpenAIEmbeddingClient("mock_key", url)
		err := c.Check()
		assert.True(t, err == nil)
		ret, err := c.Embedding("text-embedding-3-small", []string{"sentence"}, 0, "", 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret.Data[0].Index, 0)
		assert.Equal(t, ret.Data[1].Index, 1)
	}
}

func TestEmbeddingRetry(t *testing.T) {
	var res EmbeddingResponse
	res.Object = "list"
	res.Model = "text-embedding-3-small"
	res.Data = []EmbeddingData{
		{
			Object:    "embedding",
			Embedding: []float32{1.1, 2.2, 3.2, 4.5},
			Index:     2,
		},
		{
			Object:    "embedding",
			Embedding: []float32{1.1, 2.2, 3.3, 4.4},
			Index:     0,
		},
		{
			Object:    "embedding",
			Embedding: []float32{1.1, 2.2, 3.2, 4.3},
			Index:     1,
		},
	}
	res.Usage = Usage{
		PromptTokens: 1,
		TotalTokens:  100,
	}

	var count int32 = 0

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.LoadInt32(&count) < 2 {
			atomic.AddInt32(&count, 1)
			w.WriteHeader(http.StatusUnauthorized)
		} else {
			w.WriteHeader(http.StatusOK)
			data, _ := json.Marshal(res)
			w.Write(data)
		}
	}))

	defer ts.Close()
	url := ts.URL

	{
		c := NewOpenAIEmbeddingClient("mock_key", url)
		err := c.Check()
		assert.True(t, err == nil)
		ret, err := c.Embedding("text-embedding-3-small", []string{"sentence"}, 0, "", 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret.Usage, res.Usage)
		assert.Equal(t, ret.Object, res.Object)
		assert.Equal(t, ret.Model, res.Model)
		assert.Equal(t, ret.Data[0], res.Data[1])
		assert.Equal(t, ret.Data[1], res.Data[2])
		assert.Equal(t, ret.Data[2], res.Data[0])
		assert.Equal(t, atomic.LoadInt32(&count), int32(2))
	}
	{
		c := NewAzureOpenAIEmbeddingClient("mock_key", url)
		err := c.Check()
		assert.True(t, err == nil)
		ret, err := c.Embedding("text-embedding-3-small", []string{"sentence"}, 0, "", 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret.Usage, res.Usage)
		assert.Equal(t, ret.Object, res.Object)
		assert.Equal(t, ret.Model, res.Model)
		assert.Equal(t, ret.Data[0], res.Data[1])
		assert.Equal(t, ret.Data[1], res.Data[2])
		assert.Equal(t, ret.Data[2], res.Data[0])
		assert.Equal(t, atomic.LoadInt32(&count), int32(2))
	}
}

func TestEmbeddingFailed(t *testing.T) {
	var count int32 = 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&count, 1)
		w.WriteHeader(http.StatusUnauthorized)
	}))

	defer ts.Close()
	url := ts.URL

	{
		atomic.StoreInt32(&count, 0)
		c := NewOpenAIEmbeddingClient("mock_key", url)
		err := c.Check()
		assert.True(t, err == nil)
		_, err = c.Embedding("text-embedding-3-small", []string{"sentence"}, 0, "", 0)
		assert.True(t, err != nil)
		assert.Equal(t, atomic.LoadInt32(&count), int32(3))
	}
	{
		atomic.StoreInt32(&count, 0)
		c := NewAzureOpenAIEmbeddingClient("mock_key", url)
		err := c.Check()
		assert.True(t, err == nil)
		_, err = c.Embedding("text-embedding-3-small", []string{"sentence"}, 0, "", 0)
		assert.True(t, err != nil)
		assert.Equal(t, atomic.LoadInt32(&count), int32(3))
	}
}

func TestTimeout(t *testing.T) {
	var st int32 = 0
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
		atomic.AddInt32(&st, 1)
		w.WriteHeader(http.StatusUnauthorized)
	}))

	defer ts.Close()
	url := ts.URL

	{
		atomic.StoreInt32(&st, 0)
		c := NewOpenAIEmbeddingClient("mock_key", url)
		err := c.Check()
		assert.True(t, err == nil)
		_, err = c.Embedding("text-embedding-3-small", []string{"sentence"}, 0, "", 1)
		assert.True(t, err != nil)
		assert.Equal(t, atomic.LoadInt32(&st), int32(0))
		time.Sleep(3 * time.Second)
		assert.Equal(t, atomic.LoadInt32(&st), int32(1))
	}

	{
		atomic.StoreInt32(&st, 0)
		c := NewAzureOpenAIEmbeddingClient("mock_key", url)
		err := c.Check()
		assert.True(t, err == nil)
		_, err = c.Embedding("text-embedding-3-small", []string{"sentence"}, 0, "", 1)
		assert.True(t, err != nil)
		assert.Equal(t, atomic.LoadInt32(&st), int32(0))
		time.Sleep(3 * time.Second)
		assert.Equal(t, atomic.LoadInt32(&st), int32(1))
	}
}
