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

package models

import (
	// "bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestEmbeddingClientCheck(t *testing.T) {
	{
		c := OpenAIEmbeddingClient{"mock_key", "mock_uri", "unknow_model"}
		err := c.Check();
		assert.True(t, err != nil)
		fmt.Println(err)
	}

	{
		c := OpenAIEmbeddingClient{"", "mock_uri", TextEmbeddingAda002}
		err := c.Check();
		assert.True(t, err != nil)
		fmt.Println(err)
	}

	{
		c := OpenAIEmbeddingClient{"mock_key", "", TextEmbedding3Small}
		err := c.Check();
		assert.True(t, err != nil)
		fmt.Println(err)
	}

	{
		c := OpenAIEmbeddingClient{"mock_key", "mock_uri", TextEmbedding3Small}
		err := c.Check();
		assert.True(t, err == nil)
	}
}


func TestEmbeddingOK(t *testing.T) {
	var res EmbeddingResponse
	res.Object = "list"
	res.Model = TextEmbedding3Small
	res.Data = []EmbeddingData{
		{
			Object: "embedding",
			Embedding: []float32{1.1, 2.2, 3.3, 4.4},
			Index: 0,
		},
	}
	res.Usage = Usage{
		PromptTokens: 1,
		TotalTokens: 100,
	}
	
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	url := ts.URL

	{
		c := OpenAIEmbeddingClient{"mock_key", url, TextEmbedding3Small}
		err := c.Check();
		assert.True(t, err == nil)
		ret, err := c.Embedding([]string{"sentence"}, 0, "", 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret, res)
	}
}


func TestEmbeddingRetry(t *testing.T) {
	var res EmbeddingResponse
	res.Object = "list"
	res.Model = TextEmbedding3Small
	res.Data = []EmbeddingData{
		{
			Object: "embedding",
			Embedding: []float32{1.1, 2.2, 3.3, 4.4},
			Index: 0,
		},
	}
	res.Usage = Usage{
		PromptTokens: 1,
		TotalTokens: 100,
	}

	var count = 0
	
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if count < 2 {
			count += 1
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
		c := OpenAIEmbeddingClient{"mock_key", url, TextEmbedding3Small}
		err := c.Check();
		assert.True(t, err == nil)
		ret, err := c.Embedding([]string{"sentence"}, 0, "", 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret, res)
		assert.Equal(t, count, 2)
	}
}


func TestEmbeddingFailed(t *testing.T) {
	var count = 0
	
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		count += 1
		w.WriteHeader(http.StatusUnauthorized)
	}))

	defer ts.Close()
	url := ts.URL

	{
		c := OpenAIEmbeddingClient{"mock_key", url, TextEmbedding3Small}
		err := c.Check();
		assert.True(t, err == nil)
		_, err = c.Embedding([]string{"sentence"}, 0, "", 0)
		assert.True(t, err != nil)
		assert.Equal(t, count, 3)
	}
}

func TestTimeout(t *testing.T) {
	var st = "Doing"
	
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
		st = "Done"
		w.WriteHeader(http.StatusUnauthorized)
		
	}))

	defer ts.Close()
	url := ts.URL

	{
		c := OpenAIEmbeddingClient{"mock_key", url, TextEmbedding3Small}
		err := c.Check();
		assert.True(t, err == nil)
		_, err = c.Embedding([]string{"sentence"}, 0, "", 1)
		assert.True(t, err != nil)
		assert.Equal(t, st, "Doing")
		time.Sleep(3 * time.Second)
		assert.Equal(t, st, "Done")		
	}
}
