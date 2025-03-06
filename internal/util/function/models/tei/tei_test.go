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

func TestEmbeddingClientCheck(t *testing.T) {
	{
		_, err := NewTEIEmbeddingClient("", "http://mymock.com")
		assert.True(t, err == nil)
	}

	{
		c, err := NewTEIEmbeddingClient("mock_key", "mock")
		assert.True(t, c == nil)
		assert.True(t, err != nil)
	}

	{
		_, err := NewTEIEmbeddingClient("", "")
		assert.True(t, err != nil)
	}

	{
		_, err := NewTEIEmbeddingClient("", "http://")
		assert.True(t, err != nil)
	}
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
		c, _ := NewTEIEmbeddingClient("mock_key", url)
		ret, err := c.Embedding([]string{"sentence"}, true, "left", "query", 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret, [][]float32{{0.0, 0.1}, {1.0, 1.1}, {2.0, 2.1}})
	}

	{
		c, _ := NewTEIEmbeddingClient("mock_key", url)
		ret, err := c.Embedding([]string{"sentence"}, false, "", "", 0)
		assert.True(t, err == nil)
		assert.Equal(t, ret, [][]float32{{0.0, 0.1}, {1.0, 1.1}, {2.0, 2.1}})
	}
}

func TestEmbeddingFailed(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
	}))

	defer ts.Close()
	url := ts.URL

	{
		c, _ := NewTEIEmbeddingClient("mock_key", url)
		_, err := c.Embedding([]string{"sentence"}, true, "left", "query", 0)
		assert.True(t, err != nil)
	}
}
