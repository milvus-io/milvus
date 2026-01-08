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

package ark

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"
)

func TestNewArkClient(t *testing.T) {
	// Test with empty API key
	_, err := NewArkClient("", "http://localhost")
	assert.Error(t, err)

	// Test with valid API key
	client, err := NewArkClient("test-api-key", "http://localhost")
	assert.NoError(t, err)
	assert.NotNil(t, client)
}

func TestArkEmbedding(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req model.EmbeddingRequestStrings
		json.NewDecoder(r.Body).Decode(&req)
		defer r.Body.Close()

		res := model.EmbeddingResponse{
			Object: "list",
			Model:  req.Model,
			Data:   make([]model.Embedding, len(req.Input)),
			Usage: model.Usage{
				TotalTokens:  100,
				PromptTokens: 100,
			},
		}

		for i := range req.Input {
			res.Data[i] = model.Embedding{
				Object:    "embedding",
				Embedding: []float32{0.1, 0.2, 0.3, 0.4},
				Index:     i,
			}
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(res)
	}))
	defer ts.Close()

	client, err := NewArkClient("test-api-key", ts.URL)
	assert.NoError(t, err)

	resp, err := client.Embedding(context.Background(), "doubao-embedding", []string{"hello", "world"}, 0, "", false)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, 2, len(resp))
	assert.Equal(t, 4, len(resp[0].Embedding))
}

func TestArkEmbeddingError(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("Internal Server Error"))
	}))
	defer ts.Close()

	client, err := NewArkClient("test-api-key", ts.URL)
	assert.NoError(t, err)

	_, err = client.Embedding(context.Background(), "doubao-embedding", []string{"hello"}, 0, "", false)
	assert.Error(t, err)
}

func TestArkEmbeddingWithParams(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req model.EmbeddingRequestStrings
		json.NewDecoder(r.Body).Decode(&req)
		defer r.Body.Close()

		assert.Equal(t, "user-123", req.User)
		assert.Equal(t, 256, req.Dimensions)

		res := model.EmbeddingResponse{
			Object: "list",
			Model:  req.Model,
			Data:   make([]model.Embedding, len(req.Input)),
		}
		for i := range req.Input {
			res.Data[i] = model.Embedding{
				Embedding: make([]float32, 256),
				Index:     i,
			}
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(res)
	}))
	defer ts.Close()

	client, err := NewArkClient("test-api-key", ts.URL)
	assert.NoError(t, err)

	resp, err := client.Embedding(context.Background(), "doubao-embedding", []string{"hello"}, 256, "user-123", false)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, 256, len(resp[0].Embedding))
}

func TestArkEmbeddingMultiModal(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req model.MultiModalEmbeddingRequest
		json.NewDecoder(r.Body).Decode(&req)
		defer r.Body.Close()

		// Verify request inputs are correctly mapped
		assert.Equal(t, "doubao-embedding-vision", req.Model)
		assert.Equal(t, 1, len(req.Input))
		assert.Equal(t, model.MultiModalEmbeddingInputTypeText, req.Input[0].Type)
		assert.Equal(t, "multimodal test", *req.Input[0].Text)

		res := model.MultimodalEmbeddingResponse{
			Data: model.MultimodalEmbedding{
				Embedding: []float32{0.5, 0.6},
				Object:    "embedding",
			},
		}

		w.WriteHeader(http.StatusOK)
		json.NewEncoder(w).Encode(res)
	}))
	defer ts.Close()

	client, err := NewArkClient("test-api-key", ts.URL)
	assert.NoError(t, err)

	resp, err := client.Embedding(context.Background(), "doubao-embedding-vision", []string{"multimodal test"}, 0, "", true)
	assert.NoError(t, err)
	assert.NotNil(t, resp)
	assert.Equal(t, 1, len(resp))
	assert.Equal(t, 2, len(resp[0].Embedding))
	assert.Equal(t, float32(0.5), resp[0].Embedding[0])
}
