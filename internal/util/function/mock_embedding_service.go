/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package function

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"

	"github.com/milvus-io/milvus/internal/models"
)

func mockEmbedding(texts []string, dim int) [][]float32 {
	embeddings := make([][]float32, 0)
	for i := 0; i < len(texts); i++ {
		f := float32(i)
		emb := make([]float32, 0)
		for j := 0; j < dim; j++ {
			emb = append(emb, f+float32(j)*0.1)
		}
		embeddings = append(embeddings, emb)
	}
	return embeddings
}

func CreateEmbeddingServer() *httptest.Server {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req models.EmbeddingRequest
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		json.Unmarshal(body, &req)
		embs := mockEmbedding(req.Input, req.Dimensions)
		var res models.EmbeddingResponse
		res.Object = "list"
		res.Model = "text-embedding-3-small"
		for i := 0; i < len(req.Input); i++ {
			res.Data = append(res.Data, models.EmbeddingData{
				Object:    "embedding",
				Embedding: embs[i],
				Index:     i,
			})
		}

		res.Usage = models.Usage{
			PromptTokens: 1,
			TotalTokens:  100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)

	}))
	return ts
}
