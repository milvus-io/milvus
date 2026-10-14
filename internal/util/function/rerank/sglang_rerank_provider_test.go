/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package rerank

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
)

func newSGLangTestProvider(t *testing.T, endpoint string, timeoutMs string) ModelProvider {
	t.Helper()
	params := []*commonpb.KeyValuePair{
		{Key: models.EndpointParamKey, Value: endpoint},
		{Key: models.ModelNameParamKey, Value: "BAAI/bge-reranker-v2-m3"},
		{Key: models.CredentialParamKey, Value: "sglang"},
	}
	if timeoutMs != "" {
		params = append(params, &commonpb.KeyValuePair{Key: models.TimeoutMsParamKey, Value: timeoutMs})
	}
	provider, err := newSGLangProvider(params, nil, credentials.NewCredentials(map[string]string{"sglang.apikey": "test-key"}))
	require.NoError(t, err)
	return provider
}

func TestSGLangRerank(t *testing.T) {
	docs := []string{"d0", "d1", "d2"}
	t.Run("maps result indexes to request documents", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			require.Equal(t, http.MethodPost, r.Method)
			require.Equal(t, "/v1/rerank", r.URL.Path)
			require.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
			var request struct {
				Model     string   `json:"model"`
				Query     string   `json:"query"`
				Documents []string `json:"documents"`
			}
			require.NoError(t, json.NewDecoder(r.Body).Decode(&request))
			require.Equal(t, "BAAI/bge-reranker-v2-m3", request.Model)
			require.Equal(t, "q", request.Query)
			require.Equal(t, docs, request.Documents)
			_, _ = w.Write([]byte(`[{"index":2,"score":0.3},{"index":0,"score":0},{"index":1,"score":0.2}]`))
		}))
		defer server.Close()

		scores, err := newSGLangTestProvider(t, server.URL, "").Rerank(context.Background(), "q", docs)
		require.NoError(t, err)
		require.Equal(t, []float32{0, 0.2, 0.3}, scores)
	})

	for _, tc := range []struct {
		name string
		body string
		err  string
	}{
		{"duplicate index", `[{"index":0,"score":0.1},{"index":0,"score":0.2},{"index":2,"score":0.3}]`, "invalid or duplicated result index"},
		{"missing index", `[{"score":0.1},{"index":1,"score":0.2},{"index":2,"score":0.3}]`, "without index or score"},
		{"missing result", `[{"index":0,"score":0.1},{"index":1,"score":0.2}]`, "number of docs and scores does not match"},
		{"negative index", `[{"index":-1,"score":0.1},{"index":1,"score":0.2},{"index":2,"score":0.3}]`, "invalid or duplicated result index"},
		{"out of range index", `[{"index":0,"score":0.1},{"index":1,"score":0.2},{"index":3,"score":0.3}]`, "invalid or duplicated result index"},
		{"missing score", `[{"index":0},{"index":1,"score":0.2},{"index":2,"score":0.3}]`, "without index or score"},
		{"malformed response", `{`, "unmarshal response failed"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, _ = w.Write([]byte(tc.body))
			}))
			defer server.Close()

			_, err := newSGLangTestProvider(t, server.URL, "").Rerank(context.Background(), "q", docs)
			require.ErrorContains(t, err, tc.err)
		})
	}
}

func TestSGLangRerankErrorsRespectContext(t *testing.T) {
	t.Run("API error", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte(`{"message":"invalid request"}`))
		}))
		defer server.Close()

		_, err := newSGLangTestProvider(t, server.URL, "").Rerank(context.Background(), "q", []string{"d"})
		require.ErrorContains(t, err, "call service failed")
	})

	t.Run("timeout", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}))
		defer server.Close()

		_, err := newSGLangTestProvider(t, server.URL, "20").Rerank(context.Background(), "q", []string{"d"})
		require.ErrorIs(t, err, context.DeadlineExceeded)
	})

	t.Run("cancellation", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			<-r.Context().Done()
		}))
		defer server.Close()

		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		_, err := newSGLangTestProvider(t, server.URL, "").Rerank(ctx, "q", []string{"d"})
		require.True(t, errors.Is(err, context.Canceled))
	})
}
