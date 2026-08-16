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

package huggingface

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewClient(t *testing.T) {
	_, err := NewClient("", "")
	assert.Error(t, err)

	c, err := NewClient("mock_key", "")
	require.NoError(t, err)
	assert.Equal(t, DefaultBaseURL, c.baseURL)

	_, err = NewClient("mock_key", "not-a-url")
	assert.Error(t, err)
}

func TestBuildPipelineURL(t *testing.T) {
	url, err := buildPipelineURL("https://router.huggingface.co", "hf-inference", "BAAI/bge-m3", SentenceSimilarityTask)
	require.NoError(t, err)
	assert.Equal(t, "https://router.huggingface.co/hf-inference/models/BAAI/bge-m3/pipeline/sentence-similarity", url)

	url, err = buildPipelineURL("https://router.huggingface.co/base", "", "sentence-transformers/all-MiniLM-L6-v2", FeatureExtractionTask)
	require.NoError(t, err)
	assert.Equal(t, "https://router.huggingface.co/base/hf-inference/models/sentence-transformers/all-MiniLM-L6-v2/pipeline/feature-extraction", url)

	_, err = buildPipelineURL("https://router.huggingface.co", "custom-hf", "BAAI/bge-m3", FeatureExtractionTask)
	assert.ErrorContains(t, err, "only supports hf-inference")

	_, err = buildPipelineURL("https://router.huggingface.co", "hf-inference", "", FeatureExtractionTask)
	assert.Error(t, err)

	_, err = buildPipelineURL("https://router.huggingface.co", "hf-inference", "BAAI/bge-m3", "")
	assert.Error(t, err)

	_, err = buildPipelineURL("https://router.huggingface.co", "hf-inference", "../../secret", FeatureExtractionTask)
	assert.Error(t, err)

	_, err = buildPipelineURL("https://router.huggingface.co", "hf-inference", "BAAI/../secret", FeatureExtractionTask)
	assert.Error(t, err)
}

func TestFeatureExtraction(t *testing.T) {
	normalize := true
	truncate := false
	var req FeatureExtractionRequest
	var gotPath string
	var gotAuth string
	var gotContentType string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		gotContentType = r.Header.Get("Content-Type")
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		w.WriteHeader(http.StatusOK)
		_, err = w.Write([]byte(`[[0.1,0.2],[0.3,0.4]]`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	c, err := NewClient("mock_key", ts.URL)
	require.NoError(t, err)
	resp, err := c.FeatureExtraction("hf-inference", "BAAI/bge-m3", []string{"doc1", "doc2"}, map[string]any{
		"normalize":            normalize,
		"truncate":             truncate,
		"truncation_direction": "left",
		"prompt_name":          "query",
	}, 0)
	require.NoError(t, err)

	assert.Equal(t, "/hf-inference/models/BAAI/bge-m3/pipeline/feature-extraction", gotPath)
	assert.Equal(t, "Bearer mock_key", gotAuth)
	assert.Equal(t, "application/json", gotContentType)
	assert.Equal(t, []any{"doc1", "doc2"}, req["inputs"])
	assert.Equal(t, true, req["normalize"])
	assert.Equal(t, false, req["truncate"])
	assert.Equal(t, "left", req["truncation_direction"])
	assert.Equal(t, "query", req["prompt_name"])
	assert.JSONEq(t, `[[0.1,0.2],[0.3,0.4]]`, string(*resp))
}

func TestSentenceSimilarity(t *testing.T) {
	var req SentenceSimilarityRequest
	var gotPath string
	var gotAuth string

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotAuth = r.Header.Get("Authorization")
		err := json.NewDecoder(r.Body).Decode(&req)
		require.NoError(t, err)
		w.WriteHeader(http.StatusOK)
		_, err = w.Write([]byte(`[0.12,0.34,0.56]`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	c, err := NewClient("mock_key", ts.URL)
	require.NoError(t, err)
	resp, err := c.SentenceSimilarity("hf-inference", "BAAI/bge-m3", "query", []string{"doc1", "doc2", "doc3"}, 0)
	require.NoError(t, err)

	assert.Equal(t, "/hf-inference/models/BAAI/bge-m3/pipeline/sentence-similarity", gotPath)
	assert.Equal(t, "Bearer mock_key", gotAuth)
	assert.Equal(t, "query", req.Inputs.SourceSentence)
	assert.Equal(t, []string{"doc1", "doc2", "doc3"}, req.Inputs.Sentences)
	assert.Equal(t, &SentenceSimilarityResponse{0.12, 0.34, 0.56}, resp)
}

func TestFailedResponse(t *testing.T) {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte(`not json`))
		require.NoError(t, err)
	}))
	defer ts.Close()

	c, err := NewClient("mock_key", ts.URL)
	require.NoError(t, err)
	_, err = c.SentenceSimilarity("hf-inference", "BAAI/bge-m3", "query", []string{"doc1"}, 0)
	assert.Error(t, err)
}
