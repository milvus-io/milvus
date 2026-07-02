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

package rerank

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/huggingface"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func (s *RerankModelSuite) TestNewHuggingFaceProvider() {
	creds := credentials.NewCredentials(map[string]string{"mock.apikey": "mock_key"})
	extraInfo := &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"}
	{
		params := []*commonpb.KeyValuePair{
			{Key: models.CredentialParamKey, Value: "mock"},
		}
		_, err := newHuggingFaceProvider(params, nil, creds, extraInfo)
		s.ErrorContains(err, "huggingface rerank model name is required")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.ModelNameParamKey, Value: "BAAI/bge-m3"},
			{Key: models.HuggingFaceProviderParamKey, Value: ""},
		}
		_, err := newHuggingFaceProvider(params, nil, creds, extraInfo)
		s.ErrorContains(err, "hf_provider cannot be empty")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.ModelNameParamKey, Value: "BAAI/bge-m3"},
			{Key: models.MaxClientBatchSizeParamKey, Value: "0"},
		}
		_, err := newHuggingFaceProvider(params, nil, creds, extraInfo)
		s.ErrorContains(err, "must be greater than 0")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.ModelNameParamKey, Value: "BAAI/bge-m3"},
		}
		provider, err := newHuggingFaceProvider(params, nil, creds, extraInfo)
		s.NoError(err)
		hfProvider := provider.(*huggingFaceProvider)
		s.Equal(huggingface.DefaultHFProvider, hfProvider.hfProvider)
		s.Equal(32, hfProvider.maxBatch())
	}
}

func (s *RerankModelSuite) TestNewHuggingFaceProviderFromRegistry() {
	paramtable.Get().FunctionCfg.RerankModelProviders.GetFunc = func() map[string]string {
		return map[string]string{
			huggingFaceProviderName + "." + models.CredentialParamKey: "mock",
		}
	}

	params := []*commonpb.KeyValuePair{
		{Key: providerParamName, Value: huggingFaceProviderName},
		{Key: models.CredentialParamKey, Value: "mock"},
		{Key: models.ModelNameParamKey, Value: "BAAI/bge-m3"},
	}
	provider, err := newProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
	s.NoError(err)
	s.IsType(&huggingFaceProvider{}, provider)
}

func (s *RerankModelSuite) TestCallHuggingFace() {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.Equal("/hf-inference/models/BAAI/bge-m3/pipeline/sentence-similarity", r.URL.Path)
		s.Equal("Bearer mock_key", r.Header.Get("Authorization"))
		var req huggingface.SentenceSimilarityRequest
		s.NoError(json.NewDecoder(r.Body).Decode(&req))
		s.Equal("mytest", req.Inputs.SourceSentence)

		current := atomic.AddInt32(&count, 1)
		switch current {
		case 1:
			s.Equal([]string{"t1", "t2"}, req.Inputs.Sentences)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[0.1,0.2]`))
		case 2:
			s.Equal([]string{"t3"}, req.Inputs.Sentences)
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[0.3]`))
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	params := []*commonpb.KeyValuePair{
		{Key: models.CredentialParamKey, Value: "mock"},
		{Key: models.ModelNameParamKey, Value: "BAAI/bge-m3"},
		{Key: models.MaxClientBatchSizeParamKey, Value: "2"},
	}
	provider, err := newHuggingFaceProvider(params, map[string]string{models.URLParamKey: ts.URL}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock_key"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
	s.NoError(err)
	scores, err := provider.rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
	s.NoError(err)
	s.Equal([]float32{0.1, 0.2, 0.3}, scores)
	s.Equal(int32(2), atomic.LoadInt32(&count))
}

func (s *RerankModelSuite) TestCallHuggingFaceScoreCountMismatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[0.1]`))
	}))
	defer ts.Close()

	params := []*commonpb.KeyValuePair{
		{Key: models.CredentialParamKey, Value: "mock"},
		{Key: models.ModelNameParamKey, Value: "BAAI/bge-m3"},
	}
	provider, err := newHuggingFaceProvider(params, map[string]string{models.URLParamKey: ts.URL}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock_key"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
	s.NoError(err)
	_, err = provider.rerank(context.Background(), "mytest", []string{"t1", "t2"})
	s.ErrorContains(err, "the number of docs and scores does not match")
}
