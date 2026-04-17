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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestRerankModel(t *testing.T) {
	suite.Run(t, new(RerankModelSuite))
}

type RerankModelSuite struct {
	suite.Suite
}

func (s *RerankModelSuite) SetupTest() {
	paramtable.Init()
	paramtable.Get().CredentialCfg.Credential.GetFunc = func() map[string]string {
		return map[string]string{
			"mock.apikey": "mock",
		}
	}
	paramtable.Get().FunctionCfg.RerankModelProviders.GetFunc = func() map[string]string {
		return map[string]string{}
	}
}

func (s *RerankModelSuite) TestNewProvider() {
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "unknown"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.ErrorContains(err, "unknown rerank model provider")
	}

	{
		_, err := NewModelProvider([]*commonpb.KeyValuePair{}, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.ErrorContains(err, "lost rerank params")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.ErrorContains(err, "rerank function lost params endpoint")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: models.EndpointParamKey, Value: "http://localhost:80"},
			{Key: models.MaxClientBatchSizeParamKey, Value: "-1"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.ErrorContains(err, "must be greater than 0")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: models.EndpointParamKey, Value: "http://localhost:80"},
			{Key: models.MaxClientBatchSizeParamKey, Value: "NotNum"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.ErrorContains(err, "is not a valid numbe")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: models.EndpointParamKey, Value: "http://localhost:80"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: models.EndpointParamKey, Value: "http://localhost:80"},
			{Key: models.VllmTruncateParamName, Value: "unknow"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Error(err)
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: models.EndpointParamKey, Value: "http://localhost:80"},
		}
		paramtable.Get().FunctionCfg.RerankModelProviders.GetFunc = func() map[string]string {
			key := "vllm.enable"
			return map[string]string{
				key: "false",
			}
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.ErrorContains(err, "rerank provider: [vllm] is disabled")
		paramtable.Get().FunctionCfg.RerankModelProviders.GetFunc = func() map[string]string {
			return map[string]string{}
		}
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "tei"},
			{Key: models.EndpointParamKey, Value: "http://localhost:80"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "tei"},
			{Key: models.EndpointParamKey, Value: "http://localhost:80"},
			{Key: models.TruncateParamKey, Value: "unknow"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Error(err)
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "tei"},
			{Key: models.EndpointParamKey, Value: "http://localhost:80"},
			{Key: models.TruncateParamKey, Value: "true"},
			{Key: models.MaxClientBatchSizeParamKey, Value: "10"},
			{Key: models.TruncationDirectionParamKey, Value: "unknow"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Error(err)
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "tei"},
			{Key: models.EndpointParamKey, Value: "http://localhost:80"},
			{Key: models.TruncateParamKey, Value: "true"},
			{Key: models.TruncationDirectionParamKey, Value: "Left"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "tei"},
			{Key: models.EndpointParamKey, Value: "http://localhost:80"},
		}
		paramtable.Get().FunctionCfg.RerankModelProviders.GetFunc = func() map[string]string {
			key := "tei.enable"
			return map[string]string{
				key: "false",
			}
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.ErrorContains(err, "rerank provider: [tei] is disabled")
		paramtable.Get().FunctionCfg.RerankModelProviders.GetFunc = func() map[string]string {
			return map[string]string{}
		}
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "siliconflow"},
			{Key: models.CredentialParamKey, Value: "mock"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.ErrorContains(err, "siliconflow rerank model name is required")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "siliconflow"},
			{Key: models.ModelNameParamKey, Value: "siliconflow-test"},
			{Key: models.CredentialParamKey, Value: "mock"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "siliconflow"},
			{Key: models.ModelNameParamKey, Value: "siliconflow-test"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.MaxClientBatchSizeParamKey, Value: "10"},
			{Key: models.MaxChunksPerDocParamKey, Value: "10"},
			{Key: models.OverlapTokensParamKey, Value: "10"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "cohere"},
			{Key: models.CredentialParamKey, Value: "mock"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.ErrorContains(err, "cohere rerank model name is required")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "cohere"},
			{Key: models.ModelNameParamKey, Value: "cohere-test"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.MaxClientBatchSizeParamKey, Value: "10"},
			{Key: models.MaxTKsPerDocParamKey, Value: "10"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "voyageai"},
			{Key: models.CredentialParamKey, Value: "mock"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.ErrorContains(err, "voyageai rerank model name is required")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "voyageai"},
			{Key: models.ModelNameParamKey, Value: "voyageai-test"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.MaxClientBatchSizeParamKey, Value: "10"},
			{Key: models.TruncateParamKey, Value: "true"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "ali"},
			{Key: models.CredentialParamKey, Value: "mock"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.ErrorContains(err, "ali rerank model name is required")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "ali"},
			{Key: models.ModelNameParamKey, Value: "ali-test"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.MaxClientBatchSizeParamKey, Value: "10"},
		}
		_, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
	}
}

func (s *RerankModelSuite) TestCallVllm() {
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{'object': 'error', 'message': 'The model vllm-test does not exist.', 'type': 'NotFoundError', 'param': None, 'code': 404}`))
		}))
		defer ts.Close()

		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: models.EndpointParamKey, Value: ts.URL},
		}
		provder, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
		_, err = provder.Rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
		s.ErrorContains(err, "call service failed")
	}
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`Not json data`))
		}))
		defer ts.Close()

		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: models.EndpointParamKey, Value: ts.URL},
		}
		provder, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
		_, err = provder.Rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
		s.ErrorContains(err, "call service failed")
	}
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"results": [{"index": 0, "relevance_score": 0.0}, {"index": 2, "relevance_score": 0.2}, {"index": 1, "relevance_score": 0.1}]}`))
		}))
		defer ts.Close()

		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: models.EndpointParamKey, Value: ts.URL},
		}
		provder, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
		scores, err := provder.Rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
		s.NoError(err)
		s.Equal([]float32{0.0, 0.1, 0.2}, scores)
	}
}

func (s *RerankModelSuite) TestCallTEI() {
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[{"index":0,"score":0.0},{"index":1,"score":0.2}, {"index":2,"score":0.1}]`))
		}))
		defer ts.Close()

		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "tei"},
			{Key: models.EndpointParamKey, Value: ts.URL},
		}
		provder, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
		scores, err := provder.Rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
		s.NoError(err)
		s.Equal([]float32{0.0, 0.2, 0.1}, scores)
	}
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`not json`))
		}))
		defer ts.Close()

		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "tei"},
			{Key: models.EndpointParamKey, Value: ts.URL},
		}
		provder, err := NewModelProvider(params, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
		_, err = provder.Rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
		s.ErrorContains(err, "call service failed")
	}
}

func (s *RerankModelSuite) TestCallSiliconFlow() {
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"results": [{"index": 0, "relevance_score": 0.1}, {"index": 1, "relevance_score": 0.2}]}`))
		}))
		defer ts.Close()
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "siliconflow"},
			{Key: models.ModelNameParamKey, Value: "siliconflow-test"},
			{Key: models.CredentialParamKey, Value: "mock"},
		}
		provder, err := newSiliconflowProvider(params, map[string]string{models.URLParamKey: ts.URL}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
		scores, err := provder.Rerank(context.Background(), "mytest", []string{"t1", "t2"})
		s.NoError(err)
		s.Equal([]float32{0.1, 0.2}, scores)
	}
}

func (s *RerankModelSuite) TestCallCohere() {
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"results": [{"index": 0, "relevance_score": 0.1}, {"index": 1, "relevance_score": 0.2}]}`))
		}))
		defer ts.Close()
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "cohere"},
			{Key: models.ModelNameParamKey, Value: "cohere-test"},
			{Key: models.CredentialParamKey, Value: "mock"},
		}
		provder, err := newCohereProvider(params, map[string]string{models.URLParamKey: ts.URL}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
		scores, err := provder.Rerank(context.Background(), "mytest", []string{"t1", "t2"})
		s.NoError(err)
		s.Equal([]float32{0.1, 0.2}, scores)
	}
}

func (s *RerankModelSuite) TestCallVoyageAI() {
	{
		repStr := `{
			"object": "list", 
			"data": [{"object": "rerank", "index": 0, "relevance_score": 0.0}, {"object": "rerank", "index": 2, "relevance_score": 0.2}, {"object": "rerank", "index": 1, "relevance_score": 0.1}]
		}`
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(repStr))
		}))
		defer ts.Close()
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "voyageai"},
			{Key: models.ModelNameParamKey, Value: "voyageai-test"},
			{Key: models.CredentialParamKey, Value: "mock"},
		}
		provder, err := newVoyageaiProvider(params, map[string]string{models.URLParamKey: ts.URL}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
		scores, err := provder.Rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
		s.NoError(err)
		s.Equal([]float32{0.0, 0.1, 0.2}, scores)
	}
}

func (s *RerankModelSuite) TestCallAli() {
	{
		repStr := `{"output":{"results":[{"index":0,"relevance_score":0},{"index":1,"relevance_score":0.1},{"index":2,"relevance_score":0.2}]},"usage":{"total_tokens":1},"request_id":"x"}`

		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(repStr))
		}))
		defer ts.Close()
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "ali"},
			{Key: models.ModelNameParamKey, Value: "ali-test"},
			{Key: models.CredentialParamKey, Value: "mock"},
		}
		provder, err := newAliProvider(params, map[string]string{models.URLParamKey: ts.URL}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.NoError(err)
		scores, err := provder.Rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
		s.NoError(err)
		s.Equal([]float32{0.0, 0.1, 0.2}, scores)
	}
}
