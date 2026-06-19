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

package embedding

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
)

func TestYCEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(YCEmbeddingProviderSuite))
}

type YCEmbeddingProviderSuite struct {
	suite.Suite
	schema *schemapb.CollectionSchema
}

func (s *YCEmbeddingProviderSuite) SetupTest() {
	s.schema = &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
		},
	}
}

func createYCProvider(url string, schema *schemapb.FieldSchema) (textEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "emb://test/model"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.DimParamKey, Value: "4"},
		},
	}
	return NewYCEmbeddingProvider(schema, functionSchema, map[string]string{models.URLParamKey: url}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
}

func (s *YCEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateYCEmbeddingServer()
	defer ts.Close()

	provider, err := createYCProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)

	data, err := provider.CallEmbedding(context.Background(), []string{"sentence"}, models.InsertMode)
	s.NoError(err)
	s.Equal([][]float32{{0.0, 1.0, 2.0, 3.0}}, data)
}

func (s *YCEmbeddingProviderSuite) TestBatchEmbedding() {
	ts := CreateYCEmbeddingServer()
	defer ts.Close()

	provider, err := createYCProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)

	data, err := provider.CallEmbedding(context.Background(), []string{"sentence 1", "sentence 2", "sentence 3"}, models.SearchMode)
	s.NoError(err)
	s.Equal([][]float32{{0.0, 1.0, 2.0, 3.0}, {1.0, 2.0, 3.0, 4.0}, {2.0, 3.0, 4.0, 5.0}}, data)
}

func (s *YCEmbeddingProviderSuite) TestEmbeddingNumberNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := YCEmbeddingResponse{
			Embeddings: [][]float32{{1.0, 1.0, 1.0, 1.0}},
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(res)
	}))
	defer ts.Close()

	provider, err := createYCProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	_, err = provider.CallEmbedding(context.Background(), []string{"sentence", "sentence 2"}, models.InsertMode)
	s.Error(err)
}

func (s *YCEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := YCEmbeddingResponse{
			Embeddings: [][]float32{{1.0, 1.0}},
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(res)
	}))
	defer ts.Close()

	provider, err := createYCProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	_, err = provider.CallEmbedding(context.Background(), []string{"sentence"}, models.InsertMode)
	s.Error(err)
}

func (s *YCEmbeddingProviderSuite) TestMissingCredential() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "emb://test/model"},
		},
	}
	_, err := NewYCEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{}), &models.ModelExtraInfo{})
	s.ErrorContains(err, models.YandexCloudAKEnvStr)
}

func (s *YCEmbeddingProviderSuite) TestCustomAndDefaultURL() {
	ts := CreateYCEmbeddingServer()
	defer ts.Close()

	provider, err := createYCProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	s.Equal(ts.URL, provider.(*YCEmbeddingProvider).url)

	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "emb://test/model"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.DimParamKey, Value: "4"},
		},
	}
	p, err := NewYCEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
	s.NoError(err)
	s.Equal(defaultYCTextEmbeddingURL, p.url)
}

func (s *YCEmbeddingProviderSuite) TestMissingModelName() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.CredentialParamKey, Value: "mock"},
		},
	}
	_, err := NewYCEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
	s.ErrorContains(err, "yc embedding model name is required")
}

func (s *YCEmbeddingProviderSuite) TestInvalidDimParam() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "emb://test/model"},
			{Key: models.DimParamKey, Value: "invalid"},
			{Key: models.CredentialParamKey, Value: "mock"},
		},
	}
	_, err := NewYCEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
	s.ErrorContains(err, "is not a valid int")
}

func (s *YCEmbeddingProviderSuite) TestParseCredentialError() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "emb://test/model"},
			{Key: models.CredentialParamKey, Value: "mock"},
		},
	}
	_, err := NewYCEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.token": "not-apikey"}), &models.ModelExtraInfo{})
	s.Error(err)
}

func (s *YCEmbeddingProviderSuite) TestEmptyEmbeddingResponse() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(YCEmbeddingResponse{})
	}))
	defer ts.Close()

	provider, err := createYCProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	_, err = provider.CallEmbedding(context.Background(), []string{"sentence"}, models.InsertMode)
	s.ErrorContains(err, "number of texts and embeddings does not match")
}

func (s *YCEmbeddingProviderSuite) TestYCProviderBasicGetters() {
	ts := CreateYCEmbeddingServer()
	defer ts.Close()

	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "emb://test/model"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.DimParamKey, Value: "4"},
		},
	}
	provider, err := NewYCEmbeddingProvider(
		s.schema.Fields[2],
		functionSchema,
		map[string]string{models.URLParamKey: ts.URL},
		credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}),
		&models.ModelExtraInfo{BatchFactor: 3},
	)
	s.NoError(err)
	s.Equal(int64(4), provider.FieldDim())
	s.Equal(384, provider.MaxBatch())
	s.Equal("Api-Key mock", provider.headers()["Authorization"])
}

func (s *YCEmbeddingProviderSuite) TestBatchEmbeddingAcrossMultipleRequests() {
	ts := CreateYCEmbeddingServer()
	defer ts.Close()

	provider, err := createYCProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)

	ycProvider := provider.(*YCEmbeddingProvider)
	ycProvider.maxBatch = 2
	ret, err := ycProvider.CallEmbedding(context.Background(), []string{"1", "2", "3", "4", "5"}, models.InsertMode)
	s.NoError(err)
	embeddings, ok := ret.([][]float32)
	s.True(ok)
	s.Len(embeddings, 5)
	for _, emb := range embeddings {
		s.Len(emb, 4)
	}
}

func (s *YCEmbeddingProviderSuite) TestCallEmbeddingHTTPError() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer ts.Close()

	provider, err := createYCProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	_, err = provider.CallEmbedding(context.Background(), []string{"sentence"}, models.InsertMode)
	s.Error(err)
}

func TestExtractYCEmbeddings(t *testing.T) {
	resp := &YCEmbeddingResponse{}
	assert.Equal(t, [][]float32(nil), extractYCEmbeddings(resp))
}

func TestExtractYCEmbeddingsSingleEmbedding(t *testing.T) {
	resp := &YCEmbeddingResponse{Embedding: []float32{1, 2, 3, 4}}
	assert.Equal(t, [][]float32{{1, 2, 3, 4}}, extractYCEmbeddings(resp))
}
