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

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/qianfan"
)

func TestQianfanEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(QianfanEmbeddingProviderSuite))
}

type QianfanEmbeddingProviderSuite struct {
	suite.Suite
	schema *schemapb.CollectionSchema
}

func (s *QianfanEmbeddingProviderSuite) SetupTest() {
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

func createQianfanProvider(url string, schema *schemapb.FieldSchema) (textEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "embedding-v1"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.DimParamKey, Value: "4"},
		},
	}
	return NewQianfanEmbeddingProvider(schema, functionSchema, map[string]string{models.URLParamKey: url}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
}

func (s *QianfanEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateQianfanEmbeddingServer(4)
	defer ts.Close()

	provider, err := createQianfanProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)

	data, err := provider.CallEmbedding(context.Background(), []string{"sentence"}, models.InsertMode)
	s.NoError(err)
	s.Equal([][]float32{{0.0, 1.0, 2.0, 3.0}}, data)
}

func (s *QianfanEmbeddingProviderSuite) TestBatchEmbedding() {
	ts := CreateQianfanEmbeddingServer(4)
	defer ts.Close()

	provider, err := createQianfanProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)

	data, err := provider.CallEmbedding(context.Background(), []string{"sentence 1", "sentence 2", "sentence 3"}, models.SearchMode)
	s.NoError(err)
	s.Equal([][]float32{{0.0, 1.0, 2.0, 3.0}, {1.0, 2.0, 3.0, 4.0}, {2.0, 3.0, 4.0, 5.0}}, data)
}

func (s *QianfanEmbeddingProviderSuite) TestEmbeddingNumberNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := qianfan.EmbeddingResponse{
			Data: []qianfan.EmbeddingData{
				{
					Embedding: []float32{1.0, 1.0, 1.0, 1.0},
					Index:     0,
				},
			},
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(res)
	}))
	defer ts.Close()

	provider, err := createQianfanProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	_, err = provider.CallEmbedding(context.Background(), []string{"sentence", "sentence 2"}, models.InsertMode)
	s.Error(err)
}

func (s *QianfanEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := qianfan.EmbeddingResponse{
			Data: []qianfan.EmbeddingData{
				{
					Embedding: []float32{1.0, 1.0},
					Index:     0,
				},
			},
		}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(res)
	}))
	defer ts.Close()

	provider, err := createQianfanProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	_, err = provider.CallEmbedding(context.Background(), []string{"sentence"}, models.InsertMode)
	s.Error(err)
}

func (s *QianfanEmbeddingProviderSuite) TestMissingCredential() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "embedding-v1"},
		},
	}
	_, err := NewQianfanEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{}), &models.ModelExtraInfo{})
	s.ErrorContains(err, models.QianfanAKEnvStr)
}

func (s *QianfanEmbeddingProviderSuite) TestCustomAndDefaultURL() {
	ts := CreateQianfanEmbeddingServer(4)
	defer ts.Close()

	provider, err := createQianfanProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	s.Equal(ts.URL, provider.(*QianfanEmbeddingProvider).url)

	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "embedding-v1"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.DimParamKey, Value: "4"},
		},
	}
	p, err := NewQianfanEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
	s.NoError(err)
	s.Equal(defaultQianfanTextEmbeddingURL, p.url)
}

func (s *QianfanEmbeddingProviderSuite) TestMissingModelName() {
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
	_, err := NewQianfanEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
	s.ErrorContains(err, "qianfan embedding model name is required")
}

func (s *QianfanEmbeddingProviderSuite) TestInvalidDimParam() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "embedding-v1"},
			{Key: models.DimParamKey, Value: "invalid"},
			{Key: models.CredentialParamKey, Value: "mock"},
		},
	}
	_, err := NewQianfanEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
	s.ErrorContains(err, "is not a valid int")
}

func (s *QianfanEmbeddingProviderSuite) TestParseCredentialError() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "embedding-v1"},
			{Key: models.CredentialParamKey, Value: "mock"},
		},
	}
	_, err := NewQianfanEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.token": "not-apikey"}), &models.ModelExtraInfo{})
	s.Error(err)
}

func (s *QianfanEmbeddingProviderSuite) TestEmptyEmbeddingResponse() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(qianfan.EmbeddingResponse{})
	}))
	defer ts.Close()

	provider, err := createQianfanProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	_, err = provider.CallEmbedding(context.Background(), []string{"sentence"}, models.InsertMode)
	s.ErrorContains(err, "number of texts and embeddings does not match")
}

func (s *QianfanEmbeddingProviderSuite) TestQianfanProviderBasicGetters() {
	ts := CreateQianfanEmbeddingServer(4)
	defer ts.Close()

	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "embedding-v1"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.DimParamKey, Value: "4"},
		},
	}
	provider, err := NewQianfanEmbeddingProvider(
		s.schema.Fields[2],
		functionSchema,
		map[string]string{models.URLParamKey: ts.URL},
		credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}),
		&models.ModelExtraInfo{BatchFactor: 3},
	)
	s.NoError(err)
	s.Equal(int64(4), provider.FieldDim())
	s.Equal(384, provider.MaxBatch())
}

func (s *QianfanEmbeddingProviderSuite) TestBatchEmbeddingAcrossMultipleRequests() {
	ts := CreateQianfanEmbeddingServer(4)
	defer ts.Close()

	provider, err := createQianfanProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)

	qianfanProvider := provider.(*QianfanEmbeddingProvider)
	qianfanProvider.maxBatch = 2
	ret, err := qianfanProvider.CallEmbedding(context.Background(), []string{"1", "2", "3", "4", "5"}, models.InsertMode)
	s.NoError(err)
	embeddings, ok := ret.([][]float32)
	s.True(ok)
	s.Len(embeddings, 5)
	for _, emb := range embeddings {
		s.Len(emb, 4)
	}
}

func (s *QianfanEmbeddingProviderSuite) TestCallEmbeddingHTTPError() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer ts.Close()

	provider, err := createQianfanProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	_, err = provider.CallEmbedding(context.Background(), []string{"sentence"}, models.InsertMode)
	s.Error(err)
}
