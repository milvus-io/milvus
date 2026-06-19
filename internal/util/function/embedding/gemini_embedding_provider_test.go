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
	"github.com/milvus-io/milvus/internal/util/function/models/gemini"
)

func TestGeminiTextEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(GeminiTextEmbeddingProviderSuite))
}

type GeminiTextEmbeddingProviderSuite struct {
	suite.Suite
	schema *schemapb.CollectionSchema
}

func (s *GeminiTextEmbeddingProviderSuite) SetupTest() {
	s.schema = &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "768"},
				},
			},
		},
	}
}

func createGeminiProvider(url string, schema *schemapb.FieldSchema) (*GeminiEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "gemini-embedding-001"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.DimParamKey, Value: "768"},
		},
	}
	return NewGeminiEmbeddingProvider(schema, functionSchema, map[string]string{models.URLParamKey: url}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db", BatchFactor: 5})
}

func (s *GeminiTextEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateGeminiEmbeddingServer(768)
	defer ts.Close()

	provider, err := createGeminiProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	{
		data := []string{"sentence"}
		r, err := provider.CallEmbedding(context.Background(), data, models.InsertMode)
		ret := r.([][]float32)
		s.NoError(err)
		s.Equal(1, len(ret))
		s.Equal(768, len(ret[0]))
	}
	{
		data := []string{"sentence 1", "sentence 2", "sentence 3"}
		_, err := provider.CallEmbedding(context.Background(), data, models.SearchMode)
		s.NoError(err)
	}
}

func (s *GeminiTextEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res gemini.EmbeddingResponse
		res.Embeddings = append(res.Embeddings, gemini.EmbeddingValues{
			Values: []float32{1.0, 1.0, 1.0, 1.0},
		})
		res.Embeddings = append(res.Embeddings, gemini.EmbeddingValues{
			Values: []float32{1.0, 1.0},
		})
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	defer ts.Close()

	provider, err := createGeminiProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)

	data := []string{"sentence", "sentence"}
	_, err = provider.CallEmbedding(context.Background(), data, models.InsertMode)
	s.Error(err)
}

func (s *GeminiTextEmbeddingProviderSuite) TestEmbeddingNumberNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res gemini.EmbeddingResponse
		res.Embeddings = append(res.Embeddings, gemini.EmbeddingValues{
			Values: []float32{1.0, 1.0, 1.0, 1.0},
		})
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	defer ts.Close()

	provider, err := createGeminiProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)

	data := []string{"sentence", "sentence2"}
	_, err = provider.CallEmbedding(context.Background(), data, models.InsertMode)
	s.Error(err)
}

func (s *GeminiTextEmbeddingProviderSuite) TestNewGeminiEmbeddingProvider() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "gemini-embedding-001"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.DimParamKey, Value: "768"},
			{Key: models.TaskTypeParamKey, Value: "SEMANTIC_SIMILARITY"},
		},
	}
	provider, err := NewGeminiEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{models.URLParamKey: "mock"}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db", BatchFactor: 5})
	s.NoError(err)
	s.Equal(provider.FieldDim(), int64(768))
	s.True(provider.MaxBatch() > 0)
	s.Equal("SEMANTIC_SIMILARITY", provider.taskType)

	// Invalid dim
	{
		functionSchema.Params[2] = &commonpb.KeyValuePair{Key: models.DimParamKey, Value: "9"}
		_, err := NewGeminiEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Error(err)
	}

	// Invalid dim type
	{
		functionSchema.Params[2] = &commonpb.KeyValuePair{Key: models.DimParamKey, Value: "Invalid"}
		_, err := NewGeminiEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Error(err)
	}
}

func (s *GeminiTextEmbeddingProviderSuite) TestUnsupportedFieldType() {
	int8VecField := &schemapb.FieldSchema{
		FieldID: 102, Name: "vector", DataType: schemapb.DataType_Int8Vector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "768"},
		},
	}
	_, err := createGeminiProvider("mock", int8VecField)
	s.Error(err)
}
