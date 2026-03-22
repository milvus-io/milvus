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

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/minimax"
)

func TestMiniMaxTextEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(MiniMaxTextEmbeddingProviderSuite))
}

type MiniMaxTextEmbeddingProviderSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func (s *MiniMaxTextEmbeddingProviderSuite) SetupTest() {
	s.schema = &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "1536"},
				},
			},
		},
	}
	s.providers = []string{minimaxProvider}
}

func createMiniMaxProvider(url string, schema *schemapb.FieldSchema, providerName string) (textEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "embo-01"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.DimParamKey, Value: "1536"},
		},
	}
	switch providerName {
	case minimaxProvider:
		return NewMiniMaxEmbeddingProvider(schema, functionSchema, map[string]string{models.URLParamKey: url}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db", BatchFactor: 5})
	default:
		return nil, errors.New("Unknown provider")
	}
}

func (s *MiniMaxTextEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateMiniMaxEmbeddingServer(1536)
	defer ts.Close()

	for _, providerName := range s.providers {
		provider, err := createMiniMaxProvider(ts.URL, s.schema.Fields[2], providerName)
		s.NoError(err)
		{
			data := []string{"sentence"}
			r, err2 := provider.CallEmbedding(context.Background(), data, models.InsertMode)
			ret := r.([][]float32)
			s.NoError(err2)
			s.Equal(1, len(ret))
			s.Equal(1536, len(ret[0]))
		}
		{
			data := []string{"sentence 1", "sentence 2", "sentence 3"}
			_, err := provider.CallEmbedding(context.Background(), data, models.SearchMode)
			s.NoError(err)
		}
	}
}

func (s *MiniMaxTextEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := minimax.EmbeddingResponse{
			Vectors:     [][]float32{{1.0, 1.0, 1.0, 1.0}, {1.0, 1.0}},
			TotalTokens: 100,
			BaseResp: minimax.BaseResp{
				StatusCode: 0,
				StatusMsg:  "success",
			},
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	defer ts.Close()

	for _, providerName := range s.providers {
		provider, err := createMiniMaxProvider(ts.URL, s.schema.Fields[2], providerName)
		s.NoError(err)

		data := []string{"sentence", "sentence"}
		_, err2 := provider.CallEmbedding(context.Background(), data, models.InsertMode)
		s.Error(err2)
	}
}

func (s *MiniMaxTextEmbeddingProviderSuite) TestEmbeddingNumberNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := minimax.EmbeddingResponse{
			Vectors:     [][]float32{{1.0, 1.0, 1.0, 1.0}},
			TotalTokens: 100,
			BaseResp: minimax.BaseResp{
				StatusCode: 0,
				StatusMsg:  "success",
			},
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	defer ts.Close()

	for _, providerName := range s.providers {
		provider, err := createMiniMaxProvider(ts.URL, s.schema.Fields[2], providerName)
		s.NoError(err)

		data := []string{"sentence", "sentence2"}
		_, err2 := provider.CallEmbedding(context.Background(), data, models.InsertMode)
		s.Error(err2)
	}
}

func (s *MiniMaxTextEmbeddingProviderSuite) TestEmbeddingAPIError() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := minimax.EmbeddingResponse{
			Vectors: nil,
			BaseResp: minimax.BaseResp{
				StatusCode: 1001,
				StatusMsg:  "invalid api key",
			},
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	defer ts.Close()

	for _, providerName := range s.providers {
		provider, err := createMiniMaxProvider(ts.URL, s.schema.Fields[2], providerName)
		s.NoError(err)

		data := []string{"sentence"}
		_, err2 := provider.CallEmbedding(context.Background(), data, models.InsertMode)
		s.Error(err2)
		s.Contains(err2.Error(), "invalid api key")
	}
}

func (s *MiniMaxTextEmbeddingProviderSuite) TestNewMiniMaxEmbeddingProvider() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "embo-01"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.DimParamKey, Value: "1536"},
		},
	}
	provider, err := NewMiniMaxEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{models.URLParamKey: "mock"}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db", BatchFactor: 5})
	s.NoError(err)
	s.Equal(provider.FieldDim(), int64(1536))
	s.True(provider.MaxBatch() > 0)

	// Invalid dim
	{
		functionSchema.Params[2] = &commonpb.KeyValuePair{Key: models.DimParamKey, Value: "9"}
		_, err := NewMiniMaxEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Error(err)
	}

	// Invalid dim type
	{
		functionSchema.Params[2] = &commonpb.KeyValuePair{Key: models.DimParamKey, Value: "Invalid"}
		_, err := NewMiniMaxEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Error(err)
	}

	// Int8Vector not supported
	{
		int8Field := &schemapb.FieldSchema{
			FieldID: 102, Name: "vector", DataType: schemapb.DataType_Int8Vector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: "dim", Value: "1536"},
			},
		}
		functionSchema.Params[2] = &commonpb.KeyValuePair{Key: models.DimParamKey, Value: "1536"}
		_, err := NewMiniMaxEmbeddingProvider(int8Field, functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
		s.Error(err)
	}
}
