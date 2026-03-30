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
	"github.com/volcengine/volcengine-go-sdk/service/arkruntime/model"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
)

func TestArkTextEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(ArkTextEmbeddingProviderSuite))
}

type ArkTextEmbeddingProviderSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func (s *ArkTextEmbeddingProviderSuite) SetupTest() {
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
	s.providers = []string{arkProvider}
}

func createArkProvider(url string, schema *schemapb.FieldSchema, providerName string) (textEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: TestModel},
			{Key: models.CredentialParamKey, Value: "mock"},
		},
	}
	switch providerName {
	case arkProvider:
		return NewArkEmbeddingProvider(schema, functionSchema, map[string]string{models.URLParamKey: url}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{BatchFactor: 5})
	default:
		return nil, errors.New("Unknown provider")
	}
}

func (s *ArkTextEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateArkEmbeddingServer(4)

	defer ts.Close()
	for _, providerName := range s.providers {
		provider, err := createArkProvider(ts.URL, s.schema.Fields[2], providerName)
		s.NoError(err)
		{
			data := []string{"sentence"}
			r, err2 := provider.CallEmbedding(context.Background(), data, models.InsertMode)
			ret := r.([][]float32)
			s.NoError(err2)
			s.Equal(1, len(ret))
			s.Equal(4, len(ret[0]))
		}
		{
			data := []string{"sentence 1", "sentence 2", "sentence 3"}
			_, err := provider.CallEmbedding(context.Background(), data, models.SearchMode)
			s.NoError(err)
		}
	}
}

func (s *ArkTextEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res model.EmbeddingResponse
		res.Data = append(res.Data, model.Embedding{
			Object:    "embedding",
			Embedding: []float32{1.0, 1.0, 1.0, 1.0},
			Index:     0,
		})

		res.Data = append(res.Data, model.Embedding{
			Object:    "embedding",
			Embedding: []float32{1.0, 1.0},
			Index:     1,
		})
		res.Usage = model.Usage{
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	for _, providerName := range s.providers {
		provider, err := createArkProvider(ts.URL, s.schema.Fields[2], providerName)
		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence"}
		_, err2 := provider.CallEmbedding(context.Background(), data, models.InsertMode)
		s.Error(err2)
	}
}

func (s *ArkTextEmbeddingProviderSuite) TestEmbeddingNumberNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res model.EmbeddingResponse
		res.Data = append(res.Data, model.Embedding{
			Object:    "embedding",
			Embedding: []float32{1.0, 1.0, 1.0, 1.0},
			Index:     0,
		})
		res.Usage = model.Usage{
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	for _, providerName := range s.providers {
		provider, err := createArkProvider(ts.URL, s.schema.Fields[2], providerName)

		s.NoError(err)

		// embedding number not match
		data := []string{"sentence", "sentence2"}
		_, err2 := provider.CallEmbedding(context.Background(), data, models.InsertMode)
		s.Error(err2)
	}
}

func (s *ArkTextEmbeddingProviderSuite) TestNewArkEmbeddingProvider() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: TestModel},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: "user", Value: "test_user"},
		},
	}
	provider, err := NewArkEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{models.URLParamKey: "mock"}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{BatchFactor: 5})
	s.NoError(err)
	s.Equal(provider.FieldDim(), int64(4))
	s.True(provider.MaxBatch() > 0)
	s.Equal("test_user", provider.user)
}

func (s *ArkTextEmbeddingProviderSuite) TestNewArkEmbeddingProviderWithBatchSize() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: TestModel},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: "batch_size", Value: "64"},
		},
	}
	provider, err := NewArkEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{models.URLParamKey: "mock"}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{BatchFactor: 5})
	s.NoError(err)
	s.Equal(provider.FieldDim(), int64(4))

	// 320 = 64 * 5
	s.Equal(320, provider.MaxBatch())
}

func (s *ArkTextEmbeddingProviderSuite) TestNewArkEmbeddingProvider_InvalidParams() {
	schema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "4"},
		},
	}

	// Invalid dim (non-integer)
	fs1 := &schemapb.FunctionSchema{
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: TestModel},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: "dim", Value: "invalid"},
		},
	}
	_, err := NewArkEmbeddingProvider(schema, fs1, map[string]string{models.URLParamKey: "mock"}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{BatchFactor: 1})
	s.Error(err)
	s.Contains(err.Error(), "invalid 'dim' parameter value")

	// Invalid dim (negative)
	fs2 := &schemapb.FunctionSchema{
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: TestModel},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: "dim", Value: "-1"},
		},
	}
	_, err = NewArkEmbeddingProvider(schema, fs2, map[string]string{models.URLParamKey: "mock"}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{BatchFactor: 1})
	s.Error(err)
	s.Contains(err.Error(), "must be non-negative")

	// Invalid batch_size (non-integer)
	fs3 := &schemapb.FunctionSchema{
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: TestModel},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: "batch_size", Value: "invalid"},
		},
	}
	_, err = NewArkEmbeddingProvider(schema, fs3, map[string]string{models.URLParamKey: "mock"}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{BatchFactor: 1})
	s.Error(err)
	s.Contains(err.Error(), "invalid 'batch_size' parameter value")

	// Invalid batch_size (negative)
	fs4 := &schemapb.FunctionSchema{
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: TestModel},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: "batch_size", Value: "-10"},
		},
	}
	_, err = NewArkEmbeddingProvider(schema, fs4, map[string]string{models.URLParamKey: "mock"}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{BatchFactor: 1})
	s.Error(err)
	s.Contains(err.Error(), "must be positive")
}

func (s *ArkTextEmbeddingProviderSuite) TestNewArkEmbeddingProvider_Multimodal() {
	schema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "4"},
		},
	}
	fs := &schemapb.FunctionSchema{
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: TestModel},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: "model_type", Value: "multimodal"},
		},
	}
	provider, err := NewArkEmbeddingProvider(schema, fs, map[string]string{models.URLParamKey: "mock"}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{BatchFactor: 1})
	s.NoError(err)
	s.Equal(1, provider.maxBatch)
}

func (s *ArkTextEmbeddingProviderSuite) TestEmbedding_ClientError() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	fs := &schemapb.FunctionSchema{
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: TestModel},
			{Key: models.CredentialParamKey, Value: "mock"},
		},
	}
	provider, err := NewArkEmbeddingProvider(s.schema.Fields[2], fs, map[string]string{models.URLParamKey: ts.URL}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{BatchFactor: 1})
	s.Require().NoError(err)

	_, err = provider.CallEmbedding(context.Background(), []string{"test"}, models.SearchMode)
	s.Error(err)
}

func (s *ArkTextEmbeddingProviderSuite) TestCallEmbedding_DefensiveMaxBatch() {
	// Construct provider with invalid maxBatch
	provider := &ArkEmbeddingProvider{
		maxBatch:  0,
		extraInfo: &models.ModelExtraInfo{BatchFactor: 1},
	}
	_, err := provider.CallEmbedding(context.Background(), []string{"test"}, models.SearchMode)
	s.Error(err)
	s.Contains(err.Error(), "invalid maxBatch value")
}

func (s *ArkTextEmbeddingProviderSuite) TestMultimodalEmbedding_Error() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer ts.Close()

	schema := &schemapb.FieldSchema{
		DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "4"},
		},
	}
	fs := &schemapb.FunctionSchema{
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: TestModel},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: "model_type", Value: "multimodal"},
		},
	}
	provider, err := NewArkEmbeddingProvider(schema, fs, map[string]string{models.URLParamKey: ts.URL}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{BatchFactor: 1})
	s.Require().NoError(err)
	s.True(provider.isMultimodal)

	_, err = provider.CallEmbedding(context.Background(), []string{"test"}, models.SearchMode)
	s.Error(err)
	s.Contains(err.Error(), "multimodal embedding request failed")
}

func (s *ArkTextEmbeddingProviderSuite) TestNewArkEmbeddingProvider_InitializationErrors() {
	// Case 1: Missing dim in fieldSchema
	{
		schema := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			// No TypeParams with dim
		}
		fs := &schemapb.FunctionSchema{
			Params: []*commonpb.KeyValuePair{
				{Key: models.ModelNameParamKey, Value: TestModel},
				{Key: models.CredentialParamKey, Value: "mock"}, // params ok
			},
		}
		_, err := NewArkEmbeddingProvider(schema, fs, map[string]string{}, credentials.NewCredentials(map[string]string{}), &models.ModelExtraInfo{})
		s.Error(err)
	}

	// Case 2: Missing credentials
	{
		schema := &schemapb.FieldSchema{
			DataType: schemapb.DataType_FloatVector,
			TypeParams: []*commonpb.KeyValuePair{
				{Key: "dim", Value: "4"},
			},
		}
		fs := &schemapb.FunctionSchema{
			Params: []*commonpb.KeyValuePair{
				{Key: models.ModelNameParamKey, Value: TestModel},
				// No credential param
			},
		}
		// Empty credentials, empty config, empty env
		_, err := NewArkEmbeddingProvider(schema, fs, map[string]string{}, credentials.NewCredentials(map[string]string{}), &models.ModelExtraInfo{})
		s.Error(err)
	}
}
