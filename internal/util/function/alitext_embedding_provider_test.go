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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models/ali"
)

func TestAliTextEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(AliTextEmbeddingProviderSuite))
}

type AliTextEmbeddingProviderSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func (s *AliTextEmbeddingProviderSuite) SetupTest() {
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
	s.providers = []string{aliDashScopeProvider}
}

func createAliProvider(url string, schema *schemapb.FieldSchema, providerName string) (textEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: TestModel},
			{Key: credentialParamKey, Value: "mock"},
			{Key: dimParamKey, Value: "4"},
		},
	}
	switch providerName {
	case aliDashScopeProvider:
		return NewAliDashScopeEmbeddingProvider(schema, functionSchema, map[string]string{embeddingURLParamKey: url}, credentials.NewCredentialsManager(map[string]string{"mock.apikey": "mock"}))
	default:
		return nil, errors.New("Unknow provider")
	}
}

func (s *AliTextEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateAliEmbeddingServer()
	defer ts.Close()

	for _, provderName := range s.providers {
		provder, err := createAliProvider(ts.URL, s.schema.Fields[2], provderName)
		s.NoError(err)
		{
			data := []string{"sentence"}
			r, err2 := provder.CallEmbedding(data, InsertMode)
			ret := r.([][]float32)
			s.NoError(err2)
			s.Equal(1, len(ret))
			s.Equal(4, len(ret[0]))
			s.Equal([]float32{0.0, 1.0, 2.0, 3.0}, ret[0])
		}
		{
			data := []string{"sentence 1", "sentence 2", "sentence 3"}
			ret, _ := provder.CallEmbedding(data, SearchMode)
			s.Equal([][]float32{{0.0, 1.0, 2.0, 3.0}, {1.0, 2.0, 3.0, 4.0}, {2.0, 3.0, 4.0, 5.0}}, ret)
		}
	}
}

func (s *AliTextEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res ali.EmbeddingResponse
		res.Output.Embeddings = append(res.Output.Embeddings, ali.Embeddings{
			Embedding: []float32{1.0, 1.0, 1.0, 1.0},
			TextIndex: 0,
		})

		res.Output.Embeddings = append(res.Output.Embeddings, ali.Embeddings{
			Embedding: []float32{1.0, 1.0},
			TextIndex: 1,
		})
		res.Usage = ali.Usage{
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	for _, providerName := range s.providers {
		provder, err := createAliProvider(ts.URL, s.schema.Fields[2], providerName)
		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence"}
		_, err2 := provder.CallEmbedding(data, InsertMode)
		s.Error(err2)
	}
}

func (s *AliTextEmbeddingProviderSuite) TestEmbeddingNumberNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res ali.EmbeddingResponse
		res.Output.Embeddings = append(res.Output.Embeddings, ali.Embeddings{
			Embedding: []float32{1.0, 1.0, 1.0, 1.0},
			TextIndex: 0,
		})
		res.Usage = ali.Usage{
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	for _, provderName := range s.providers {
		provder, err := createAliProvider(ts.URL, s.schema.Fields[2], provderName)

		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence2"}
		_, err2 := provder.CallEmbedding(data, InsertMode)
		s.Error(err2)
	}
}

func (s *AliTextEmbeddingProviderSuite) TestCreateAliEmbeddingClient() {
	_, err := createAliEmbeddingClient("", "")
	s.Error(err)
}

func (s *AliTextEmbeddingProviderSuite) TestNewAliDashScopeEmbeddingProvider() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: TestModel},
			{Key: dimParamKey, Value: "4"},
		},
	}
	// invalid dim
	functionSchema.Params[1] = &commonpb.KeyValuePair{Key: dimParamKey, Value: "Invalid"}
	_, err := NewAliDashScopeEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentialsManager(map[string]string{"mock.apikey": "mock"}))
	s.Error(err)
}
