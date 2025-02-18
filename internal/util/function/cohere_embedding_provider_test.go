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
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/models/ali"
)

func TestCohereTextEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(CohereTextEmbeddingProviderSuite))
}

type CohereTextEmbeddingProviderSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func (s *CohereTextEmbeddingProviderSuite) SetupTest() {
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
	s.providers = []string{cohereProvider}
}

func createCohereProvider(url string, schema *schemapb.FieldSchema, providerName string) (textEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: embedEnglishLightV20},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: embeddingURLParamKey, Value: url},
		},
	}
	switch providerName {
	case cohereProvider:
		return NewCohereEmbeddingProvider(schema, functionSchema)
	default:
		return nil, fmt.Errorf("Unknow provider")
	}
}

func (s *CohereTextEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateCohereEmbeddingServer()

	defer ts.Close()
	for _, providerName := range s.providers {
		provider, err := createCohereProvider(ts.URL, s.schema.Fields[2], providerName)
		s.True(provider.MaxBatch() > 0)
		s.Equal(provider.FieldDim(), int64(4))
		s.NoError(err)
		{
			data := []string{"sentence"}
			ret, err2 := provider.CallEmbedding(data, InsertMode)
			s.NoError(err2)
			s.Equal(1, len(ret))
			s.Equal(4, len(ret[0]))
			s.Equal([]float32{0.0, 0.1, 0.2, 0.3}, ret[0])
		}
		{
			data := []string{"sentence 1", "sentence 2", "sentence 3"}
			ret, _ := provider.CallEmbedding(data, SearchMode)
			s.Equal([][]float32{{0.0, 0.1, 0.2, 0.3}, {1.0, 1.1, 1.2, 1.3}, {2.0, 2.1, 2.2, 2.3}}, ret)
		}
	}
}

func (s *CohereTextEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
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
		provder, err := createCohereProvider(ts.URL, s.schema.Fields[2], providerName)
		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence"}
		_, err2 := provder.CallEmbedding(data, InsertMode)
		s.Error(err2)
	}
}

func (s *CohereTextEmbeddingProviderSuite) TestEmbeddingNumberNotMatch() {
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
		provder, err := createCohereProvider(ts.URL, s.schema.Fields[2], provderName)

		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence2"}
		_, err2 := provder.CallEmbedding(data, InsertMode)
		s.Error(err2)
	}
}

func (s *CohereTextEmbeddingProviderSuite) TestNewCohereProvider() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: embedEnglishLightV20},
			{Key: apiKeyParamKey, Value: "mock"},
		},
	}

	provider, err := NewCohereEmbeddingProvider(s.schema.Fields[2], functionSchema)
	s.NoError(err)
	s.Equal(provider.truncate, "END")

	functionSchema.Params = append(functionSchema.Params, &commonpb.KeyValuePair{Key: truncateParamKey, Value: "START"})
	provider, err = NewCohereEmbeddingProvider(s.schema.Fields[2], functionSchema)
	s.NoError(err)
	s.Equal(provider.truncate, "START")

	// Invalid truncateParam
	functionSchema.Params[2].Value = "Unknow"
	_, err = NewCohereEmbeddingProvider(s.schema.Fields[2], functionSchema)
	s.Error(err)

	// Invalid ModelName
	functionSchema.Params[2].Value = "END"
	functionSchema.Params[0].Value = "Unknow"
	_, err = NewCohereEmbeddingProvider(s.schema.Fields[2], functionSchema)
	s.Error(err)
}

func (s *CohereTextEmbeddingProviderSuite) TestGetInputType() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: embedEnglishLightV20},
			{Key: apiKeyParamKey, Value: "mock"},
		},
	}

	provider, err := NewCohereEmbeddingProvider(s.schema.Fields[2], functionSchema)
	s.NoError(err)
	s.Equal(provider.getInputType(InsertMode), "")
	s.Equal(provider.getInputType(SearchMode), "")

	functionSchema.Params[0].Value = embedEnglishLightV30
	provider, err = NewCohereEmbeddingProvider(s.schema.Fields[2], functionSchema)
	s.NoError(err)
	s.Equal(provider.getInputType(InsertMode), "search_document")
	s.Equal(provider.getInputType(SearchMode), "search_query")
}

func (s *CohereTextEmbeddingProviderSuite) TestCreateCohereEmbeddingClient() {
	_, err := createCohereEmbeddingClient("", "")
	s.Error(err)

	os.Setenv(cohereAIAKEnvStr, "mockKey")
	defer os.Unsetenv(openaiAKEnvStr)

	_, err = createCohereEmbeddingClient("", "")
	s.NoError(err)
}

func (s *CohereTextEmbeddingProviderSuite) TestRuntimeDimNotMatch() {
	ts := CreateCohereEmbeddingServer()
	defer ts.Close()
	s.schema = &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "5"},
				},
			},
		},
	}
	provider, err := createCohereProvider(ts.URL, s.schema.Fields[2], cohereProvider)
	s.NoError(err)
	data := []string{"sentence"}
	_, err2 := provider.CallEmbedding(data, InsertMode)
	s.Error(err2)
}
