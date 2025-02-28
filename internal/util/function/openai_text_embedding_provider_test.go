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
	"github.com/milvus-io/milvus/internal/util/function/models/openai"
)

func TestOpenAITextEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(OpenAITextEmbeddingProviderSuite))
}

type OpenAITextEmbeddingProviderSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func (s *OpenAITextEmbeddingProviderSuite) SetupTest() {
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
	s.providers = []string{openAIProvider, azureOpenAIProvider}
}

func createOpenAIProvider(url string, schema *schemapb.FieldSchema, providerName string) (textEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: "text-embedding-ada-002"},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: dimParamKey, Value: "4"},
			{Key: embeddingURLParamKey, Value: url},
		},
	}
	switch providerName {
	case openAIProvider:
		return NewOpenAIEmbeddingProvider(schema, functionSchema)
	case azureOpenAIProvider:
		return NewAzureOpenAIEmbeddingProvider(schema, functionSchema)
	default:
		return nil, fmt.Errorf("Unknow provider")
	}
}

func (s *OpenAITextEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateOpenAIEmbeddingServer()

	defer ts.Close()
	for _, provderName := range s.providers {
		provder, err := createOpenAIProvider(ts.URL, s.schema.Fields[2], provderName)
		s.NoError(err)
		{
			data := []string{"sentence"}
			ret, err2 := provder.CallEmbedding(data, InsertMode)
			s.NoError(err2)
			s.Equal(1, len(ret))
			s.Equal(4, len(ret[0]))
			s.Equal([]float32{0.0, 0.1, 0.2, 0.3}, ret[0])
		}
		{
			data := []string{"sentence 1", "sentence 2", "sentence 3"}
			ret, _ := provder.CallEmbedding(data, SearchMode)
			s.Equal([][]float32{{0.0, 0.1, 0.2, 0.3}, {1.0, 1.1, 1.2, 1.3}, {2.0, 2.1, 2.2, 2.3}}, ret)
		}
	}
}

func (s *OpenAITextEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res openai.EmbeddingResponse
		res.Object = "list"
		res.Model = "text-embedding-3-small"
		res.Data = append(res.Data, openai.EmbeddingData{
			Object:    "embedding",
			Embedding: []float32{1.0, 1.0, 1.0, 1.0},
			Index:     0,
		})

		res.Data = append(res.Data, openai.EmbeddingData{
			Object:    "embedding",
			Embedding: []float32{1.0, 1.0},
			Index:     1,
		})
		res.Usage = openai.Usage{
			PromptTokens: 1,
			TotalTokens:  100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	for _, provderName := range s.providers {
		provder, err := createOpenAIProvider(ts.URL, s.schema.Fields[2], provderName)
		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence"}
		_, err2 := provder.CallEmbedding(data, InsertMode)
		s.Error(err2)
	}
}

func (s *OpenAITextEmbeddingProviderSuite) TestEmbeddingNubmerNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res openai.EmbeddingResponse
		res.Object = "list"
		res.Model = "text-embedding-3-small"
		res.Data = append(res.Data, openai.EmbeddingData{
			Object:    "embedding",
			Embedding: []float32{1.0, 1.0, 1.0, 1.0},
			Index:     0,
		})
		res.Usage = openai.Usage{
			PromptTokens: 1,
			TotalTokens:  100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	for _, provderName := range s.providers {
		provder, err := createOpenAIProvider(ts.URL, s.schema.Fields[2], provderName)

		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence2"}
		_, err2 := provder.CallEmbedding(data, InsertMode)
		s.Error(err2)
	}
}

func (s *OpenAITextEmbeddingProviderSuite) TestCreateOpenAIEmbeddingClient() {
	_, err := createOpenAIEmbeddingClient("", "")
	s.Error(err)

	os.Setenv(openaiAKEnvStr, "mockKey")
	defer os.Unsetenv(openaiAKEnvStr)

	_, err = createOpenAIEmbeddingClient("", "")
	s.NoError(err)
}

func (s *OpenAITextEmbeddingProviderSuite) TestCreateAzureOpenAIEmbeddingClient() {
	_, err := createAzureOpenAIEmbeddingClient("", "")
	s.Error(err)

	os.Setenv(azureOpenaiAKEnvStr, "mockKey")
	defer os.Unsetenv(azureOpenaiAKEnvStr)

	_, err = createAzureOpenAIEmbeddingClient("", "")
	s.Error(err)

	os.Setenv(azureOpenaiResourceName, "mockResource")
	defer os.Unsetenv(azureOpenaiResourceName)

	_, err = createAzureOpenAIEmbeddingClient("", "")
	s.NoError(err)
}
