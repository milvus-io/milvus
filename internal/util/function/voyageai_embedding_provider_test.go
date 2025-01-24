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
	"github.com/milvus-io/milvus/internal/util/function/models/voyageai"
)

func TestVoyageAITextEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(VoyageAITextEmbeddingProviderSuite))
}

type VoyageAITextEmbeddingProviderSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func (s *VoyageAITextEmbeddingProviderSuite) SetupTest() {
	s.schema = &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "1024"},
				},
			},
		},
	}
	s.providers = []string{voyageAIProvider}
}

func createVoyageAIProvider(url string, schema *schemapb.FieldSchema, providerName string) (textEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: voyage3Large},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: embeddingURLParamKey, Value: url},
			{Key: dimParamKey, Value: "1024"},
		},
	}
	switch providerName {
	case voyageAIProvider:
		return NewVoyageAIEmbeddingProvider(schema, functionSchema)
	default:
		return nil, fmt.Errorf("Unknow provider")
	}
}

func (s *VoyageAITextEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateVoyageAIEmbeddingServer()

	defer ts.Close()
	for _, provderName := range s.providers {
		provder, err := createVoyageAIProvider(ts.URL, s.schema.Fields[2], provderName)
		s.NoError(err)
		{
			data := []string{"sentence"}
			ret, err2 := provder.CallEmbedding(data, InsertMode)
			s.NoError(err2)
			s.Equal(1, len(ret))
			s.Equal(1024, len(ret[0]))
		}
		{
			data := []string{"sentence 1", "sentence 2", "sentence 3"}
			_, err := provder.CallEmbedding(data, SearchMode)
			s.NoError(err)
		}
	}
}

func (s *VoyageAITextEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res voyageai.EmbeddingResponse
		res.Data = append(res.Data, voyageai.EmbeddingData{
			Object:    "list",
			Embedding: []float32{1.0, 1.0, 1.0, 1.0},
			Index:     0,
		})

		res.Data = append(res.Data, voyageai.EmbeddingData{
			Object:    "list",
			Embedding: []float32{1.0, 1.0},
			Index:     1,
		})
		res.Usage = voyageai.Usage{
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	for _, providerName := range s.providers {
		provder, err := createVoyageAIProvider(ts.URL, s.schema.Fields[2], providerName)
		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence"}
		_, err2 := provder.CallEmbedding(data, InsertMode)
		s.Error(err2)
	}
}

func (s *VoyageAITextEmbeddingProviderSuite) TestEmbeddingNumberNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res voyageai.EmbeddingResponse
		res.Data = append(res.Data, voyageai.EmbeddingData{
			Object:    "list",
			Embedding: []float32{1.0, 1.0, 1.0, 1.0},
			Index:     0,
		})
		res.Usage = voyageai.Usage{
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	for _, provderName := range s.providers {
		provder, err := createVoyageAIProvider(ts.URL, s.schema.Fields[2], provderName)

		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence2"}
		_, err2 := provder.CallEmbedding(data, InsertMode)
		s.Error(err2)
	}
}

func (s *VoyageAITextEmbeddingProviderSuite) TestCreateVoyageAIEmbeddingClient() {
	_, err := createVoyageAIEmbeddingClient("", "")
	s.Error(err)

	os.Setenv(voyageAIAKEnvStr, "mockKey")
	defer os.Unsetenv(voyageAIAKEnvStr)
	_, err = createVoyageAIEmbeddingClient("", "")
	s.NoError(err)
}

func (s *VoyageAITextEmbeddingProviderSuite) TestNewVoyageAIEmbeddingProvider() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: voyage3Large},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: embeddingURLParamKey, Value: "mock"},
			{Key: dimParamKey, Value: "1024"},
		},
	}
	provider, err := NewVoyageAIEmbeddingProvider(s.schema.Fields[2], functionSchema)
	s.NoError(err)
	s.Equal(provider.FieldDim(), int64(1024))
	s.True(provider.MaxBatch() > 0)

	// Invalid model
	{
		functionSchema.Params[0] = &commonpb.KeyValuePair{Key: modelNameParamKey, Value: "UnkownModel"}
		_, err := NewVoyageAIEmbeddingProvider(s.schema.Fields[2], functionSchema)
		s.Error(err)
	}

	// Invalid dim
	{
		functionSchema.Params[3] = &commonpb.KeyValuePair{Key: dimParamKey, Value: "9"}
		_, err := NewVoyageAIEmbeddingProvider(s.schema.Fields[2], functionSchema)
		s.Error(err)
	}

	// Invalid dim type
	{
		functionSchema.Params[3] = &commonpb.KeyValuePair{Key: dimParamKey, Value: "Invalied"}
		_, err := NewVoyageAIEmbeddingProvider(s.schema.Fields[2], functionSchema)
		s.Error(err)
	}
}
