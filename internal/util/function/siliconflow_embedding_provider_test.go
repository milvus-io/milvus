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
	"github.com/milvus-io/milvus/internal/util/function/models/siliconflow"
)

func TestSiliconflowTextEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(SiliconflowTextEmbeddingProviderSuite))
}

type SiliconflowTextEmbeddingProviderSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func (s *SiliconflowTextEmbeddingProviderSuite) SetupTest() {
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
	s.providers = []string{siliconflowProvider}
}

func createSiliconflowProvider(url string, schema *schemapb.FieldSchema, providerName string) (textEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: TestModel},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: embeddingURLParamKey, Value: url},
		},
	}
	switch providerName {
	case siliconflowProvider:
		return NewSiliconflowEmbeddingProvider(schema, functionSchema, map[string]string{})
	default:
		return nil, errors.New("Unknow provider")
	}
}

func (s *SiliconflowTextEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateSiliconflowEmbeddingServer(4)

	defer ts.Close()
	for _, provderName := range s.providers {
		provder, err := createSiliconflowProvider(ts.URL, s.schema.Fields[2], provderName)
		s.NoError(err)
		{
			data := []string{"sentence"}
			r, err2 := provder.CallEmbedding(data, InsertMode)
			ret := r.([][]float32)
			s.NoError(err2)
			s.Equal(1, len(ret))
			s.Equal(4, len(ret[0]))
		}
		{
			data := []string{"sentence 1", "sentence 2", "sentence 3"}
			_, err := provder.CallEmbedding(data, SearchMode)
			s.NoError(err)
		}
	}
}

func (s *SiliconflowTextEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res siliconflow.EmbeddingResponse
		res.Data = append(res.Data, siliconflow.EmbeddingData{
			Object:    "list",
			Embedding: []float32{1.0, 1.0, 1.0, 1.0},
			Index:     0,
		})

		res.Data = append(res.Data, siliconflow.EmbeddingData{
			Object:    "list",
			Embedding: []float32{1.0, 1.0},
			Index:     1,
		})
		res.Usage = siliconflow.Usage{
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	for _, providerName := range s.providers {
		provder, err := createSiliconflowProvider(ts.URL, s.schema.Fields[2], providerName)
		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence"}
		_, err2 := provder.CallEmbedding(data, InsertMode)
		s.Error(err2)
	}
}

func (s *SiliconflowTextEmbeddingProviderSuite) TestEmbeddingNumberNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res siliconflow.EmbeddingResponse
		res.Data = append(res.Data, siliconflow.EmbeddingData{
			Object:    "list",
			Embedding: []float32{1.0, 1.0, 1.0, 1.0},
			Index:     0,
		})
		res.Usage = siliconflow.Usage{
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	for _, provderName := range s.providers {
		provder, err := createSiliconflowProvider(ts.URL, s.schema.Fields[2], provderName)

		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence2"}
		_, err2 := provder.CallEmbedding(data, InsertMode)
		s.Error(err2)
	}
}

func (s *SiliconflowTextEmbeddingProviderSuite) TestCreateSiliconflowEmbeddingClient() {
	_, err := createSiliconflowEmbeddingClient("", "")
	s.Error(err)
}

func (s *SiliconflowTextEmbeddingProviderSuite) TestNewSiliconflowEmbeddingProvider() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: TestModel},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: embeddingURLParamKey, Value: "mock"},
		},
	}
	provider, err := NewSiliconflowEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{})
	s.NoError(err)
	s.Equal(provider.FieldDim(), int64(4))
	s.True(provider.MaxBatch() > 0)
}
