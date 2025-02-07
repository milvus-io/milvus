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
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/models/vertexai"
)

func TestVertexAITextEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(VertexAITextEmbeddingProviderSuite))
}

type VertexAITextEmbeddingProviderSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func (s *VertexAITextEmbeddingProviderSuite) SetupTest() {
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

func createVertexAIProvider(url string, schema *schemapb.FieldSchema) (textEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: textEmbedding005},
			{Key: locationParamKey, Value: "mock_local"},
			{Key: projectIDParamKey, Value: "mock_id"},
			{Key: taskTypeParamKey, Value: vertexAICodeRetrival},
			{Key: embeddingURLParamKey, Value: url},
			{Key: dimParamKey, Value: "4"},
		},
	}
	mockClient := vertexai.NewVertexAIEmbedding(url, []byte{1, 2, 3}, "mock scope", "mock token")
	return NewVertexAIEmbeddingProvider(schema, functionSchema, mockClient)
}

func (s *VertexAITextEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateVertexAIEmbeddingServer()

	defer ts.Close()
	provder, err := createVertexAIProvider(ts.URL, s.schema.Fields[2])
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

func (s *VertexAITextEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res vertexai.EmbeddingResponse
		res.Predictions = append(res.Predictions, vertexai.Prediction{
			Embeddings: vertexai.Embeddings{
				Statistics: vertexai.Statistics{
					Truncated:  false,
					TokenCount: 10,
				},
				Values: []float32{1.0, 1.0, 1.0, 1.0},
			},
		})
		res.Predictions = append(res.Predictions, vertexai.Prediction{
			Embeddings: vertexai.Embeddings{
				Statistics: vertexai.Statistics{
					Truncated:  false,
					TokenCount: 10,
				},
				Values: []float32{1.0, 1.0},
			},
		})

		res.Metadata = vertexai.Metadata{
			BillableCharacterCount: 100,
		}

		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	provder, err := createVertexAIProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)

	// embedding dim not match
	data := []string{"sentence", "sentence"}
	_, err2 := provder.CallEmbedding(data, InsertMode)
	s.Error(err2)
}

func (s *VertexAITextEmbeddingProviderSuite) TestEmbeddingNubmerNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res vertexai.EmbeddingResponse
		res.Predictions = append(res.Predictions, vertexai.Prediction{
			Embeddings: vertexai.Embeddings{
				Statistics: vertexai.Statistics{
					Truncated:  false,
					TokenCount: 10,
				},
				Values: []float32{1.0, 1.0, 1.0, 1.0},
			},
		})
		res.Metadata = vertexai.Metadata{
			BillableCharacterCount: 100,
		}

		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	provder, err := createVertexAIProvider(ts.URL, s.schema.Fields[2])

	s.NoError(err)

	// embedding dim not match
	data := []string{"sentence", "sentence2"}
	_, err2 := provder.CallEmbedding(data, InsertMode)
	s.Error(err2)
}

func (s *VertexAITextEmbeddingProviderSuite) TestCheckVertexAITask() {
	err := checkTask(textMultilingualEmbedding002, "UnkownTask")
	s.Error(err)

	// textMultilingualEmbedding002 not support vertexAICodeRetrival task
	err = checkTask(textMultilingualEmbedding002, vertexAICodeRetrival)
	s.Error(err)

	err = checkTask(textEmbedding005, vertexAICodeRetrival)
	s.NoError(err)

	err = checkTask(textMultilingualEmbedding002, vertexAISTS)
	s.NoError(err)
}

func (s *VertexAITextEmbeddingProviderSuite) TestGetVertexAIJsonKey() {
	os.Setenv(vertexServiceAccountJSONEnv, "ErrorPath")
	defer os.Unsetenv(vertexServiceAccountJSONEnv)
	_, err := getVertexAIJsonKey()
	s.Error(err)
}

func (s *VertexAITextEmbeddingProviderSuite) TestGetTaskType() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: textEmbedding005},
			{Key: projectIDParamKey, Value: "mock_id"},
			{Key: dimParamKey, Value: "4"},
		},
	}
	mockClient := vertexai.NewVertexAIEmbedding("mock_url", []byte{1, 2, 3}, "mock scope", "mock token")

	{
		provider, err := NewVertexAIEmbeddingProvider(s.schema.Fields[2], functionSchema, mockClient)
		s.NoError(err)
		s.Equal(provider.getTaskType(InsertMode), "RETRIEVAL_DOCUMENT")
		s.Equal(provider.getTaskType(SearchMode), "RETRIEVAL_QUERY")
	}

	{
		functionSchema.Params = append(functionSchema.Params, &commonpb.KeyValuePair{Key: taskTypeParamKey, Value: vertexAICodeRetrival})
		provider, err := NewVertexAIEmbeddingProvider(s.schema.Fields[2], functionSchema, mockClient)
		s.NoError(err)
		s.Equal(provider.getTaskType(InsertMode), "RETRIEVAL_DOCUMENT")
		s.Equal(provider.getTaskType(SearchMode), "CODE_RETRIEVAL_QUERY")
	}

	{
		functionSchema.Params[3] = &commonpb.KeyValuePair{Key: taskTypeParamKey, Value: vertexAISTS}
		provider, err := NewVertexAIEmbeddingProvider(s.schema.Fields[2], functionSchema, mockClient)
		s.NoError(err)
		s.Equal(provider.getTaskType(InsertMode), "SEMANTIC_SIMILARITY")
		s.Equal(provider.getTaskType(SearchMode), "SEMANTIC_SIMILARITY")
	}

	// invalid task
	{
		functionSchema.Params[3] = &commonpb.KeyValuePair{Key: taskTypeParamKey, Value: "UnkownTask"}
		_, err := NewVertexAIEmbeddingProvider(s.schema.Fields[2], functionSchema, mockClient)
		s.Error(err)
	}
}

func (s *VertexAITextEmbeddingProviderSuite) TestCreateVertexAIEmbeddingClient() {
	os.Setenv(vertexServiceAccountJSONEnv, "ErrorPath")
	defer os.Unsetenv(vertexServiceAccountJSONEnv)
	_, err := createVertexAIEmbeddingClient("https://mock_url.com")
	s.Error(err)
}

func (s *VertexAITextEmbeddingProviderSuite) TestNewVertexAIEmbeddingProvider() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: textEmbedding005},
			{Key: projectIDParamKey, Value: "mock_id"},
			{Key: dimParamKey, Value: "4"},
		},
	}
	mockClient := vertexai.NewVertexAIEmbedding("mock_url", []byte{1, 2, 3}, "mock scope", "mock token")
	provider, err := NewVertexAIEmbeddingProvider(s.schema.Fields[2], functionSchema, mockClient)
	s.NoError(err)
	s.True(provider.MaxBatch() > 0)
	s.Equal(provider.FieldDim(), int64(4))

	// check model name
	functionSchema.Params[0] = &commonpb.KeyValuePair{Key: modelNameParamKey, Value: "UnkownModel"}
	_, err = NewVertexAIEmbeddingProvider(s.schema.Fields[2], functionSchema, mockClient)
	s.Error(err)
}
