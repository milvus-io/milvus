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

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/models/vertexai"
)

func TestVertextAITextEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(VertextAITextEmbeddingProviderSuite))
}

type VertextAITextEmbeddingProviderSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func (s *VertextAITextEmbeddingProviderSuite) SetupTest() {
	s.schema = &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				}},
		},
	}
}

func createVertextAIProvider(url string, schema *schemapb.FieldSchema) (TextEmbeddingProvider, error) {
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
			{Key: embeddingUrlParamKey, Value: url},
			{Key: dimParamKey, Value: "4"},
		},
	}
	mockClient := vertexai.NewVertexAIEmbedding(url, []byte{1, 2, 3}, "mock scope", "mock token")
	return NewVertextAIEmbeddingProvider(schema, functionSchema, mockClient)
}

func (s *VertextAITextEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateVertexAIEmbeddingServer()

	defer ts.Close()
	provder, err := createVertextAIProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	{
		data := []string{"sentence"}
		ret, err2 := provder.CallEmbedding(data, false, InsertMode)
		s.NoError(err2)
		s.Equal(1, len(ret))
		s.Equal(4, len(ret[0]))
		s.Equal([]float32{0.0, 0.1, 0.2, 0.3}, ret[0])
	}
	{
		data := []string{"sentence 1", "sentence 2", "sentence 3"}
		ret, _ := provder.CallEmbedding(data, false, SearchMode)
		s.Equal([][]float32{{0.0, 0.1, 0.2, 0.3}, {1.0, 1.1, 1.2, 1.3}, {2.0, 2.1, 2.2, 2.3}}, ret)
	}

}

func (s *VertextAITextEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
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
	provder, err := createVertextAIProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)

	// embedding dim not match
	data := []string{"sentence", "sentence"}
	_, err2 := provder.CallEmbedding(data, false, InsertMode)
	s.Error(err2)
}

func (s *VertextAITextEmbeddingProviderSuite) TestEmbeddingNubmerNotMatch() {
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
	provder, err := createVertextAIProvider(ts.URL, s.schema.Fields[2])

	s.NoError(err)

	// embedding dim not match
	data := []string{"sentence", "sentence2"}
	_, err2 := provder.CallEmbedding(data, false, InsertMode)
	s.Error(err2)
}
