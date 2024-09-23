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
	"io"
	"fmt"
	"testing"
	"net/http"
	"net/http/httptest"
	"encoding/json"	

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"

	"github.com/milvus-io/milvus/internal/models"	
)


func TestOpenAIEmbeddingFunction(t *testing.T) {
	suite.Run(t, new(OpenAIEmbeddingFunctionSuite))
}

type OpenAIEmbeddingFunctionSuite struct {
	suite.Suite
	schema *schemapb.CollectionSchema
}

func (s *OpenAIEmbeddingFunctionSuite) SetupTest() {
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

func createData(texts []string) []*schemapb.FieldData{
	data := []*schemapb.FieldData{}
	f := schemapb.FieldData{
		Type: schemapb.DataType_VarChar,
		FieldId: 101,
		IsDynamic: false,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: texts,
					},
				},
			},
		},
	}
	data = append(data, &f)
	return data
}

func createEmbedding(texts []string, dim int) [][]float32 {
	embeddings := make([][]float32, 0)
	for i := 0; i < len(texts); i++ {
		f := float32(i)
		emb := make([]float32, 0)
		for j := 0; j < dim; j++ {
			emb = append(emb, f + float32(j) * 0.1)
		}
		embeddings = append(embeddings, emb)
	}
	return embeddings
}

func createRunner(url string, schema *schemapb.CollectionSchema) (*OpenAIEmbeddingFunction, error) {
	return NewOpenAIEmbeddingFunction(schema, &schemapb.FunctionSchema{
		Name:           "test",
		Type:           schemapb.FunctionType_Unknown,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: ModelNameParamKey, Value: "text-embedding-ada-002"},
			{Key: OpenaiApiKeyParamKey, Value: "mock"},
			{Key: OpenaiEmbeddingUrlParamKey, Value: url},
		},
	}, InsertMode)
}

func (s *OpenAIEmbeddingFunctionSuite) TestEmbedding() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req models.EmbeddingRequest
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		json.Unmarshal(body, &req)

		var res models.EmbeddingResponse
		res.Object = "list"
		res.Model = "text-embedding-3-small"
		embs := createEmbedding(req.Input, 4)
		for i := 0; i < len(req.Input); i++ {
			res.Data = append(res.Data, models.EmbeddingData{
				Object: "embedding",
				Embedding: embs[i],
				Index: i,
			})
		}

		res.Usage = models.Usage{
			PromptTokens: 1,
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
		
	}))

	defer ts.Close()
	runner, err := createRunner(ts.URL, s.schema)
	s.NoError(err)
	{
		data := createData([]string{"sentence"})
		ret, err2 := runner.Run(data)
		s.NoError(err2)
		s.Equal(1, len(ret))
		s.Equal(int64(4), ret[0].GetVectors().Dim)
		s.Equal([]float32{0.0, 0.1, 0.2, 0.3}, ret[0].GetVectors().GetFloatVector().Data)
	}
	{
		data := createData([]string{"sentence 1", "sentence 2", "sentence 3"})
		ret, _ := runner.Run(data)
		s.Equal([]float32{0.0, 0.1, 0.2, 0.3, 1.0, 1.1, 1.2, 1.3, 2.0, 2.1, 2.2, 2.3}, ret[0].GetVectors().GetFloatVector().Data)
	}
}

func (s *OpenAIEmbeddingFunctionSuite) TestEmbeddingDimNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res models.EmbeddingResponse
		res.Object = "list"
		res.Model = "text-embedding-3-small"
		res.Data = append(res.Data, models.EmbeddingData{
			Object: "embedding",
			Embedding: []float32{1.0, 1.0, 1.0, 1.0},
			Index: 0,
		})

		res.Data = append(res.Data, models.EmbeddingData{
			Object: "embedding",
			Embedding: []float32{1.0, 1.0},
			Index: 1,
		})
		res.Usage = models.Usage{
			PromptTokens: 1,
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	runner, err := createRunner(ts.URL, s.schema)
	s.NoError(err)

	// embedding dim not match
	data := createData([]string{"sentence", "sentence"})
	_, err2 := runner.Run(data)
	s.Error(err2)
	fmt.Println(err2.Error())
	// s.NoError(err2)
}

func (s *OpenAIEmbeddingFunctionSuite) TestEmbeddingNubmerNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var res models.EmbeddingResponse
		res.Object = "list"
		res.Model = "text-embedding-3-small"
		res.Data = append(res.Data, models.EmbeddingData{
			Object: "embedding",
			Embedding: []float32{1.0, 1.0, 1.0, 1.0},
			Index: 0,
		})
		res.Usage = models.Usage{
			PromptTokens: 1,
			TotalTokens: 100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	runner, err := createRunner(ts.URL, s.schema)
	
	s.NoError(err)

	// embedding dim not match
	data := createData([]string{"sentence", "sentence2"})
	_, err2 := runner.Run(data)
	s.Error(err2)
	fmt.Println(err2.Error())
	// s.NoError(err2)
}

func (s *OpenAIEmbeddingFunctionSuite) TestRunnerParamsErr() {
	// outputfield datatype mismatch
	{
		schema := &schemapb.CollectionSchema{
			Name: "test",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "vector", DataType: schemapb.DataType_BFloat16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "4"},
					}},
			},
		}

		_, err := NewOpenAIEmbeddingFunction(schema, &schemapb.FunctionSchema{
			Name:           "test",
			Type:           schemapb.FunctionType_Unknown,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: ModelNameParamKey, Value: "text-embedding-ada-002"},
				{Key: DimParamKey, Value: "4"},
				{Key: OpenaiApiKeyParamKey, Value: "mock"},
				{Key: OpenaiEmbeddingUrlParamKey, Value: "mock"},
			},
		}, InsertMode)
		s.Error(err)
		fmt.Println(err.Error())
	}

	// outputfield number mismatc
	{
		schema := &schemapb.CollectionSchema{
			Name: "test",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
				{FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "4"},
					}},
				{FieldID: 103, Name: "vector", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "4"},
					}},
			},
		}
		_, err := NewOpenAIEmbeddingFunction(schema, &schemapb.FunctionSchema{
			Name:           "test",
			Type:           schemapb.FunctionType_Unknown,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102, 103},
			Params: []*commonpb.KeyValuePair{
				{Key: ModelNameParamKey, Value: "text-embedding-ada-002"},
				{Key: DimParamKey, Value: "4"},
				{Key: OpenaiApiKeyParamKey, Value: "mock"},
				{Key: OpenaiEmbeddingUrlParamKey, Value: "mock"},
			},
		}, InsertMode)
		s.Error(err)
		fmt.Println(err.Error())
	}

	// outputfield miss
	{
		_, err := NewOpenAIEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
			Name:           "test",
			Type:           schemapb.FunctionType_Unknown,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{103},
			Params: []*commonpb.KeyValuePair{
				{Key: ModelNameParamKey, Value: "text-embedding-ada-002"},
				{Key: DimParamKey, Value: "4"},
				{Key: OpenaiApiKeyParamKey, Value: "mock"},
				{Key: OpenaiEmbeddingUrlParamKey, Value: "mock"},
			}, 
		}, InsertMode)
		s.Error(err)
		fmt.Println(err.Error())
	}

	// error model name
	{
		_, err := NewOpenAIEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
			Name:           "test",
			Type:           schemapb.FunctionType_Unknown,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: ModelNameParamKey, Value: "text-embedding-ada-004"},
				{Key: DimParamKey, Value: "4"},
				{Key: OpenaiApiKeyParamKey, Value: "mock"},
				{Key: OpenaiEmbeddingUrlParamKey, Value: "mock"},
			},
		}, InsertMode)
		s.Error(err)
		fmt.Println(err.Error())
	}

	// no openai api  key
	{
		_, err := NewOpenAIEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
			Name:           "test",
			Type:           schemapb.FunctionType_Unknown,
			InputFieldIds:  []int64{101},
			OutputFieldIds: []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: ModelNameParamKey, Value: "text-embedding-ada-003"},
			},
		}, InsertMode)
		s.Error(err)
		fmt.Println(err.Error())
	}
}
