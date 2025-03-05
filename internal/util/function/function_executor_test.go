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
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/models/openai"
	"github.com/milvus-io/milvus/pkg/v2/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v2/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
)

func TestFunctionExecutor(t *testing.T) {
	suite.Run(t, new(FunctionExecutorSuite))
}

type FunctionExecutorSuite struct {
	suite.Suite
}

func (s *FunctionExecutorSuite) creataSchema(url string) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
				IsFunctionOutput: true,
			},
			{
				FieldID: 103, Name: "vector2", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "8"},
				},
				IsFunctionOutput: true,
			},
		},
		Functions: []*schemapb.FunctionSchema{
			{
				Name:             "test",
				Type:             schemapb.FunctionType_TextEmbedding,
				InputFieldIds:    []int64{101},
				InputFieldNames:  []string{"text"},
				OutputFieldIds:   []int64{102},
				OutputFieldNames: []string{"vector"},
				Params: []*commonpb.KeyValuePair{
					{Key: Provider, Value: openAIProvider},
					{Key: modelNameParamKey, Value: "text-embedding-ada-002"},
					{Key: apiKeyParamKey, Value: "mock"},
					{Key: embeddingURLParamKey, Value: url},
					{Key: dimParamKey, Value: "4"},
				},
			},
			{
				Name:             "test",
				Type:             schemapb.FunctionType_TextEmbedding,
				InputFieldIds:    []int64{101},
				InputFieldNames:  []string{"text"},
				OutputFieldIds:   []int64{103},
				OutputFieldNames: []string{"vector2"},
				Params: []*commonpb.KeyValuePair{
					{Key: Provider, Value: openAIProvider},
					{Key: modelNameParamKey, Value: "text-embedding-ada-002"},
					{Key: apiKeyParamKey, Value: "mock"},
					{Key: embeddingURLParamKey, Value: url},
					{Key: dimParamKey, Value: "8"},
				},
			},
		},
	}
}

func (s *FunctionExecutorSuite) createMsg(texts []string) *msgstream.InsertMsg {
	data := []*schemapb.FieldData{}
	f := schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldId:   101,
		FieldName: "text",
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

	msg := msgstream.InsertMsg{
		InsertRequest: &msgpb.InsertRequest{
			FieldsData: data,
		},
	}
	return &msg
}

func (s *FunctionExecutorSuite) createEmbedding(texts []string, dim int) [][]float32 {
	embeddings := make([][]float32, 0)
	for i := 0; i < len(texts); i++ {
		f := float32(i)
		emb := make([]float32, 0)
		for j := 0; j < dim; j++ {
			emb = append(emb, f+float32(j)*0.1)
		}
		embeddings = append(embeddings, emb)
	}
	return embeddings
}

func (s *FunctionExecutorSuite) TestExecutor() {
	ts := CreateOpenAIEmbeddingServer()
	defer ts.Close()
	schema := s.creataSchema(ts.URL)
	exec, err := NewFunctionExecutor(schema)
	s.NoError(err)
	msg := s.createMsg([]string{"sentence", "sentence"})
	exec.ProcessInsert(msg)
	s.Equal(len(msg.FieldsData), 3)
}

func (s *FunctionExecutorSuite) TestErrorEmbedding() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var req openai.EmbeddingRequest
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		json.Unmarshal(body, &req)

		var res openai.EmbeddingResponse
		res.Object = "list"
		res.Model = "text-embedding-3-small"
		for i := 0; i < len(req.Input); i++ {
			res.Data = append(res.Data, openai.EmbeddingData{
				Object:    "embedding",
				Embedding: []float32{},
				Index:     i,
			})
		}

		res.Usage = openai.Usage{
			PromptTokens: 1,
			TotalTokens:  100,
		}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))
	defer ts.Close()
	schema := s.creataSchema(ts.URL)
	exec, err := NewFunctionExecutor(schema)
	s.NoError(err)
	msg := s.createMsg([]string{"sentence", "sentence"})
	err = exec.ProcessInsert(msg)
	s.Error(err)
}

func (s *FunctionExecutorSuite) TestErrorSchema() {
	schema := s.creataSchema("http://localhost")
	schema.Functions[0].Type = schemapb.FunctionType_Unknown
	_, err := NewFunctionExecutor(schema)
	s.Error(err)
}

func (s *FunctionExecutorSuite) TestInternalPrcessSearch() {
	ts := CreateOpenAIEmbeddingServer()
	defer ts.Close()
	schema := s.creataSchema(ts.URL)
	exec, err := NewFunctionExecutor(schema)
	s.NoError(err)

	{
		f := &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldId:   101,
			IsDynamic: false,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: strings.Split("helle,world", ","),
						},
					},
				},
			},
		}
		placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(f)
		s.NoError(err)

		req := &internalpb.SearchRequest{
			Nq:               2,
			PlaceholderGroup: placeholderGroupBytes,
			IsAdvanced:       false,
			FieldId:          102,
		}
		err = exec.ProcessSearch(req)
		s.NoError(err)

		// No function found
		req = &internalpb.SearchRequest{
			Nq:               2,
			PlaceholderGroup: placeholderGroupBytes,
			IsAdvanced:       false,
			FieldId:          111,
		}
		err = exec.ProcessSearch(req)
		s.Error(err)

		// Large search nq
		req = &internalpb.SearchRequest{
			Nq:               1000,
			PlaceholderGroup: placeholderGroupBytes,
			IsAdvanced:       false,
			FieldId:          102,
		}
		err = exec.ProcessSearch(req)
		s.Error(err)
	}

	// AdvanceSearch
	{
		f := &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldId:   101,
			IsDynamic: false,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: strings.Split("helle,world", ","),
						},
					},
				},
			},
		}
		placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(f)
		s.NoError(err)

		subReq := &internalpb.SubSearchRequest{
			PlaceholderGroup: placeholderGroupBytes,
			Nq:               2,
			FieldId:          102,
		}
		req := &internalpb.SearchRequest{
			IsAdvanced: true,
			SubReqs:    []*internalpb.SubSearchRequest{subReq},
		}
		err = exec.ProcessSearch(req)
		s.NoError(err)

		// Large nq
		subReq.Nq = 1000
		err = exec.ProcessSearch(req)
		s.Error(err)
	}
}

func (s *FunctionExecutorSuite) TestInternalPrcessSearchFailed() {
	ts := CreateErrorEmbeddingServer()
	defer ts.Close()

	schema := s.creataSchema(ts.URL)
	exec, err := NewFunctionExecutor(schema)
	s.NoError(err)
	f := &schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldId:   101,
		IsDynamic: false,
		Field: &schemapb.FieldData_Scalars{
			Scalars: &schemapb.ScalarField{
				Data: &schemapb.ScalarField_StringData{
					StringData: &schemapb.StringArray{
						Data: strings.Split("helle,world", ","),
					},
				},
			},
		},
	}
	placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(f)
	s.NoError(err)

	{
		req := &internalpb.SearchRequest{
			Nq:               2,
			PlaceholderGroup: placeholderGroupBytes,
			IsAdvanced:       false,
			FieldId:          102,
		}
		err = exec.ProcessSearch(req)
		s.Error(err)
	}
	// AdvanceSearch
	{
		subReq := &internalpb.SubSearchRequest{
			PlaceholderGroup: placeholderGroupBytes,
			Nq:               2,
			FieldId:          102,
		}
		req := &internalpb.SearchRequest{
			IsAdvanced: true,
			SubReqs:    []*internalpb.SubSearchRequest{subReq},
		}
		err = exec.ProcessSearch(req)
		s.Error(err)
	}
}
