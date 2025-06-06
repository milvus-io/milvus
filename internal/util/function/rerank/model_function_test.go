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

package rerank

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestRerankModel(t *testing.T) {
	suite.Run(t, new(RerankModelSuite))
}

type RerankModelSuite struct {
	suite.Suite
}

func (s *RerankModelSuite) SetupTest() {
	paramtable.Init()
	paramtable.Get().FunctionCfg.RerankModelProviders.GetFunc = func() map[string]string {
		return map[string]string{}
	}
}

func (s *RerankModelSuite) TestNewProvider() {
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "unknown"},
		}
		_, err := newProvider(params)
		s.ErrorContains(err, "Unknow rerank provider")
	}

	{
		_, err := newProvider([]*commonpb.KeyValuePair{})
		s.ErrorContains(err, "Lost rerank params")
	}

	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: function.EndpointParamKey, Value: "illegal"},
		}
		_, err := newProvider(params)
		s.ErrorContains(err, "is not a valid http/https link")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: function.EndpointParamKey, Value: "http://"},
		}
		_, err := newProvider(params)
		s.ErrorContains(err, "is not a valid http/https link")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
		}
		_, err := newProvider(params)
		s.ErrorContains(err, "Rerank function lost params endpoint")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: function.EndpointParamKey, Value: "http://localhost:80"},
			{Key: maxBatchKeyName, Value: "-1"},
		}
		_, err := newProvider(params)
		s.ErrorContains(err, "Rerank function params max_batch must > 0")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: function.EndpointParamKey, Value: "http://localhost:80"},
			{Key: maxBatchKeyName, Value: "NotNum"},
		}
		_, err := newProvider(params)
		s.ErrorContains(err, "Rerank params error, maxBatch")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: function.EndpointParamKey, Value: "http://localhost:80"},
		}
		provder, err := newProvider(params)
		s.NoError(err)
		s.Equal(provder.getURL(), "http://localhost:80/v2/rerank")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "tei"},
			{Key: function.EndpointParamKey, Value: "http://localhost:80"},
		}
		provder, err := newProvider(params)
		s.NoError(err)
		s.Equal(provder.getURL(), "http://localhost:80/rerank")
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "tei"},
			{Key: function.EndpointParamKey, Value: "http://localhost:80"},
		}
		paramtable.Get().FunctionCfg.RerankModelProviders.GetFunc = func() map[string]string {
			key := "tei.enable"
			return map[string]string{
				key: "false",
			}
		}
		_, err := newProvider(params)
		s.ErrorContains(err, "TEI rerank is disabled")
		paramtable.Get().FunctionCfg.RerankModelProviders.GetFunc = func() map[string]string {
			return map[string]string{}
		}
		os.Setenv(function.EnableTeiEnvStr, "false")
		_, err = newProvider(params)
		s.ErrorContains(err, "TEI rerank is disabled")
		os.Unsetenv(function.EnableTeiEnvStr)
	}
	{
		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: function.EndpointParamKey, Value: "http://localhost:80"},
		}
		paramtable.Get().FunctionCfg.RerankModelProviders.GetFunc = func() map[string]string {
			key := "vllm.enable"
			return map[string]string{
				key: "false",
			}
		}
		_, err := newProvider(params)
		s.ErrorContains(err, "Vllm rerank is disabled")
		paramtable.Get().FunctionCfg.RerankModelProviders.GetFunc = func() map[string]string {
			return map[string]string{}
		}
		os.Setenv(function.EnableVllmEnvStr, "false")
		_, err = newProvider(params)
		s.ErrorContains(err, "Vllm rerank is disabled")
		os.Unsetenv(function.EnableVllmEnvStr)
	}
}

func (s *RerankModelSuite) TestCallVllm() {
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"results": [{"index": 0, "relevance_score": 0.1}, {"index": 1, "relevance_score": 0.2}]}`))
		}))
		defer ts.Close()

		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: function.EndpointParamKey, Value: ts.URL},
		}
		provder, err := newProvider(params)
		s.NoError(err)
		_, err = provder.rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
		s.ErrorContains(err, "Call Rerank service failed, 3 docs but got 2 scores")
	}
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusBadRequest)
			w.Write([]byte(`{'object': 'error', 'message': 'The model vllm-test does not exist.', 'type': 'NotFoundError', 'param': None, 'code': 404}`))
		}))
		defer ts.Close()

		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: function.EndpointParamKey, Value: ts.URL},
		}
		provder, err := newProvider(params)
		s.NoError(err)
		_, err = provder.rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
		s.ErrorContains(err, "Call rerank model failed")
	}
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`Not json data`))
		}))
		defer ts.Close()

		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: function.EndpointParamKey, Value: ts.URL},
		}
		provder, err := newProvider(params)
		s.NoError(err)
		_, err = provder.rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
		s.ErrorContains(err, "Rerank error, parsing vllm response failed")
	}
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"results": [{"index": 0, "relevance_score": 0.0}, {"index": 2, "relevance_score": 0.2}, {"index": 1, "relevance_score": 0.1}]}`))
		}))
		defer ts.Close()

		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: function.EndpointParamKey, Value: ts.URL},
		}
		provder, err := newProvider(params)
		s.NoError(err)
		scores, err := provder.rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
		s.NoError(err)
		s.Equal([]float32{0.0, 0.1, 0.2}, scores)
	}
}

func (s *RerankModelSuite) TestCallTEI() {
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[{"index":0,"score":0.0},{"index":1,"score":0.2}, {"index":2,"score":0.1}]`))
		}))
		defer ts.Close()

		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "tei"},
			{Key: function.EndpointParamKey, Value: ts.URL},
		}
		provder, err := newProvider(params)
		s.NoError(err)
		scores, err := provder.rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
		s.NoError(err)
		s.Equal([]float32{0.0, 0.2, 0.1}, scores)
	}
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`not json`))
		}))
		defer ts.Close()

		params := []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "tei"},
			{Key: function.EndpointParamKey, Value: ts.URL},
		}
		provder, err := newProvider(params)
		s.NoError(err)
		_, err = provder.rerank(context.Background(), "mytest", []string{"t1", "t2", "t3"})
		s.ErrorContains(err, "Rerank error, parsing TEI response failed")
	}
}

func (s *RerankModelSuite) TestNewModelFunction() {
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
			{FieldID: 102, Name: "ts", DataType: schemapb.DataType_Int64},
		},
	}
	functionSchema := &schemapb.FunctionSchema{
		Name:            "test",
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"text"},
		Params: []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "tei"},
			{Key: function.EndpointParamKey, Value: "http://localhost:80"},
			{Key: queryKeyName, Value: `["q1"]`},
		},
	}
	{
		functionSchema.InputFieldNames = []string{"text", "ts"}
		_, err := newModelFunction(schema, functionSchema)
		s.ErrorContains(err, "Rerank model only supports single input")
	}
	{
		functionSchema.InputFieldNames = []string{"ts"}
		_, err := newModelFunction(schema, functionSchema)
		s.ErrorContains(err, "Rerank model only support varchar")
		functionSchema.InputFieldNames = []string{"text"}
	}
	{
		functionSchema.Params[2] = &commonpb.KeyValuePair{Key: queryKeyName, Value: `NotJson`}
		_, err := newModelFunction(schema, functionSchema)
		s.ErrorContains(err, "Parse rerank params [queries] failed")
	}
	{
		functionSchema.Params[2] = &commonpb.KeyValuePair{Key: queryKeyName, Value: `[]`}
		_, err := newModelFunction(schema, functionSchema)
		s.ErrorContains(err, "Rerank function lost params queries")
	}
	{
		functionSchema.Params[2] = &commonpb.KeyValuePair{Key: queryKeyName, Value: `["test"]`}
		_, err := newModelFunction(schema, functionSchema)
		s.NoError(err)
	}
	{
		schema.Fields[0] = &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true}
		_, err := newModelFunction(schema, functionSchema)
		s.NoError(err)
	}
	{
		functionSchema.Params[0] = &commonpb.KeyValuePair{Key: providerParamName, Value: `notExist`}
		_, err := newModelFunction(schema, functionSchema)
		s.ErrorContains(err, "Unknow rerank provider")
	}
	{
		functionSchema.Params[0] = &commonpb.KeyValuePair{Key: "NotExist", Value: `notExist`}
		_, err := newModelFunction(schema, functionSchema)
		s.ErrorContains(err, "Lost rerank params")
	}
}

func (s *RerankModelSuite) TestRerankProcess() {
	schema := &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
		},
	}
	{
		ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`{"results": [{"index": 0, "relevance_score": 0.1}, {"index": 1, "relevance_score": 0.2}]}`))
		}))
		defer ts.Close()
		functionSchema := &schemapb.FunctionSchema{
			Name:            "test",
			Type:            schemapb.FunctionType_Rerank,
			InputFieldNames: []string{"text"},
			Params: []*commonpb.KeyValuePair{
				{Key: providerParamName, Value: "tei"},
				{Key: function.EndpointParamKey, Value: ts.URL},
				{Key: queryKeyName, Value: `["q1"]`},
			},
		}

		// empty
		{
			nq := int64(1)
			f, err := newModelFunction(schema, functionSchema)
			s.NoError(err)
			inputs, _ := newRerankInputs([]*schemapb.SearchResultData{}, f.GetInputFieldIDs(), false)
			ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE"}), inputs)
			s.NoError(err)
			s.Equal(int64(3), ret.searchResultData.TopK)
			s.Equal([]int64{}, ret.searchResultData.Topks)
		}

		// no input field exist
		{
			nq := int64(1)
			f, err := newModelFunction(schema, functionSchema)
			data := function.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "noExist", 1000)
			s.NoError(err)

			_, err = newRerankInputs([]*schemapb.SearchResultData{data}, f.GetInputFieldIDs(), false)
			s.ErrorContains(err, "Search reaults mismatch rerank inputs")
		}
	}

	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		req := map[string]any{}
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		json.Unmarshal(body, &req)
		ret := map[string][]map[string]any{}
		ret["results"] = []map[string]any{}
		for i := range req["documents"].([]any) {
			d := map[string]any{}
			d["index"] = i
			d["relevance_score"] = float32(i) / 10
			ret["results"] = append(ret["results"], d)
		}
		jsonData, _ := json.Marshal(ret)
		w.Write(jsonData)
	}))
	defer ts.Close()

	// singleSearchResultData
	functionSchema := &schemapb.FunctionSchema{
		Name:            "test",
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"text"},
		Params: []*commonpb.KeyValuePair{
			{Key: providerParamName, Value: "vllm"},
			{Key: function.EndpointParamKey, Value: ts.URL},
			{Key: queryKeyName, Value: `["q1", "q2"]`},
		},
	}

	{
		nq := int64(1)
		{
			f, err := newModelFunction(schema, functionSchema)
			s.NoError(err)
			data := function.GenSearchResultData(nq, 10, schemapb.DataType_VarChar, "text", 101)
			inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data}, f.GetInputFieldIDs(), false)
			_, err = f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE"}), inputs)
			s.ErrorContains(err, "nq must equal to queries size, but got nq [1], queries size [2]")
		}
		{
			functionSchema.Params[2].Value = `["q1"]`
			f, err := newModelFunction(schema, functionSchema)
			s.NoError(err)
			data := function.GenSearchResultData(nq, 10, schemapb.DataType_VarChar, "text", 101)
			inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data}, f.GetInputFieldIDs(), false)
			ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 0, -1, -1, 1, false, "", []string{"COSINE"}), inputs)
			s.NoError(err)
			s.Equal([]int64{3}, ret.searchResultData.Topks)
			s.Equal(int64(3), ret.searchResultData.TopK)
		}

		{
			nq := int64(3)
			functionSchema.Params[2].Value = `["q1", "q2", "q3"]`
			f, err := newModelFunction(schema, functionSchema)
			s.NoError(err)
			data := function.GenSearchResultData(nq, 10, schemapb.DataType_VarChar, "text", 101)
			inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data}, f.GetInputFieldIDs(), false)
			ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE", "COSINE", "COSINE"}), inputs)
			s.NoError(err)
			s.Equal([]int64{3, 3, 3}, ret.searchResultData.Topks)
			s.Equal(int64(3), ret.searchResultData.TopK)
		}
	}

	// // multipSearchResultData
	// has empty inputs
	{
		nq := int64(1)
		functionSchema.Params[2].Value = `["q1"]`
		f, err := newModelFunction(schema, functionSchema)
		s.NoError(err)
		data1 := function.GenSearchResultData(nq, 10, schemapb.DataType_VarChar, "text", 101)
		// empty
		data2 := function.GenSearchResultData(nq, 0, schemapb.DataType_VarChar, "text", 101)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data1, data2}, f.GetInputFieldIDs(), false)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
	}
	// nq = 1
	{
		nq := int64(1)
		f, err := newModelFunction(schema, functionSchema)
		s.NoError(err)
		// ts/id data: 0 - 9
		data1 := function.GenSearchResultData(nq, 10, schemapb.DataType_VarChar, "text", 101)
		// ts/id data: 0 - 3
		data2 := function.GenSearchResultData(nq, 4, schemapb.DataType_VarChar, "text", 101)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data1, data2}, f.GetInputFieldIDs(), false)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE", "COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
	}
	// // nq = 3
	{
		nq := int64(3)
		functionSchema.Params[2].Value = `["q1", "q2", "q3"]`
		f, err := newModelFunction(schema, functionSchema)
		s.NoError(err)
		data1 := function.GenSearchResultData(nq, 10, schemapb.DataType_VarChar, "text", 101)
		data2 := function.GenSearchResultData(nq, 4, schemapb.DataType_VarChar, "text", 101)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data1, data2}, f.GetInputFieldIDs(), false)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, 1, -1, 1, false, "", []string{"COSINE", "COSINE", "COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{3, 3, 3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
	}
}
