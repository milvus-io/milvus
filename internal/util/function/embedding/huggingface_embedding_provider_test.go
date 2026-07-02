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

package embedding

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
	"github.com/milvus-io/milvus/internal/util/function/models/huggingface"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func (s *TextEmbeddingFunctionSuite) TestNewHuggingFaceEmbeddingProvider() {
	field := s.schema.Fields[2]
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.CredentialParamKey, Value: "mock"},
		},
	}
	creds := credentials.NewCredentials(map[string]string{"mock.apikey": "mock_key"})
	extraInfo := &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db", BatchFactor: 1}
	_, err := NewHuggingFaceEmbeddingProvider(field, functionSchema, nil, creds, extraInfo)
	s.ErrorContains(err, "huggingface embedding model name is required")

	functionSchema.Params = append(functionSchema.Params, &commonpb.KeyValuePair{Key: models.ModelNameParamKey, Value: "BAAI/bge-m3"})
	int8Field := &schemapb.FieldSchema{FieldID: 103, Name: "int8_vector", DataType: schemapb.DataType_Int8Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "4"}}}
	_, err = NewHuggingFaceEmbeddingProvider(int8Field, functionSchema, nil, creds, extraInfo)
	s.ErrorContains(err, "only supports FloatVector")
}

func (s *TextEmbeddingFunctionSuite) TestCallHuggingFaceEmbedding() {
	var count int32
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		s.Equal("/hf-inference/models/BAAI/bge-m3/pipeline/feature-extraction", r.URL.Path)
		s.Equal("Bearer mock_key", r.Header.Get("Authorization"))
		var req huggingface.FeatureExtractionRequest
		body, _ := io.ReadAll(r.Body)
		defer r.Body.Close()
		s.NoError(json.Unmarshal(body, &req))
		s.Equal("query", req["prompt_name"])
		s.Equal("left", req["truncation_direction"])
		s.Equal(true, req["normalize"])

		current := atomic.AddInt32(&count, 1)
		switch current {
		case 1:
			s.Equal([]any{"t1", "t2"}, req["inputs"])
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[[0,1,2,3],[1,2,3,4]]`))
		case 2:
			s.Equal([]any{"t3"}, req["inputs"])
			w.WriteHeader(http.StatusOK)
			w.Write([]byte(`[[2,3,4,5]]`))
		default:
			w.WriteHeader(http.StatusInternalServerError)
		}
	}))
	defer ts.Close()

	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.ModelNameParamKey, Value: "BAAI/bge-m3"},
			{Key: models.MaxClientBatchSizeParamKey, Value: "2"},
			{Key: models.NormalizeParamKey, Value: "true"},
			{Key: models.TruncationDirectionParamKey, Value: "left"},
			{Key: models.HuggingFacePromptNameParamKey, Value: "query"},
		},
	}
	provider, err := NewHuggingFaceEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{models.URLParamKey: ts.URL}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock_key"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db", BatchFactor: 1})
	s.NoError(err)
	embs, err := provider.CallEmbedding(context.Background(), []string{"t1", "t2", "t3"}, models.InsertMode)
	s.NoError(err)
	s.Equal([][]float32{{0, 1, 2, 3}, {1, 2, 3, 4}, {2, 3, 4, 5}}, embs)
	s.Equal(int32(2), atomic.LoadInt32(&count))
}

func (s *TextEmbeddingFunctionSuite) TestParseHuggingFaceFeatureExtractionResponse() {
	_, err := parseFeatureExtractionResponse([]byte(`[[[0,1,2,3]]]`), 1, 4)
	s.ErrorContains(err, "unsupported")

	_, err = parseFeatureExtractionResponse([]byte(`[[0,1,2,3]]`), 2, 4)
	s.ErrorContains(err, "does not match")

	_, err = parseFeatureExtractionResponse([]byte(`[[0,1,2]]`), 1, 4)
	s.ErrorContains(err, "required embedding dim")

	_, err = parseFeatureExtractionResponse([]byte(`{"unexpected":true}`), 1, 4)
	s.ErrorContains(err, "unsupported")
}

func (s *TextEmbeddingFunctionSuite) TestHuggingFaceTextEmbeddingFunctionRegistry() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(`[[0,1,2,3]]`))
	}))
	defer ts.Close()
	paramtable.Get().FunctionCfg.TextEmbeddingProviders.GetFunc = func() map[string]string {
		return map[string]string{
			huggingFaceProvider + "." + models.URLParamKey:        ts.URL,
			huggingFaceProvider + "." + models.CredentialParamKey: "mock",
		}
	}

	runner, err := NewTextEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: Provider, Value: huggingFaceProvider},
			{Key: models.ModelNameParamKey, Value: "BAAI/bge-m3"},
			{Key: models.CredentialParamKey, Value: "mock"},
		},
	}, &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
	s.NoError(err)
	s.Equal(huggingFaceProvider, runner.GetFunctionProvider())
}
