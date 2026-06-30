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
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
)

func TestTwelveLabsEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(TwelveLabsEmbeddingProviderSuite))
}

type TwelveLabsEmbeddingProviderSuite struct {
	suite.Suite
	schema *schemapb.CollectionSchema
}

func (s *TwelveLabsEmbeddingProviderSuite) SetupTest() {
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

func createTwelveLabsProvider(url string, schema *schemapb.FieldSchema) (textEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "marengo3.0"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.DimParamKey, Value: "4"},
		},
	}
	return NewTwelveLabsEmbeddingProvider(schema, functionSchema, map[string]string{models.URLParamKey: url}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
}

func (s *TwelveLabsEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateTwelveLabsEmbeddingServer(4)
	defer ts.Close()

	provider, err := createTwelveLabsProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)

	data, err := provider.CallEmbedding(context.Background(), []string{"a dog running"}, models.InsertMode)
	s.NoError(err)
	s.Equal([][]float32{{0.0, 1.0, 2.0, 3.0}}, data)
}

func (s *TwelveLabsEmbeddingProviderSuite) TestBatchEmbedding() {
	ts := CreateTwelveLabsEmbeddingServer(4)
	defer ts.Close()

	provider, err := createTwelveLabsProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)

	// The endpoint embeds one text per request; the provider issues one request
	// per row, so every row gets the same mock vector.
	data, err := provider.CallEmbedding(context.Background(), []string{"q1", "q2", "q3"}, models.SearchMode)
	s.NoError(err)
	s.Equal([][]float32{{0.0, 1.0, 2.0, 3.0}, {0.0, 1.0, 2.0, 3.0}, {0.0, 1.0, 2.0, 3.0}}, data)
}

func (s *TwelveLabsEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := twelveLabsEmbeddingResponse{}
		res.TextEmbedding.Segments = []struct {
			Float []float32 `json:"float"`
		}{{Float: []float32{1.0, 1.0}}}
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(res)
	}))
	defer ts.Close()

	provider, err := createTwelveLabsProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	_, err = provider.CallEmbedding(context.Background(), []string{"sentence"}, models.InsertMode)
	s.ErrorContains(err, "embedding dim")
}

func (s *TwelveLabsEmbeddingProviderSuite) TestEmptyEmbeddingResponse() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(twelveLabsEmbeddingResponse{})
	}))
	defer ts.Close()

	provider, err := createTwelveLabsProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	_, err = provider.CallEmbedding(context.Background(), []string{"sentence"}, models.InsertMode)
	s.ErrorContains(err, "no text embedding segments")
}

func (s *TwelveLabsEmbeddingProviderSuite) TestCallEmbeddingHTTPError() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
		_, _ = w.Write([]byte("internal error"))
	}))
	defer ts.Close()

	provider, err := createTwelveLabsProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	_, err = provider.CallEmbedding(context.Background(), []string{"sentence"}, models.InsertMode)
	s.Error(err)
}

func (s *TwelveLabsEmbeddingProviderSuite) TestMissingCredential() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "marengo3.0"},
		},
	}
	_, err := NewTwelveLabsEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{}), &models.ModelExtraInfo{})
	s.ErrorContains(err, models.TwelveLabsAKEnvStr)
}

func (s *TwelveLabsEmbeddingProviderSuite) TestMissingModelName() {
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
	_, err := NewTwelveLabsEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
	s.ErrorContains(err, "twelvelabs embedding model name is required")
}

func (s *TwelveLabsEmbeddingProviderSuite) TestInvalidDimParam() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "marengo3.0"},
			{Key: models.DimParamKey, Value: "invalid"},
			{Key: models.CredentialParamKey, Value: "mock"},
		},
	}
	_, err := NewTwelveLabsEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
	s.ErrorContains(err, "is not a valid int")
}

func (s *TwelveLabsEmbeddingProviderSuite) TestUnsupportedOutputType() {
	int8Field := &schemapb.FieldSchema{
		FieldID: 102, Name: "vector", DataType: schemapb.DataType_Int8Vector,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: "dim", Value: "4"},
		},
	}
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "marengo3.0"},
			{Key: models.CredentialParamKey, Value: "mock"},
		},
	}
	_, err := NewTwelveLabsEmbeddingProvider(int8Field, functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
	s.ErrorContains(err, "FloatVector")
}

func (s *TwelveLabsEmbeddingProviderSuite) TestCustomAndDefaultURL() {
	ts := CreateTwelveLabsEmbeddingServer(4)
	defer ts.Close()

	provider, err := createTwelveLabsProvider(ts.URL, s.schema.Fields[2])
	s.NoError(err)
	s.Equal(ts.URL, provider.(*TwelveLabsEmbeddingProvider).url)

	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "marengo3.0"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.DimParamKey, Value: "4"},
		},
	}
	p, err := NewTwelveLabsEmbeddingProvider(s.schema.Fields[2], functionSchema, map[string]string{}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
	s.NoError(err)
	s.Equal(defaultTwelveLabsEmbeddingURL, p.url)
}

func (s *TwelveLabsEmbeddingProviderSuite) TestBasicGetters() {
	ts := CreateTwelveLabsEmbeddingServer(4)
	defer ts.Close()

	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "marengo3.0"},
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.DimParamKey, Value: "4"},
		},
	}
	provider, err := NewTwelveLabsEmbeddingProvider(
		s.schema.Fields[2],
		functionSchema,
		map[string]string{models.URLParamKey: ts.URL},
		credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}),
		&models.ModelExtraInfo{BatchFactor: 3},
	)
	s.NoError(err)
	s.Equal(int64(4), provider.FieldDim())
	s.Equal(192, provider.MaxBatch())
}
