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
)

func TestTEITextEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(TEITextEmbeddingProviderSuite))
}

type TEITextEmbeddingProviderSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func (s *TEITextEmbeddingProviderSuite) SetupTest() {
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
	s.providers = []string{teiProvider}
}

func createTEIProvider(url string, schema *schemapb.FieldSchema, providerName string) (textEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: endpointParamKey, Value: url},
			{Key: ingestionPromptParamKey, Value: "doc:"},
			{Key: searchPromptParamKey, Value: "query:"},
		},
	}
	switch providerName {
	case teiProvider:
		return NewTEIEmbeddingProvider(schema, functionSchema)
	default:
		return nil, fmt.Errorf("Unknow provider")
	}
}

func (s *TEITextEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateTEIEmbeddingServer(4)

	defer ts.Close()
	for _, provderName := range s.providers {
		provder, err := createTEIProvider(ts.URL, s.schema.Fields[2], provderName)
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

func (s *TEITextEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := [][]float32{{0.1, 0.1, 0.1, 0.1}, {0.1, 0.1, 0.1}}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	for _, providerName := range s.providers {
		provder, err := createTEIProvider(ts.URL, s.schema.Fields[2], providerName)
		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence"}
		_, err2 := provder.CallEmbedding(data, InsertMode)
		s.Error(err2)
	}
}

func (s *TEITextEmbeddingProviderSuite) TestEmbeddingNumberNotMatch() {
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		res := [][]float32{{0.1, 0.1, 0.1, 0.1}}
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal(res)
		w.Write(data)
	}))

	defer ts.Close()
	for _, provderName := range s.providers {
		provder, err := createTEIProvider(ts.URL, s.schema.Fields[2], provderName)

		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence2"}
		_, err2 := provder.CallEmbedding(data, InsertMode)
		s.Error(err2)
	}
}

func (s *TEITextEmbeddingProviderSuite) TestCreateTEIEmbeddingClient() {
	_, err := createTEIEmbeddingClient("", "")
	s.Error(err)

	_, err = createTEIEmbeddingClient("", "http://mymock.com")
	s.NoError(err)

	os.Setenv(enableTeiEnvStr, "false")
	defer os.Unsetenv(enableTeiEnvStr)
	_, err = createTEIEmbeddingClient("", "http://mymock.com")
	s.Error(err)
}

func (s *TEITextEmbeddingProviderSuite) TestNewTEIEmbeddingProvider() {
	functionSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_Unknown,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: endpointParamKey, Value: "http://mymock.com"},
		},
	}
	provider, err := NewTEIEmbeddingProvider(s.schema.Fields[2], functionSchema)
	s.NoError(err)
	s.Equal(provider.FieldDim(), int64(4))
	s.True(provider.MaxBatch() == 32*5)

	// Invalid truncate
	{
		functionSchema.Params = append(functionSchema.Params, &commonpb.KeyValuePair{Key: truncateParamKey, Value: "Invalid"})
		_, err := NewTEIEmbeddingProvider(s.schema.Fields[2], functionSchema)
		s.Error(err)
	}
	// Invalid truncationDirection
	{
		functionSchema.Params[2] = &commonpb.KeyValuePair{Key: truncateParamKey, Value: "true"}
		functionSchema.Params = append(functionSchema.Params, &commonpb.KeyValuePair{Key: truncationDirectionParamKey, Value: "Invalid"})
		_, err := NewTEIEmbeddingProvider(s.schema.Fields[2], functionSchema)
		s.Error(err)
	}

	// truncationDirection
	{
		functionSchema.Params[3] = &commonpb.KeyValuePair{Key: truncationDirectionParamKey, Value: "Left"}
		_, err := NewTEIEmbeddingProvider(s.schema.Fields[2], functionSchema)
		s.NoError(err)
	}

	// Invalid max batch
	{
		functionSchema.Params = append(functionSchema.Params, &commonpb.KeyValuePair{Key: maxClientBatchSizeParamKey, Value: "Invalid"})
		_, err := NewTEIEmbeddingProvider(s.schema.Fields[2], functionSchema)
		s.Error(err)
	}

	// Valid max batch
	{
		functionSchema.Params[4] = &commonpb.KeyValuePair{Key: maxClientBatchSizeParamKey, Value: "128"}
		pv, err := NewTEIEmbeddingProvider(s.schema.Fields[2], functionSchema)
		s.NoError(err)
		s.True(pv.MaxBatch() == 128*5)
	}
}
