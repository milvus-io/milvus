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
	"sync/atomic"
	"testing"

	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
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
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.EndpointParamKey, Value: url},
			{Key: models.IngestionPromptParamKey, Value: "doc:"},
			{Key: models.SearchPromptParamKey, Value: "query:"},
		},
	}
	switch providerName {
	case teiProvider:
		conf := map[string]string{models.CredentialEndpointsConfKey: url}
		return NewTEIEmbeddingProvider(schema, functionSchema, conf, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{BatchFactor: 5})
	default:
		return nil, errors.New("Unknow provider")
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
			r, err2 := provder.CallEmbedding(context.Background(), data, models.InsertMode)
			ret := r.([][]float32)
			s.NoError(err2)
			s.Equal(1, len(ret))
			s.Equal(4, len(ret[0]))
		}
		{
			data := []string{"sentence 1", "sentence 2", "sentence 3"}
			_, err := provder.CallEmbedding(context.Background(), data, models.SearchMode)
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
		_, err2 := provder.CallEmbedding(context.Background(), data, models.InsertMode)
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
		_, err2 := provder.CallEmbedding(context.Background(), data, models.InsertMode)
		s.Error(err2)
	}
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
			{Key: models.CredentialParamKey, Value: "mock"},
			{Key: models.EndpointParamKey, Value: "http://mymock.com"},
		},
	}
	batchFactor := 5
	conf := map[string]string{models.CredentialEndpointsConfKey: "http://mymock.com"}
	provider, err := NewTEIEmbeddingProvider(s.schema.Fields[2], functionSchema, conf, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{BatchFactor: batchFactor})
	s.NoError(err)
	s.Equal(provider.FieldDim(), int64(4))
	s.True(provider.MaxBatch() == 32*batchFactor)

	// Invalid truncate
	{
		functionSchema.Params = append(functionSchema.Params, &commonpb.KeyValuePair{Key: models.TruncateParamKey, Value: "Invalid"})
		_, err := NewTEIEmbeddingProvider(s.schema.Fields[2], functionSchema, conf, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
		s.Error(err)
	}
	// Invalid truncationDirection
	{
		functionSchema.Params[2] = &commonpb.KeyValuePair{Key: models.TruncateParamKey, Value: "true"}
		functionSchema.Params = append(functionSchema.Params, &commonpb.KeyValuePair{Key: models.TruncationDirectionParamKey, Value: "Invalid"})
		_, err := NewTEIEmbeddingProvider(s.schema.Fields[2], functionSchema, conf, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
		s.Error(err)
	}

	// truncationDirection
	{
		functionSchema.Params[3] = &commonpb.KeyValuePair{Key: models.TruncationDirectionParamKey, Value: "Left"}
		_, err := NewTEIEmbeddingProvider(s.schema.Fields[2], functionSchema, conf, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
		s.NoError(err)
	}

	// Invalid max batch
	{
		functionSchema.Params = append(functionSchema.Params, &commonpb.KeyValuePair{Key: models.MaxClientBatchSizeParamKey, Value: "Invalid"})
		_, err := NewTEIEmbeddingProvider(s.schema.Fields[2], functionSchema, conf, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{})
		s.Error(err)
	}

	// Valid max batch
	{
		functionSchema.Params[4] = &commonpb.KeyValuePair{Key: models.MaxClientBatchSizeParamKey, Value: "128"}
		pv, err := NewTEIEmbeddingProvider(s.schema.Fields[2], functionSchema, conf, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{BatchFactor: batchFactor})
		s.NoError(err)
		s.True(pv.MaxBatch() == 128*batchFactor)
	}
}

func (s *TEITextEmbeddingProviderSuite) TestCredentialOnlySentToListedEndpoint() {
	var hits atomic.Int32
	var authHeader atomic.Value
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		hits.Add(1)
		authHeader.Store(r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusOK)
		data, _ := json.Marshal([][]float32{{0.1, 0.1, 0.1, 0.1}})
		w.Write(data)
	}))
	defer ts.Close()

	creds := credentials.NewCredentials(map[string]string{"mock.apikey": "mock", "other.apikey": "other"})
	newProvider := func(conf map[string]string, params ...*commonpb.KeyValuePair) (*TeiEmbeddingProvider, error) {
		functionSchema := &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector"},
			Params:           append([]*commonpb.KeyValuePair{{Key: models.EndpointParamKey, Value: ts.URL}}, params...),
		}
		return NewTEIEmbeddingProvider(s.schema.Fields[2], functionSchema, conf, creds, &models.ModelExtraInfo{BatchFactor: 1})
	}
	embed := func(provider *TeiEmbeddingProvider) string {
		_, err := provider.CallEmbedding(context.Background(), []string{"sentence"}, models.InsertMode)
		s.NoError(err)
		return authHeader.Load().(string)
	}

	// a credential named in the function params, endpoint not listed
	_, err := newProvider(map[string]string{}, &commonpb.KeyValuePair{Key: models.CredentialParamKey, Value: "other"})
	s.ErrorContains(err, models.CredentialEndpointsConfKey)
	_, err = newProvider(map[string]string{models.CredentialEndpointsConfKey: "http://tei.internal:8080"}, &commonpb.KeyValuePair{Key: models.CredentialParamKey, Value: "other"})
	s.ErrorContains(err, models.CredentialEndpointsConfKey)
	// the provider credential configured in milvus.yaml, endpoint not listed
	_, err = newProvider(map[string]string{models.CredentialParamKey: "mock"})
	s.ErrorContains(err, models.CredentialEndpointsConfKey)
	s.Equal(int32(0), hits.Load())

	// no credential: the endpoint is called without an Authorization header
	provider, err := newProvider(map[string]string{})
	s.NoError(err)
	s.Equal("", embed(provider))

	// listed endpoint: the credential is attached
	provider, err = newProvider(map[string]string{models.CredentialParamKey: "mock", models.CredentialEndpointsConfKey: ts.URL})
	s.NoError(err)
	s.Equal("Bearer mock", embed(provider))
	provider, err = newProvider(map[string]string{models.CredentialEndpointsConfKey: "http://tei.internal:8080," + ts.URL + "/"}, &commonpb.KeyValuePair{Key: models.CredentialParamKey, Value: "other"})
	s.NoError(err)
	s.Equal("Bearer other", embed(provider))
	s.Equal(int32(3), hits.Load())
}
