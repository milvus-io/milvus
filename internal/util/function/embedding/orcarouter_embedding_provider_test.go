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
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/credentials"
	"github.com/milvus-io/milvus/internal/util/function/models"
)

func TestOrcaRouterTextEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(OrcaRouterTextEmbeddingProviderSuite))
}

type OrcaRouterTextEmbeddingProviderSuite struct {
	suite.Suite
	schema *schemapb.CollectionSchema
}

func (s *OrcaRouterTextEmbeddingProviderSuite) SetupTest() {
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

func (s *OrcaRouterTextEmbeddingProviderSuite) TestEmbedding() {
	ts := CreateOpenAIEmbeddingServer()
	defer ts.Close()

	provider, err := NewOrcaRouterEmbeddingProvider(s.schema.Fields[2], &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "text-embedding-3-small"},
			{Key: models.DimParamKey, Value: "4"},
			{Key: models.CredentialParamKey, Value: "mock"},
		},
	}, map[string]string{models.URLParamKey: ts.URL}, credentials.NewCredentials(map[string]string{"mock.apikey": "mock"}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
	s.NoError(err)

	{
		data := []string{"sentence"}
		r, err := provider.CallEmbedding(context.Background(), data, models.InsertMode)
		s.NoError(err)
		ret := r.([][]float32)
		s.Equal(1, len(ret))
		s.Equal(4, len(ret[0]))
		s.Equal([]float32{0.0, 1.0, 2.0, 3.0}, ret[0])
	}
	{
		data := []string{"sentence 1", "sentence 2", "sentence 3"}
		r, err := provider.CallEmbedding(context.Background(), data, models.SearchMode)
		s.NoError(err)
		ret := r.([][]float32)
		s.Equal([][]float32{{0.0, 1.0, 2.0, 3.0}, {1.0, 2.0, 3.0, 4.0}, {2.0, 3.0, 4.0, 5.0}}, ret)
	}
}

func (s *OrcaRouterTextEmbeddingProviderSuite) TestMissingCredential() {
	_, err := NewOrcaRouterEmbeddingProvider(s.schema.Fields[2], &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: models.ModelNameParamKey, Value: "text-embedding-3-small"},
			{Key: models.DimParamKey, Value: "4"},
		},
	}, map[string]string{}, credentials.NewCredentials(map[string]string{}), &models.ModelExtraInfo{ClusterID: "test-cluster", DBName: "test-db"})
	s.ErrorContains(err, "missing credentials config or configure the MILVUS_ORCAROUTER_API_KEY")
}
