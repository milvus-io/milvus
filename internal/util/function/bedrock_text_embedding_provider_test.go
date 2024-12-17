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
	"fmt"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

func TestBedrockTextEmbeddingProvider(t *testing.T) {
	suite.Run(t, new(BedrockTextEmbeddingProviderSuite))
}

type BedrockTextEmbeddingProviderSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func (s *BedrockTextEmbeddingProviderSuite) SetupTest() {
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
	s.providers = []string{BedrockProvider}
}

func createBedrockProvider(schema *schemapb.FieldSchema, providerName string, dim int) (TextEmbeddingProvider, error) {
	functionSchema := &schemapb.FunctionSchema{
		Name:           "test",
		Type:           schemapb.FunctionType_Unknown,
		InputFieldIds:  []int64{101},
		OutputFieldIds: []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: modelNameParamKey, Value: BedRockTitanTextEmbeddingsV2},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: dimParamKey, Value: "4"},
		},
	}
	switch providerName {
	case BedrockProvider:
		return NewBedrockEmbeddingProvider(schema, functionSchema, &MockBedrockClient{dim: dim})
	default:
		return nil, fmt.Errorf("Unknow provider")
	}
}

func (s *BedrockTextEmbeddingProviderSuite) TestEmbedding() {
	for _, provderName := range s.providers {
		provder, err := createBedrockProvider(s.schema.Fields[2], provderName, 4)
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
			s.Equal([][]float32{{0.0, 0.1, 0.2, 0.3}, {0.0, 0.1, 0.2, 0.3}, {0.0, 0.1, 0.2, 0.3}}, ret)
		}

	}
}

func (s *BedrockTextEmbeddingProviderSuite) TestEmbeddingDimNotMatch() {
	for _, provderName := range s.providers {
		provder, err := createBedrockProvider(s.schema.Fields[2], provderName, 2)
		s.NoError(err)

		// embedding dim not match
		data := []string{"sentence", "sentence"}
		_, err2 := provder.CallEmbedding(data, false, InsertMode)
		s.Error(err2)

	}
}
