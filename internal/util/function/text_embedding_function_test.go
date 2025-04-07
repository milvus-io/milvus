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
	"context"
	"strings"
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v2/util/funcutil"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

func TestTextEmbeddingFunction(t *testing.T) {
	suite.Run(t, new(TextEmbeddingFunctionSuite))
}

type TextEmbeddingFunctionSuite struct {
	suite.Suite
	schema *schemapb.CollectionSchema
}

func (s *TextEmbeddingFunctionSuite) SetupTest() {
	paramtable.Init()
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

func createData(texts []string) []*schemapb.FieldData {
	data := []*schemapb.FieldData{}
	f := schemapb.FieldData{
		Type:      schemapb.DataType_VarChar,
		FieldId:   101,
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

func (s *TextEmbeddingFunctionSuite) TestInvalidProvider() {
	fSchema := &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: Provider, Value: openAIProvider},
			{Key: modelNameParamKey, Value: "text-embedding-ada-002"},
			{Key: dimParamKey, Value: "4"},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: embeddingURLParamKey, Value: "mock"},
		},
	}
	providerName, err := getProvider(fSchema)
	s.Equal(providerName, openAIProvider)
	s.NoError(err)

	fSchema.Params = []*commonpb.KeyValuePair{}
	providerName, err = getProvider(fSchema)
	s.Equal(providerName, "")
	s.Error(err)
}

func (s *TextEmbeddingFunctionSuite) TestProcessInsert() {
	ts := CreateOpenAIEmbeddingServer()
	defer ts.Close()
	{
		runner, err := NewTextEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: openAIProvider},
				{Key: modelNameParamKey, Value: "text-embedding-ada-002"},
				{Key: dimParamKey, Value: "4"},
				{Key: apiKeyParamKey, Value: "mock"},
				{Key: embeddingURLParamKey, Value: ts.URL},
			},
		})
		s.NoError(err)

		{
			data := createData([]string{"sentence"})
			ret, err2 := runner.ProcessInsert(context.Background(), data)
			s.NoError(err2)
			s.Equal(1, len(ret))
			s.Equal(int64(4), ret[0].GetVectors().Dim)
			s.Equal([]float32{0.0, 1.0, 2.0, 3.0}, ret[0].GetVectors().GetFloatVector().Data)
		}
		{
			data := createData([]string{"sentence 1", "sentence 2", "sentence 3"})
			ret, _ := runner.ProcessInsert(context.Background(), data)
			s.Equal([]float32{0.0, 1.0, 2.0, 3.0, 1.0, 2.0, 3.0, 4.0, 2.0, 3.0, 4.0, 5.0}, ret[0].GetVectors().GetFloatVector().Data)
		}
	}
	{
		runner, err := NewTextEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: azureOpenAIProvider},
				{Key: modelNameParamKey, Value: "text-embedding-ada-002"},
				{Key: dimParamKey, Value: "4"},
				{Key: apiKeyParamKey, Value: "mock"},
				{Key: embeddingURLParamKey, Value: ts.URL},
			},
		})
		s.NoError(err)

		{
			data := createData([]string{"sentence"})
			ret, err2 := runner.ProcessInsert(context.Background(), data)
			s.NoError(err2)
			s.Equal(1, len(ret))
			s.Equal(int64(4), ret[0].GetVectors().Dim)
			s.Equal([]float32{0.0, 1.0, 2.0, 3.0}, ret[0].GetVectors().GetFloatVector().Data)
		}
		{
			data := createData([]string{"sentence 1", "sentence 2", "sentence 3"})
			ret, _ := runner.ProcessInsert(context.Background(), data)
			s.Equal([]float32{0.0, 1.0, 2.0, 3.0, 1.0, 2.0, 3.0, 4.0, 2.0, 3.0, 4.0, 5.0}, ret[0].GetVectors().GetFloatVector().Data)
		}
	}
}

func (s *TextEmbeddingFunctionSuite) TestAliEmbedding() {
	ts := CreateAliEmbeddingServer()
	defer ts.Close()

	runner, err := NewTextEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: Provider, Value: aliDashScopeProvider},
			{Key: modelNameParamKey, Value: TestModel},
			{Key: dimParamKey, Value: "4"},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: embeddingURLParamKey, Value: ts.URL},
		},
	})
	s.NoError(err)

	{
		data := createData([]string{"sentence"})
		ret, err2 := runner.ProcessInsert(context.Background(), data)
		s.NoError(err2)
		s.Equal(1, len(ret))
		s.Equal(int64(4), ret[0].GetVectors().Dim)
		s.Equal([]float32{0.0, 1.0, 2.0, 3.0}, ret[0].GetVectors().GetFloatVector().Data)
	}
	{
		data := createData([]string{"sentence 1", "sentence 2", "sentence 3"})
		ret, _ := runner.ProcessInsert(context.Background(), data)
		s.Equal([]float32{0.0, 1.0, 2.0, 3.0, 1.0, 2.0, 3.0, 4.0, 2.0, 3.0, 4.0, 5.0}, ret[0].GetVectors().GetFloatVector().Data)
	}

	// multi-input
	{
		data := []*schemapb.FieldData{}
		f := schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldId:   101,
			IsDynamic: false,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: []string{},
						},
					},
				},
			},
		}
		data = append(data, &f)
		data = append(data, &f)
		_, err := runner.ProcessInsert(context.Background(), data)
		s.Error(err)
	}

	// wrong input data type
	{
		data := []*schemapb.FieldData{}
		f := schemapb.FieldData{
			Type:      schemapb.DataType_Int32,
			FieldId:   101,
			IsDynamic: false,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{},
			},
		}
		data = append(data, &f)
		_, err := runner.ProcessInsert(context.Background(), data)
		s.Error(err)
	}
	// empty input
	{
		data := []*schemapb.FieldData{}
		f := schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldId:   101,
			IsDynamic: false,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{},
			},
		}
		data = append(data, &f)
		_, err := runner.ProcessInsert(context.Background(), data)
		s.Error(err)
	}
	// large input data
	{
		data := []*schemapb.FieldData{}
		f := schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldId:   101,
			IsDynamic: false,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: strings.Split(strings.Repeat("Element,", 1000), ",")[:999],
						},
					},
				},
			},
		}
		data = append(data, &f)
		_, err := runner.ProcessInsert(context.Background(), data)
		s.Error(err)
	}

	// empty string
	{
		data := []*schemapb.FieldData{}
		f := schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldId:   101,
			IsDynamic: false,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: strings.Split(strings.Repeat("Element,", 10), ","),
						},
					},
				},
			},
		}
		data = append(data, &f)
		_, err := runner.ProcessInsert(context.Background(), data)
		s.Error(err)
	}
}

func (s *TextEmbeddingFunctionSuite) TestRunnerParamsErr() {
	// outputfield datatype mismatch
	{
		schema := &schemapb.CollectionSchema{
			Name: "test",
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
				{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
				{
					FieldID: 102, Name: "vector", DataType: schemapb.DataType_BFloat16Vector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "4"},
					},
				},
			},
		}

		_, err := NewTextEmbeddingFunction(schema, &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: openAIProvider},
				{Key: modelNameParamKey, Value: "text-embedding-ada-002"},
				{Key: dimParamKey, Value: "4"},
				{Key: apiKeyParamKey, Value: "mock"},
				{Key: embeddingURLParamKey, Value: "mock"},
			},
		})
		s.Error(err)
	}

	// outputfield number mismatc
	{
		schema := &schemapb.CollectionSchema{
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
				{
					FieldID: 103, Name: "vector2", DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: "dim", Value: "4"},
					},
				},
			},
		}
		_, err := NewTextEmbeddingFunction(schema, &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector", "vector2"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{102, 103},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: openAIProvider},
				{Key: modelNameParamKey, Value: "text-embedding-ada-002"},
				{Key: dimParamKey, Value: "4"},
				{Key: apiKeyParamKey, Value: "mock"},
				{Key: embeddingURLParamKey, Value: "mock"},
			},
		})
		s.Error(err)
	}

	// outputfield miss
	{
		_, err := NewTextEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector2"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{103},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: openAIProvider},
				{Key: modelNameParamKey, Value: "text-embedding-ada-002"},
				{Key: dimParamKey, Value: "4"},
				{Key: apiKeyParamKey, Value: "mock"},
				{Key: embeddingURLParamKey, Value: "mock"},
			},
		})
		s.Error(err)
	}

	// no openai api  key
	{
		_, err := NewTextEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: openAIProvider},
				{Key: modelNameParamKey, Value: "text-embedding-ada-003"},
			},
		})
		s.Error(err)
	}
}

func (s *TextEmbeddingFunctionSuite) TestNewTextEmbeddings() {
	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: bedrockProvider},
				{Key: modelNameParamKey, Value: TestModel},
				{Key: awsAKIdParamKey, Value: "mock"},
				{Key: awsSAKParamKey, Value: "mock"},
				{Key: regionParamKey, Value: "mock"},
			},
		}

		_, err := NewTextEmbeddingFunction(s.schema, fSchema)
		s.NoError(err)
		fSchema.Params = []*commonpb.KeyValuePair{}
		_, err = NewTextEmbeddingFunction(s.schema, fSchema)
		s.Error(err)
	}

	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: aliDashScopeProvider},
				{Key: modelNameParamKey, Value: TestModel},
				{Key: apiKeyParamKey, Value: "mock"},
			},
		}

		_, err := NewTextEmbeddingFunction(s.schema, fSchema)
		s.NoError(err)
		fSchema.Params = []*commonpb.KeyValuePair{}
		_, err = NewTextEmbeddingFunction(s.schema, fSchema)
		s.Error(err)
	}

	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: voyageAIProvider},
				{Key: modelNameParamKey, Value: TestModel},
				{Key: apiKeyParamKey, Value: "mock"},
			},
		}

		_, err := NewTextEmbeddingFunction(s.schema, fSchema)
		s.NoError(err)
		fSchema.Params = []*commonpb.KeyValuePair{}
		_, err = NewTextEmbeddingFunction(s.schema, fSchema)
		s.Error(err)
	}

	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: siliconflowProvider},
				{Key: modelNameParamKey, Value: TestModel},
				{Key: apiKeyParamKey, Value: "mock"},
			},
		}

		_, err := NewTextEmbeddingFunction(s.schema, fSchema)
		s.NoError(err)
		fSchema.Params = []*commonpb.KeyValuePair{}
		_, err = NewTextEmbeddingFunction(s.schema, fSchema)
		s.Error(err)
	}

	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: cohereProvider},
				{Key: modelNameParamKey, Value: TestModel},
				{Key: apiKeyParamKey, Value: "mock"},
			},
		}

		_, err := NewTextEmbeddingFunction(s.schema, fSchema)
		s.NoError(err)
		fSchema.Params = []*commonpb.KeyValuePair{}
		_, err = NewTextEmbeddingFunction(s.schema, fSchema)
		s.Error(err)
	}

	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: "tei"},
				{Key: "endpoint", Value: "http://mock.com"},
			},
		}
		_, err := NewTextEmbeddingFunction(s.schema, fSchema)
		s.NoError(err)
		fSchema.Params = []*commonpb.KeyValuePair{}
		_, err = NewTextEmbeddingFunction(s.schema, fSchema)
		s.Error(err)
	}

	// Invalid params
	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{102},
			Params:           []*commonpb.KeyValuePair{},
		}

		_, err := NewTextEmbeddingFunction(s.schema, fSchema)
		s.Error(err)
	}

	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"vector"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{102},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: "unkownProvider"},
			},
		}

		_, err := NewTextEmbeddingFunction(s.schema, fSchema)
		s.Error(err)
	}

	// Invalid output field
	{
		fSchema := &schemapb.FunctionSchema{
			Name:             "test",
			Type:             schemapb.FunctionType_TextEmbedding,
			InputFieldNames:  []string{"text"},
			OutputFieldNames: []string{"int64"},
			InputFieldIds:    []int64{101},
			OutputFieldIds:   []int64{100},
			Params: []*commonpb.KeyValuePair{
				{Key: Provider, Value: "tei"},
			},
		}
		_, err := NewTextEmbeddingFunction(s.schema, fSchema)
		s.Error(err)
	}
}

func (s *TextEmbeddingFunctionSuite) TestProcessSearchFloat32() {
	ts := CreateOpenAIEmbeddingServer()
	defer ts.Close()
	runner, err := NewTextEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: Provider, Value: openAIProvider},
			{Key: modelNameParamKey, Value: "text-embedding-ada-002"},
			{Key: dimParamKey, Value: "4"},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: embeddingURLParamKey, Value: ts.URL},
		},
	})
	s.NoError(err)

	// Large inputs
	{
		f := &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldId:   101,
			IsDynamic: false,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: strings.Split(strings.Repeat("Element,", 1000), ",")[0:999],
						},
					},
				},
			},
		}

		placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(f)
		s.NoError(err)
		placeholderGroup := commonpb.PlaceholderGroup{}
		proto.Unmarshal(placeholderGroupBytes, &placeholderGroup)
		_, err = runner.ProcessSearch(context.Background(), &placeholderGroup)
		s.Error(err)
	}

	// Normal inputs
	{
		f := &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldId:   101,
			IsDynamic: false,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: strings.Split(strings.Repeat("Element,", 100), ",")[:99],
						},
					},
				},
			},
		}

		placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(f)
		s.NoError(err)
		placeholderGroup := commonpb.PlaceholderGroup{}
		proto.Unmarshal(placeholderGroupBytes, &placeholderGroup)
		_, err = runner.ProcessSearch(context.Background(), &placeholderGroup)
		s.NoError(err)
	}
}

func (s *TextEmbeddingFunctionSuite) TestProcessInsertInt8() {
	ts := CreateCohereEmbeddingServer[int8]()
	defer ts.Close()

	s.schema = &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_Int8Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
		},
	}
	runner, err := NewTextEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: Provider, Value: cohereProvider},
			{Key: modelNameParamKey, Value: TestModel},
			{Key: dimParamKey, Value: "4"},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: embeddingURLParamKey, Value: ts.URL},
		},
	})
	s.NoError(err)

	{
		data := createData([]string{"sentence"})
		ret, err2 := runner.ProcessInsert(context.Background(), data)
		s.NoError(err2)
		s.Equal(1, len(ret))
		s.Equal(int64(4), ret[0].GetVectors().Dim)
		int8Bytes := ret[0].GetVectors().GetInt8Vector()
		int8Vec := make([]int8, 0, len(int8Bytes))
		for _, item := range int8Bytes {
			int8Vec = append(int8Vec, int8(item))
		}

		s.Equal([]int8{0, 1, 2, 3}, int8Vec)
	}
}

func (s *TextEmbeddingFunctionSuite) TestUnsupportedVec() {
	s.schema = &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_BFloat16Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
		},
	}
	_, err := NewTextEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: Provider, Value: cohereProvider},
			{Key: modelNameParamKey, Value: TestModel},
			{Key: dimParamKey, Value: "4"},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: embeddingURLParamKey, Value: "mock"},
		},
	})
	s.Error(err)
}

func (s *TextEmbeddingFunctionSuite) TestProcessSearchInt8() {
	ts := CreateCohereEmbeddingServer[int8]()
	defer ts.Close()

	s.schema = &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_Int8Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
		},
	}
	runner, err := NewTextEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: Provider, Value: cohereProvider},
			{Key: modelNameParamKey, Value: TestModel},
			{Key: dimParamKey, Value: "4"},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: embeddingURLParamKey, Value: ts.URL},
		},
	})
	s.NoError(err)

	// Normal inputs
	{
		f := &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldId:   101,
			IsDynamic: false,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: strings.Split(strings.Repeat("Element,", 100), ",")[:99],
						},
					},
				},
			},
		}

		placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(f)
		s.NoError(err)
		placeholderGroup := commonpb.PlaceholderGroup{}
		proto.Unmarshal(placeholderGroupBytes, &placeholderGroup)
		_, err = runner.ProcessSearch(context.Background(), &placeholderGroup)
		s.NoError(err)
	}

	// empty text
	{
		f := &schemapb.FieldData{
			Type:      schemapb.DataType_VarChar,
			FieldId:   101,
			IsDynamic: false,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_StringData{
						StringData: &schemapb.StringArray{
							Data: strings.Split(strings.Repeat("Element,", 100), ","),
						},
					},
				},
			},
		}

		placeholderGroupBytes, err := funcutil.FieldDataToPlaceholderGroupBytes(f)
		s.NoError(err)
		placeholderGroup := commonpb.PlaceholderGroup{}
		proto.Unmarshal(placeholderGroupBytes, &placeholderGroup)
		_, err = runner.ProcessSearch(context.Background(), &placeholderGroup)
		s.Error(err)
	}
}

func (s *TextEmbeddingFunctionSuite) TestProcessBulkInsertFloat32() {
	ts := CreateOpenAIEmbeddingServer()
	defer ts.Close()
	runner, err := NewTextEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: Provider, Value: openAIProvider},
			{Key: modelNameParamKey, Value: "text-embedding-ada-002"},
			{Key: dimParamKey, Value: "4"},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: embeddingURLParamKey, Value: ts.URL},
		},
	})
	s.NoError(err)

	data, err := testutil.CreateInsertData(s.schema, 100)
	s.NoError(err)
	{
		input := []storage.FieldData{data.Data[101]}
		_, err := runner.ProcessBulkInsert(input)
		s.NoError(err)
	}

	// Multi-input
	{
		input := []storage.FieldData{data.Data[101], data.Data[101]}
		_, err := runner.ProcessBulkInsert(input)
		s.Error(err)
	}

	// Error input type
	{
		input := []storage.FieldData{data.Data[102]}
		_, err := runner.ProcessBulkInsert(input)
		s.Error(err)
	}

	// empty texts
	{
		input := []storage.FieldData{data.Data[101]}
		err := input[0].AppendRow("")
		s.NoError(err)
		_, err = runner.ProcessBulkInsert(input)
		s.Error(err)
	}
}

func (s *TextEmbeddingFunctionSuite) TestProcessBulkInsertInt8() {
	ts := CreateCohereEmbeddingServer[int8]()
	defer ts.Close()
	s.schema = &schemapb.CollectionSchema{
		Name: "test",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "int64", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{
				FieldID: 102, Name: "vector", DataType: schemapb.DataType_Int8Vector,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
		},
	}
	runner, err := NewTextEmbeddingFunction(s.schema, &schemapb.FunctionSchema{
		Name:             "test",
		Type:             schemapb.FunctionType_TextEmbedding,
		InputFieldNames:  []string{"text"},
		OutputFieldNames: []string{"vector"},
		InputFieldIds:    []int64{101},
		OutputFieldIds:   []int64{102},
		Params: []*commonpb.KeyValuePair{
			{Key: Provider, Value: cohereProvider},
			{Key: modelNameParamKey, Value: TestModel},
			{Key: dimParamKey, Value: "4"},
			{Key: apiKeyParamKey, Value: "mock"},
			{Key: embeddingURLParamKey, Value: ts.URL},
		},
	})
	s.NoError(err)

	data, err := testutil.CreateInsertData(s.schema, 100)
	s.NoError(err)
	{
		input := []storage.FieldData{data.Data[101]}
		_, err := runner.ProcessBulkInsert(input)
		s.NoError(err)
	}
}
