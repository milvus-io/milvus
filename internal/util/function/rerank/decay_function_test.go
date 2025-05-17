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
	"testing"

	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/util/testutils"
)

func TestDecayFunction(t *testing.T) {
	suite.Run(t, new(DecayFunctionSuite))
}

type DecayFunctionSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func (s *DecayFunctionSuite) TestNewDecayErrors() {
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
		Name:             "test",
		Type:             schemapb.FunctionType_Rerank,
		InputFieldNames:  []string{"ts"},
		OutputFieldNames: []string{"text"},
		Params: []*commonpb.KeyValuePair{
			{Key: originKey, Value: "4"},
			{Key: scaleKey, Value: "4"},
			{Key: offsetKey, Value: "4"},
			{Key: decayKey, Value: "0.5"},
			{Key: functionKey, Value: "gauss"},
		},
	}

	{
		_, err := newDecayFunction(schema, functionSchema)
		s.ErrorContains(err, "Rerank function output field names should be empty")
	}

	{
		functionSchema.OutputFieldNames = []string{}
		functionSchema.InputFieldNames = []string{""}
		_, err := newDecayFunction(schema, functionSchema)
		s.ErrorContains(err, "Rerank input field name cannot be empty string")
	}

	{
		functionSchema.InputFieldNames = []string{"ts", "ts"}
		_, err := newDecayFunction(schema, functionSchema)
		s.ErrorContains(err, "Each function input field should be used exactly once in the same function")
	}

	{
		functionSchema.InputFieldNames = []string{"ts", "pk"}
		_, err := newDecayFunction(schema, functionSchema)
		s.ErrorContains(err, "Decay function only supoorts single input, but gets")
	}

	{
		functionSchema.InputFieldNames = []string{"notExists"}
		_, err := newDecayFunction(schema, functionSchema)
		s.ErrorContains(err, "Function input field not found:")
	}

	{
		functionSchema.InputFieldNames = []string{"vector"}
		_, err := newDecayFunction(schema, functionSchema)
		s.ErrorContains(err, "Decay rerank: unsupported input field type")
	}

	{
		functionSchema.InputFieldNames = []string{"ts"}
		_, err := newDecayFunction(schema, functionSchema)
		s.NoError(err)
	}

	{
		for i := 0; i < 4; i++ {
			functionSchema.Params[i].Value = "NotNum"
			_, err := newDecayFunction(schema, functionSchema)
			s.ErrorContains(err, "is not a number")
			functionSchema.Params[i].Value = "0.9"
		}
	}

	{
		fs := []string{gaussFunction, linearFunction, expFunction}
		for i := 0; i < 3; i++ {
			functionSchema.Params[4].Value = fs[i]
			_, err := newDecayFunction(schema, functionSchema)
			s.NoError(err)
		}
		functionSchema.Params[4].Value = "NotExist"
		_, err := newDecayFunction(schema, functionSchema)
		s.ErrorContains(err, "Invaild decay function:")
		functionSchema.Params[4].Value = "exp"
	}

	{
		newSchema := proto.Clone(schema).(*schemapb.CollectionSchema)
		newSchema.Fields[0].IsPrimaryKey = false
		_, err := newDecayFunction(newSchema, functionSchema)
		s.ErrorContains(err, " can not found pk field")
	}
}

func (s *DecayFunctionSuite) TestAllTypesInput() {
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
		InputFieldNames: []string{"ts"},
		Params: []*commonpb.KeyValuePair{
			{Key: functionKey, Value: "gauss"},
			{Key: originKey, Value: "4"},
			{Key: scaleKey, Value: "4"},
			{Key: offsetKey, Value: "4"},
			{Key: decayKey, Value: "0.5"},
		},
	}
	inputTypes := []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_Int32, schemapb.DataType_Int16, schemapb.DataType_Int8, schemapb.DataType_Float, schemapb.DataType_Double, schemapb.DataType_Bool}
	for i, inputType := range inputTypes {
		schema.Fields[3].DataType = inputType
		_, err := newDecayFunction(schema, functionSchema)
		if i < len(inputTypes)-1 {
			s.NoError(err)
		} else {
			s.ErrorContains(err, "Decay rerank: unsupported input field type")
		}
	}

	schema.Fields[0].DataType = schemapb.DataType_String
	for i, inputType := range inputTypes {
		schema.Fields[3].DataType = inputType
		_, err := newDecayFunction(schema, functionSchema)
		if i < len(inputTypes)-1 {
			s.NoError(err)
		} else {
			s.ErrorContains(err, "Decay rerank: unsupported input field type")
		}
	}

	schema.Fields[3].DataType = schemapb.DataType_Double

	{
		functionSchema.Params[1].Key = "N"
		_, err := newDecayFunction(schema, functionSchema)
		s.ErrorContains(err, "Decay function lost param: origin")
		functionSchema.Params[1].Key = originKey
	}
	{
		functionSchema.Params[2].Key = "N"
		_, err := newDecayFunction(schema, functionSchema)
		s.ErrorContains(err, "Decay function lost param: scale")
		functionSchema.Params[2].Key = scaleKey
	}

	{
		functionSchema.Params[2].Value = "-1"
		_, err := newDecayFunction(schema, functionSchema)
		s.ErrorContains(err, "Decay function param: scale must > 0,")
		functionSchema.Params[2].Value = "0.5"
	}

	{
		functionSchema.Params[3].Value = "-1"
		_, err := newDecayFunction(schema, functionSchema)
		s.ErrorContains(err, "Decay function param: offset must >= 0")
		functionSchema.Params[3].Value = "0.5"
	}

	{
		functionSchema.Params[4].Value = "10"
		_, err := newDecayFunction(schema, functionSchema)
		s.ErrorContains(err, "Decay function param: decay must 0 < decay < 1")
		functionSchema.Params[2].Value = "0.5"
	}
}

func (s *DecayFunctionSuite) TestRerankProcess() {
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
		InputFieldNames: []string{"ts"},
		Params: []*commonpb.KeyValuePair{
			{Key: functionKey, Value: "gauss"},
			{Key: originKey, Value: "0"},
			{Key: scaleKey, Value: "4"},
			{Key: offsetKey, Value: "2"},
		},
	}

	// empty
	{
		nq := int64(1)
		f, err := newDecayFunction(schema, functionSchema)
		s.NoError(err)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{}, f.GetInputFieldIDs())
		ret, err := f.Process(context.Background(), &SearchParams{nq, 3, 2, -1, -1, 1, false, []string{"COSINE"}}, inputs)
		s.NoError(err)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{}, ret.searchResultData.Topks)
	}

	// no input field exist
	{
		nq := int64(1)
		f, err := newDecayFunction(schema, functionSchema)
		data := genSearchResultData(nq, 10, schemapb.DataType_Int64, "noExist", 1000)
		s.NoError(err)

		_, err = newRerankInputs([]*schemapb.SearchResultData{data}, f.GetInputFieldIDs())
		s.ErrorContains(err, "Search reaults mismatch rerank inputs")
	}

	// singleSearchResultData
	// nq = 1
	{
		// source scores: [0 1 2 3 4 5 6 7 8 9]
		// decay scores:  [1 1 1 0.9576033 0.8408964 0.6771278 0.5 0.3385639 0.2102241 0.11970041]
		// Final scores:  [0, 1, 2, 2.87281, 3.3635857, 3.385639, 3, 2.3699472, 1.6817929, 1.0773036]
		// top ids     :  [5, 4, 6, 3, 7, 2, 8, 9, 1, 0]
		// top3, offset 2: [6, 3, 7]
		nq := int64(1)
		f, err := newDecayFunction(schema, functionSchema)
		s.NoError(err)
		data := genSearchResultData(nq, 10, schemapb.DataType_Int64, "ts", 102)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data}, f.GetInputFieldIDs())
		ret, err := f.Process(context.Background(), &SearchParams{nq, 3, 2, -1, -1, 1, false, []string{"COSINE"}}, inputs)
		s.NoError(err)
		s.Equal([]int64{3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{6, 3, 7}, ret.searchResultData.Ids.GetIntId().Data)
	}
	// // nq = 3
	{
		nq := int64(3)
		f, err := newDecayFunction(schema, functionSchema)
		s.NoError(err)
		data := genSearchResultData(nq, 10, schemapb.DataType_Int64, "ts", 102)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data}, f.GetInputFieldIDs())
		ret, err := f.Process(context.Background(), &SearchParams{nq, 3, 2, -1, -1, 1, false, []string{"COSINE"}}, inputs)
		s.NoError(err)
		s.Equal([]int64{3, 3, 3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{6, 3, 7, 12, 13, 14, 22, 23, 24}, ret.searchResultData.Ids.GetIntId().Data)
	}

	// // multipSearchResultData
	functionSchema2 := &schemapb.FunctionSchema{
		Name:            "test",
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"ts"},
		Params: []*commonpb.KeyValuePair{
			{Key: functionKey, Value: "gauss"},
			{Key: originKey, Value: "5"},
			{Key: scaleKey, Value: "4"},
			{Key: offsetKey, Value: "2"},
		},
	}
	// has empty inputs
	{
		nq := int64(1)
		f, err := newDecayFunction(schema, functionSchema2)
		s.NoError(err)
		// ts/id data: 0 - 9
		data1 := genSearchResultData(nq, 10, schemapb.DataType_Int64, "ts", 102)
		// empty
		data2 := genSearchResultData(nq, 0, schemapb.DataType_Int64, "ts", 102)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data1, data2}, f.GetInputFieldIDs())
		ret, err := f.Process(context.Background(), &SearchParams{nq, 3, 2, -1, -1, 1, false, []string{"COSINE", "COSINE"}}, inputs)
		s.NoError(err)
		s.Equal([]int64{3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{7, 6, 5}, ret.searchResultData.Ids.GetIntId().Data)
	}
	// nq = 1
	{
		nq := int64(1)
		f, err := newDecayFunction(schema, functionSchema2)
		s.NoError(err)
		// ts/id data: 0 - 9
		data1 := genSearchResultData(nq, 10, schemapb.DataType_Int64, "ts", 102)
		// ts/id data: 0 - 3
		data2 := genSearchResultData(nq, 4, schemapb.DataType_Int64, "ts", 102)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data1, data2}, f.GetInputFieldIDs())
		ret, err := f.Process(context.Background(), &SearchParams{nq, 3, 2, -1, -1, 1, false, []string{"COSINE", "COSINE"}}, inputs)
		s.NoError(err)
		s.Equal([]int64{3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{7, 6, 5}, ret.searchResultData.Ids.GetIntId().Data)
	}
	// // nq = 3
	{
		nq := int64(3)
		f, err := newDecayFunction(schema, functionSchema2)
		s.NoError(err)
		// nq1 ts/id data: 0 - 9
		// nq2 ts/id data: 10 - 19
		// nq3 ts/id data: 20 - 29
		data1 := genSearchResultData(nq, 10, schemapb.DataType_Int64, "ts", 102)
		// nq1 ts/id data: 0 - 3
		// nq2 ts/id data: 4 - 7
		// nq3 ts/id data: 8 - 11
		data2 := genSearchResultData(nq, 4, schemapb.DataType_Int64, "ts", 102)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data1, data2}, f.GetInputFieldIDs())
		ret, err := f.Process(context.Background(), &SearchParams{nq, 3, 2, 1, -1, 1, false, []string{"COSINE", "COSINE"}}, inputs)
		s.NoError(err)
		s.Equal([]int64{3, 3, 3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{7, 6, 5, 6, 11, 5, 10, 11, 20}, ret.searchResultData.Ids.GetIntId().Data)
	}
}

func (s *DecayFunctionSuite) TestDecay() {
	s.Equal(gaussianDecay(0, 1, 0.5, 5, 4), 1.0)
	s.Equal(gaussianDecay(0, 1, 0.5, 5, 5), 1.0)
	s.Less(gaussianDecay(0, 1, 0.5, 5, 6), 1.0)

	s.Equal(expDecay(0, 1, 0.5, 5, 4), 1.0)
	s.Equal(expDecay(0, 1, 0.5, 5, 5), 1.0)
	s.Less(expDecay(0, 1, 0.5, 5, 6), 1.0)

	s.Equal(linearDecay(0, 1, 0.5, 5, 4), 1.0)
	s.Equal(linearDecay(0, 1, 0.5, 5, 5), 1.0)
	s.Less(linearDecay(0, 1, 0.5, 5, 6), 1.0)
}

func genSearchResultData(nq int64, topk int64, dType schemapb.DataType, fieldName string, fieldId int64) *schemapb.SearchResultData {
	tops := make([]int64, nq)
	for i := 0; i < int(nq); i++ {
		tops[i] = topk
	}
	data := &schemapb.SearchResultData{
		NumQueries: nq,
		TopK:       topk,
		Scores:     testutils.GenerateFloat32Array(int(nq * topk)),
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: testutils.GenerateInt64Array(int(nq * topk)),
				},
			},
		},
		Topks:      tops,
		FieldsData: []*schemapb.FieldData{testutils.GenerateScalarFieldData(dType, fieldName, int(nq*topk))},
	}
	data.FieldsData[0].FieldId = fieldId
	return data
}
