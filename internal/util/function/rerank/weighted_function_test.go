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

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/embedding"
)

func TestWeightedFunction(t *testing.T) {
	suite.Run(t, new(WeightedFunctionSuite))
}

type WeightedFunctionSuite struct {
	suite.Suite
}

func (s *WeightedFunctionSuite) TestNewWeightedFuction() {
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
		InputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: WeightsParamsKey, Value: `[0.1, 0.9]`},
		},
	}

	{
		_, err := newWeightedFunction(schema, functionSchema)
		s.NoError(err)
	}
	{
		schema.Fields[0] = &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true}
		_, err := newWeightedFunction(schema, functionSchema)
		s.NoError(err)
	}
	{
		functionSchema.Params[0] = &commonpb.KeyValuePair{Key: WeightsParamsKey, Value: "NotNum"}
		_, err := newWeightedFunction(schema, functionSchema)
		s.ErrorContains(err, "param failed, weight should be []float")
	}
	{
		functionSchema.Params[0] = &commonpb.KeyValuePair{Key: WeightsParamsKey, Value: `[10]`}
		_, err := newWeightedFunction(schema, functionSchema)
		s.ErrorContains(err, "rank param weight should be in range [0, 1]")
	}
	{
		functionSchema.Params[0] = &commonpb.KeyValuePair{Key: "NotExist", Value: `[10]`}
		_, err := newWeightedFunction(schema, functionSchema)
		s.ErrorContains(err, "not found")
		functionSchema.Params[0] = &commonpb.KeyValuePair{Key: WeightsParamsKey, Value: `[0.1, 0.9]`}
	}
	{
		functionSchema.InputFieldNames = []string{"ts"}
		_, err := newWeightedFunction(schema, functionSchema)
		s.ErrorContains(err, "The weighted function does not support input parameters,")
	}
}

func (s *WeightedFunctionSuite) TestWeightedFuctionProcess() {
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
		InputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: WeightsParamsKey, Value: `[0.1]`},
		},
	}

	// empty
	{
		nq := int64(1)
		f, err := newWeightedFunction(schema, functionSchema)
		s.NoError(err)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{}, f.GetInputFieldIDs(), false)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE"}), inputs)
		s.NoError(err)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{}, ret.searchResultData.Topks)
	}

	// singleSearchResultData
	// nq = 1
	{
		nq := int64(1)
		f, err := newWeightedFunction(schema, functionSchema)
		s.NoError(err)
		data := embedding.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "", 0)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data}, f.GetInputFieldIDs(), false)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{7, 6, 5}, ret.searchResultData.Ids.GetIntId().Data)
	}
	// nq = 3
	{
		nq := int64(3)
		f, err := newWeightedFunction(schema, functionSchema)
		s.NoError(err)
		data := embedding.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "", 0)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data}, f.GetInputFieldIDs(), false)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 0, -1, -1, 1, false, "", []string{"COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{3, 3, 3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{9, 8, 7, 19, 18, 17, 29, 28, 27}, ret.searchResultData.Ids.GetIntId().Data)
	}

	// number of weigts not equal to search data
	functionSchema.Params[0] = &commonpb.KeyValuePair{Key: WeightsParamsKey, Value: `[0.1, 0.9]`}
	{
		nq := int64(1)
		f, err := newWeightedFunction(schema, functionSchema)
		s.NoError(err)
		data := embedding.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "", 0)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data}, f.GetInputFieldIDs(), false)
		_, err = f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE", "COSINE"}), inputs)
		s.ErrorContains(err, "the length of weights param mismatch with ann search requests")
	}
	// has empty inputs
	{
		nq := int64(1)
		f, err := newWeightedFunction(schema, functionSchema)
		s.NoError(err)
		// id data: 0 - 9
		data1 := embedding.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "", 0)
		// empty
		data2 := embedding.GenSearchResultData(nq, 0, schemapb.DataType_Int64, "", 0)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data1, data2}, f.GetInputFieldIDs(), false)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 0, -1, -1, 1, false, "", []string{"COSINE", "COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{9, 8, 7}, ret.searchResultData.Ids.GetIntId().Data)
		s.True(embedding.FloatsAlmostEqual([]float32{0.9, 0.8, 0.7}, ret.searchResultData.Scores, 0.001))
	}
	// nq = 1
	{
		nq := int64(1)
		f, err := newWeightedFunction(schema, functionSchema)
		s.NoError(err)
		// id data: 0 - 9
		data1 := embedding.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "", 0)
		// id data: 0 - 3
		data2 := embedding.GenSearchResultData(nq, 4, schemapb.DataType_Int64, "", 0)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data1, data2}, f.GetInputFieldIDs(), false)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE", "COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{1, 9, 8}, ret.searchResultData.Ids.GetIntId().Data)
		s.True(embedding.FloatsAlmostEqual([]float32{1, 0.9, 0.8}, ret.searchResultData.Scores, 0.001))
	}
	// // nq = 3
	{
		nq := int64(3)
		f, err := newWeightedFunction(schema, functionSchema)
		s.NoError(err)
		// nq1 id data: 0 - 9
		// nq2 id data: 10 - 19
		// nq3 id data: 20 - 29
		data1 := embedding.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "", 0)
		// nq1 id data: 0 - 3
		// nq2 id data: 4 - 7
		// nq3 id data: 8 - 11
		data2 := embedding.GenSearchResultData(nq, 4, schemapb.DataType_Int64, "", 0)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data1, data2}, f.GetInputFieldIDs(), false)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 0, 1, -1, 1, false, "", []string{"COSINE", "COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{3, 3, 3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{3, 2, 1, 7, 6, 5, 11, 10, 9}, ret.searchResultData.Ids.GetIntId().Data)
		s.True(embedding.FloatsAlmostEqual([]float32{3, 2, 1, 6.3, 5.4, 4.5, 9.9, 9, 8.1}, ret.searchResultData.Scores, 0.001))
	}
	// // nq = 3， grouping = true, grouping size = 1
	{
		nq := int64(3)
		f, err := newWeightedFunction(schema, functionSchema)
		s.NoError(err)
		// nq1 id data: 0 - 9
		// nq2 id data: 10 - 19
		// nq3 id data: 20 - 29
		data1 := embedding.GenSearchResultDataWithGrouping(nq, 10, schemapb.DataType_Int64, "", 0, "ts", 102, 1)
		// nq1 id data: 0 - 3
		// nq2 id data: 4 - 7
		// nq3 id data: 8 - 11
		data2 := embedding.GenSearchResultDataWithGrouping(nq, 4, schemapb.DataType_Int64, "", 0, "ts", 102, 1)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data1, data2}, f.GetInputFieldIDs(), true)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 0, 1, 102, 1, true, "", []string{"COSINE", "COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{3, 3, 3}, ret.searchResultData.Topks)
		s.Equal([]int64{3, 2, 1, 7, 6, 5, 11, 10, 9}, ret.searchResultData.Ids.GetIntId().Data)
		s.True(embedding.FloatsAlmostEqual([]float32{3, 2, 1, 6.3, 5.4, 4.5, 9.9, 9, 8.1}, ret.searchResultData.Scores, 0.001))
	}

	// // nq = 3， grouping = true, grouping size = 3
	{
		nq := int64(3)
		f, err := newWeightedFunction(schema, functionSchema)
		s.NoError(err)

		// nq1 id data: 0 - 29, group value: 0,0,0,1,1,1, ... , 9,9,9
		// nq2 id data: 30 - 59, group value: 10,10,10,11,11,11, ... , 19,19,19
		// nq3 id data: 60 - 99, group value: 20,20,20,21,21,21, ... , 29,29,29
		data1 := embedding.GenSearchResultDataWithGrouping(nq, 10, schemapb.DataType_Int64, "", 0, "ts", 102, 3)
		// nq1 id data: 0 - 11, group value: 0,0,0,1,1,1,2,2,2,3,3,3,
		// nq2 id data: 12 - 23, group value: 4,4,4,5,5,5,6,6,6,7,7,7
		// nq3 id data: 24 - 35, group value: 8,8,8,9,9,9,10,10,10,11,11,11
		data2 := embedding.GenSearchResultDataWithGrouping(nq, 4, schemapb.DataType_Int64, "", 0, "ts", 102, 3)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data1, data2}, f.GetInputFieldIDs(), true)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, 1, 102, 3, true, "", []string{"COSINE", "COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{9, 9, 9}, ret.searchResultData.Topks)
		s.Equal(int64(9), ret.searchResultData.TopK)
		s.Equal([]int64{
			5, 4, 3, 29, 28, 27, 26, 25, 24,
			17, 16, 15, 14, 13, 12, 59, 58, 57,
			29, 28, 27, 26, 25, 24, 89, 88, 87,
		},
			ret.searchResultData.Ids.GetIntId().Data)
	}
}
