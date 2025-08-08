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

func TestRRFFunction(t *testing.T) {
	suite.Run(t, new(RRFFunctionSuite))
}

type RRFFunctionSuite struct {
	suite.Suite
}

func (s *RRFFunctionSuite) TestNewRRFFuction() {
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
			{Key: RRFParamsKey, Value: "70"},
		},
	}

	{
		_, err := newRRFFunction(schema, functionSchema)
		s.NoError(err)
	}
	{
		schema.Fields[0] = &schemapb.FieldSchema{FieldID: 100, Name: "pk", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true}
		_, err := newRRFFunction(schema, functionSchema)
		s.NoError(err)
	}
	{
		functionSchema.Params[0] = &commonpb.KeyValuePair{Key: RRFParamsKey, Value: "NotNum"}
		_, err := newRRFFunction(schema, functionSchema)
		s.ErrorContains(err, "is not a number")
		functionSchema.Params[0] = &commonpb.KeyValuePair{Key: RRFParamsKey, Value: "-1"}
		_, err = newRRFFunction(schema, functionSchema)
		s.ErrorContains(err, "he rank params k should be in range")
		functionSchema.Params[0] = &commonpb.KeyValuePair{Key: RRFParamsKey, Value: "100"}
	}
	{
		functionSchema.InputFieldNames = []string{"ts"}
		_, err := newRRFFunction(schema, functionSchema)
		s.ErrorContains(err, "The rrf function does not support input parameters")
	}
}

func (s *RRFFunctionSuite) TestRRFFuctionProcess() {
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
		Params:          []*commonpb.KeyValuePair{},
	}

	// empty
	{
		nq := int64(1)
		f, err := newRRFFunction(schema, functionSchema)
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
		f, err := newRRFFunction(schema, functionSchema)
		s.NoError(err)
		data := embedding.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "", 0)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data}, f.GetInputFieldIDs(), false)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{2, 3, 4}, ret.searchResultData.Ids.GetIntId().Data)
	}
	// nq = 3
	{
		nq := int64(3)
		f, err := newRRFFunction(schema, functionSchema)
		s.NoError(err)
		data := embedding.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "", 0)
		inputs, _ := newRerankInputs([]*schemapb.SearchResultData{data}, f.GetInputFieldIDs(), false)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{3, 3, 3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{2, 3, 4, 12, 13, 14, 22, 23, 24}, ret.searchResultData.Ids.GetIntId().Data)
	}

	// has empty inputs
	{
		nq := int64(1)
		f, err := newRRFFunction(schema, functionSchema)
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
		s.Equal([]int64{0, 1, 2}, ret.searchResultData.Ids.GetIntId().Data)
	}
	// nq = 1
	{
		nq := int64(1)
		f, err := newRRFFunction(schema, functionSchema)
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
		s.Equal([]int64{2, 3, 4}, ret.searchResultData.Ids.GetIntId().Data)
	}
	// // nq = 3
	{
		nq := int64(3)
		f, err := newRRFFunction(schema, functionSchema)
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
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE", "COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{3, 3, 3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{2, 3, 4, 5, 11, 6, 9, 21, 10}, ret.searchResultData.Ids.GetIntId().Data)
	}
	// // nq = 3， grouping = true, grouping size = 1
	{
		nq := int64(3)
		f, err := newRRFFunction(schema, functionSchema)
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
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, 102, 1, true, "", []string{"COSINE", "COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{3, 3, 3}, ret.searchResultData.Topks)
		s.Equal(int64(3), ret.searchResultData.TopK)
		s.Equal([]int64{2, 3, 4, 5, 11, 6, 9, 21, 10}, ret.searchResultData.Ids.GetIntId().Data)
	}

	// // nq = 3， grouping = true, grouping size = 3
	{
		nq := int64(3)
		f, err := newRRFFunction(schema, functionSchema)
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
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, 3, 102, 3, true, "", []string{"COSINE", "COSINE"}), inputs)
		s.NoError(err)
		s.Equal([]int64{9, 9, 9}, ret.searchResultData.Topks)
		s.Equal(int64(9), ret.searchResultData.TopK)
		s.Equal([]int64{
			6, 7, 8, 9, 10, 11, 12, 13, 14,
			15, 16, 17, 33, 34, 35, 18, 19, 20,
			27, 28, 29, 63, 64, 65, 30, 31, 32,
		},
			ret.searchResultData.Ids.GetIntId().Data)
	}
}
