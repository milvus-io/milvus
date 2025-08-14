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
	"math"
	"testing"

	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function"
)

func TestFunctionScore(t *testing.T) {
	suite.Run(t, new(FunctionScoreSuite))
}

type FunctionScoreSuite struct {
	suite.Suite
	schema    *schemapb.CollectionSchema
	providers []string
}

func (s *FunctionScoreSuite) TestNewFunctionScore() {
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
			{Key: reranker, Value: DecayFunctionName},
			{Key: originKey, Value: "4"},
			{Key: scaleKey, Value: "4"},
			{Key: offsetKey, Value: "4"},
			{Key: decayKey, Value: "0.5"},
			{Key: functionKey, Value: "gauss"},
		},
	}

	segmentScorer := &schemapb.FunctionSchema{
		Name: "test",
		Type: schemapb.FunctionType_Rerank,
		Params: []*commonpb.KeyValuePair{
			{Key: reranker, Value: BoostName},
			{Key: WeightKey, Value: "2"},
		},
	}
	funcScores := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{functionSchema},
	}

	f, err := NewFunctionScore(schema, funcScores)
	s.NoError(err)
	s.Equal([]string{"ts"}, f.GetAllInputFieldNames())
	s.Equal([]int64{102}, f.GetAllInputFieldIDs())
	s.Equal(true, f.IsSupportGroup())
	s.Equal("decay", f.reranker.GetRankName())

	// two ranker but one was boost scorer
	{
		funcScores.Functions = append(funcScores.Functions, segmentScorer)
		_, err := NewFunctionScore(schema, funcScores)
		s.NoError(err)
		funcScores.Functions = funcScores.Functions[:1]
	}

	{
		schema.Fields[3].Nullable = true
		_, err := NewFunctionScore(schema, funcScores)
		s.ErrorContains(err, "Function input field cannot be nullable")
		schema.Fields[3].Nullable = false
	}

	{
		funcScores.Functions[0].Params[0].Value = "NotExist"
		_, err := NewFunctionScore(schema, funcScores)
		s.ErrorContains(err, "Unsupported rerank function")
		funcScores.Functions[0].Params[0].Value = DecayFunctionName
	}

	{
		funcScores.Functions = append(funcScores.Functions, functionSchema)
		_, err := NewFunctionScore(schema, funcScores)
		s.ErrorContains(err, "Currently only supports one rerank")
		funcScores.Functions = funcScores.Functions[:1]
	}

	{
		funcScores.Functions[0].Type = schemapb.FunctionType_BM25
		_, err := NewFunctionScore(schema, funcScores)
		s.ErrorContains(err, "is not rerank function")
		funcScores.Functions[0].Type = schemapb.FunctionType_Rerank
	}

	{
		funcScores.Functions[0].OutputFieldNames = []string{"text"}
		_, err := NewFunctionScore(schema, funcScores)
		s.ErrorContains(err, "Rerank function should not have output field")
		funcScores.Functions[0].OutputFieldNames = []string{""}
	}
}

func (s *FunctionScoreSuite) TestFunctionScoreProcess() {
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
			{Key: reranker, Value: DecayFunctionName},
			{Key: originKey, Value: "4"},
			{Key: scaleKey, Value: "4"},
			{Key: offsetKey, Value: "4"},
			{Key: decayKey, Value: "0.5"},
			{Key: functionKey, Value: "gauss"},
		},
	}
	funcScores := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{functionSchema},
	}

	f, err := NewFunctionScore(schema, funcScores)
	s.NoError(err)

	// empty inputs
	{
		nq := int64(1)
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE"}), []*milvuspb.SearchResults{})
		s.NoError(err)
		s.Equal(int64(3), ret.Results.TopK)
		s.Equal(0, len(ret.Results.FieldsData))
	}

	// single input
	// nq = 1
	{
		nq := int64(1)
		data := function.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "ts", 102)
		searchData := &milvuspb.SearchResults{
			Results: data,
		}
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE"}), []*milvuspb.SearchResults{searchData})
		s.NoError(err)
		s.Equal(int64(3), ret.Results.TopK)
		s.Equal([]int64{3}, ret.Results.Topks)
	}
	// nq=1, input is empty
	{
		nq := int64(1)
		data := function.GenSearchResultData(nq, 0, schemapb.DataType_Int64, "ts", 102)
		searchData := &milvuspb.SearchResults{
			Results: data,
		}
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE"}), []*milvuspb.SearchResults{searchData})
		s.NoError(err)
		s.Equal(int64(3), ret.Results.TopK)
		s.Equal([]int64{0}, ret.Results.Topks)
	}
	// nq=3
	{
		nq := int64(3)
		data := function.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "ts", 102)
		searchData := &milvuspb.SearchResults{
			Results: data,
		}
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE"}), []*milvuspb.SearchResults{searchData})
		s.NoError(err)
		s.Equal(int64(3), ret.Results.TopK)
		s.Equal([]int64{3, 3, 3}, ret.Results.Topks)
	}
	// nq=3, all input is empty
	{
		nq := int64(3)
		data := function.GenSearchResultData(nq, 0, schemapb.DataType_Int64, "ts", 102)
		searchData := &milvuspb.SearchResults{
			Results: data,
		}
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE"}), []*milvuspb.SearchResults{searchData})
		s.NoError(err)
		s.Equal(int64(3), ret.Results.TopK)
		s.Equal([]int64{0, 0, 0}, ret.Results.Topks)
	}

	// multi inputs
	// nq = 1
	{
		nq := int64(1)
		searchData1 := &milvuspb.SearchResults{
			Results: function.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "ts", 102),
		}

		searchData2 := &milvuspb.SearchResults{
			Results: function.GenSearchResultData(nq, 20, schemapb.DataType_Int64, "ts", 102),
		}
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE", "COSINE"}), []*milvuspb.SearchResults{searchData1, searchData2})
		s.NoError(err)
		s.Equal(int64(3), ret.Results.TopK)
		s.Equal([]int64{3}, ret.Results.Topks)
	}
	// nq=1, all input is empty
	{
		nq := int64(1)
		searchData1 := &milvuspb.SearchResults{
			Results: function.GenSearchResultData(nq, 0, schemapb.DataType_Int64, "ts", 102),
		}

		searchData2 := &milvuspb.SearchResults{
			Results: function.GenSearchResultData(nq, 0, schemapb.DataType_Int64, "ts", 102),
		}
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE", "COSINE"}), []*milvuspb.SearchResults{searchData1, searchData2})
		s.NoError(err)
		s.Equal(int64(3), ret.Results.TopK)
		s.Equal([]int64{0}, ret.Results.Topks)
	}
	// nq=1, has empty input
	{
		nq := int64(1)
		searchData1 := &milvuspb.SearchResults{
			Results: function.GenSearchResultData(nq, 0, schemapb.DataType_Int64, "ts", 102),
		}

		searchData2 := &milvuspb.SearchResults{
			Results: function.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "ts", 102),
		}
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE", "COSINE"}), []*milvuspb.SearchResults{searchData1, searchData2})
		s.NoError(err)
		s.Equal(int64(3), ret.Results.TopK)
		s.Equal([]int64{3}, ret.Results.Topks)
	}
	// nq = 3
	{
		nq := int64(3)
		searchData1 := &milvuspb.SearchResults{
			Results: function.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "ts", 102),
		}

		searchData2 := &milvuspb.SearchResults{
			Results: function.GenSearchResultData(nq, 20, schemapb.DataType_Int64, "ts", 102),
		}
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE", "COSINE"}), []*milvuspb.SearchResults{searchData1, searchData2})
		s.NoError(err)
		s.Equal(int64(3), ret.Results.TopK)
		s.Equal([]int64{3, 3, 3}, ret.Results.Topks)
	}
	// nq=3, all input is empty
	{
		nq := int64(3)
		searchData1 := &milvuspb.SearchResults{
			Results: function.GenSearchResultData(nq, 0, schemapb.DataType_Int64, "ts", 102),
		}

		searchData2 := &milvuspb.SearchResults{
			Results: function.GenSearchResultData(nq, 0, schemapb.DataType_Int64, "ts", 102),
		}
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE", "COSINE"}), []*milvuspb.SearchResults{searchData1, searchData2})
		s.NoError(err)
		s.Equal(int64(3), ret.Results.TopK)
		s.Equal([]int64{0, 0, 0}, ret.Results.Topks)
	}
	// nq=3, has empty input
	{
		nq := int64(3)
		searchData1 := &milvuspb.SearchResults{
			Results: function.GenSearchResultData(nq, 0, schemapb.DataType_Int64, "ts", 102),
		}

		searchData2 := &milvuspb.SearchResults{
			Results: function.GenSearchResultData(nq, 10, schemapb.DataType_Int64, "ts", 102),
		}
		ret, err := f.Process(context.Background(), NewSearchParams(nq, 3, 2, -1, -1, 1, false, "", []string{"COSINE", "COSINE"}), []*milvuspb.SearchResults{searchData1, searchData2})
		s.NoError(err)
		s.Equal(int64(3), ret.Results.TopK)
		s.Equal([]int64{3, 3, 3}, ret.Results.Topks)
	}
}

func (s *FunctionScoreSuite) TestlegacyFunction() {
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
	{
		rankParams := []*commonpb.KeyValuePair{}
		f, err := NewFunctionScoreWithlegacy(schema, rankParams)
		s.NoError(err)
		s.Equal(f.RerankName(), RRFName)
	}
	{
		rankParams := []*commonpb.KeyValuePair{
			{Key: legacyRankTypeKey, Value: "invalid"},
			{Key: legacyRankParamsKey, Value: `{"k": "v"}`},
		}
		_, err := NewFunctionScoreWithlegacy(schema, rankParams)
		s.ErrorContains(err, "unsupported rank type")
	}
	{
		rankParams := []*commonpb.KeyValuePair{
			{Key: legacyRankTypeKey, Value: "rrf"},
			{Key: legacyRankParamsKey, Value: "invalid"},
		}
		_, err := NewFunctionScoreWithlegacy(schema, rankParams)
		s.ErrorContains(err, "Parse rerank params failed")
	}
	{
		rankParams := []*commonpb.KeyValuePair{
			{Key: legacyRankTypeKey, Value: "rrf"},
			{Key: legacyRankParamsKey, Value: `{"k": "invalid"}`},
		}
		_, err := NewFunctionScoreWithlegacy(schema, rankParams)
		s.ErrorContains(err, "The type of rank param k should be float")
	}
	{
		rankParams := []*commonpb.KeyValuePair{
			{Key: legacyRankTypeKey, Value: "rrf"},
			{Key: legacyRankParamsKey, Value: `{"k": 1.0}`},
		}
		_, err := NewFunctionScoreWithlegacy(schema, rankParams)
		s.NoError(err)
	}
	{
		rankParams := []*commonpb.KeyValuePair{
			{Key: legacyRankTypeKey, Value: "weighted"},
			{Key: legacyRankParamsKey, Value: `{"weights": [1.0]}`},
		}
		f, err := NewFunctionScoreWithlegacy(schema, rankParams)
		s.NoError(err)
		s.Equal(f.reranker.GetRankName(), WeightedName)
	}
	{
		rankParams := []*commonpb.KeyValuePair{
			{Key: legacyRankTypeKey, Value: "weighted"},
			{Key: legacyRankParamsKey, Value: `{"weights": [1.0], "norm_score": "Invalid"}`},
		}
		_, err := NewFunctionScoreWithlegacy(schema, rankParams)
		s.ErrorContains(err, "Weighted rerank err, norm_score should been bool type")
	}
	{
		rankParams := []*commonpb.KeyValuePair{
			{Key: legacyRankTypeKey, Value: "weighted"},
			{Key: legacyRankParamsKey, Value: `{"weights": [1.0], "norm_score": false}`},
		}
		_, err := NewFunctionScoreWithlegacy(schema, rankParams)
		s.NoError(err)
	}
	{
		rankParams := []*commonpb.KeyValuePair{
			{Key: legacyRankTypeKey, Value: "weighted"},
			{Key: legacyRankParamsKey, Value: `{"weights": [1.0], "norm_score": "false"}`},
		}
		_, err := NewFunctionScoreWithlegacy(schema, rankParams)
		s.ErrorContains(err, "Weighted rerank err, norm_score should been bool type")
	}
}

func (s *FunctionScoreSuite) TestFunctionUtil() {
	g1 := &Group[int64]{
		idList:    []int64{1, 2, 3},
		scoreList: []float32{1.0, 2.0, 3.0},
		groupVal:  3,
		maxScore:  3.0,
		sumScore:  6.0,
	}
	s1, err := groupScore(g1, maxScorer)
	s.NoError(err)
	s.True(math.Abs(float64(s1-3.0)) < 0.001)

	s2, err := groupScore(g1, sumScorer)
	s.NoError(err)
	s.True(math.Abs(float64(s2-6.0)) < 0.001)

	s3, err := groupScore(g1, avgScorer)
	s.NoError(err)
	s.True(math.Abs(float64(s3-2.0)) < 0.001)

	_, err = groupScore(g1, "NotSupported")
	s.ErrorContains(err, "is not supported")

	g1.idList = []int64{}
	_, err = groupScore(g1, avgScorer)
	s.ErrorContains(err, "input group for score must have at least one id, must be sth wrong within code")
}
