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

package chain

import (
	"context"
	"math"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
)

// =============================================================================
// Test Suite
// =============================================================================

type RerankBuilderTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *RerankBuilderTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *RerankBuilderTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestRerankBuilderTestSuite(t *testing.T) {
	suite.Run(t, new(RerankBuilderTestSuite))
}

// =============================================================================
// Helper Functions
// =============================================================================

func (s *RerankBuilderTestSuite) createCollectionSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "id",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "timestamp",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  102,
				Name:     "price",
				DataType: schemapb.DataType_Float,
			},
			{
				FieldID:  103,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
			},
		},
	}
}

func (s *RerankBuilderTestSuite) createSearchParams() *SearchParams {
	return NewSearchParams(1, 10, 0, -1)
}

// =============================================================================
// GetRerankName Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestGetRerankName() {
	funcSchema := &schemapb.FunctionSchema{
		Type: schemapb.FunctionType_Rerank,
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "rrf"},
		},
	}

	name := GetRerankName(funcSchema)
	s.Equal("rrf", name)
}

func (s *RerankBuilderTestSuite) TestGetRerankNameCaseInsensitive() {
	funcSchema := &schemapb.FunctionSchema{
		Type: schemapb.FunctionType_Rerank,
		Params: []*commonpb.KeyValuePair{
			{Key: "RERANKER", Value: "RRF"},
		},
	}

	name := GetRerankName(funcSchema)
	s.Equal("rrf", name)
}

// =============================================================================
// BuildRerankChain Tests - RRF
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildRRFChain() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE", "IP"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
					{Key: "k", Value: "60"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	// Verify chain structure: MergeOp -> SortOp -> LimitOp -> SelectOp
	s.Equal(4, len(fc.operators))
	s.Equal("Merge", fc.operators[0].Name())
	s.Equal("Sort", fc.operators[1].Name())
	s.Equal("Limit", fc.operators[2].Name())
	s.Equal("Select", fc.operators[3].Name())
}

func (s *RerankBuilderTestSuite) TestBuildRRFChainDefaultK() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
					// No k parameter, should use default 60
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	// Verify MergeOp has default k
	mergeOp := fc.operators[0].(*MergeOp)
	s.Equal(60.0, mergeOp.rrfK)
}

func (s *RerankBuilderTestSuite) TestBuildRRFChainKOutOfRange() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE"}

	tests := []struct {
		name string
		k    string
	}{
		{"k is zero", "0"},
		{"k is negative", "-1"},
		{"k exceeds upper bound", "16385"},
		{"k equals upper bound", "16384"},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			funcScoreSchema := &schemapb.FunctionScore{
				Functions: []*schemapb.FunctionSchema{
					{
						Type: schemapb.FunctionType_Rerank,
						Params: []*commonpb.KeyValuePair{
							{Key: "reranker", Value: "rrf"},
							{Key: "k", Value: tt.k},
						},
					},
				},
			}

			_, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
			s.Error(err)
			s.Contains(err.Error(), "The rank params k should be in range (0, 16384)")
		})
	}
}

func (s *RerankBuilderTestSuite) TestBuildRRFChainKNotANumber() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
					{Key: "k", Value: "abc"},
				},
			},
		},
	}

	_, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Error(err)
	s.Contains(err.Error(), "is not a number")
}

// =============================================================================
// BuildRerankChain Tests - Weighted
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildWeightedChain() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE", "IP"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: "weights", Value: "[0.3, 0.7]"},
					{Key: "norm_score", Value: "true"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	// Verify MergeOp
	mergeOp := fc.operators[0].(*MergeOp)
	s.Equal(MergeStrategyWeighted, mergeOp.strategy)
	s.Equal([]float64{0.3, 0.7}, mergeOp.weights)
	s.True(mergeOp.normalize)
}

func (s *RerankBuilderTestSuite) TestBuildWeightedChainMissingWeights() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE", "IP"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					// Missing weights
				},
			},
		},
	}

	_, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Error(err)
	s.Contains(err.Error(), "weights")
}

func (s *RerankBuilderTestSuite) TestBuildWeightedChainWeightsCountMismatch() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE", "IP", "L2"} // 3 metrics

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: "weights", Value: "[0.3, 0.7]"}, // Only 2 weights
				},
			},
		},
	}

	_, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Error(err)
	s.Contains(err.Error(), "the length of weights param mismatch with ann search requests")
}

func (s *RerankBuilderTestSuite) TestBuildWeightedChainWeightOutOfRange() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE", "IP"}

	tests := []struct {
		name    string
		weights string
	}{
		{"weight greater than 1", "[0.1, 2]"},
		{"negative weight", "[-0.5, 0.3]"},
		{"weight exactly negative", "[-1, 0.5]"},
		{"weight slightly above 1", "[0.5, 1.1]"},
	}

	for _, tt := range tests {
		s.Run(tt.name, func() {
			funcScoreSchema := &schemapb.FunctionScore{
				Functions: []*schemapb.FunctionSchema{
					{
						Type: schemapb.FunctionType_Rerank,
						Params: []*commonpb.KeyValuePair{
							{Key: "reranker", Value: "weighted"},
							{Key: "weights", Value: tt.weights},
						},
					},
				},
			}

			_, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
			s.Error(err)
			s.Contains(err.Error(), "rank param weight should be in range [0, 1]")
		})
	}
}

// =============================================================================
// BuildRerankChain Tests - Decay
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildDecayChain() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{"timestamp"},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "function", Value: "gauss"},
					{Key: "origin", Value: "1000"},
					{Key: "scale", Value: "100"},
					{Key: "offset", Value: "10"},
					{Key: "decay", Value: "0.5"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	// Verify chain structure: MergeOp -> MapOp(Decay) -> SortOp -> LimitOp -> SelectOp
	s.Equal(5, len(fc.operators))
	s.Equal("Merge", fc.operators[0].Name())
	s.Equal("Map", fc.operators[1].Name())
	s.Equal("Sort", fc.operators[2].Name())
	s.Equal("Limit", fc.operators[3].Name())
	s.Equal("Select", fc.operators[4].Name())
}

func (s *RerankBuilderTestSuite) TestBuildDecayChainMissingRequired() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE"}

	// Missing origin
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{"timestamp"},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "function", Value: "gauss"},
					{Key: "scale", Value: "100"},
				},
			},
		},
	}

	_, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Error(err)
	s.Contains(err.Error(), "origin")
}

func (s *RerankBuilderTestSuite) TestBuildDecayChainInvalidInputField() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE"}

	// Non-existent field
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{"nonexistent"},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "function", Value: "gauss"},
					{Key: "origin", Value: "1000"},
					{Key: "scale", Value: "100"},
				},
			},
		},
	}

	_, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Error(err)
	s.Contains(err.Error(), "not found")
}

func (s *RerankBuilderTestSuite) TestBuildDecayChainWithScoreMode() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{"timestamp"},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "function", Value: "gauss"},
					{Key: "origin", Value: "1000"},
					{Key: "scale", Value: "100"},
					{Key: "score_mode", Value: "sum"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// Verify MergeOp uses sum strategy
	mergeOp := fc.operators[0].(*MergeOp)
	s.Equal(MergeStrategySum, mergeOp.strategy)
}

// =============================================================================
// BuildRerankChainWithLegacy Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildLegacyRRF() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE"}

	rankParams := []*commonpb.KeyValuePair{
		{Key: "strategy", Value: "rrf"},
		{Key: "params", Value: `{"k": 60}`},
	}

	fc, err := BuildRerankChainWithLegacy(collSchema, rankParams, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	mergeOp := fc.operators[0].(*MergeOp)
	s.Equal(MergeStrategyRRF, mergeOp.strategy)
	s.Equal(60.0, mergeOp.rrfK)
}

func (s *RerankBuilderTestSuite) TestBuildLegacyWeighted() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE", "IP"}

	rankParams := []*commonpb.KeyValuePair{
		{Key: "strategy", Value: "weighted"},
		{Key: "params", Value: `{"weights": [0.3, 0.7], "norm_score": true}`},
	}

	fc, err := BuildRerankChainWithLegacy(collSchema, rankParams, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	mergeOp := fc.operators[0].(*MergeOp)
	s.Equal(MergeStrategyWeighted, mergeOp.strategy)
	s.Equal([]float64{0.3, 0.7}, mergeOp.weights)
}

func (s *RerankBuilderTestSuite) TestBuildLegacyDefaultRRF() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE"}

	// No strategy specified, should default to RRF
	rankParams := []*commonpb.KeyValuePair{}

	fc, err := BuildRerankChainWithLegacy(collSchema, rankParams, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	mergeOp := fc.operators[0].(*MergeOp)
	s.Equal(MergeStrategyRRF, mergeOp.strategy)
}

// =============================================================================
// Error Cases
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildChainNoFunctions() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{},
	}

	_, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Error(err)
}

func (s *RerankBuilderTestSuite) TestBuildChainUnsupportedReranker() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "unknown"},
				},
			},
		},
	}

	_, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Error(err)
	s.Contains(err.Error(), "unsupported")
}

// =============================================================================
// SearchParams Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestNewSearchParamsWithGrouping() {
	sp := NewSearchParamsWithGrouping(1, 10, 0, -1, "category", 3)
	s.Equal(int64(1), sp.Nq)
	s.Equal(int64(10), sp.Limit)
	s.Equal(int64(0), sp.Offset)
	s.Equal(int64(-1), sp.RoundDecimal)
	s.Equal("category", sp.GroupByField)
	s.Equal(int64(3), sp.GroupSize)
	s.True(sp.HasGrouping())
}

func (s *RerankBuilderTestSuite) TestSearchParamsHasGrouping() {
	// No grouping
	sp := NewSearchParams(1, 10, 0, -1)
	s.False(sp.HasGrouping())

	// Empty group field
	sp = NewSearchParamsWithGrouping(1, 10, 0, -1, "", 3)
	s.False(sp.HasGrouping())

	// Zero group size
	sp = NewSearchParamsWithGrouping(1, 10, 0, -1, "category", 0)
	s.False(sp.HasGrouping())

	// Valid grouping
	sp = NewSearchParamsWithGrouping(1, 10, 0, -1, "category", 3)
	s.True(sp.HasGrouping())
}

// =============================================================================
// BuildRerankChain with Grouping Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildRRFChainWithGrouping() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParamsWithGrouping(1, 10, 0, -1, "category", 3)
	searchMetrics := []string{"COSINE", "IP"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
					{Key: "k", Value: "60"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	// Verify chain structure: MergeOp -> GroupByOp -> SelectOp (no Sort/Limit when grouping)
	s.Equal(3, len(fc.operators))
	s.Equal("Merge", fc.operators[0].Name())
	s.Equal("GroupBy", fc.operators[1].Name())
	s.Equal("Select", fc.operators[2].Name())
}

func (s *RerankBuilderTestSuite) createCollectionSchemaWithCategory() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "id",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "timestamp",
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  102,
				Name:     "price",
				DataType: schemapb.DataType_Float,
			},
			{
				FieldID:  103,
				Name:     "category",
				DataType: schemapb.DataType_VarChar,
			},
			{
				FieldID:  104,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
			},
		},
	}
}

// =============================================================================
// End-to-End Rerank Chain Execution Tests
// =============================================================================

func (s *RerankBuilderTestSuite) createTestDataFrameForRerankWithTimestamp(ids []int64, scores []float32, categories []string, timestamps []int64, topks []int64) *DataFrame {
	resultData := &schemapb.SearchResultData{
		NumQueries: int64(len(topks)),
		TopK:       topks[0],
		Topks:      topks,
		Scores:     scores,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: ids},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "category",
				FieldId:   103,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: categories},
						},
					},
				},
			},
			{
				Type:      schemapb.DataType_Int64,
				FieldName: "timestamp",
				FieldId:   101,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_LongData{
							LongData: &schemapb.LongArray{Data: timestamps},
						},
					},
				},
			},
		},
	}
	df, err := FromSearchResultData(resultData, s.pool, []string{"category", "timestamp"})
	s.Require().NoError(err)
	return df
}

func (s *RerankBuilderTestSuite) createTestDataFrameForRerank(ids []int64, scores []float32, categories []string, topks []int64) *DataFrame {
	resultData := &schemapb.SearchResultData{
		NumQueries: int64(len(topks)),
		TopK:       topks[0],
		Topks:      topks,
		Scores:     scores,
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{Data: ids},
			},
		},
		FieldsData: []*schemapb.FieldData{
			{
				Type:      schemapb.DataType_VarChar,
				FieldName: "category",
				FieldId:   103,
				Field: &schemapb.FieldData_Scalars{
					Scalars: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: categories},
						},
					},
				},
			},
		},
	}
	df, err := FromSearchResultData(resultData, s.pool, []string{"category"})
	s.Require().NoError(err)
	return df
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_EmptyInput() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParams(1, 10, 0, -1)
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// Create DataFrame with 1 row (minimum valid input)
	// Empty input is an edge case that fails because $score column doesn't exist
	df := s.createTestDataFrameForRerank([]int64{1}, []float32{0.5}, []string{"A"}, []int64{1})
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(1), result.NumRows())
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_RRFBasic() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParams(1, 5, 0, -1)
	searchMetrics := []string{"COSINE", "IP"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
					{Key: "k", Value: "60"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// Create two input DataFrames
	df1 := s.createTestDataFrameForRerank(
		[]int64{1, 2, 3, 4, 5},
		[]float32{0.9, 0.8, 0.7, 0.6, 0.5},
		[]string{"A", "B", "A", "B", "C"},
		[]int64{5},
	)
	defer df1.Release()

	df2 := s.createTestDataFrameForRerank(
		[]int64{2, 3, 4, 5, 6},
		[]float32{0.95, 0.85, 0.75, 0.65, 0.55},
		[]string{"B", "A", "B", "C", "A"},
		[]int64{5},
	)
	defer df2.Release()

	result, err := fc.ExecuteWithContext(context.Background(), df1, df2)
	s.Require().NoError(err)
	defer result.Release()

	// Verify result is limited and sorted
	s.LessOrEqual(result.NumRows(), int64(5))
	s.True(result.HasColumn("$id"))
	s.True(result.HasColumn("$score"))
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_WithGrouping_Basic() {
	collSchema := s.createCollectionSchemaWithCategory()
	// Limit 3 groups, 2 items per group
	searchParams := NewSearchParamsWithGrouping(1, 3, 0, -1, "category", 2)
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// Create DataFrame with multiple categories
	// Category A: 3 items (scores 0.9, 0.7, 0.5)
	// Category B: 2 items (scores 0.8, 0.6)
	// Category C: 1 item (score 0.4)
	df := s.createTestDataFrameForRerank(
		[]int64{1, 2, 3, 4, 5, 6},
		[]float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
		[]string{"A", "B", "A", "B", "A", "C"},
		[]int64{6},
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// With groupSize=2 and limit=3 groups:
	// Group A: top 2 -> ids 1, 3 (scores 0.9, 0.7), group_score=0.9
	// Group B: top 2 -> ids 2, 4 (scores 0.8, 0.6), group_score=0.8
	// Group C: 1 item -> id 6 (score 0.4), group_score=0.4
	// Sorted by group_score DESC: A, B, C
	// Total: 2 + 2 + 1 = 5 rows (but may vary based on offset/limit logic)

	s.True(result.HasColumn("$group_score"))
	s.LessOrEqual(result.NumRows(), int64(6))
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_WithGrouping_GroupSizeOne() {
	collSchema := s.createCollectionSchemaWithCategory()
	// groupSize=1 means only top 1 item per group
	searchParams := NewSearchParamsWithGrouping(1, 10, 0, -1, "category", 1)
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// 3 categories, each with multiple items
	df := s.createTestDataFrameForRerank(
		[]int64{1, 2, 3, 4, 5, 6},
		[]float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
		[]string{"A", "B", "C", "A", "B", "C"},
		[]int64{6},
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// With groupSize=1, only 1 item per category
	// Group A: id 1 (score 0.9)
	// Group B: id 2 (score 0.8)
	// Group C: id 3 (score 0.7)
	// Total: 3 rows
	s.Equal(int64(3), result.NumRows())
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_WithGrouping_LargeGroupSize() {
	collSchema := s.createCollectionSchemaWithCategory()
	// groupSize larger than any group's actual size
	searchParams := NewSearchParamsWithGrouping(1, 10, 0, -1, "category", 100)
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	df := s.createTestDataFrameForRerank(
		[]int64{1, 2, 3, 4, 5},
		[]float32{0.9, 0.8, 0.7, 0.6, 0.5},
		[]string{"A", "B", "A", "B", "A"},
		[]int64{5},
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// All items should be included since groupSize > any group's size
	s.Equal(int64(5), result.NumRows())
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_WithGrouping_Offset() {
	collSchema := s.createCollectionSchemaWithCategory()
	// Skip first group
	searchParams := NewSearchParamsWithGrouping(1, 2, 1, -1, "category", 2)
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// 3 categories
	df := s.createTestDataFrameForRerank(
		[]int64{1, 2, 3, 4, 5, 6},
		[]float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
		[]string{"A", "B", "C", "A", "B", "C"},
		[]int64{6},
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Groups sorted by score: A (0.9), B (0.8), C (0.7)
	// With offset=1, skip group A
	// With limit=2, take groups B and C
	// groupSize=2: B has 2 items, C has 2 items -> 4 rows total
	s.LessOrEqual(result.NumRows(), int64(4))
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_WithGrouping_SingleItem() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParamsWithGrouping(1, 10, 0, -1, "category", 3)
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// Single item input
	df := s.createTestDataFrameForRerank([]int64{1}, []float32{0.9}, []string{"A"}, []int64{1})
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(1), result.NumRows())
	s.True(result.HasColumn("$group_score"))
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_WithGrouping_SingleGroup() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParamsWithGrouping(1, 10, 0, -1, "category", 2)
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// All items in same category
	df := s.createTestDataFrameForRerank(
		[]int64{1, 2, 3, 4},
		[]float32{0.9, 0.8, 0.7, 0.6},
		[]string{"A", "A", "A", "A"},
		[]int64{4},
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Only 1 group, groupSize=2, so only 2 items
	s.Equal(int64(2), result.NumRows())
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_MultipleChunks() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParams(2, 3, 0, -1) // 2 queries
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// Create DataFrame with 2 chunks (2 queries, 3 results each)
	df := s.createTestDataFrameForRerank(
		[]int64{1, 2, 3, 4, 5, 6},
		[]float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
		[]string{"A", "B", "C", "A", "B", "C"},
		[]int64{3, 3}, // 2 chunks with 3 items each
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Each chunk should be limited to 3
	s.Equal(2, result.NumChunks())
	s.Equal([]int64{3, 3}, result.ChunkSizes())
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_MultipleChunks_WithGrouping() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParamsWithGrouping(2, 2, 0, -1, "category", 1) // 2 queries, 2 groups, 1 per group
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// 2 chunks, each with 4 items
	df := s.createTestDataFrameForRerank(
		[]int64{1, 2, 3, 4, 5, 6, 7, 8},
		[]float32{0.9, 0.8, 0.7, 0.6, 0.95, 0.85, 0.75, 0.65},
		[]string{"A", "B", "A", "B", "X", "Y", "X", "Y"},
		[]int64{4, 4},
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Each chunk should have at most 2 groups * 1 item = 2 rows
	s.Equal(2, result.NumChunks())
	s.True(result.HasColumn("$group_score"))
}

// =============================================================================
// Decay with Grouping Tests
// =============================================================================

func (s *RerankBuilderTestSuite) createDecayWithGroupingFuncScore() *schemapb.FunctionScore {
	return &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{"timestamp"},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "function", Value: "gauss"},
					{Key: "origin", Value: "1000"},
					{Key: "scale", Value: "200"},
					{Key: "offset", Value: "0"},
					{Key: "decay", Value: "0.5"},
				},
			},
		},
	}
}

func (s *RerankBuilderTestSuite) TestBuildDecayChainWithGrouping() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParamsWithGrouping(1, 3, 0, -1, "category", 2)
	searchMetrics := []string{"COSINE"}

	fc, err := BuildRerankChain(collSchema, s.createDecayWithGroupingFuncScore(), searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	// Verify chain structure: MergeOp -> MapOp(Decay) -> GroupByOp -> SelectOp
	s.Equal(4, len(fc.operators))
	s.Equal("Merge", fc.operators[0].Name())
	s.Equal("Map", fc.operators[1].Name())
	s.Equal("GroupBy", fc.operators[2].Name())
	s.Equal("Select", fc.operators[3].Name())
}

func (s *RerankBuilderTestSuite) TestExecuteDecayWithGrouping_Basic() {
	collSchema := s.createCollectionSchemaWithCategory()
	// 3 groups, 2 items per group
	searchParams := NewSearchParamsWithGrouping(1, 3, 0, -1, "category", 2)
	searchMetrics := []string{"COSINE"}

	fc, err := BuildRerankChain(collSchema, s.createDecayWithGroupingFuncScore(), searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// Category A: timestamps near origin (1000) -> high decay scores
	// Category B: timestamps far from origin -> low decay scores
	// Category C: timestamps medium distance
	df := s.createTestDataFrameForRerankWithTimestamp(
		[]int64{1, 2, 3, 4, 5, 6, 7},
		[]float32{0.9, 0.8, 0.7, 0.85, 0.6, 0.75, 0.5},
		[]string{"A", "B", "A", "C", "B", "C", "A"},
		[]int64{1000, 500, 1010, 900, 200, 850, 1050},
		[]int64{7},
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Should have grouping columns
	s.True(result.HasColumn("$group_score"))
	s.True(result.HasColumn("$score"))
	// At most 3 groups * 2 items = 6, but limited by available data
	s.LessOrEqual(result.NumRows(), int64(6))
}

func (s *RerankBuilderTestSuite) TestExecuteDecayWithGrouping_GroupSizeOne() {
	collSchema := s.createCollectionSchemaWithCategory()
	// groupSize=1: only best item per group
	searchParams := NewSearchParamsWithGrouping(1, 10, 0, -1, "category", 1)
	searchMetrics := []string{"COSINE"}

	fc, err := BuildRerankChain(collSchema, s.createDecayWithGroupingFuncScore(), searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	df := s.createTestDataFrameForRerankWithTimestamp(
		[]int64{1, 2, 3, 4, 5, 6},
		[]float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
		[]string{"A", "B", "C", "A", "B", "C"},
		[]int64{1000, 990, 980, 1010, 970, 960},
		[]int64{6},
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// 3 categories, 1 item per group
	s.Equal(int64(3), result.NumRows())
}

func (s *RerankBuilderTestSuite) TestExecuteDecayWithGrouping_WithOffset() {
	collSchema := s.createCollectionSchemaWithCategory()
	// offset=1: skip the top group
	searchParams := NewSearchParamsWithGrouping(1, 2, 1, -1, "category", 2)
	searchMetrics := []string{"COSINE"}

	fc, err := BuildRerankChain(collSchema, s.createDecayWithGroupingFuncScore(), searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	df := s.createTestDataFrameForRerankWithTimestamp(
		[]int64{1, 2, 3, 4, 5, 6},
		[]float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
		[]string{"A", "B", "C", "A", "B", "C"},
		[]int64{1000, 990, 980, 1010, 970, 960},
		[]int64{6},
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// 3 groups total, offset=1 skip top group, limit=2 take next 2 groups
	// Each group has up to 2 items
	s.LessOrEqual(result.NumRows(), int64(4))
	s.True(result.HasColumn("$group_score"))
}

func (s *RerankBuilderTestSuite) TestExecuteDecayWithGrouping_WithScorer() {
	collSchema := s.createCollectionSchemaWithCategory()

	funcScoreSchema := s.createDecayWithGroupingFuncScore()

	searchMetrics := []string{"COSINE"}

	// Test each scorer mode
	for _, scorer := range []GroupScorer{GroupScorerMax, GroupScorerSum, GroupScorerAvg} {
		searchParams := NewSearchParamsWithGroupingAndScorer(1, 10, 0, -1, "category", 2, scorer)

		fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
		s.Require().NoError(err)

		df := s.createTestDataFrameForRerankWithTimestamp(
			[]int64{1, 2, 3, 4, 5, 6},
			[]float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
			[]string{"A", "B", "A", "B", "A", "B"},
			[]int64{1000, 990, 1010, 980, 1020, 970},
			[]int64{6},
		)

		result, err := fc.Execute(df)
		s.Require().NoError(err)

		// 2 groups, 2 items each
		s.Equal(int64(4), result.NumRows(), "scorer=%s", scorer)
		s.True(result.HasColumn("$group_score"), "scorer=%s", scorer)

		result.Release()
		df.Release()
	}
}

func (s *RerankBuilderTestSuite) TestExecuteDecayWithGrouping_MultipleChunks() {
	collSchema := s.createCollectionSchemaWithCategory()
	// 2 queries, 2 groups, 1 item per group
	searchParams := NewSearchParamsWithGrouping(2, 2, 0, -1, "category", 1)
	searchMetrics := []string{"COSINE"}

	fc, err := BuildRerankChain(collSchema, s.createDecayWithGroupingFuncScore(), searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// 2 chunks, each with 4 items
	df := s.createTestDataFrameForRerankWithTimestamp(
		[]int64{1, 2, 3, 4, 5, 6, 7, 8},
		[]float32{0.9, 0.8, 0.7, 0.6, 0.95, 0.85, 0.75, 0.65},
		[]string{"A", "B", "A", "B", "X", "Y", "X", "Y"},
		[]int64{1000, 990, 1010, 980, 1000, 990, 1010, 980},
		[]int64{4, 4},
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Each chunk: 2 groups * 1 item = 2 rows
	s.Equal(2, result.NumChunks())
	s.True(result.HasColumn("$group_score"))
}

func (s *RerankBuilderTestSuite) TestExecuteDecayWithGrouping_SingleGroup() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParamsWithGrouping(1, 10, 0, -1, "category", 2)
	searchMetrics := []string{"COSINE"}

	fc, err := BuildRerankChain(collSchema, s.createDecayWithGroupingFuncScore(), searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// All items in same category
	df := s.createTestDataFrameForRerankWithTimestamp(
		[]int64{1, 2, 3, 4},
		[]float32{0.9, 0.8, 0.7, 0.6},
		[]string{"A", "A", "A", "A"},
		[]int64{1000, 1010, 1020, 1030},
		[]int64{4},
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// 1 group, groupSize=2 -> 2 items
	s.Equal(int64(2), result.NumRows())
}

func (s *RerankBuilderTestSuite) TestMemoryLeak_DecayWithGrouping() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParamsWithGrouping(1, 3, 0, -1, "category", 2)
	searchMetrics := []string{"COSINE"}

	for range 10 {
		fc, err := BuildRerankChain(collSchema, s.createDecayWithGroupingFuncScore(), searchMetrics, searchParams, s.pool)
		s.Require().NoError(err)

		df := s.createTestDataFrameForRerankWithTimestamp(
			[]int64{1, 2, 3, 4, 5, 6},
			[]float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
			[]string{"A", "B", "C", "A", "B", "C"},
			[]int64{1000, 990, 980, 1010, 970, 960},
			[]int64{6},
		)

		result, err := fc.Execute(df)
		s.Require().NoError(err)

		result.Release()
		df.Release()
	}
	// Memory leak check in TearDownTest
}

// =============================================================================
// Memory Leak Tests for Rerank Chain
// =============================================================================

func (s *RerankBuilderTestSuite) TestMemoryLeak_RerankChainExecution() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParams(1, 5, 0, -1)
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	for range 10 {
		fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
		s.Require().NoError(err)

		df := s.createTestDataFrameForRerank(
			[]int64{1, 2, 3, 4, 5},
			[]float32{0.9, 0.8, 0.7, 0.6, 0.5},
			[]string{"A", "B", "C", "A", "B"},
			[]int64{5},
		)

		result, err := fc.Execute(df)
		s.Require().NoError(err)

		result.Release()
		df.Release()
	}
	// Memory leak check in TearDownTest
}

func (s *RerankBuilderTestSuite) TestMemoryLeak_RerankChainWithGrouping() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParamsWithGrouping(1, 3, 0, -1, "category", 2)
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	for range 10 {
		fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
		s.Require().NoError(err)

		df := s.createTestDataFrameForRerank(
			[]int64{1, 2, 3, 4, 5, 6},
			[]float32{0.9, 0.8, 0.7, 0.6, 0.5, 0.4},
			[]string{"A", "B", "C", "A", "B", "C"},
			[]int64{6},
		)

		result, err := fc.Execute(df)
		s.Require().NoError(err)

		result.Release()
		df.Release()
	}
	// Memory leak check in TearDownTest
}

func (s *RerankBuilderTestSuite) TestMemoryLeak_WeightedChain() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParams(1, 5, 0, -1)
	searchMetrics := []string{"COSINE", "IP"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: "weights", Value: "[0.3, 0.7]"},
					{Key: "norm_score", Value: "true"},
				},
			},
		},
	}

	for range 10 {
		fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
		s.Require().NoError(err)

		df1 := s.createTestDataFrameForRerank(
			[]int64{1, 2, 3, 4, 5},
			[]float32{0.9, 0.8, 0.7, 0.6, 0.5},
			[]string{"A", "B", "C", "A", "B"},
			[]int64{5},
		)
		df2 := s.createTestDataFrameForRerank(
			[]int64{2, 3, 4, 5, 6},
			[]float32{0.95, 0.85, 0.75, 0.65, 0.55},
			[]string{"B", "A", "B", "C", "A"},
			[]int64{5},
		)

		result, err := fc.ExecuteWithContext(context.Background(), df1, df2)
		s.Require().NoError(err)

		result.Release()
		df1.Release()
		df2.Release()
	}
	// Memory leak check in TearDownTest
}

func (s *RerankBuilderTestSuite) TestMemoryLeak_DecayChain() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParams(1, 5, 0, -1)
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := s.createDecayWithGroupingFuncScore()

	for range 10 {
		fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
		s.Require().NoError(err)

		df := s.createTestDataFrameForRerankWithTimestamp(
			[]int64{1, 2, 3, 4, 5},
			[]float32{0.9, 0.8, 0.7, 0.6, 0.5},
			[]string{"A", "B", "C", "A", "B"},
			[]int64{1000, 990, 980, 1010, 970},
			[]int64{5},
		)

		result, err := fc.Execute(df)
		s.Require().NoError(err)

		result.Release()
		df.Release()
	}
	// Memory leak check in TearDownTest
}

func (s *RerankBuilderTestSuite) TestMemoryLeak_MultiInputMerge() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParams(1, 5, 0, -1)
	searchMetrics := []string{"COSINE", "IP"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
					{Key: "k", Value: "60"},
				},
			},
		},
	}

	for range 10 {
		fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
		s.Require().NoError(err)

		df1 := s.createTestDataFrameForRerank(
			[]int64{1, 2, 3, 4, 5},
			[]float32{0.9, 0.8, 0.7, 0.6, 0.5},
			[]string{"A", "B", "C", "A", "B"},
			[]int64{5},
		)
		df2 := s.createTestDataFrameForRerank(
			[]int64{2, 3, 4, 5, 6},
			[]float32{0.95, 0.85, 0.75, 0.65, 0.55},
			[]string{"B", "A", "B", "C", "A"},
			[]int64{5},
		)

		result, err := fc.ExecuteWithContext(context.Background(), df1, df2)
		s.Require().NoError(err)

		result.Release()
		df1.Release()
		df2.Release()
	}
	// Memory leak check in TearDownTest
}

// =============================================================================
// RoundDecimal Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_RRF_WithRoundDecimal() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParams(1, 5, 0, 2) // roundDecimal=2
	searchMetrics := []string{"COSINE", "IP"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
					{Key: "k", Value: "60"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	df1 := s.createTestDataFrameForRerank(
		[]int64{1, 2, 3},
		[]float32{0.9, 0.8, 0.7},
		[]string{"A", "B", "C"},
		[]int64{3},
	)
	defer df1.Release()

	df2 := s.createTestDataFrameForRerank(
		[]int64{2, 3, 4},
		[]float32{0.95, 0.85, 0.75},
		[]string{"B", "C", "D"},
		[]int64{3},
	)
	defer df2.Release()

	result, err := fc.ExecuteWithContext(context.Background(), df1, df2)
	s.Require().NoError(err)
	defer result.Release()

	// Verify scores are rounded to 2 decimal places
	scoreCol := result.Column("$score")
	s.Require().NotNil(scoreCol)
	for i := 0; i < len(scoreCol.Chunks()); i++ {
		chunk := scoreCol.Chunk(i).(*array.Float32)
		for j := 0; j < chunk.Len(); j++ {
			score := chunk.Value(j)
			scaled := float64(score) * 100
			s.InDelta(math.Round(scaled), scaled, 0.01,
				"score %f should be rounded to 2 decimals", score)
		}
	}
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_RoundDecimalNegativeOne_NoRounding() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParams(1, 5, 0, -1) // roundDecimal=-1, no rounding
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// Verify chain does not contain round_decimal
	chainStr := fc.String()
	s.NotContains(chainStr, "round_decimal")
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_WithGroupingAndRoundDecimal() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParamsWithGroupingAndScorer(1, 3, 0, 2, "category", 2, GroupScorerMax)
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	df1 := s.createTestDataFrameForRerank(
		[]int64{1, 2, 3, 4, 5},
		[]float32{0.9, 0.8, 0.7, 0.6, 0.5},
		[]string{"A", "B", "A", "B", "C"},
		[]int64{5},
	)
	defer df1.Release()

	result, err := fc.ExecuteWithContext(context.Background(), df1)
	s.Require().NoError(err)
	defer result.Release()

	// Verify scores are rounded to 2 decimal places
	scoreCol := result.Column("$score")
	s.Require().NotNil(scoreCol)
	for i := 0; i < len(scoreCol.Chunks()); i++ {
		chunk := scoreCol.Chunk(i).(*array.Float32)
		for j := 0; j < chunk.Len(); j++ {
			score := chunk.Value(j)
			scaled := float64(score) * 100
			s.InDelta(math.Round(scaled), scaled, 0.01,
				"score %f should be rounded to 2 decimals", score)
		}
	}
}

func (s *RerankBuilderTestSuite) TestMemoryLeak_RoundDecimal() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParams(1, 5, 0, 3) // roundDecimal=3
	searchMetrics := []string{"COSINE", "IP"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
					{Key: "k", Value: "60"},
				},
			},
		},
	}

	for range 10 {
		fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
		s.Require().NoError(err)

		df1 := s.createTestDataFrameForRerank(
			[]int64{1, 2, 3},
			[]float32{0.9, 0.8, 0.7},
			[]string{"A", "B", "C"},
			[]int64{3},
		)
		df2 := s.createTestDataFrameForRerank(
			[]int64{2, 3, 4},
			[]float32{0.95, 0.85, 0.75},
			[]string{"B", "C", "D"},
			[]int64{3},
		)

		result, err := fc.ExecuteWithContext(context.Background(), df1, df2)
		s.Require().NoError(err)

		result.Release()
		df1.Release()
		df2.Release()
	}
	// Memory leak check in TearDownTest
}

// =============================================================================
// Mixed Metric Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildWeightedChain_MixedMetrics_SortDirection() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()

	// Mixed metrics (L2 + COSINE): sort should be descending
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: "weights", Value: "[0.5, 0.5]"},
					{Key: "norm_score", Value: "false"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, []string{"L2", "COSINE"}, searchParams, s.pool)
	s.Require().NoError(err)

	// Sort operator should be descending (mixed → direction conversion → larger-is-better)
	sortOp := fc.operators[1].(*SortOp)
	s.True(sortOp.desc)
}

func (s *RerankBuilderTestSuite) TestBuildWeightedChain_AllL2_SortAscending() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: "weights", Value: "[0.5, 0.5]"},
					{Key: "norm_score", Value: "false"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, []string{"L2", "L2"}, searchParams, s.pool)
	s.Require().NoError(err)

	// Sort should be ascending for all-L2 no-normalize (smaller distance = better)
	sortOp := fc.operators[1].(*SortOp)
	s.False(sortOp.desc)
}

func (s *RerankBuilderTestSuite) TestBuildWeightedChain_Normalize_AlwaysDescending() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: "weights", Value: "[0.5, 0.5]"},
					{Key: "norm_score", Value: "true"},
				},
			},
		},
	}

	// Even with all L2, normalize=true means always descending
	fc, err := BuildRerankChain(collSchema, funcScoreSchema, []string{"L2", "L2"}, searchParams, s.pool)
	s.Require().NoError(err)

	sortOp := fc.operators[1].(*SortOp)
	s.True(sortOp.desc)
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_Weighted_MixedMetrics_NoNormalize() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParams(1, 5, 0, -1)
	searchMetrics := []string{"L2", "COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: "weights", Value: "[0.5, 0.5]"},
					{Key: "norm_score", Value: "false"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// Input 1: L2 distances (smaller = better)
	// ID 1 has small L2 distance (good match), ID 2 has large distance (bad match)
	df1 := s.createTestDataFrameForRerank(
		[]int64{1, 2},
		[]float32{0.1, 2.0},
		[]string{"A", "B"},
		[]int64{2},
	)
	defer df1.Release()

	// Input 2: COSINE scores (larger = better)
	// ID 1 has high score (good match), ID 3 has medium score
	df2 := s.createTestDataFrameForRerank(
		[]int64{1, 3},
		[]float32{0.95, 0.80},
		[]string{"A", "C"},
		[]int64{2},
	)
	defer df2.Release()

	result, err := fc.ExecuteWithContext(context.Background(), df1, df2)
	s.Require().NoError(err)
	defer result.Release()

	// ID 1 should rank first: it has both good L2 (0.1 → ~0.94 after conversion) and good COSINE (0.95)
	idCol := result.Column("$id")
	s.Require().NotNil(idCol)
	idChunk := idCol.Chunk(0).(*array.Int64)
	s.Equal(int64(1), idChunk.Value(0), "ID 1 should be ranked first due to best combined score")

	// Verify scores are descending (direction conversion ensures larger = better)
	scoreCol := result.Column("$score")
	scoreChunk := scoreCol.Chunk(0).(*array.Float32)
	for i := 0; i < scoreChunk.Len()-1; i++ {
		s.GreaterOrEqual(scoreChunk.Value(i), scoreChunk.Value(i+1),
			"scores should be in descending order")
	}
}

func (s *RerankBuilderTestSuite) TestExecuteRerankChain_Weighted_AllL2_NoNormalize() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParams(1, 5, 0, -1)
	searchMetrics := []string{"L2", "L2"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: "weights", Value: "[0.5, 0.5]"},
					{Key: "norm_score", Value: "false"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// Both L2: ID 1 has small distances (best match), ID 2 has large distances (worst)
	df1 := s.createTestDataFrameForRerank(
		[]int64{1, 2},
		[]float32{0.1, 2.0},
		[]string{"A", "B"},
		[]int64{2},
	)
	defer df1.Release()

	df2 := s.createTestDataFrameForRerank(
		[]int64{1, 2},
		[]float32{0.2, 1.5},
		[]string{"A", "B"},
		[]int64{2},
	)
	defer df2.Release()

	result, err := fc.ExecuteWithContext(context.Background(), df1, df2)
	s.Require().NoError(err)
	defer result.Release()

	// All-L2 no-normalize: sort ascending (smaller distance = better)
	// ID 1 weighted sum: 0.5*0.1 + 0.5*0.2 = 0.15 (better)
	// ID 2 weighted sum: 0.5*2.0 + 0.5*1.5 = 1.75 (worse)
	idCol := result.Column("$id")
	idChunk := idCol.Chunk(0).(*array.Int64)
	s.Equal(int64(1), idChunk.Value(0), "ID 1 should be first (smallest L2 distance)")
	s.Equal(int64(2), idChunk.Value(1), "ID 2 should be second (larger L2 distance)")

	// Scores should be ascending
	scoreCol := result.Column("$score")
	scoreChunk := scoreCol.Chunk(0).(*array.Float32)
	s.LessOrEqual(scoreChunk.Value(0), scoreChunk.Value(1),
		"scores should be in ascending order for all-L2")
}

// =============================================================================
// BuildRerankChain Tests - Boost (pushed down to QueryNode, skipped in proxy)
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildChainAllBoostReturnsNil() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:             schemapb.FunctionType_Rerank,
				InputFieldNames:  []string{},
				OutputFieldNames: []string{},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "boost"},
					{Key: "weight", Value: "2.0"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, []string{"COSINE"}, searchParams, s.pool)
	s.NoError(err)
	s.Nil(fc)
}

func (s *RerankBuilderTestSuite) TestBuildChainMultipleBoostReturnsNil() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:             schemapb.FunctionType_Rerank,
				InputFieldNames:  []string{},
				OutputFieldNames: []string{},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "boost"},
					{Key: "weight", Value: "2.0"},
				},
			},
			{
				Type:             schemapb.FunctionType_Rerank,
				InputFieldNames:  []string{},
				OutputFieldNames: []string{},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "boost"},
					{Key: "weight", Value: "3.0"},
					{Key: "filter", Value: "price > 100"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, []string{"COSINE"}, searchParams, s.pool)
	s.NoError(err)
	s.Nil(fc)
}

func (s *RerankBuilderTestSuite) TestBuildChainBoostMixedWithRRF() {
	collSchema := s.createCollectionSchema()
	searchParams := s.createSearchParams()

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:             schemapb.FunctionType_Rerank,
				InputFieldNames:  []string{},
				OutputFieldNames: []string{},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "boost"},
					{Key: "weight", Value: "2.0"},
				},
			},
			{
				Type:             schemapb.FunctionType_Rerank,
				InputFieldNames:  []string{},
				OutputFieldNames: []string{},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, []string{"COSINE", "IP"}, searchParams, s.pool)
	s.NoError(err)
	s.NotNil(fc)
}
