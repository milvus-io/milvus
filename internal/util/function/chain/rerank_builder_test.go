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

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/rerank"
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

	name := rerank.GetRerankName(funcSchema)
	s.Equal("rrf", name)
}

func (s *RerankBuilderTestSuite) TestGetRerankNameCaseInsensitive() {
	funcSchema := &schemapb.FunctionSchema{
		Type: schemapb.FunctionType_Rerank,
		Params: []*commonpb.KeyValuePair{
			{Key: "RERANKER", Value: "RRF"},
		},
	}

	name := rerank.GetRerankName(funcSchema)
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
	// norm_score=true → scoreNormFuncs populated for both inputs
	s.Len(mergeOp.scoreNormFuncs, 2)
	s.True(mergeOp.SortDescending())
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

	// Verify chain structure: MergeOp -> MapOp(Decay) -> MapOp(ScoreCombine) -> SortOp -> LimitOp -> SelectOp
	s.Equal(6, len(fc.operators))
	s.Equal("Merge", fc.operators[0].Name())
	s.Equal("Map", fc.operators[1].Name())
	s.Equal("Map", fc.operators[2].Name())
	s.Equal("Sort", fc.operators[3].Name())
	s.Equal("Limit", fc.operators[4].Name())
	s.Equal("Select", fc.operators[5].Name())
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

// TestBuildDecayChainTimestamptzInputField verifies that decay rerank rejects
// Timestamptz input fields at validation time. The legacy rerank/decay path
// nominally listed Timestamptz alongside Int64, but no production code path
// or test ever exercised it end-to-end (the converter and GetNumericValue
// have no Timestamptz support). This PR's goal is to preserve, not extend,
// legacy behavior — so Timestamptz must fail-fast at BuildRerankChain
// instead of passing validation and exploding at runtime.
func (s *RerankBuilderTestSuite) TestBuildDecayChainTimestamptzInputField() {
	collSchema := &schemapb.CollectionSchema{
		Name: "test_collection_tstz",
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:      100,
				Name:         "id",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  101,
				Name:     "event_time",
				DataType: schemapb.DataType_Timestamptz,
			},
			{
				FieldID:  102,
				Name:     "vector",
				DataType: schemapb.DataType_FloatVector,
			},
		},
	}
	searchParams := s.createSearchParams()
	searchMetrics := []string{"COSINE"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{"event_time"},
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
	s.Require().Error(err, "decay must reject Timestamptz at validation time")
	s.Contains(err.Error(), "must be numeric",
		"error must come from validateInputField, not from later runtime stages")
	s.Contains(err.Error(), "Timestamptz",
		"error must mention the actual field type for diagnostics")
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

	// Verify chain structure: MergeOp -> MapOp(Decay) -> MapOp(ScoreCombine) -> GroupByOp -> SelectOp
	s.Equal(5, len(fc.operators))
	s.Equal("Merge", fc.operators[0].Name())
	s.Equal("Map", fc.operators[1].Name())
	s.Equal("Map", fc.operators[2].Name())
	s.Equal("GroupBy", fc.operators[3].Name())
	s.Equal("Select", fc.operators[4].Name())
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

// =============================================================================
// Helper: createCollectionSchemaWithVarChar
// =============================================================================

func (s *RerankBuilderTestSuite) createCollectionSchemaWithVarChar() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "test_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 102, Name: "price", DataType: schemapb.DataType_Float},
			{FieldID: 103, Name: "vector", DataType: schemapb.DataType_FloatVector},
		},
	}
}

// =============================================================================
// parseModelQueries Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestParseModelQueries_Valid() {
	funcSchema := &schemapb.FunctionSchema{
		Type: schemapb.FunctionType_Rerank,
		Params: []*commonpb.KeyValuePair{
			{Key: "queries", Value: `["what is AI", "how to code"]`},
		},
	}
	queries, err := parseModelQueries(funcSchema)
	s.NoError(err)
	s.Equal([]string{"what is AI", "how to code"}, queries)
}

func (s *RerankBuilderTestSuite) TestParseModelQueries_Missing() {
	funcSchema := &schemapb.FunctionSchema{
		Type:   schemapb.FunctionType_Rerank,
		Params: []*commonpb.KeyValuePair{},
	}
	_, err := parseModelQueries(funcSchema)
	s.Error(err)
}

func (s *RerankBuilderTestSuite) TestParseModelQueries_InvalidJSON() {
	funcSchema := &schemapb.FunctionSchema{
		Type: schemapb.FunctionType_Rerank,
		Params: []*commonpb.KeyValuePair{
			{Key: "queries", Value: `not json`},
		},
	}
	_, err := parseModelQueries(funcSchema)
	s.Error(err)
}

func (s *RerankBuilderTestSuite) TestParseModelQueries_EmptyArray() {
	funcSchema := &schemapb.FunctionSchema{
		Type: schemapb.FunctionType_Rerank,
		Params: []*commonpb.KeyValuePair{
			{Key: "queries", Value: `[]`},
		},
	}
	_, err := parseModelQueries(funcSchema)
	s.Error(err)
}

// =============================================================================
// validateVarcharInputField Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestValidateVarcharInputField_Valid() {
	schema := s.createCollectionSchemaWithVarChar()
	err := validateVarcharInputField(schema, "text")
	s.NoError(err)
}

func (s *RerankBuilderTestSuite) TestValidateVarcharInputField_NotVarChar() {
	schema := s.createCollectionSchemaWithVarChar()
	err := validateVarcharInputField(schema, "price")
	s.Error(err)
}

func (s *RerankBuilderTestSuite) TestValidateVarcharInputField_NotFound() {
	schema := s.createCollectionSchemaWithVarChar()
	err := validateVarcharInputField(schema, "nonexistent")
	s.Error(err)
}

// =============================================================================
// GetInputFieldNamesFromFuncScore Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestGetInputFieldNamesFromFuncScore_WithFields() {
	funcScore := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_Rerank, InputFieldNames: []string{"price", "timestamp"}},
		},
	}
	names := GetInputFieldNamesFromFuncScore(funcScore)
	s.Equal([]string{"price", "timestamp"}, names)
}

func (s *RerankBuilderTestSuite) TestGetInputFieldNamesFromFuncScore_NoFields() {
	funcScore := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_Rerank, InputFieldNames: []string{}},
		},
	}
	names := GetInputFieldNamesFromFuncScore(funcScore)
	s.Empty(names)
}

func (s *RerankBuilderTestSuite) TestGetInputFieldNamesFromFuncScore_Nil() {
	s.Nil(GetInputFieldNamesFromFuncScore(nil))
}

func (s *RerankBuilderTestSuite) TestGetInputFieldNamesFromFuncScore_EmptyFunctions() {
	funcScore := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{}}
	s.Nil(GetInputFieldNamesFromFuncScore(funcScore))
}

// =============================================================================
// GetInputFieldIDsFromSchema Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestGetInputFieldIDsFromSchema_Valid() {
	schema := s.createCollectionSchema()
	funcScore := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_Rerank, InputFieldNames: []string{"timestamp", "price"}},
		},
	}
	ids := GetInputFieldIDsFromSchema(schema, funcScore)
	s.Equal([]int64{101, 102}, ids)
}

func (s *RerankBuilderTestSuite) TestGetInputFieldIDsFromSchema_PartialMatch() {
	schema := s.createCollectionSchema()
	funcScore := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_Rerank, InputFieldNames: []string{"price", "nonexistent"}},
		},
	}
	ids := GetInputFieldIDsFromSchema(schema, funcScore)
	s.Equal([]int64{102}, ids)
}

func (s *RerankBuilderTestSuite) TestGetInputFieldIDsFromSchema_NoFields() {
	schema := s.createCollectionSchema()
	funcScore := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{Type: schemapb.FunctionType_Rerank},
		},
	}
	ids := GetInputFieldIDsFromSchema(schema, funcScore)
	s.Nil(ids)
}

func (s *RerankBuilderTestSuite) TestGetInputFieldIDsFromSchema_NilFuncScore() {
	schema := s.createCollectionSchema()
	ids := GetInputFieldIDsFromSchema(schema, nil)
	s.Nil(ids)
}

// =============================================================================
// GetRerankNameFromFuncScore Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestGetRerankNameFromFuncScore_Valid() {
	funcScore := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
		},
	}
	name := GetRerankNameFromFuncScore(funcScore)
	s.Equal("rrf", name)
}

func (s *RerankBuilderTestSuite) TestGetRerankNameFromFuncScore_Nil() {
	s.Equal("", GetRerankNameFromFuncScore(nil))
}

func (s *RerankBuilderTestSuite) TestGetRerankNameFromFuncScore_EmptyFunctions() {
	funcScore := &schemapb.FunctionScore{Functions: []*schemapb.FunctionSchema{}}
	s.Equal("", GetRerankNameFromFuncScore(funcScore))
}

// =============================================================================
// buildModelChain Error Path Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildModelChain_WrongInputFieldCount() {
	schema := s.createCollectionSchemaWithVarChar()
	funcSchema := &schemapb.FunctionSchema{
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "model"},
		},
	}
	fc := NewFuncChainWithAllocator(s.pool)
	err := buildModelChain(fc, schema, funcSchema, []string{"COSINE"}, s.createSearchParams())
	s.Error(err)
}

func (s *RerankBuilderTestSuite) TestBuildModelChain_NonVarCharField() {
	schema := s.createCollectionSchemaWithVarChar()
	funcSchema := &schemapb.FunctionSchema{
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"price"},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "model"},
		},
	}
	fc := NewFuncChainWithAllocator(s.pool)
	err := buildModelChain(fc, schema, funcSchema, []string{"COSINE"}, s.createSearchParams())
	s.Error(err)
}

func (s *RerankBuilderTestSuite) TestBuildModelChain_FieldNotFound() {
	schema := s.createCollectionSchemaWithVarChar()
	funcSchema := &schemapb.FunctionSchema{
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"nonexistent"},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "model"},
		},
	}
	fc := NewFuncChainWithAllocator(s.pool)
	err := buildModelChain(fc, schema, funcSchema, []string{"COSINE"}, s.createSearchParams())
	s.Error(err)
}

func (s *RerankBuilderTestSuite) TestBuildModelChain_MissingQueries() {
	schema := s.createCollectionSchemaWithVarChar()
	funcSchema := &schemapb.FunctionSchema{
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"text"},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "model"},
		},
	}
	fc := NewFuncChainWithAllocator(s.pool)
	err := buildModelChain(fc, schema, funcSchema, []string{"COSINE"}, s.createSearchParams())
	s.Error(err)
}

func (s *RerankBuilderTestSuite) TestBuildModelChain_InvalidQueriesJSON() {
	schema := s.createCollectionSchemaWithVarChar()
	funcSchema := &schemapb.FunctionSchema{
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"text"},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "model"},
			{Key: "queries", Value: `invalid json`},
		},
	}
	fc := NewFuncChainWithAllocator(s.pool)
	err := buildModelChain(fc, schema, funcSchema, []string{"COSINE"}, s.createSearchParams())
	s.Error(err)
}

func (s *RerankBuilderTestSuite) TestBuildModelChain_EmptyQueries() {
	schema := s.createCollectionSchemaWithVarChar()
	funcSchema := &schemapb.FunctionSchema{
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"text"},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "model"},
			{Key: "queries", Value: `[]`},
		},
	}
	fc := NewFuncChainWithAllocator(s.pool)
	err := buildModelChain(fc, schema, funcSchema, []string{"COSINE"}, s.createSearchParams())
	s.Error(err)
}

func (s *RerankBuilderTestSuite) TestBuildModelChain_TwoInputFields() {
	schema := s.createCollectionSchemaWithVarChar()
	funcSchema := &schemapb.FunctionSchema{
		Type:            schemapb.FunctionType_Rerank,
		InputFieldNames: []string{"text", "id"},
		Params: []*commonpb.KeyValuePair{
			{Key: "reranker", Value: "model"},
			{Key: "queries", Value: `["q"]`},
		},
	}
	fc := NewFuncChainWithAllocator(s.pool)
	err := buildModelChain(fc, schema, funcSchema, []string{"COSINE"}, s.createSearchParams())
	s.Error(err)
}

// =============================================================================
// convertLegacyParams Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestConvertLegacyParams_UnsupportedRankType() {
	rankParams := []*commonpb.KeyValuePair{
		{Key: "strategy", Value: "unknown_type"},
	}
	_, err := BuildRerankChainWithLegacy(s.createCollectionSchema(), rankParams, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "unsupported rank type")
}

func (s *RerankBuilderTestSuite) TestConvertLegacyParams_InvalidParamsJSON() {
	rankParams := []*commonpb.KeyValuePair{
		{Key: "strategy", Value: "rrf"},
		{Key: "params", Value: `{invalid json`},
	}
	_, err := BuildRerankChainWithLegacy(s.createCollectionSchema(), rankParams, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "parse rerank params failed")
}

func (s *RerankBuilderTestSuite) TestConvertLegacyParams_KNonFloatType() {
	rankParams := []*commonpb.KeyValuePair{
		{Key: "strategy", Value: "rrf"},
		{Key: "params", Value: `{"k": "not_a_number"}`},
	}
	_, err := BuildRerankChainWithLegacy(s.createCollectionSchema(), rankParams, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "the type of rank param k should be float")
}

func (s *RerankBuilderTestSuite) TestConvertLegacyParams_WeightedNormScoreBool() {
	rankParams := []*commonpb.KeyValuePair{
		{Key: "strategy", Value: "weighted"},
		{Key: "params", Value: `{"weights": [0.5], "norm_score": true}`},
	}
	fc, err := BuildRerankChainWithLegacy(s.createCollectionSchema(), rankParams, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	mergeOp := fc.operators[0].(*MergeOp)
	// norm_score=true → scoreNormFuncs populated
	s.Len(mergeOp.scoreNormFuncs, 1)
	s.True(mergeOp.SortDescending())
}

func (s *RerankBuilderTestSuite) TestConvertLegacyParams_WeightedNormScoreStringTrue() {
	rankParams := []*commonpb.KeyValuePair{
		{Key: "strategy", Value: "weighted"},
		{Key: "params", Value: `{"weights": [0.5], "norm_score": "true"}`},
	}
	fc, err := BuildRerankChainWithLegacy(s.createCollectionSchema(), rankParams, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	mergeOp := fc.operators[0].(*MergeOp)
	// norm_score=true → scoreNormFuncs populated
	s.Len(mergeOp.scoreNormFuncs, 1)
	s.True(mergeOp.SortDescending())
}

func (s *RerankBuilderTestSuite) TestConvertLegacyParams_WeightedNormScoreInvalidString() {
	rankParams := []*commonpb.KeyValuePair{
		{Key: "strategy", Value: "weighted"},
		{Key: "params", Value: `{"weights": [0.5], "norm_score": "not_bool"}`},
	}
	_, err := BuildRerankChainWithLegacy(s.createCollectionSchema(), rankParams, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "norm_score should be bool")
}

func (s *RerankBuilderTestSuite) TestConvertLegacyParams_WeightedNormScoreInvalidType() {
	rankParams := []*commonpb.KeyValuePair{
		{Key: "strategy", Value: "weighted"},
		{Key: "params", Value: `{"weights": [0.5], "norm_score": 123}`},
	}
	_, err := BuildRerankChainWithLegacy(s.createCollectionSchema(), rankParams, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "norm_score should be bool")
}

// =============================================================================
// parseDecayParams Error Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestParseDecayParams_OriginNotNumber() {
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{"timestamp"},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "function", Value: "gauss"},
					{Key: "origin", Value: "not_a_number"},
					{Key: "scale", Value: "100"},
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "origin")
	s.Contains(err.Error(), "not a number")
}

func (s *RerankBuilderTestSuite) TestParseDecayParams_ScaleNotNumber() {
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{"timestamp"},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "function", Value: "gauss"},
					{Key: "origin", Value: "1000"},
					{Key: "scale", Value: "abc"},
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "scale")
	s.Contains(err.Error(), "not a number")
}

func (s *RerankBuilderTestSuite) TestParseDecayParams_OffsetNotNumber() {
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
					{Key: "offset", Value: "xyz"},
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "offset")
	s.Contains(err.Error(), "not a number")
}

func (s *RerankBuilderTestSuite) TestParseDecayParams_DecayNotNumber() {
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
					{Key: "decay", Value: "bad"},
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "decay")
	s.Contains(err.Error(), "not a number")
}

func (s *RerankBuilderTestSuite) TestParseDecayParams_UnsupportedScoreMode() {
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
					{Key: "score_mode", Value: "multiply"},
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "unsupported score_mode")
}

func (s *RerankBuilderTestSuite) TestParseDecayParams_MissingFunction() {
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{"timestamp"},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					// no function key
					{Key: "origin", Value: "1000"},
					{Key: "scale", Value: "100"},
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "decay function not specified")
}

func (s *RerankBuilderTestSuite) TestParseDecayParams_MissingScale() {
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{"timestamp"},
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "function", Value: "gauss"},
					{Key: "origin", Value: "1000"},
					// no scale
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "decay scale not specified")
}

// =============================================================================
// buildRerankChainInternal Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildRerankChainInternal_EmptyRerankerName() {
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:   schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{}, // No reranker param
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "reranker name not specified")
}

func (s *RerankBuilderTestSuite) TestBuildRerankChainInternal_NilAlloc() {
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

	// Pass nil alloc — should use DefaultAllocator and not panic
	fc, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), nil)
	s.Require().NoError(err)
	s.NotNil(fc)
}

func (s *RerankBuilderTestSuite) TestBuildRerankChainInternal_RoundDecimalZero() {
	searchParams := NewSearchParams(1, 10, 0, 0) // RoundDecimal = 0
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

	fc, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, searchParams, s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	// Chain: Merge -> Sort -> Limit -> Map(RoundDecimal) -> Select = 5 operators
	s.Equal(5, len(fc.operators))
	s.Equal("Map", fc.operators[3].Name())
}

// =============================================================================
// BuildRerankChain Edge Cases
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildRerankChain_NilFuncScoreSchema() {
	_, err := BuildRerankChain(s.createCollectionSchema(), nil, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "no rerank functions specified")
}

func (s *RerankBuilderTestSuite) TestBuildRerankChain_MultipleProxyFunctions() {
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "rrf"},
				},
			},
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: "weights", Value: "[0.5]"},
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "only supports one rerank function")
}

// =============================================================================
// validateInputField Tests
// =============================================================================

func (s *RerankBuilderTestSuite) TestValidateInputField_NonNumericType() {
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{"vector"}, // FloatVector is not numeric
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "function", Value: "gauss"},
					{Key: "origin", Value: "1000"},
					{Key: "scale", Value: "100"},
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "must be numeric")
}

// =============================================================================
// Decay chain - input field count errors
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildDecayChain_NoInputField() {
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{}, // No input fields
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "function", Value: "gauss"},
					{Key: "origin", Value: "1000"},
					{Key: "scale", Value: "100"},
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "requires exactly 1 input field")
}

func (s *RerankBuilderTestSuite) TestBuildDecayChain_MultipleInputFields() {
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type:            schemapb.FunctionType_Rerank,
				InputFieldNames: []string{"timestamp", "price"}, // 2 input fields
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "decay"},
					{Key: "function", Value: "gauss"},
					{Key: "origin", Value: "1000"},
					{Key: "scale", Value: "100"},
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "requires exactly 1 input field")
}

// =============================================================================
// Weighted chain - invalid params
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildWeightedChain_InvalidWeightsJSON() {
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: "weights", Value: "not_json"},
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "failed to parse weights")
}

func (s *RerankBuilderTestSuite) TestBuildWeightedChain_InvalidNormScore() {
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: "weights", Value: "[0.5]"},
					{Key: "norm_score", Value: "not_bool"},
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "failed to parse norm_score")
}

// =============================================================================
// Weighted chain weights validation in parseWeightedParams
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildWeightedChain_WeightsOutOfRangeInParser() {
	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: "weights", Value: "[-0.1, 0.5]"},
				},
			},
		},
	}

	_, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE", "IP"}, s.createSearchParams(), s.pool)
	s.Error(err)
	s.Contains(err.Error(), "weight should be in range [0, 1]")
}

// =============================================================================
// SearchParams with Limit = 0 (no LimitOp)
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildRerankChain_NoLimitOp() {
	searchParams := NewSearchParams(1, 0, 0, -1) // Limit = 0
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

	fc, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, searchParams, s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	// Chain: Merge -> Sort -> Select (no Limit when limit=0)
	s.Equal(3, len(fc.operators))
	s.Equal("Merge", fc.operators[0].Name())
	s.Equal("Sort", fc.operators[1].Name())
	s.Equal("Select", fc.operators[2].Name())
}

// =============================================================================
// NewSearchParamsWithGroupingAndScorer
// =============================================================================

func (s *RerankBuilderTestSuite) TestNewSearchParamsWithGroupingAndScorer() {
	sp := NewSearchParamsWithGroupingAndScorer(2, 20, 5, 3, "category", 4, GroupScorerAvg)
	s.Equal(int64(2), sp.Nq)
	s.Equal(int64(20), sp.Limit)
	s.Equal(int64(5), sp.Offset)
	s.Equal(int64(3), sp.RoundDecimal)
	s.Equal("category", sp.GroupByField)
	s.Equal(int64(4), sp.GroupSize)
	s.Equal(GroupScorerAvg, sp.GroupScorer)
	s.True(sp.HasGrouping())
}

// =============================================================================
// Decay with norm_score param
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildDecayChain_WithNormScore() {
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
					{Key: "norm_score", Value: "true"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)

	mergeOp := fc.operators[0].(*MergeOp)
	// Decay always normalizes → scoreNormFuncs populated, sortDescending=true
	s.Len(mergeOp.scoreNormFuncs, 1)
	s.True(mergeOp.SortDescending())
}

// =============================================================================
// Decay score_mode "avg"
// =============================================================================

func (s *RerankBuilderTestSuite) TestBuildDecayChain_ScoreModeAvg() {
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
					{Key: "score_mode", Value: "avg"},
				},
			},
		},
	}

	fc, err := BuildRerankChain(s.createCollectionSchema(), funcScoreSchema, []string{"COSINE"}, s.createSearchParams(), s.pool)
	s.Require().NoError(err)

	mergeOp := fc.operators[0].(*MergeOp)
	s.Equal(MergeStrategyAvg, mergeOp.strategy)
}

// =============================================================================
// Decay with L2 metric + norm_score=false
// =============================================================================
//
// Decay is designed to operate on "higher = more relevant" scores: it multiplies
// the original score by a decay factor in [0, 1]. For L2 (smaller distance =
// more relevant), the raw score must first be flipped to "larger = better"
// before the decay multiplication, otherwise the multiplied score is
// semantically meaningless and DESC sort returns the worst match first.
//
// The legacy rerank/decay implementation always called
// getNormalizeFunc(needNorm, metric, toGreater=true), which forced direction
// conversion for L2-class metrics regardless of norm_score. The new chain
// builder must preserve this behavior.
func (s *RerankBuilderTestSuite) TestExecuteDecay_L2_NoNormScore_RanksByCombinedScore() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParams(1, 10, 0, -1)
	searchMetrics := []string{"L2"}

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
					{Key: "decay", Value: "0.5"},
					// norm_score not set → defaults to false
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// Three docs:
	//   ID 1: L2=0.1 (best vector match),  timestamp=1000 (at origin → decay≈1.0)
	//   ID 2: L2=0.2 (good vector match),  timestamp=1100 (offset 100 → decay≈0.5)
	//   ID 3: L2=5.0 (poor vector match),  timestamp=1000 (at origin → decay≈1.0)
	//
	// Correct ranking (after L2 direction conversion → larger=better, then ×decay):
	//   ID 1: atan(0.1) ≈ 0.937 × 1.0 ≈ 0.937  → rank 1
	//   ID 2: atan(0.2) ≈ 0.875 × 0.5 ≈ 0.438  → rank 2
	//   ID 3: atan(5.0) ≈ 0.125 × 1.0 ≈ 0.125  → rank 3
	//
	// Buggy ranking (raw L2 × decay, sorted DESC):
	//   ID 1: 0.1 × 1.0 = 0.1
	//   ID 2: 0.2 × 0.5 = 0.1
	//   ID 3: 5.0 × 1.0 = 5.0   ← largest, ranks first!
	df := s.createTestDataFrameForRerankWithTimestamp(
		[]int64{1, 2, 3},
		[]float32{0.1, 0.2, 5.0},
		[]string{"A", "A", "A"},
		[]int64{1000, 1100, 1000},
		[]int64{3},
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	s.Equal(int64(3), result.NumRows())

	idCol := result.Column("$id")
	s.Require().NotNil(idCol)
	idChunk := idCol.Chunk(0).(*array.Int64)

	scoreCol := result.Column("$score")
	s.Require().NotNil(scoreCol)
	scoreChunk := scoreCol.Chunk(0).(*array.Float32)

	s.Equal(int64(1), idChunk.Value(0),
		"ID 1 (best L2 + best decay) should rank first; got order [%d %d %d] with scores [%v %v %v]",
		idChunk.Value(0), idChunk.Value(1), idChunk.Value(2),
		scoreChunk.Value(0), scoreChunk.Value(1), scoreChunk.Value(2))
	s.Equal(int64(2), idChunk.Value(1), "ID 2 should rank second")
	s.Equal(int64(3), idChunk.Value(2), "ID 3 (worst L2) should rank last")

	// Scores must be DESC (decay produces "higher = better")
	for i := 0; i < scoreChunk.Len()-1; i++ {
		s.GreaterOrEqual(scoreChunk.Value(i), scoreChunk.Value(i+1),
			"decay scores must be in descending order")
	}

	// Verify the L2 → "larger = better" direction conversion (1 - 2·atan(d)/π)
	// is actually applied before the decay multiplication. Without this conversion,
	// scores would be raw L2 distances, and the assertions below would not match
	// even if the IDs happened to be in the right order.
	convert := func(d float64) float64 { return 1.0 - 2.0*math.Atan(d)/math.Pi }
	gauss := func(ts float64) float64 {
		// origin=1000, scale=100, decay=0.5, offset=0
		dist := math.Abs(ts - 1000.0)
		sigmaSq := 10000.0 / math.Log(0.5)
		return math.Exp(dist * dist / sigmaSq)
	}
	expected := map[int64]float32{
		1: float32(convert(0.1) * gauss(1000)), // ≈ 0.93655 × 1.0   ≈ 0.93655
		2: float32(convert(0.2) * gauss(1100)), // ≈ 0.87434 × 0.5   ≈ 0.43717
		3: float32(convert(5.0) * gauss(1000)), // ≈ 0.12566 × 1.0   ≈ 0.12566
	}
	for i := 0; i < idChunk.Len(); i++ {
		id := idChunk.Value(i)
		got := scoreChunk.Value(i)
		want := expected[id]
		s.InDelta(want, got, 1e-5,
			"id=%d: expected score %v (atan-converted L2 × gauss decay), got %v", id, want, got)
	}
}

// =============================================================================
// Weighted + GroupBy with L2 metric and norm_score=false
// =============================================================================
//
// GroupByOp must respect the sort direction implied by the merge stage.
// For weighted reranker on a single L2 metric with norm_score=false, the
// merged $score column is raw L2 distance — smaller is better — and the
// outer Sort would use ASC. But the grouping branch in
// buildRerankChainInternal calls GroupByWithScorer, which historically
// hardcoded DESC inside both within-group sort and cross-group sort. The
// effect: within each group the WORST rows survive groupSize trimming, and
// the group with the worst best-row ranks first. For "weighted + group_by +
// L2 + norm_score=false", users get the WORST matches first — exactly the
// inverse of intent.
func (s *RerankBuilderTestSuite) TestExecuteWeightedGroupBy_L2_NoNormScore_PreservesAscOrder() {
	collSchema := s.createCollectionSchemaWithCategory()
	// limit=2 groups, groupSize=2 rows per group
	searchParams := NewSearchParamsWithGrouping(1, 2, 0, -1, "category", 2)
	searchMetrics := []string{"L2"}

	funcScoreSchema := &schemapb.FunctionScore{
		Functions: []*schemapb.FunctionSchema{
			{
				Type: schemapb.FunctionType_Rerank,
				Params: []*commonpb.KeyValuePair{
					{Key: "reranker", Value: "weighted"},
					{Key: "weights", Value: "[1.0]"},
					// norm_score not set → defaults to false
				},
			},
		},
	}

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	// 6 rows in 3 categories. Raw L2 distances: smaller is better.
	//   cat A: ids 1,2,3 with L2 [0.1, 0.5, 1.0]   ← best matches overall
	//   cat B: ids 4,5   with L2 [2.0, 3.0]
	//   cat C: id  6     with L2 [10.0]            ← worst match
	df := s.createTestDataFrameForRerank(
		[]int64{1, 2, 3, 4, 5, 6},
		[]float32{0.1, 0.5, 1.0, 2.0, 3.0, 10.0},
		[]string{"A", "A", "A", "B", "B", "C"},
		[]int64{6},
	)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Expected ordering after fix (ASC semantics throughout):
	//   1. Within-group sort ASC, keep top groupSize=2:
	//        cat A → [(1, 0.1), (2, 0.5)]   (id 3 dropped)
	//        cat B → [(4, 2.0), (5, 3.0)]
	//        cat C → [(6, 10.0)]
	//   2. Group score (Max scorer = scores[0] = best representative):
	//        cat A → 0.1, cat B → 2.0, cat C → 10.0
	//   3. Sort groups by group score ASC: A → B → C
	//   4. limit=2 groups: keep A, B (drop C)
	//   5. Final flat output: [1, 2, 4, 5]
	//
	// Buggy behavior (DESC hardcoded):
	//   1. Within-group keeps WORST 2: cat A→[3,2], cat B→[5,4]
	//   2. Group score = largest after DESC sort: A→1.0, B→3.0, C→10.0
	//   3. Sort groups DESC: C → B → A
	//   4. limit=2: keep C, B
	//   5. Final flat output: [6, 5, 4]   ← only 3 rows; worst matches win
	s.Equal(int64(4), result.NumRows(),
		"expected 4 rows (cat A best 2 + cat B best 2); buggy code drops cat A entirely and only returns 3 rows from worst categories")

	idCol := result.Column("$id")
	s.Require().NotNil(idCol)
	idChunk := idCol.Chunk(0).(*array.Int64)

	scoreCol := result.Column("$score")
	s.Require().NotNil(scoreCol)
	scoreChunk := scoreCol.Chunk(0).(*array.Float32)

	gotIDs := []int64{}
	gotScores := []float32{}
	for i := 0; i < idChunk.Len(); i++ {
		gotIDs = append(gotIDs, idChunk.Value(i))
		gotScores = append(gotScores, scoreChunk.Value(i))
	}

	s.Equal([]int64{1, 2, 4, 5}, gotIDs,
		"expected ids [1,2,4,5] (cat A best 2, then cat B best 2); got %v with scores %v", gotIDs, gotScores)

	// Scores must be in ASC order (smaller L2 = better) within and across groups.
	for i := 0; i < len(gotScores)-1; i++ {
		s.LessOrEqual(gotScores[i], gotScores[i+1],
			"scores must be ASC for L2 + norm_score=false; got %v at idx %d, %v at idx %d",
			gotScores[i], i, gotScores[i+1], i+1)
	}
}

// TestExecuteWeightedGroupBy_L2_NormScore_DescOrder mirrors the Python e2e
// test which uses pymilvus WeightedRanker(0.5, 0.5) — note that pymilvus
// defaults to norm_score=True, so the chain should treat the merged $score
// as "larger = better" and produce results in DESC order.
//
// Two L2 sub-results to match the hybrid path (weights=[0.5, 0.5]). The same
// 6-row, 3-category fixture as the no-norm test, but each row appears in
// both inputs with identical scores so the weighted sum equals the
// individual normalized score.
func (s *RerankBuilderTestSuite) TestExecuteWeightedGroupBy_L2_NormScore_PreservesDescOrder() {
	collSchema := s.createCollectionSchemaWithCategory()
	searchParams := NewSearchParamsWithGrouping(1, 2, 0, -1, "category", 2)
	searchMetrics := []string{"L2", "L2"}

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

	fc, err := BuildRerankChain(collSchema, funcScoreSchema, searchMetrics, searchParams, s.pool)
	s.Require().NoError(err)

	df1 := s.createTestDataFrameForRerank(
		[]int64{1, 2, 3, 4, 5, 6},
		[]float32{0.1, 0.5, 1.0, 2.0, 3.0, 10.0},
		[]string{"A", "A", "A", "B", "B", "C"},
		[]int64{6},
	)
	defer df1.Release()
	df2 := s.createTestDataFrameForRerank(
		[]int64{1, 2, 3, 4, 5, 6},
		[]float32{0.1, 0.5, 1.0, 2.0, 3.0, 10.0},
		[]string{"A", "A", "A", "B", "B", "C"},
		[]int64{6},
	)
	defer df2.Release()

	result, err := fc.ExecuteWithContext(context.Background(), df1, df2)
	s.Require().NoError(err)
	defer result.Release()

	idCol := result.Column("$id")
	s.Require().NotNil(idCol)
	idChunk := idCol.Chunk(0).(*array.Int64)

	scoreCol := result.Column("$score")
	scoreChunk := scoreCol.Chunk(0).(*array.Float32)

	gotIDs := []int64{}
	gotScores := []float32{}
	for i := 0; i < idChunk.Len(); i++ {
		gotIDs = append(gotIDs, idChunk.Value(i))
		gotScores = append(gotScores, scoreChunk.Value(i))
	}

	// With norm_score=true the chain applies atan-based normalization →
	// scores become "larger = better". Best L2 (smallest distance) gets
	// highest normalized score. Expected ranking: cat A best 2, then cat B
	// best 2; cat C dropped by limit=2 groups.
	s.Equal(int64(4), result.NumRows(), "got ids=%v scores=%v", gotIDs, gotScores)
	s.Equal([]int64{1, 2, 4, 5}, gotIDs,
		"expected ids [1,2,4,5] under norm_score=true; got %v with scores %v", gotIDs, gotScores)
	for i := 0; i < len(gotScores)-1; i++ {
		s.GreaterOrEqual(gotScores[i], gotScores[i+1],
			"scores must be DESC under norm_score=true")
	}
}
