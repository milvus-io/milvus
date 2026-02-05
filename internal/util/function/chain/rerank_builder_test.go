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
	"testing"

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

	// Verify chain structure: MergeOp -> SortOp -> LimitOp
	s.Equal(3, len(fc.operators))
	s.Equal("Merge", fc.operators[0].Name())
	s.Equal("Sort", fc.operators[1].Name())
	s.Equal("Limit", fc.operators[2].Name())
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
	s.Contains(err.Error(), "weights count")
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

	// Verify chain structure: MergeOp -> MapOp(Decay) -> SortOp -> LimitOp
	s.Equal(4, len(fc.operators))
	s.Equal("Merge", fc.operators[0].Name())
	s.Equal("Map", fc.operators[1].Name())
	s.Equal("Sort", fc.operators[2].Name())
	s.Equal("Limit", fc.operators[3].Name())
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
