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

package expr_test

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/suite"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain"
	"github.com/milvus-io/milvus/internal/util/function/chain/expr"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

// =============================================================================
// Integration Test Suite
// =============================================================================

type DecayExprIntegrationTestSuite struct {
	suite.Suite
	pool *memory.CheckedAllocator
}

func (s *DecayExprIntegrationTestSuite) SetupTest() {
	s.pool = memory.NewCheckedAllocator(memory.NewGoAllocator())
}

func (s *DecayExprIntegrationTestSuite) TearDownTest() {
	s.pool.AssertSize(s.T(), 0)
}

func TestDecayExprIntegrationTestSuite(t *testing.T) {
	suite.Run(t, new(DecayExprIntegrationTestSuite))
}

// =============================================================================
// Helper Functions
// =============================================================================

func (s *DecayExprIntegrationTestSuite) createTestDataFrame(fieldType schemapb.DataType) *chain.DataFrame {
	var fieldData *schemapb.FieldData

	switch fieldType {
	case schemapb.DataType_Int64:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int64,
			FieldName: "distance",
			FieldId:   100,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_LongData{
						LongData: &schemapb.LongArray{
							Data: []int64{0, 50, 100, 150, 200, 0, 100, 200, 300},
						},
					},
				},
			},
		}
	case schemapb.DataType_Float:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Float,
			FieldName: "distance",
			FieldId:   100,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_FloatData{
						FloatData: &schemapb.FloatArray{
							Data: []float32{0, 50, 100, 150, 200, 0, 100, 200, 300},
						},
					},
				},
			},
		}
	case schemapb.DataType_Double:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Double,
			FieldName: "distance",
			FieldId:   100,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_DoubleData{
						DoubleData: &schemapb.DoubleArray{
							Data: []float64{0, 50, 100, 150, 200, 0, 100, 200, 300},
						},
					},
				},
			},
		}
	case schemapb.DataType_Int32:
		fieldData = &schemapb.FieldData{
			Type:      schemapb.DataType_Int32,
			FieldName: "distance",
			FieldId:   100,
			Field: &schemapb.FieldData_Scalars{
				Scalars: &schemapb.ScalarField{
					Data: &schemapb.ScalarField_IntData{
						IntData: &schemapb.IntArray{
							Data: []int32{0, 50, 100, 150, 200, 0, 100, 200, 300},
						},
					},
				},
			},
		}
	}

	resultData := &schemapb.SearchResultData{
		NumQueries: 2,
		TopK:       5,
		Topks:      []int64{5, 4},
		Scores:     []float32{1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0, 1.0},
		Ids: &schemapb.IDs{
			IdField: &schemapb.IDs_IntId{
				IntId: &schemapb.LongArray{
					Data: []int64{1, 2, 3, 4, 5, 6, 7, 8, 9},
				},
			},
		},
		FieldsData: []*schemapb.FieldData{fieldData},
	}

	df, err := chain.FromSearchResultData(resultData, s.pool)
	s.Require().NoError(err)
	return df
}

// =============================================================================
// Integration Tests
// =============================================================================

func (s *DecayExprIntegrationTestSuite) TestIntegration_ChainWithDecay() {
	df := s.createTestDataFrame(schemapb.DataType_Int64)
	defer df.Release()

	decayExpr, err := expr.NewDecayExpr(expr.GaussFunction, 100, 50, 0, 0.5)
	s.Require().NoError(err)

	// Create chain with decay -> sort -> limit
	// Column mapping is now at operator level
	result, err := chain.NewFuncChainWithAllocator(s.pool).
		SetStage(types.StageL2Rerank).
		Map(decayExpr, []string{"distance", types.ScoreFieldName}, []string{types.ScoreFieldName}).
		Sort(types.ScoreFieldName, true). // descending
		Limit(3).
		Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	// Should have 3 results per chunk
	s.Equal([]int64{3, 3}, result.ChunkSizes())
}

func (s *DecayExprIntegrationTestSuite) TestIntegration_ParseFromJSON() {
	// Column mapping is now at operator level (inputs/outputs)
	jsonRepr := `{
		"name": "decay-test",
		"stage": "L2_rerank",
		"operators": [{
			"type": "map",
			"inputs": ["distance", "$score"],
			"outputs": ["$score"],
			"function": {
				"name": "decay",
				"params": {
					"function": "gauss",
					"origin": 100,
					"scale": 50,
					"decay": 0.5
				}
			}
		}]
	}`

	fc, err := chain.ParseFuncChainRepr(jsonRepr, s.pool)
	s.Require().NoError(err)

	df := s.createTestDataFrame(schemapb.DataType_Int64)
	defer df.Release()

	result, err := fc.Execute(df)
	s.Require().NoError(err)
	defer result.Release()

	s.True(result.HasColumn(types.ScoreFieldName))
}

func (s *DecayExprIntegrationTestSuite) TestIntegration_ScoreCombine_ParseFromJSON() {
	jsonRepr := `{
		"name": "score-combine-test",
		"stage": "L2_rerank",
		"operators": [{
			"type": "map",
			"inputs": ["$score", "_func_score"],
			"outputs": ["$score"],
			"function": {
				"name": "score_combine",
				"params": {
					"input_count": 2,
					"mode": "multiply"
				}
			}
		}]
	}`

	fc, err := chain.ParseFuncChainRepr(jsonRepr, s.pool)
	s.Require().NoError(err)
	s.NotNil(fc)
}
