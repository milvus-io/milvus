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
	"math"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"

	"github.com/milvus-io/milvus/internal/util/function/chain/types"
)

func (s *GroupByOpTestSuite) nanGroupByFrame(ids []int64, scores []float32, categories []string) *DataFrame {
	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes([]int64{int64(len(ids))})
	idBuilder := array.NewInt64Builder(s.pool)
	idBuilder.AppendValues(ids, nil)
	idChunk := idBuilder.NewArray()
	idBuilder.Release()
	s.Require().NoError(builder.AddColumnFromChunks(types.IDFieldName, []arrow.Array{idChunk}))
	scoreBuilder := array.NewFloat32Builder(s.pool)
	scoreBuilder.AppendValues(scores, nil)
	scoreChunk := scoreBuilder.NewArray()
	scoreBuilder.Release()
	s.Require().NoError(builder.AddColumnFromChunks(types.ScoreFieldName, []arrow.Array{scoreChunk}))
	categoryBuilder := array.NewStringBuilder(s.pool)
	categoryBuilder.AppendValues(categories, nil)
	categoryChunk := categoryBuilder.NewArray()
	categoryBuilder.Release()
	s.Require().NoError(builder.AddColumnFromChunks("category", []arrow.Array{categoryChunk}))
	return builder.Build()
}

func (s *GroupByOpTestSuite) TestGroupByOp_NaNTopK() {
	nan := float32(math.NaN())
	for _, scorer := range []GroupScorer{GroupScorerMax, GroupScorerSum, GroupScorerAvg} {
		for _, desc := range []bool{true, false} {
			direction, expected := "descending", []int64{4, 1}
			if !desc {
				direction, expected = "ascending", []int64{2, 1}
			}
			for _, withinGroup := range []bool{false, true} {
				scope := "between_groups"
				categories := []string{"A", "B", "C", "D"}
				groupSize, limit := int64(1), int64(2)
				if withinGroup {
					scope = "within_group"
					categories = []string{"A", "A", "A", "A"}
					groupSize, limit = 2, 1
				}
				s.Run(string(scorer)+"/"+direction+"/"+scope, func() {
					// NaN must not prevent the last, highest-scoring row/group
					// from moving ahead of earlier candidates before truncation.
					df := s.nanGroupByFrame([]int64{1, 2, 3, 4}, []float32{5, 3, nan, 9}, categories)
					defer df.Release()
					op := NewGroupByOpWithScorer("category", groupSize, limit, 0, scorer).SetSortDescending(desc)
					result, err := NewFuncChainWithAllocator(s.pool).SetStage(types.StageL2Rerank).Add(op).Execute(df)
					s.Require().NoError(err)
					defer result.Release()
					s.Equal(expected, s.getChunkInt64Values(result.Column(types.IDFieldName), 0))
					s.NoError(ValidateScoreChunk(result.Column(types.ScoreFieldName).Chunk(0), 0))
				})
			}
		}
	}
}

func (s *GroupByOpTestSuite) TestGroupByOp_NaNTiesAndInfinities() {
	nan := float32(math.NaN())
	for _, desc := range []bool{true, false} {
		direction := "descending"
		if !desc {
			direction = "ascending"
		}
		for _, test := range []struct {
			name       string
			ids        []int64
			scores     []float32
			categories []string
			groupSize  int64
			expected   []int64
		}{
			// NaN ties must still use id ASC within a group.
			{"nan_row_ties", []int64{40, 10, 30, 20}, []float32{nan, nan, nan, nan}, []string{"A", "A", "A", "A"}, 2, []int64{10, 20}},
			// NaN group ties use group size DESC, then first id ASC.
			{"nan_group_ties", []int64{40, 30, 20, 10}, []float32{nan, nan, nan, nan}, []string{"A", "B", "B", "C"}, 2, []int64{20, 30, 10, 40}},
			{"infinity_groups", []int64{1, 2, 3, 4}, []float32{float32(math.Inf(1)), nan, float32(math.Inf(-1)), 0}, []string{"A", "B", "C", "D"}, 1, nil},
			{"infinity_rows", []int64{1, 2, 3, 4}, []float32{float32(math.Inf(1)), nan, float32(math.Inf(-1)), 0}, []string{"A", "A", "A", "A"}, 4, nil},
		} {
			s.Run(direction+"/"+test.name, func() {
				expected := test.expected
				if expected == nil {
					expected = []int64{1, 4, 3, 2}
					if !desc {
						expected = []int64{3, 4, 1, 2}
					}
				}
				df := s.nanGroupByFrame(test.ids, test.scores, test.categories)
				defer df.Release()
				op := NewGroupByOp("category", test.groupSize, 4, 0).SetSortDescending(desc)
				result, err := NewFuncChainWithAllocator(s.pool).SetStage(types.StageL2Rerank).Add(op).Execute(df)
				s.Require().NoError(err)
				defer result.Release()
				s.Equal(expected, s.getChunkInt64Values(result.Column(types.IDFieldName), 0))
			})
		}
	}
}

func (s *GroupByOpTestSuite) TestGroupByOp_AggregationNaN() {
	// Even without NaN input, +Inf + -Inf produces NaN for sum/avg.
	for _, scorer := range []GroupScorer{GroupScorerSum, GroupScorerAvg} {
		for _, desc := range []bool{true, false} {
			direction, expected := "descending", []int64{4, 1}
			if !desc {
				direction, expected = "ascending", []int64{1, 4}
			}
			s.Run(string(scorer)+"/"+direction, func() {
				df := s.nanGroupByFrame([]int64{1, 2, 3, 4}, []float32{5, float32(math.Inf(1)), float32(math.Inf(-1)), 9}, []string{"A", "B", "B", "C"})
				defer df.Release()
				op := NewGroupByOpWithScorer("category", 2, 2, 0, scorer).SetSortDescending(desc)
				result, err := NewFuncChainWithAllocator(s.pool).SetStage(types.StageL2Rerank).Add(op).Execute(df)
				s.Require().NoError(err)
				defer result.Release()
				s.Equal(expected, s.getChunkInt64Values(result.Column(types.IDFieldName), 0))
				s.NoError(ValidateScoreChunk(result.Column(types.ScoreFieldName).Chunk(0), 0))
			})
		}
	}
}
