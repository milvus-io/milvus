// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package tasks

import (
	"bytes"
	"encoding/binary"
	"math"
	"math/rand"
	"testing"

	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/querypb"
)

func TestTakeForOutputRequestDecision(t *testing.T) {
	t.Run("request limit", func(t *testing.T) {
		assert.True(t, takeForOutputAllowed(10000, 10000))
		assert.False(t, takeForOutputAllowed(10001, 10000))
		assert.False(t, takeForOutputAllowed(unknownTakeForOutputResultCount, 10000))
		assert.True(t, takeForOutputAllowed(unknownTakeForOutputResultCount, 0))
	})

	t.Run("search count", func(t *testing.T) {
		assert.Equal(t, int64(600), searchTakeForOutputResultCount(10, 20, 3))
		assert.Equal(t, int64(200), searchTakeForOutputResultCount(10, 20, 0))
		assert.Equal(t, int64(0), searchTakeForOutputResultCount(0, 20, 1))
		assert.Equal(t, unknownTakeForOutputResultCount, searchTakeForOutputResultCount(-1, 20, 1))
		assert.Equal(t, int64(math.MaxInt64), searchTakeForOutputResultCount(math.MaxInt64, 2, 1))
	})

	t.Run("retrieve primary key term", func(t *testing.T) {
		plan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_TermExpr{
							TermExpr: &planpb.TermExpr{
								ColumnInfo: &planpb.ColumnInfo{IsPrimaryKey: true},
								Values: []*planpb.GenericValue{
									{}, {}, {},
								},
							},
						},
					},
				},
			},
		}
		req := &internalpb.RetrieveRequest{Limit: -1}
		assert.Equal(t, int64(3), retrieveTakeForOutputResultCount(req, plan))
		req.Limit = 0
		assert.Equal(t, int64(3), retrieveTakeForOutputResultCount(req, plan))
		req.Limit = 2
		assert.Equal(t, int64(2), retrieveTakeForOutputResultCount(req, plan))
	})

	t.Run("retrieve limit and unknown", func(t *testing.T) {
		assert.Equal(t, int64(128), retrieveTakeForOutputResultCount(
			&internalpb.RetrieveRequest{Limit: 128},
			&planpb.PlanNode{},
		))
		nonPrimaryKeyPlan := &planpb.PlanNode{
			Node: &planpb.PlanNode_Query{
				Query: &planpb.QueryPlanNode{
					Predicates: &planpb.Expr{
						Expr: &planpb.Expr_TermExpr{
							TermExpr: &planpb.TermExpr{
								ColumnInfo: &planpb.ColumnInfo{},
							},
						},
					},
				},
			},
		}
		assert.Equal(t, int64(128), retrieveTakeForOutputResultCount(
			&internalpb.RetrieveRequest{Limit: 128},
			nonPrimaryKeyPlan,
		))
		assert.Equal(t, unknownTakeForOutputResultCount, retrieveTakeForOutputResultCount(
			&internalpb.RetrieveRequest{Limit: -1},
			&planpb.PlanNode{},
		))
		assert.Equal(t, unknownTakeForOutputResultCount, retrieveTakeForOutputResultCount(
			&internalpb.RetrieveRequest{Limit: 0},
			&planpb.PlanNode{},
		))
		assert.Equal(t, unknownTakeForOutputResultCount, retrieveTakeForOutputResultCount(nil, nil))
	})
}

type SearchTaskSuite struct {
	suite.Suite
}

func (s *SearchTaskSuite) composePlaceholderGroup(nq int, dim int) []byte {
	placeHolderGroup := &commonpb.PlaceholderGroup{
		Placeholders: []*commonpb.PlaceholderValue{
			{
				Tag:  "$0",
				Type: commonpb.PlaceholderType_FloatVector,
				Values: lo.RepeatBy(nq, func(_ int) []byte {
					bs := make([]byte, 0, dim*4)
					for j := 0; j < dim; j++ {
						var buffer bytes.Buffer
						f := rand.Float32()
						err := binary.Write(&buffer, common.Endian, f)
						s.Require().NoError(err)
						bs = append(bs, buffer.Bytes()...)
					}
					return bs
				}),
			},
		},
	}

	bs, err := proto.Marshal(placeHolderGroup)
	s.Require().NoError(err)
	return bs
}

func (s *SearchTaskSuite) composeEmptyPlaceholderGroup() []byte {
	placeHolderGroup := &commonpb.PlaceholderGroup{}

	bs, err := proto.Marshal(placeHolderGroup)
	s.Require().NoError(err)
	return bs
}

func (s *SearchTaskSuite) TestCombinePlaceHolderGroups() {
	s.Run("normal", func() {
		task := &SearchTask{
			placeholderGroup: s.composePlaceholderGroup(1, 128),
			others: []*SearchTask{
				{
					placeholderGroup: s.composePlaceholderGroup(1, 128),
				},
			},
		}

		task.combinePlaceHolderGroups()
	})

	s.Run("tasked_not_merged", func() {
		task := &SearchTask{}

		err := task.combinePlaceHolderGroups()
		s.NoError(err)
	})

	s.Run("empty_placeholdergroup", func() {
		task := &SearchTask{
			placeholderGroup: s.composeEmptyPlaceholderGroup(),
			others: []*SearchTask{
				{
					placeholderGroup: s.composePlaceholderGroup(1, 128),
				},
			},
		}

		err := task.combinePlaceHolderGroups()
		s.Error(err)

		task = &SearchTask{
			placeholderGroup: s.composePlaceholderGroup(1, 128),
			others: []*SearchTask{
				{
					placeholderGroup: s.composeEmptyPlaceholderGroup(),
				},
			},
		}

		err = task.combinePlaceHolderGroups()
		s.Error(err)
	})

	s.Run("unmarshal_fail", func() {
		task := &SearchTask{
			placeholderGroup: []byte{0x12, 0x34},
			others: []*SearchTask{
				{
					placeholderGroup: s.composePlaceholderGroup(1, 128),
				},
			},
		}

		err := task.combinePlaceHolderGroups()
		s.Error(err)

		task = &SearchTask{
			placeholderGroup: s.composePlaceholderGroup(1, 128),
			others: []*SearchTask{
				{
					placeholderGroup: []byte{0x12, 0x34},
				},
			},
		}

		err = task.combinePlaceHolderGroups()
		s.Error(err)
	})
}

func (s *SearchTaskSuite) TestMergeFilterOnly() {
	s.Run("same_filter_only_can_merge", func() {
		task1 := &SearchTask{
			nq:   10,
			topk: 100,
			req: &querypb.SearchRequest{
				FilterOnly: true,
				Req: &internalpb.SearchRequest{
					DbID:               1,
					CollectionID:       1000,
					MvccTimestamp:      100,
					PartitionIDs:       []int64{1, 2},
					SerializedExprPlan: []byte("plan"),
				},
				DmlChannels: []string{"channel1"},
				SegmentIDs:  []int64{1, 2, 3},
			},
			originTopks: []int64{100},
			originNqs:   []int64{10},
		}
		task2 := &SearchTask{
			nq:   5,
			topk: 100,
			req: &querypb.SearchRequest{
				FilterOnly: true,
				Req: &internalpb.SearchRequest{
					DbID:               1,
					CollectionID:       1000,
					MvccTimestamp:      100,
					PartitionIDs:       []int64{1, 2},
					SerializedExprPlan: []byte("plan"),
				},
				DmlChannels: []string{"channel1"},
				SegmentIDs:  []int64{1, 2, 3},
			},
			originTopks: []int64{100},
			originNqs:   []int64{5},
		}

		merged := task1.Merge(task2)
		s.True(merged, "tasks with same FilterOnly=true should merge")
		s.Equal(int64(15), task1.nq)
	})

	s.Run("different_filter_only_cannot_merge", func() {
		task1 := &SearchTask{
			nq:   10,
			topk: 100,
			req: &querypb.SearchRequest{
				FilterOnly: true,
				Req: &internalpb.SearchRequest{
					DbID:               1,
					CollectionID:       1000,
					MvccTimestamp:      100,
					PartitionIDs:       []int64{1, 2},
					SerializedExprPlan: []byte("plan"),
				},
				DmlChannels: []string{"channel1"},
				SegmentIDs:  []int64{1, 2, 3},
			},
			originTopks: []int64{100},
			originNqs:   []int64{10},
		}
		task2 := &SearchTask{
			nq:   5,
			topk: 100,
			req: &querypb.SearchRequest{
				FilterOnly: false,
				Req: &internalpb.SearchRequest{
					DbID:               1,
					CollectionID:       1000,
					MvccTimestamp:      100,
					PartitionIDs:       []int64{1, 2},
					SerializedExprPlan: []byte("plan"),
				},
				DmlChannels: []string{"channel1"},
				SegmentIDs:  []int64{1, 2, 3},
			},
			originTopks: []int64{100},
			originNqs:   []int64{5},
		}

		merged := task1.Merge(task2)
		s.False(merged, "tasks with different FilterOnly should not merge")
	})

	s.Run("different_enable_expr_cache_cannot_merge", func() {
		task1 := &SearchTask{
			nq:   10,
			topk: 100,
			req: &querypb.SearchRequest{
				FilterOnly:      false,
				EnableExprCache: true,
				Req: &internalpb.SearchRequest{
					DbID:               1,
					CollectionID:       1000,
					MvccTimestamp:      100,
					PartitionIDs:       []int64{1, 2},
					SerializedExprPlan: []byte("plan"),
				},
				DmlChannels: []string{"channel1"},
				SegmentIDs:  []int64{1, 2, 3},
			},
			originTopks: []int64{100},
			originNqs:   []int64{10},
		}
		task2 := &SearchTask{
			nq:   5,
			topk: 100,
			req: &querypb.SearchRequest{
				FilterOnly:      false,
				EnableExprCache: false,
				Req: &internalpb.SearchRequest{
					DbID:               1,
					CollectionID:       1000,
					MvccTimestamp:      100,
					PartitionIDs:       []int64{1, 2},
					SerializedExprPlan: []byte("plan"),
				},
				DmlChannels: []string{"channel1"},
				SegmentIDs:  []int64{1, 2, 3},
			},
			originTopks: []int64{100},
			originNqs:   []int64{5},
		}

		merged := task1.Merge(task2)
		s.False(merged, "tasks with different EnableExprCache should not merge")
	})

	s.Run("filter_only_false_can_merge", func() {
		task1 := &SearchTask{
			nq:   10,
			topk: 100,
			req: &querypb.SearchRequest{
				FilterOnly: false,
				Req: &internalpb.SearchRequest{
					DbID:               1,
					CollectionID:       1000,
					MvccTimestamp:      100,
					PartitionIDs:       []int64{1, 2},
					SerializedExprPlan: []byte("plan"),
				},
				DmlChannels: []string{"channel1"},
				SegmentIDs:  []int64{1, 2, 3},
			},
			originTopks: []int64{100},
			originNqs:   []int64{10},
		}
		task2 := &SearchTask{
			nq:   5,
			topk: 100,
			req: &querypb.SearchRequest{
				FilterOnly: false,
				Req: &internalpb.SearchRequest{
					DbID:               1,
					CollectionID:       1000,
					MvccTimestamp:      100,
					PartitionIDs:       []int64{1, 2},
					SerializedExprPlan: []byte("plan"),
				},
				DmlChannels: []string{"channel1"},
				SegmentIDs:  []int64{1, 2, 3},
			},
			originTopks: []int64{100},
			originNqs:   []int64{5},
		}

		merged := task1.Merge(task2)
		s.True(merged, "tasks with same FilterOnly=false should merge")
		s.Equal(int64(15), task1.nq)
	})
}

func (s *SearchTaskSuite) TestSearchTaskMinNQ() {
	s.Run("fallback_to_total_nq_without_origin", func() {
		task := &SearchTask{nq: 8}
		s.Equal(int64(8), task.MinNQ())
	})

	s.Run("minimum_origin_nq", func() {
		task := &SearchTask{
			nq:        11,
			originNqs: []int64{5, 2, 4},
		}
		s.Equal(int64(2), task.MinNQ())
	})
}

func TestSearchTask(t *testing.T) {
	suite.Run(t, new(SearchTaskSuite))
}
