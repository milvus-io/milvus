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

package querymeter

import (
	"math"
	"testing"

	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
)

func TestPredicateFieldIDs(t *testing.T) {
	rangeOn := func(id int64) *planpb.Expr {
		return &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
			ColumnInfo: &planpb.ColumnInfo{FieldId: id},
			Op:         planpb.OpType_GreaterThan,
		}}}
	}
	predicate := &planpb.Expr{Expr: &planpb.Expr_BinaryExpr{BinaryExpr: &planpb.BinaryExpr{
		Op:   planpb.BinaryExpr_LogicalAnd,
		Left: rangeOn(100),
		Right: &planpb.Expr{Expr: &planpb.Expr_CallExpr{CallExpr: &planpb.CallExpr{
			FunctionParameters: []*planpb.Expr{rangeOn(101), rangeOn(100)},
		}}},
	}}}
	require.ElementsMatch(t, []int64{100, 101, 100}, PredicateFieldIDs(predicate))
	require.Empty(t, PredicateFieldIDs(nil))
	require.Empty(t, PredicateFieldIDs(&planpb.Expr{Expr: &planpb.Expr_AlwaysTrueExpr{AlwaysTrueExpr: &planpb.AlwaysTrueExpr{}}}))
}

func TestQueryFieldIDs(t *testing.T) {
	predicate := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
		ColumnInfo: &planpb.ColumnInfo{FieldId: 100},
		Op:         planpb.OpType_GreaterThan,
	}}}
	query := &planpb.QueryPlanNode{
		Predicates:      predicate,
		GroupByFieldIds: []int64{101},
		Aggregates: []*planpb.Aggregate{
			{Op: planpb.AggregateOp_count},
			{Op: planpb.AggregateOp_count, FieldId: 101},
			{Op: planpb.AggregateOp_sum, FieldId: 100},
			{Op: planpb.AggregateOp_avg, FieldId: 100},
		},
		OrderByFields: []*planpb.OrderByField{{FieldId: 101}},
	}
	plan := &planpb.PlanNode{
		Node:           &planpb.PlanNode_Query{Query: query},
		OutputFieldIds: []int64{102},
	}
	// The group and aggregate share a projection. The predicate and sort are independent.
	require.Equal(t, []int64{100, 101, 100, 101}, QueryFieldIDs(plan))

	query.Predicates = nil
	require.Equal(t, []int64{101, 100, 101}, QueryFieldIDs(plan))
	require.Equal(t, []int64{100}, QueryFieldIDs(&planpb.PlanNode{
		Node: &planpb.PlanNode_Predicates{Predicates: predicate},
	}))
}

func TestSearchFieldIDs(t *testing.T) {
	predicate := &planpb.Expr{Expr: &planpb.Expr_UnaryRangeExpr{UnaryRangeExpr: &planpb.UnaryRangeExpr{
		ColumnInfo: &planpb.ColumnInfo{FieldId: 100},
		Op:         planpb.OpType_GreaterThan,
	}}}
	plan := &planpb.PlanNode{
		Node: &planpb.PlanNode_VectorAnns{VectorAnns: &planpb.VectorANNS{
			FieldId: 101, Predicates: predicate,
			QueryInfo: &planpb.QueryInfo{GroupByFieldIds: []int64{102, 103}},
		}},
		OutputFieldIds: []int64{104},
		Scorers:        []*planpb.ScoreFunction{{Filter: predicate}},
	}
	require.Equal(t, []int64{100, 101, 100, 102, 103}, SearchFieldIDs(plan, false))
	require.Equal(t, []int64{100}, SearchFieldIDs(plan, true))
}

func TestSearchReadUsageWeightsEachContribution(t *testing.T) {
	column := &internalpb.QueryReadUsage{DataBytes: 20, ValidityBits: map[int64]uint64{100: 2}}
	first, err := ScaleSearchUsage(column, 2)
	require.NoError(t, err)
	second, err := ScaleSearchUsage(column, 3)
	require.NoError(t, err)
	total, err := CollectSearchUsage([]*internalpb.SearchResults{
		{SearchReadUsage: first}, {SearchReadUsage: second}, {SearchReadUsage: &internalpb.QueryReadUsage{}},
	})
	require.NoError(t, err)
	bytes, err := BillableBytes(total, 8)
	require.NoError(t, err)
	require.EqualValues(t, 110, bytes)

	total, err = CollectSearchUsage(nil)
	require.NoError(t, err)
	require.Zero(t, total.GetDataBytes())
	_, err = CollectSearchUsage([]*internalpb.SearchResults{{}})
	require.Error(t, err)
	_, err = ScaleSearchUsage(&internalpb.QueryReadUsage{DataBytes: math.MaxUint64}, 2)
	require.Error(t, err)
}
