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
	"testing"

	"github.com/stretchr/testify/require"

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
