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

package rls

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/parser/planparserv2"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func logicalChainRLSSchema(t *testing.T) *typeutil.SchemaHelper {
	t.Helper()
	helper, err := typeutil.CreateSchemaHelper(&schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "age", DataType: schemapb.DataType_Int64, Nullable: true},
			{FieldID: 102, Name: "owner", DataType: schemapb.DataType_VarChar},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: 200, Name: "items", Fields: []*schemapb.FieldSchema{{
				FieldID: 201, Name: "items[value]", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64,
			}},
		}},
	})
	require.NoError(t, err)
	return helper
}

func logicalChainWrapperPredicate(t *testing.T, expr *planpb.Expr) *planpb.Expr {
	t.Helper()
	if sampler := expr.GetRandomSampleExpr(); sampler != nil {
		require.Equal(t, float32(0.5), sampler.GetSampleFactor())
		return sampler.GetPredicate()
	}
	filter := expr.GetElementFilterExpr()
	require.NotNil(t, filter, "the wrapper must remain at the plan root")
	require.Equal(t, "items", filter.GetStructName())
	require.NotNil(t, filter.GetElementExpr())
	return filter.GetPredicate()
}

func logicalChainAndLeaves(t *testing.T, expr *planpb.Expr) []*planpb.Expr {
	t.Helper()
	require.NotNil(t, expr)
	if binary := expr.GetBinaryExpr(); binary != nil {
		require.Equal(t, planpb.BinaryExpr_LogicalAnd, binary.GetOp())
		return append(logicalChainAndLeaves(t, binary.GetLeft()), logicalChainAndLeaves(t, binary.GetRight())...)
	}
	return []*planpb.Expr{expr}
}

func TestMergeNormalizedPredicateToPlanLogicalChainWrappers(t *testing.T) {
	helper := logicalChainRLSSchema(t)
	for _, optimize := range []bool{false, true} {
		t.Run(fmt.Sprintf("optimize=%t", optimize), func(t *testing.T) {
			old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(fmt.Sprint(optimize))
			t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
			for _, wrapper := range []string{"random_sample(0.5)", "element_filter(items, $[value] > 10)"} {
				t.Run(wrapper, func(t *testing.T) {
					filter := "age > {minimum} AND ((id IN [1,2] AND " + wrapper + ") OR false)"
					_, err := planparserv2.ParseExpr(helper, filter, nil)
					require.Error(t, err, "neither folding nor a wrapper may hide a missing template")
					_, err = planparserv2.ParseExpr(helper, filter, map[string]*schemapb.TemplateValue{
						"minimum": {Val: &schemapb.TemplateValue_StringVal{StringVal: "invalid integer"}},
					})
					require.Error(t, err)
					plan, err := planparserv2.CreateRetrievePlanArgs(helper, filter, map[string]*schemapb.TemplateValue{
						"minimum": {Val: &schemapb.TemplateValue_Int64Val{Int64Val: 21}},
					}, &planparserv2.ParserVisitorArgs{})
					require.NoError(t, err)
					user := plan.GetQuery().GetPredicates()
					userBefore := proto.Clone(user).(*planpb.Expr)
					policy, err := planparserv2.ParseExpr(helper, `owner == "alice"`, nil)
					require.NoError(t, err)
					policyBefore := proto.Clone(policy).(*planpb.Expr)
					require.NoError(t, MergeNormalizedPredicateToPlan(plan, policy))
					merged := plan.GetQuery().GetPredicates()
					require.True(t, proto.Equal(userBefore, user), "RLS must not mutate a shared user predicate")
					require.True(t, proto.Equal(policyBefore, policy), "RLS must not mutate a shared policy")
					if element := user.GetElementFilterExpr(); element != nil {
						require.Same(t, element.GetElementExpr(), merged.GetElementFilterExpr().GetElementExpr())
					}
					seen := map[int64]bool{}
					for _, leaf := range logicalChainAndLeaves(t, logicalChainWrapperPredicate(t, merged)) {
						if term := leaf.GetTermExpr(); term != nil {
							require.Equal(t, int64(100), term.GetColumnInfo().GetFieldId())
							require.Len(t, term.GetValues(), 2)
							require.Equal(t, int64(1), term.GetValues()[0].GetInt64Val())
							require.Equal(t, int64(2), term.GetValues()[1].GetInt64Val())
							require.NotContains(t, seen, int64(100))
							seen[100] = true
							continue
						}
						comparison := leaf.GetUnaryRangeExpr()
						require.NotNil(t, comparison)
						fieldID := comparison.GetColumnInfo().GetFieldId()
						require.NotContains(t, seen, fieldID)
						seen[fieldID] = true
						switch fieldID {
						case 101:
							require.Equal(t, planpb.OpType_GreaterThan, comparison.GetOp())
							require.Equal(t, int64(21), comparison.GetValue().GetInt64Val())
						case 102:
							require.Equal(t, planpb.OpType_Equal, comparison.GetOp())
							require.Equal(t, "alice", comparison.GetValue().GetStringVal())
						default:
							t.Fatalf("unexpected predicate on field %d", fieldID)
						}
					}
					require.Equal(t, map[int64]bool{100: true, 101: true, 102: true}, seen)
				})
			}
		})
	}
}

func TestMergeNormalizedPredicateToPlanLogicalChainNullableIn(t *testing.T) {
	helper := logicalChainRLSSchema(t)
	for _, optimize := range []bool{false, true} {
		t.Run(fmt.Sprintf("optimize=%t", optimize), func(t *testing.T) {
			old := paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(fmt.Sprint(optimize))
			t.Cleanup(func() { paramtable.Get().CommonCfg.EnabledOptimizeExpr.SwapTempValue(old) })
			for _, wrapper := range []string{"random_sample(0.5)", "element_filter(items, $[value] > 10)"} {
				t.Run(wrapper, func(t *testing.T) {
					plan, err := planparserv2.CreateRetrievePlanArgs(helper,
						"age IN [-1,0] AND ((age IN [1,2] AND "+wrapper+") OR false)", nil, &planparserv2.ParserVisitorArgs{})
					require.NoError(t, err)
					policy, err := planparserv2.ParseExpr(helper, "age == 1", nil)
					require.NoError(t, err)
					require.NoError(t, MergeNormalizedPredicateToPlan(plan, policy))
					// The empty intersection is UNKNOWN on NULL. A same-field policy
					// must not consume just one IN and accidentally permit age=1.
					leaves := logicalChainAndLeaves(t, logicalChainWrapperPredicate(t, plan.GetQuery().GetPredicates()))
					require.Len(t, leaves, 3)
					var sets [][]int64
					comparisons := 0
					for _, leaf := range leaves {
						if term := leaf.GetTermExpr(); term != nil {
							require.Equal(t, int64(101), term.GetColumnInfo().GetFieldId())
							require.True(t, term.GetColumnInfo().GetNullable())
							require.Len(t, term.GetValues(), 2)
							sets = append(sets, []int64{term.GetValues()[0].GetInt64Val(), term.GetValues()[1].GetInt64Val()})
							continue
						}
						comparison := leaf.GetUnaryRangeExpr()
						require.NotNil(t, comparison)
						require.Equal(t, int64(101), comparison.GetColumnInfo().GetFieldId())
						require.True(t, comparison.GetColumnInfo().GetNullable())
						require.Equal(t, planpb.OpType_Equal, comparison.GetOp())
						require.Equal(t, int64(1), comparison.GetValue().GetInt64Val())
						comparisons++
					}
					require.ElementsMatch(t, [][]int64{{-1, 0}, {1, 2}}, sets)
					require.Equal(t, 1, comparisons)
				})
			}
		})
	}
}
