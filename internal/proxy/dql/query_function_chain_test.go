// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package dql

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/milvuspb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/proto/internalpb"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func queryStageTestChain() *schemapb.FunctionChain {
	return &schemapb.FunctionChain{
		Name: "errors", Stage: schemapb.FunctionChainStage_FunctionChainStageQueryScan,
		OutputColumns: []string{"matches"},
		Ops: []*schemapb.FunctionChainOp{{
			Op: "grep", Inputs: []string{"message"}, Outputs: []string{"matches"},
			Params: map[string]*schemapb.FunctionParamValue{
				"arguments": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: `-C 2 -e "timeout"`}},
			},
		}},
	}
}

func queryStageTestTask() *QueryTask {
	return &QueryTask{
		request: &milvuspb.QueryRequest{
			Expr: "id >= 0", OutputFields: []string{"id"},
			FunctionChains: []*schemapb.FunctionChain{queryStageTestChain()},
		},
		RetrieveRequest: &internalpb.RetrieveRequest{},
		queryParams:     &queryParams{},
		schema: mustNewSchemaInfo(&schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: 101, Name: "message", DataType: schemapb.DataType_VarChar},
		}}),
	}
}

// The chain is rewritten into `filter AND grep(...)` plus a GREP_MATCHES
// output item; nothing is forwarded to the QueryNode as a chain.
func TestQueryChainIsRewrittenIntoPredicateAndOutputItem(t *testing.T) {
	paramtable.Init()
	task := queryStageTestTask()
	before := proto.Clone(task.request)
	require.NoError(t, task.createPlan(context.Background()))
	require.True(t, proto.Equal(before, task.request))
	require.Empty(t, task.plan.GetQuerynodeFunctionChains())
	require.NotContains(t, task.plan.GetOutputFieldIds(), int64(101))

	and := task.plan.GetQuery().GetPredicates().GetBinaryExpr()
	require.NotNil(t, and)
	require.Equal(t, planpb.BinaryExpr_LogicalAnd, and.GetOp())
	require.NotNil(t, and.GetLeft().GetUnaryRangeExpr())
	grep := and.GetRight().GetGrepExpr()
	require.NotNil(t, grep)
	require.Equal(t, int64(101), grep.GetColumnInfo().GetFieldId())
	require.Equal(t, []string{"timeout"}, grep.GetPatterns())
	require.False(t, grep.GetSelectNone())

	require.Len(t, task.plan.GetProjectionItems(), 1)
	item := task.plan.GetProjectionItems()[0]
	require.Equal(t, requestLocalOutputIDStart, item.GetOutputId())
	require.Equal(t, schemapb.DataType_JSON, item.GetDataType())
	require.True(t, proto.Equal(grep, item.GetGrepMatches().GetSpec()))
	require.Equal(t, uint64(2), item.GetGrepMatches().GetBeforeContext())
	require.Equal(t, uint64(2), item.GetGrepMatches().GetAfterContext())
	require.False(t, item.GetGrepMatches().GetHasMaxCount())
	require.Equal(t, []string{"id", "matches"}, task.userOutputFields)
	require.Equal(t, []int64{requestLocalOutputIDStart}, task.outputProjection.queryNodeItemIDs(false))

	data, err := proto.Marshal(task.plan)
	require.NoError(t, err)
	got := &planpb.PlanNode{}
	require.NoError(t, proto.Unmarshal(data, got))
	require.True(t, proto.Equal(task.plan, got))
}

func TestQueryChainRewriteVariants(t *testing.T) {
	paramtable.Init()
	t.Run("empty filter", func(t *testing.T) {
		task := queryStageTestTask()
		task.request.Expr = ""
		require.NoError(t, task.createPlan(context.Background()))
		require.NotNil(t, task.plan.GetQuery().GetPredicates().GetGrepExpr())
	})
	t.Run("not exported", func(t *testing.T) {
		task := queryStageTestTask()
		task.request.FunctionChains[0].OutputColumns = nil
		require.NoError(t, task.createPlan(context.Background()))
		require.NotNil(t, task.plan.GetQuery().GetPredicates().GetBinaryExpr().GetRight().GetGrepExpr())
		require.Empty(t, task.plan.GetProjectionItems())
		require.Equal(t, []string{"id"}, task.userOutputFields)
	})
	t.Run("max count stays on the output item", func(t *testing.T) {
		task := queryStageTestTask()
		task.request.FunctionChains[0].Ops[0].Params["arguments"] = &schemapb.FunctionParamValue{
			Value: &schemapb.FunctionParamValue_StringValue{StringValue: `-F -m 3 -e timeout`},
		}
		require.NoError(t, task.createPlan(context.Background()))
		item := task.plan.GetProjectionItems()[0].GetGrepMatches()
		require.True(t, item.GetHasMaxCount())
		require.Equal(t, uint64(3), item.GetMaxCount())
		require.False(t, item.GetSpec().GetSelectNone())
	})
	t.Run("zero max count selects no row", func(t *testing.T) {
		task := queryStageTestTask()
		task.request.FunctionChains[0].Ops[0].Params["arguments"] = &schemapb.FunctionParamValue{
			Value: &schemapb.FunctionParamValue_StringValue{StringValue: `-e timeout -m 0`},
		}
		require.NoError(t, task.createPlan(context.Background()))
		require.True(t, task.plan.GetQuery().GetPredicates().GetBinaryExpr().GetRight().GetGrepExpr().GetSelectNone())
	})
	t.Run("invalid arguments", func(t *testing.T) {
		task := queryStageTestTask()
		task.request.FunctionChains[0].Ops[0].Params["arguments"] = &schemapb.FunctionParamValue{
			Value: &schemapb.FunctionParamValue_StringValue{StringValue: `--no-such-option`},
		}
		require.Error(t, task.createPlan(context.Background()))
	})
	t.Run("output conflicts with an output field alias", func(t *testing.T) {
		task := queryStageTestTask()
		task.request.OutputFields = []string{"id", "message AS matches"}
		require.ErrorIs(t, task.createPlan(context.Background()), merr.ErrParameterInvalid)
	})
	t.Run("ids follow the request's own virtual columns", func(t *testing.T) {
		task := queryStageTestTask()
		task.request.OutputFields = []string{"id", "$seg_id"}
		require.NoError(t, task.createPlan(context.Background()))
		require.Len(t, task.plan.GetProjectionItems(), 2)
		require.Equal(t, requestLocalOutputIDStart-1, task.plan.GetProjectionItems()[1].GetOutputId())
		require.NotNil(t, task.plan.GetProjectionItems()[1].GetGrepMatches())
	})
}

func TestQueryChainPlanRejectsUnsupportedQueryModes(t *testing.T) {
	for name, mutate := range map[string]func(*QueryTask){
		"count":     func(t *QueryTask) { t.plan.GetQuery().IsCount = true },
		"group_by":  func(t *QueryTask) { t.plan.GetQuery().GroupByFieldIds = []int64{100} },
		"aggregate": func(t *QueryTask) { t.plan.GetQuery().Aggregates = []*planpb.Aggregate{{}} },
		"order_by":  func(t *QueryTask) { t.plan.GetQuery().OrderByFields = []*planpb.OrderByField{{FieldId: 100}} },
		"iterator":  func(t *QueryTask) { t.queryParams.isIterator = true },
	} {
		t.Run(name, func(t *testing.T) {
			task := queryStageTestTask()
			task.plan = &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}}}
			mutate(task)
			err := task.prepareQueryFunctionChains()
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.Empty(t, task.plan.GetQuerynodeFunctionChains())
		})
	}
	task := queryStageTestTask()
	require.ErrorIs(t, task.prepareQueryFunctionChains(), merr.ErrServiceInternal)
	task.request.FunctionChains = nil
	require.NoError(t, task.prepareQueryFunctionChains())
}

func TestSearchRejectsQueryOutputDeclarations(t *testing.T) {
	pb := l0FunctionChain(mapOp("temp", "round_decimal", columnArg("$score")))
	pb.OutputColumns = []string{"temp"}
	_, _, err := splitFunctionChainsByStage([]*schemapb.FunctionChain{pb})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestQueryChainProxyRejectsInvalidChain(t *testing.T) {
	paramtable.Init()
	for name, mutate := range map[string]func(*QueryTask){
		"nil chain": func(t *QueryTask) { t.request.FunctionChains[0] = nil },
		"wrong stage": func(t *QueryTask) {
			t.request.FunctionChains[0].Stage = schemapb.FunctionChainStage_FunctionChainStageL0Rerank
		},
		"two chains": func(t *QueryTask) {
			t.request.FunctionChains = append(t.request.FunctionChains, proto.Clone(t.request.FunctionChains[0]).(*schemapb.FunctionChain))
		},
		"empty operators":  func(t *QueryTask) { t.request.FunctionChains[0].Ops = nil },
		"unknown operator": func(t *QueryTask) { t.request.FunctionChains[0].Ops[0].Op = "future_operator" },
		"unknown field":    func(t *QueryTask) { t.request.FunctionChains[0].Ops[0].Inputs = []string{"missing"} },
		"non-text field":   func(t *QueryTask) { t.request.FunctionChains[0].Ops[0].Inputs = []string{"id"} },
		"nil operator":     func(t *QueryTask) { t.request.FunctionChains[0].Ops[0] = nil },
		"legacy limit": func(t *QueryTask) {
			t.request.FunctionChains[0].Ops[0].Params["limit"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 10}}
		},
		"multiple operators": func(t *QueryTask) {
			t.request.FunctionChains[0].Ops = append(t.request.FunctionChains[0].Ops, &schemapb.FunctionChainOp{Op: "future_operator"})
		},
		"unknown output":   func(t *QueryTask) { t.request.FunctionChains[0].OutputColumns = []string{"not_produced"} },
		"duplicate output": func(t *QueryTask) { t.request.FunctionChains[0].OutputColumns = []string{"matches", "matches"} },
		"output is a schema field": func(t *QueryTask) {
			t.request.FunctionChains[0].Ops[0].Outputs = []string{"id"}
			t.request.FunctionChains[0].OutputColumns = []string{"id"}
		},
		"output equals input":     func(t *QueryTask) { t.request.FunctionChains[0].Ops[0].Outputs = []string{"message"} },
		"exported without a name": func(t *QueryTask) { t.request.FunctionChains[0].Name = " " },
		"expression operator": func(t *QueryTask) {
			t.request.FunctionChains[0].Ops[0].Expr = &schemapb.FunctionChainExpr{}
		},
		"arguments not a string": func(t *QueryTask) {
			t.request.FunctionChains[0].Ops[0].Params["arguments"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 1}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			task := queryStageTestTask()
			task.plan = &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}}}
			mutate(task)
			require.ErrorIs(t, task.prepareQueryFunctionChains(), merr.ErrParameterInvalid)
			require.Empty(t, task.plan.QuerynodeFunctionChains)
			require.Nil(t, task.plan.GetQuery().GetPredicates())
			require.Empty(t, task.plan.GetProjectionItems())
		})
	}
}
