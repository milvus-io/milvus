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

func TestQueryChainCreatePlanPreservesArgumentsAndProjection(t *testing.T) {
	paramtable.Init()
	task := queryStageTestTask()
	before := proto.Clone(task.request)
	require.NoError(t, task.createPlan(context.Background()))
	require.True(t, proto.Equal(before, task.request))
	require.Len(t, task.plan.GetQuerynodeFunctionChains(), 1)
	require.True(t, proto.Equal(task.request.FunctionChains[0], task.plan.QuerynodeFunctionChains[0]))
	require.NotContains(t, task.plan.GetOutputFieldIds(), int64(101))
	data, err := proto.Marshal(task.plan)
	require.NoError(t, err)
	got := &planpb.PlanNode{}
	require.NoError(t, proto.Unmarshal(data, got))
	require.True(t, proto.Equal(task.plan, got))
	task.request.FunctionChains[0].OutputColumns[0] = "changed"
	require.Equal(t, []string{"matches"}, got.QuerynodeFunctionChains[0].OutputColumns)
	require.Equal(t, []string{"matches"}, task.plan.QuerynodeFunctionChains[0].OutputColumns)
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

func TestQueryChainProxyOnlyChecksEnvelope(t *testing.T) {
	paramtable.Init()
	for name, mutate := range map[string]func(*schemapb.FunctionChain){
		"unknown operator": func(p *schemapb.FunctionChain) { p.Ops[0].Op = "future_operator" },
		"unknown field":    func(p *schemapb.FunctionChain) { p.Ops[0].Inputs = []string{"missing"} },
		"nil operator":     func(p *schemapb.FunctionChain) { p.Ops[0] = nil },
		"legacy limit": func(p *schemapb.FunctionChain) {
			p.Ops[0].Params["limit"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 10}}
		},
		"multiple operators": func(p *schemapb.FunctionChain) {
			p.Ops = append(p.Ops, &schemapb.FunctionChainOp{Op: "future_operator"})
		},
		"unknown output":       func(p *schemapb.FunctionChain) { p.OutputColumns = []string{"not_produced"} },
		"unchanged parameters": func(p *schemapb.FunctionChain) {},
	} {
		t.Run(name, func(t *testing.T) {
			task := queryStageTestTask()
			task.plan = &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}}}
			mutate(task.request.FunctionChains[0])
			before := proto.Clone(task.request.FunctionChains[0])
			require.NoError(t, task.prepareQueryFunctionChains())
			require.True(t, proto.Equal(before, task.plan.QuerynodeFunctionChains[0]))
			require.NotSame(t, task.request.FunctionChains[0], task.plan.QuerynodeFunctionChains[0])
		})
	}
}

func TestQueryChainProxyRejectsInvalidEnvelope(t *testing.T) {
	for name, mutate := range map[string]func(*QueryTask){
		"nil chain": func(t *QueryTask) { t.request.FunctionChains[0] = nil },
		"wrong stage": func(t *QueryTask) {
			t.request.FunctionChains[0].Stage = schemapb.FunctionChainStage_FunctionChainStageL0Rerank
		},
		"duplicate stage": func(t *QueryTask) {
			t.request.FunctionChains = append(t.request.FunctionChains, proto.Clone(t.request.FunctionChains[0]).(*schemapb.FunctionChain))
		},
		"empty operators": func(t *QueryTask) { t.request.FunctionChains[0].Ops = nil },
	} {
		t.Run(name, func(t *testing.T) {
			task := queryStageTestTask()
			task.plan = &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}}}
			mutate(task)
			require.ErrorIs(t, task.prepareQueryFunctionChains(), merr.ErrParameterInvalid)
			require.Empty(t, task.plan.QuerynodeFunctionChains)
		})
	}
}
