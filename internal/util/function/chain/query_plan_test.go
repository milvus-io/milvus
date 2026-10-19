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

package chain

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/proto/planpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

func queryChainFixture() *schemapb.FunctionChain {
	paramtable.Init()
	return &schemapb.FunctionChain{
		Name: "errors", Stage: schemapb.FunctionChainStage_FunctionChainStageQueryScan,
		OutputColumns: []string{"matches"},
		Ops: []*schemapb.FunctionChainOp{{
			Op: "grep", Inputs: []string{"message"}, Outputs: []string{"matches"},
			Params: map[string]*schemapb.FunctionParamValue{
				"arguments": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: `-i -e "timeout"`}},
			},
		}},
	}
}

func querySchemaFixture() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "message", DataType: schemapb.DataType_Text, Nullable: true},
	}}
}

func TestPrepareQueryChainPreservesRawArgumentsAndOwnership(t *testing.T) {
	for _, args := range []string{`-i -e "timeout"`, `-C 2`, `-e '['`, ""} {
		pb := queryChainFixture()
		pb.Ops[0].Params["arguments"].Value = &schemapb.FunctionParamValue_StringValue{StringValue: args}
		before := proto.Clone(pb)
		prepared, err := PrepareQueryChain([]*schemapb.FunctionChain{pb}, querySchemaFixture())
		require.NoError(t, err)
		require.True(t, proto.Equal(before, pb))
		require.True(t, proto.Equal(pb, prepared.Chain))
		require.Equal(t, []int64{101}, prepared.InputPlan.PhysicalFieldIDs())
		require.True(t, prepared.InputPlan.Inputs[0].Nullable)
		pb.Ops[0].Params["arguments"].Value = &schemapb.FunctionParamValue_StringValue{StringValue: "changed"}
		require.Equal(t, args, prepared.Chain.Ops[0].Params["arguments"].GetStringValue())
		pb.OutputColumns[0] = "changed"
		require.Equal(t, []string{"matches"}, prepared.Chain.OutputColumns)
	}
}

func TestPrepareQueryChainRejectsInvalidStructure(t *testing.T) {
	for name, mutate := range map[string]func(*schemapb.FunctionChain, *schemapb.CollectionSchema){
		"wrong stage": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) {
			p.Stage = schemapb.FunctionChainStage_FunctionChainStageL0Rerank
		},
		"empty":  func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) { p.Ops = nil },
		"nil op": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) { p.Ops[0] = nil },
		"expression": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) {
			p.Ops[0].Expr = &schemapb.FunctionChainExpr{Name: "grep"}
		},
		"map grep": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) { p.Ops[0].Op = "map" },
		"filter": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) {
			p.Ops = append(p.Ops, &schemapb.FunctionChainOp{Op: "filter"})
		},
		"multiple grep": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) {
			p.Ops = append(p.Ops, proto.Clone(p.Ops[0]).(*schemapb.FunctionChainOp))
		},
		"missing input": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) { p.Ops[0].Inputs = nil },
		"unknown input": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) { p.Ops[0].Inputs[0] = "missing" },
		"unknown type": func(_ *schemapb.FunctionChain, s *schemapb.CollectionSchema) {
			s.Fields[1].DataType = schemapb.DataType_JSON
		},
		"dynamic field": func(_ *schemapb.FunctionChain, s *schemapb.CollectionSchema) { s.Fields[1].IsDynamic = true },
		"multiple inputs": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) {
			p.Ops[0].Inputs = append(p.Ops[0].Inputs, "id")
		},
		"missing output":   func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) { p.Ops[0].Outputs = nil },
		"system output":    func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) { p.Ops[0].Outputs[0] = "$id" },
		"schema overwrite": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) { p.Ops[0].Outputs[0] = "id" },
		"row id overwrite": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) {
			p.Ops[0].Outputs[0] = "RowID"
			p.OutputColumns[0] = "RowID"
		},
		"timestamp overwrite": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) {
			p.Ops[0].Outputs[0] = "Timestamp"
			p.OutputColumns[0] = "Timestamp"
		},
		"space output":   func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) { p.Ops[0].Outputs[0] = " x " },
		"missing name":   func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) { p.Name = "" },
		"unknown export": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) { p.OutputColumns[0] = "missing" },
		"duplicate export": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) {
			p.OutputColumns = append(p.OutputColumns, "matches")
		},
		"missing arguments": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) { delete(p.Ops[0].Params, "arguments") },
		"legacy limit": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) {
			p.Ops[0].Params["limit"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 10}}
		},
		"offset": func(p *schemapb.FunctionChain, _ *schemapb.CollectionSchema) {
			p.Ops[0].Params["offset"] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 1}}
		},
	} {
		t.Run(name, func(t *testing.T) {
			pb, schema := queryChainFixture(), querySchemaFixture()
			mutate(pb, schema)
			result, err := PrepareQueryChain([]*schemapb.FunctionChain{pb}, schema)
			require.Nil(t, result)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.Equal(t, merr.InputError, merr.GetErrorType(err))
		})
	}
	_, err := PrepareQueryChain([]*schemapb.FunctionChain{nil}, querySchemaFixture())
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	_, err = PrepareQueryChain([]*schemapb.FunctionChain{queryChainFixture()}, nil)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}

func TestQueryChainReprAndExecutionBoundary(t *testing.T) {
	pb := queryChainFixture()
	repr, err := ProtoChainToRepr(pb)
	require.NoError(t, err)
	require.Equal(t, types.StageQueryScan, repr.Stage)
	require.Equal(t, []string{types.SegOffsetFieldName}, repr.Info.RequiredInputs)
	require.Equal(t, []string{"message"}, repr.Operators[0].Inputs)
	pb.OutputColumns[0] = "changed"
	require.Equal(t, []string{"matches"}, repr.OutputColumns)
	pb.OutputColumns = nil
	pb.Name = ""
	_, err = PrepareQueryChain([]*schemapb.FunctionChain{pb}, querySchemaFixture())
	require.NoError(t, err)
	pb.Stage = schemapb.FunctionChainStage_FunctionChainStageL0Rerank
	_, err = ProtoChainToRepr(pb)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestQueryPlanChainExecutionChecks(t *testing.T) {
	plan := &planpb.PlanNode{Node: &planpb.PlanNode_Query{Query: &planpb.QueryPlanNode{}}, QuerynodeFunctionChains: []*schemapb.FunctionChain{queryChainFixture()}}
	for name, mutate := range map[string]func(*planpb.QueryPlanNode){
		"element": func(q *planpb.QueryPlanNode) {
			q.Predicates = &planpb.Expr{Expr: &planpb.Expr_ElementFilterExpr{ElementFilterExpr: &planpb.ElementFilterExpr{}}}
		},
		"count":       func(q *planpb.QueryPlanNode) { q.IsCount = true },
		"group":       func(q *planpb.QueryPlanNode) { q.GroupByFieldIds = []int64{100} },
		"aggregation": func(q *planpb.QueryPlanNode) { q.Aggregates = []*planpb.Aggregate{{}} },
		"sort":        func(q *planpb.QueryPlanNode) { q.OrderByFields = []*planpb.OrderByField{{FieldId: 100}} },
		"cursor":      func(q *planpb.QueryPlanNode) { q.QueryIteratorCursor = &planpb.QueryIteratorCursor{} },
	} {
		t.Run(name, func(t *testing.T) {
			p := proto.Clone(plan).(*planpb.PlanNode)
			mutate(p.GetQuery())
			prepared, err := PrepareQueryPlanChainExecution(p, querySchemaFixture(), false)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			require.Nil(t, prepared)
		})
	}
	prepared, err := PrepareQueryPlanChainExecution(plan, querySchemaFixture(), true)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Nil(t, prepared)
	prepared, err = PrepareQueryPlanChainExecution(plan, querySchemaFixture(), false)
	require.NoError(t, err)
	require.NotNil(t, prepared)
	require.True(t, proto.Equal(plan.QuerynodeFunctionChains[0], prepared.Chain))
	require.NotSame(t, plan.QuerynodeFunctionChains[0], prepared.Chain)
	require.Len(t, prepared.InputPlan.Inputs, 1)
	plan.QuerynodeFunctionChains[0].Ops[0].Inputs = []string{"missing"}
	prepared, err = PrepareQueryPlanChainExecution(plan, querySchemaFixture(), false)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Nil(t, prepared)
	plan.QuerynodeFunctionChains = nil
	prepared, err = PrepareQueryPlanChainExecution(plan, nil, false)
	require.NoError(t, err)
	require.Nil(t, prepared)
	prepared, err = PrepareQueryPlanChainExecution(nil, nil, false)
	require.NoError(t, err)
	require.Nil(t, prepared)
}

func TestPrepareQueryChainRejectsStructParentOutput(t *testing.T) {
	schema := querySchemaFixture()
	schema.StructArrayFields = []*schemapb.StructArrayFieldSchema{{Name: "matches", FieldID: 110, Fields: []*schemapb.FieldSchema{
		{Name: "label", FieldID: 111, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar},
	}}}
	result, err := PrepareQueryChain([]*schemapb.FunctionChain{queryChainFixture()}, schema)
	require.Nil(t, result)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Equal(t, merr.InputError, merr.GetErrorType(err))
}
