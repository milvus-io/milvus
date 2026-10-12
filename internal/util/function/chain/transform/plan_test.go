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

package transform

import (
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func inputExpr() *schemapb.FunctionChainExpr {
	return &schemapb.FunctionChainExpr{
		Name: "py_udf",
		Args: []*schemapb.FunctionChainExprArg{{Arg: &schemapb.FunctionChainExprArg_Column{
			Column: &schemapb.FunctionChainColumnArg{Name: "text"},
		}}},
		Params: map[string]*schemapb.FunctionParamValue{
			"udf_params": {Value: &schemapb.FunctionParamValue_ObjectValue{ObjectValue: &schemapb.FunctionParamObject{
				Fields: map[string]*schemapb.FunctionParamValue{
					"limits": {Value: &schemapb.FunctionParamValue_ArrayValue{ArrayValue: &schemapb.FunctionParamArray{
						Values: []*schemapb.FunctionParamValue{
							{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 128}},
							{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}},
						},
					}}},
				},
			}}},
		},
	}
}

func TestPlanProtobufAndOwnership(t *testing.T) {
	expr := inputExpr()
	flatMap, err := FlatMap("chunk", expr, ExplodeOptions{Position: "chunk_index"})
	require.NoError(t, err)
	selectOp, err := Select([]string{"doc_id", "chunk.text"}, []string{"doc_id", "text"})
	require.NoError(t, err)
	plan, err := PreInsert("documents", flatMap, selectOp)
	require.NoError(t, err)
	require.Equal(t, schemapb.FunctionChainStage_FunctionChainStageIngestion, plan.Stage)
	require.Equal(t, OpFlatMap, plan.Ops[0].Op)
	require.Empty(t, plan.Ops[0].Inputs)
	require.Equal(t, []string{"chunk", "chunk_index"}, plan.Ops[0].Outputs)
	require.IsType(t, &schemapb.FunctionParamValue_BoolValue{}, plan.Ops[0].Params["outer"].Value)
	require.False(t, plan.Ops[0].Params["outer"].GetBoolValue())

	before := proto.Clone(plan).(*schemapb.FunctionChain)
	expr.Params["udf_params"].GetObjectValue().Fields["limits"].GetArrayValue().Values[0].Value = &schemapb.FunctionParamValue_Int64Value{Int64Value: 999}
	flatMap.Outputs[0] = "changed"
	selectOp.Inputs[0] = "changed"
	require.True(t, proto.Equal(before, plan), "builders must not retain caller-owned mutable protobufs")

	wire, err := proto.Marshal(plan)
	require.NoError(t, err)
	decoded := &schemapb.FunctionChain{}
	require.NoError(t, proto.Unmarshal(wire, decoded))
	require.True(t, proto.Equal(plan, decoded))
	require.NoError(t, ValidatePlan(decoded))
	require.True(t, proto.Equal(before, plan), "validation must not mutate a public plan")
}

func TestBuilderContracts(t *testing.T) {
	inputs := []string{"doc_id", "text"}
	selectOp, err := Select(inputs, nil)
	require.NoError(t, err)
	inputs[0] = "changed"
	require.Equal(t, []string{"doc_id", "text"}, selectOp.Inputs)
	require.Equal(t, selectOp.Inputs, selectOp.Outputs)
	implicit := &schemapb.FunctionChainOp{Op: OpSelect, Inputs: []string{"text", "doc_id"}}
	_, err = PreInsert("implicit select", implicit)
	require.NoError(t, err)
	require.Empty(t, implicit.Outputs, "validation does not materialize implicit output names")
	_, err = PreInsert("implicit duplicates", &schemapb.FunctionChainOp{Op: OpSelect, Inputs: []string{"text", "text"}})
	require.ErrorIs(t, err, merr.ErrParameterInvalid)

	explode, err := Explode("chunks", "chunk", ExplodeOptions{Outer: true, Position: "index"})
	require.NoError(t, err)
	require.Equal(t, []string{"chunks"}, explode.Inputs)
	require.Equal(t, []string{"chunk", "index"}, explode.Outputs)
	require.True(t, explode.Params["outer"].GetBoolValue())
	require.Nil(t, explode.Expr)

	for _, stage := range []schemapb.FunctionChainStage{
		schemapb.FunctionChainStage_FunctionChainStagePreProcess,
		schemapb.FunctionChainStage_FunctionChainStagePostProcess,
	} {
		constructor := PreProcess
		if stage == schemapb.FunctionChainStage_FunctionChainStagePostProcess {
			constructor = PostProcess
		}
		plan, err := constructor("projection", selectOp)
		require.NoError(t, err)
		require.Equal(t, stage, plan.Stage)
		_, err = constructor("expansion", explode)
		if stage == schemapb.FunctionChainStage_FunctionChainStagePreProcess {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		}
	}
}

func TestMalformedPlans(t *testing.T) {
	validSelect, err := Select([]string{"text"}, nil)
	require.NoError(t, err)
	validFlatMap, err := FlatMap("chunk", inputExpr(), ExplodeOptions{})
	require.NoError(t, err)
	copyOp := func(op *schemapb.FunctionChainOp, change func(*schemapb.FunctionChainOp)) *schemapb.FunctionChainOp {
		copy := proto.Clone(op).(*schemapb.FunctionChainOp)
		change(copy)
		return copy
	}
	tests := []struct {
		name string
		op   *schemapb.FunctionChainOp
	}{
		{"nil op", nil},
		{"unknown op", &schemapb.FunctionChainOp{Op: "unknown"}},
		{"aggregate reserved", &schemapb.FunctionChainOp{Op: "aggregate"}},
		{"reduce reserved", &schemapb.FunctionChainOp{Op: "reduce"}},
		{"empty column", &schemapb.FunctionChainOp{Op: OpSelect, Inputs: []string{" "}, Outputs: []string{"text"}}},
		{"projection mismatch", &schemapb.FunctionChainOp{Op: OpSelect, Inputs: []string{"text"}, Outputs: []string{"a", "b"}}},
		{"duplicate aliases", &schemapb.FunctionChainOp{Op: OpSelect, Inputs: []string{"a", "b"}, Outputs: []string{"c", "c"}}},
		{"projection expr", copyOp(validSelect, func(op *schemapb.FunctionChainOp) { op.Expr = inputExpr() })},
		{"explode multiple inputs", &schemapb.FunctionChainOp{Op: OpExplode, Inputs: []string{"a", "b"}, Outputs: []string{"element"}}},
		{"explode unknown option", &schemapb.FunctionChainOp{Op: OpExplode, Inputs: []string{"a"}, Outputs: []string{"element"}, Params: map[string]*schemapb.FunctionParamValue{"zip": {Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}}}}},
		{"explode outer not bool", &schemapb.FunctionChainOp{Op: OpExplode, Inputs: []string{"a"}, Outputs: []string{"element"}, Params: map[string]*schemapb.FunctionParamValue{"outer": {Value: &schemapb.FunctionParamValue_StringValue{StringValue: "false"}}}}},
		{"flat map no expr", &schemapb.FunctionChainOp{Op: OpFlatMap, Outputs: []string{"element"}}},
		{"expression explicit inputs", copyOp(validFlatMap, func(op *schemapb.FunctionChainOp) { op.Inputs = []string{"ignored"} })},
		{"expression no name", copyOp(validFlatMap, func(op *schemapb.FunctionChainOp) { op.Expr.Name = "" })},
		{"nil arg", copyOp(validFlatMap, func(op *schemapb.FunctionChainOp) { op.Expr.Args[0] = nil })},
		{"unset arg", copyOp(validFlatMap, func(op *schemapb.FunctionChainOp) { op.Expr.Args[0] = &schemapb.FunctionChainExprArg{} })},
		{"nil column", copyOp(validFlatMap, func(op *schemapb.FunctionChainOp) { op.Expr.Args[0].Arg = &schemapb.FunctionChainExprArg_Column{} })},
		{"unset param", copyOp(validFlatMap, func(op *schemapb.FunctionChainOp) { op.Expr.Params["bad"] = &schemapb.FunctionParamValue{} })},
		{"nil nested param", copyOp(validFlatMap, func(op *schemapb.FunctionChainOp) {
			op.Expr.Params["udf_params"].GetObjectValue().Fields["limits"].GetArrayValue().Values[0] = nil
		})},
		{"filter output", &schemapb.FunctionChainOp{Op: "filter", Expr: inputExpr(), Outputs: []string{"bad"}}},
		{"map missing output", &schemapb.FunctionChainOp{Op: "map", Expr: inputExpr()}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := PreInsert("bad", test.op)
			require.Error(t, err)
			require.Equal(t, merr.InputError, merr.GetErrorType(err))
		})
	}
	require.ErrorIs(t, ValidatePlan(nil), merr.ErrParameterMissing)
	_, err = PreInsert("empty")
	require.ErrorIs(t, err, merr.ErrParameterMissing)
	require.ErrorIs(t, ValidatePlan(&schemapb.FunctionChain{Stage: schemapb.FunctionChainStage_FunctionChainStageL2Rerank, Ops: []*schemapb.FunctionChainOp{validSelect}}), merr.ErrParameterInvalid)
}

func TestStageRestrictionAndExpressionParams(t *testing.T) {
	mapOp := &schemapb.FunctionChainOp{Op: "map", Expr: inputExpr(), Outputs: []string{"translated"}}
	filterOp := &schemapb.FunctionChainOp{Op: "filter", Expr: inputExpr()}
	flatMap, err := FlatMap("chunk", inputExpr(), ExplodeOptions{})
	require.NoError(t, err)
	for index, constructor := range []func(string, ...*schemapb.FunctionChainOp) (*schemapb.FunctionChain, error){PreProcess, PostProcess} {
		_, err := constructor("map", mapOp)
		require.NoError(t, err)
		_, err = constructor("filter", filterOp)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		_, err = constructor("flat map", flatMap)
		if index == 0 {
			require.NoError(t, err)
		} else {
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		}
	}
	_, err = PreInsert("filter", filterOp)
	require.NoError(t, err)

	tooDeep := &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: 1}}
	for range 66 {
		tooDeep = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_ArrayValue{ArrayValue: &schemapb.FunctionParamArray{Values: []*schemapb.FunctionParamValue{tooDeep}}}}
	}
	mapOp.Expr.Params["nested"] = tooDeep
	_, err = PreInsert("deep", mapOp)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
}

func TestTypedNilValuesAreRejectedBeforeClone(t *testing.T) {
	var column *schemapb.FunctionChainExprArg_Column
	var literal *schemapb.FunctionChainExprArg_Literal
	for _, arg := range []*schemapb.FunctionChainExprArg{{Arg: column}, {Arg: literal}} {
		expr := inputExpr()
		expr.Args[0] = arg
		_, err := FlatMap("chunk", expr, ExplodeOptions{})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		_, err = PreInsert("typed nil arg", &schemapb.FunctionChainOp{Op: OpFlatMap, Expr: expr, Outputs: []string{"chunk"}})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	}
	var boolValue *schemapb.FunctionParamValue_BoolValue
	var arrayValue *schemapb.FunctionParamValue_ArrayValue
	var objectValue *schemapb.FunctionParamValue_ObjectValue
	values := []*schemapb.FunctionParamValue{{Value: boolValue}, {Value: arrayValue}, {Value: objectValue}}
	for _, value := range values {
		expr := inputExpr()
		expr.Params["bad"] = value
		_, err := FlatMap("chunk", expr, ExplodeOptions{})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		require.ErrorIs(t, ValidatePlan(&schemapb.FunctionChain{
			Stage: schemapb.FunctionChainStage_FunctionChainStageIngestion,
			Ops:   []*schemapb.FunctionChainOp{{Op: OpExplode, Inputs: []string{"chunks"}, Outputs: []string{"chunk"}, Params: map[string]*schemapb.FunctionParamValue{"outer": value}}},
		}), merr.ErrParameterInvalid)
	}
}

func TestInputTypeHintContract(t *testing.T) {
	expr := inputExpr()
	expr.Args = append(expr.Args,
		&schemapb.FunctionChainExprArg{Arg: &schemapb.FunctionChainExprArg_Literal{Literal: &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: false}}}},
		&schemapb.FunctionChainExprArg{Arg: &schemapb.FunctionChainExprArg_Column{Column: &schemapb.FunctionChainColumnArg{Name: "text"}}},
	)
	flatMap, err := FlatMap("chunk", expr, ExplodeOptions{})
	require.NoError(t, err)
	hint := func(values ...int64) *schemapb.FunctionParamValue {
		items := make([]*schemapb.FunctionParamValue, len(values))
		for i, value := range values {
			items[i] = &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: value}}
		}
		return &schemapb.FunctionParamValue{Value: &schemapb.FunctionParamValue_ArrayValue{ArrayValue: &schemapb.FunctionParamArray{Values: items}}}
	}
	flatMap.Params["$input_data_types"] = hint(int64(schemapb.DataType_VarChar), int64(schemapb.DataType_VarChar))
	_, err = PreInsert("occurrence aligned hints", flatMap)
	require.NoError(t, err, "two repeated columns require two hints; literals occupy no hint slot")
	for _, value := range []*schemapb.FunctionParamValue{
		nil,
		{Value: &schemapb.FunctionParamValue_StringValue{StringValue: "VarChar"}},
		hint(int64(schemapb.DataType_VarChar)),
		hint(int64(schemapb.DataType_VarChar), 99999),
		hint(int64(schemapb.DataType_VarChar), 1<<40),
		{Value: &schemapb.FunctionParamValue_ArrayValue{ArrayValue: &schemapb.FunctionParamArray{Values: []*schemapb.FunctionParamValue{
			{Value: &schemapb.FunctionParamValue_BoolValue{BoolValue: true}},
			{Value: &schemapb.FunctionParamValue_Int64Value{Int64Value: int64(schemapb.DataType_VarChar)}},
		}}}},
	} {
		flatMap.Params["$input_data_types"] = value
		_, err := PreInsert("bad hints", flatMap)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	}
}

func TestProtectedIdentityColumns(t *testing.T) {
	for _, name := range []string{"$id", "$score", "$lineage"} {
		_, err := PreProcess("rewrite identity", &schemapb.FunctionChainOp{Op: "map", Expr: inputExpr(), Outputs: []string{name}})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		_, err = FlatMap(name, inputExpr(), ExplodeOptions{})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		_, err = Explode("chunks", "chunk", ExplodeOptions{Position: name})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		_, err = Select([]string{"text"}, []string{name})
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
		passthrough, err := Select([]string{name}, []string{name})
		require.NoError(t, err)
		_, err = PostProcess("keep identity", passthrough)
		require.NoError(t, err)
		_, err = PostProcess("implicit keep identity", &schemapb.FunctionChainOp{Op: OpSelect, Inputs: []string{name}})
		require.NoError(t, err)
	}
	_, err := Select([]string{"text", " text "}, nil)
	require.ErrorIs(t, err, merr.ErrParameterInvalid, "normalized duplicate aliases are rejected")
}
