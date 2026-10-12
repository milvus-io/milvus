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
	"context"
	"sync"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

const aggregateInterfaceFunction = "test_aggregate_public_interface"

var registerAggregateInterfaceFunction sync.Once

// This adapter observes the complete public expression build and the actual
// selected input schema, without changing the production Python factory.
type aggregateInterfaceExpr struct {
	operatorTransformExpr
	buildContext types.FunctionBuildContext
	args         []*schemapb.FunctionChainExprArg
	inputSchema  *arrow.Schema
	calls        int
	wrongRows    bool
}

func (*aggregateInterfaceExpr) SupportsReduction() bool { return true }

func (expr *aggregateInterfaceExpr) ExecuteWithSchema(ctx *types.FuncContext, schema *arrow.Schema, columns []*arrow.Chunked) (*arrow.Schema, []*arrow.Chunked, error) {
	expr.calls++
	expr.inputSchema = schema
	if !ctx.TableResult() || !ctx.Reduction() {
		return nil, nil, merr.WrapErrServiceInternal("test custom aggregate requires the grouped TABLE contract")
	}
	chunks := make([]arrow.Array, len(columns[0].Chunks()))
	for group := range chunks {
		var total int64
		for _, column := range columns {
			values := column.Chunk(group).(*array.Int64)
			for row := range values.Len() {
				if !values.IsNull(row) {
					total += values.Value(row)
				}
			}
		}
		builder := array.NewInt64Builder(ctx.Pool())
		builder.Append(total)
		if expr.wrongRows {
			builder.Append(total)
		}
		chunks[group] = builder.NewArray()
		builder.Release()
	}
	output := arrow.NewChunked(arrow.PrimitiveTypes.Int64, chunks)
	releaseAggregateArrays(chunks)
	metadata := arrow.NewMetadata([]string{"producer"}, []string{"aggregate_interface"})
	return arrow.NewSchema([]arrow.Field{{Name: "total", Type: arrow.PrimitiveTypes.Int64}}, &metadata), []*arrow.Chunked{output}, nil
}

func installAggregateInterfaceFunction() {
	registerAggregateInterfaceFunction.Do(func() {
		types.MustRegisterFunction(aggregateInterfaceFunction, func(ctx types.FunctionBuildContext, cfg types.FunctionConfig) (types.FunctionExpr, error) {
			if cfg.Params["without_reduction"].GetBoolValue() {
				return &operatorTransformExpr{name: cfg.Name, stages: []string{types.StagePostProcess}}, nil
			}
			return &aggregateInterfaceExpr{
				operatorTransformExpr: operatorTransformExpr{name: cfg.Name, stages: []string{types.StagePostProcess}},
				buildContext:          ctx,
				args:                  cfg.Args,
				wrongRows:             cfg.Params["wrong_rows"].GetBoolValue(),
			}, nil
		})
	})
}

func aggregateInterfaceProto() *schemapb.FunctionChainOp {
	return &schemapb.FunctionChainOp{
		Op:      types.OpTypeAggregate,
		Outputs: []string{"document", "total"},
		Params: map[string]*schemapb.FunctionParamValue{
			"keys":                    arrayParam(stringParam("doc_id")),
			types.InputDataTypesParam: arrayParam(intParam(int64(schemapb.DataType_None)), intParam(int64(schemapb.DataType_Int64)), intParam(int64(schemapb.DataType_Int64)), intParam(int64(schemapb.DataType_Int64))),
		},
		Expr: &schemapb.FunctionChainExpr{
			Name: aggregateInterfaceFunction,
			Args: []*schemapb.FunctionChainExprArg{columnArg("value"), columnArg("value"), columnArg("weight")},
		},
	}
}

func aggregateInterfacePlan(ops ...*schemapb.FunctionChainOp) *schemapb.FunctionChain {
	return &schemapb.FunctionChain{
		Stage: schemapb.FunctionChainStage_FunctionChainStagePostProcess,
		PostProcessOptions: &schemapb.PostProcessOptions{
			ResultMode: schemapb.FunctionChainResultMode_FunctionChainResultModeTable,
		},
		Ops: ops,
	}
}

func TestAggregateInterfaceRejectsRemovedOpcodes(t *testing.T) {
	for _, name := range []string{"reduce", "concat"} {
		t.Run(name, func(t *testing.T) {
			factory, found := GetOperatorFactory(name)
			require.False(t, found)
			require.Nil(t, factory)
			plan := aggregateInterfacePlan(&schemapb.FunctionChainOp{Op: name})
			require.ErrorIs(t, transform.ValidatePlan(plan), merr.ErrParameterInvalid)
			_, err := ProtoChainToRepr(plan)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			_, err = ParseFuncChainProto(plan, memory.DefaultAllocator)
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
			_, err = operatorFromRepr(&OperatorRepr{Type: name})
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}
	for _, name := range []string{types.OpTypeAggregate, types.OpTypeUnionAll} {
		factory, found := GetOperatorFactory(name)
		require.True(t, found)
		require.NotNil(t, factory)
	}
}

func TestAggregateInterfaceNativeOptionalExpressionDoesNotRelaxMap(t *testing.T) {
	native, err := NewAggregateOp(nil, []string{"rows"}, []AggregateSpec{{Op: "count"}})
	require.NoError(t, err)
	require.Nil(t, native.Expression())
	require.NoError(t, NewFuncChainWithAllocator(nil).SetStage(types.StagePostProcess).SetTableResult(true).Add(native).Validate())

	for _, op := range []Operator{&MapOp{}, &AggregateOp{}} {
		err := NewFuncChainWithAllocator(nil).SetStage(types.StagePostProcess).SetTableResult(true).Add(op).Validate()
		require.ErrorIs(t, err, merr.ErrServiceInternal, op.Name())
		require.Contains(t, err.Error(), "nil function")
	}
}

func TestAggregateInterfaceRejectsAmbiguousAndMalformedConfiguration(t *testing.T) {
	installAggregateInterfaceFunction()
	for _, value := range []*schemapb.FunctionParamValue{
		nil,
		arrayParam(),
		arrayParam(objectParam(map[string]*schemapb.FunctionParamValue{"op": stringParam("count")})),
	} {
		op := aggregateInterfaceProto()
		op.Params["aggregates"] = value
		require.ErrorIs(t, transform.ValidatePlan(aggregateInterfacePlan(op)), merr.ErrParameterInvalid)
		repr, err := ProtoOpToRepr(op)
		require.NoError(t, err)
		_, err = NewAggregateOpFromRepr(repr)
		require.ErrorIs(t, err, merr.ErrParameterInvalid)
	}

	for _, tc := range []struct {
		name  string
		param string
		value *schemapb.FunctionParamValue
	}{
		{"unknown_param", "unsupported", boolParam(true)},
		{"wrong_keys_type", "keys", intParam(1)},
		{"nil_keys", "keys", nil},
		{"empty_key", "keys", arrayParam(stringParam(""))},
		{"duplicate_key", "keys", arrayParam(stringParam("doc_id"), stringParam("doc_id"))},
	} {
		t.Run(tc.name, func(t *testing.T) {
			op := aggregateInterfaceProto()
			delete(op.Params, types.InputDataTypesParam)
			op.Params[tc.param] = tc.value
			plan := aggregateInterfacePlan(op)
			err := transform.ValidatePlan(plan)
			if err == nil {
				var repr *ChainRepr
				repr, err = ProtoChainToRepr(plan)
				if err == nil {
					_, err = FuncChainFromRepr(repr, memory.DefaultAllocator)
				}
			}
			require.ErrorIs(t, err, merr.ErrParameterInvalid)
		})
	}

	for _, output := range []string{"$score", "$query_index", "document"} {
		op := aggregateInterfaceProto()
		op.Outputs[1] = output
		repr, err := ProtoOpToRepr(op)
		require.NoError(t, err)
		_, err = NewAggregateOpFromRepr(repr)
		require.ErrorIs(t, err, merr.ErrParameterInvalid, output)
	}

	nonReducing := aggregateInterfaceProto()
	nonReducing.Expr.Params = map[string]*schemapb.FunctionParamValue{"without_reduction": boolParam(true)}
	_, err := ParseFuncChainProto(aggregateInterfacePlan(nonReducing), memory.DefaultAllocator)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Contains(t, err.Error(), "does not support reduction")

	explicitInputs := aggregateInterfaceProto()
	explicitInputs.Inputs = []string{"doc_id", "value", "weight"}
	require.ErrorIs(t, transform.ValidatePlan(aggregateInterfacePlan(explicitInputs)), merr.ErrParameterInvalid)
}

func TestAggregateInterfaceRefreshInfoPreservesPositionalHints(t *testing.T) {
	for _, prefixed := range []bool{false, true} {
		t.Run(map[bool]string{false: "expression_refs", true: "keys_and_refs"}[prefixed], func(t *testing.T) {
			inputs := []string{"value", "value", "weight"}
			hints := []schemapb.DataType{schemapb.DataType_Int64, schemapb.DataType_Int64, schemapb.DataType_Int64}
			if prefixed {
				inputs = append([]string{"doc_id"}, inputs...)
				hints = append([]schemapb.DataType{schemapb.DataType_VarChar}, hints...)
			}
			repr := &ChainRepr{Stage: types.StagePostProcess, TableResult: true, Operators: []OperatorRepr{{
				Type: types.OpTypeAggregate, Inputs: inputs, InputDataTypes: hints, Outputs: []string{"document", "total"},
				Params:   map[string]*schemapb.FunctionParamValue{"keys": arrayParam(stringParam("doc_id"))},
				Function: &FunctionRepr{Name: aggregateInterfaceFunction, Args: aggregateInterfaceProto().Expr.Args},
			}}}
			for range 2 {
				require.NoError(t, repr.RefreshInfo())
				require.Equal(t, []string{"doc_id", "value", "value", "weight"}, repr.Operators[0].Inputs)
				expectedKeyHint := schemapb.DataType_None
				if prefixed {
					expectedKeyHint = schemapb.DataType_VarChar
				}
				require.Equal(t, []schemapb.DataType{expectedKeyHint, schemapb.DataType_Int64, schemapb.DataType_Int64, schemapb.DataType_Int64}, repr.Operators[0].InputDataTypes)
				require.Equal(t, []string{"doc_id", "value", "weight"}, repr.Info.RequiredInputs)
			}
		})
	}
	for _, inputs := range [][]string{{"value"}, {"wrong_key", "value"}} {
		repr := &ChainRepr{Stage: types.StagePostProcess, TableResult: true, Operators: []OperatorRepr{{
			Type: types.OpTypeAggregate, Inputs: inputs, Outputs: []string{"document", "total"},
			Params:   map[string]*schemapb.FunctionParamValue{"keys": arrayParam(stringParam("doc_id"))},
			Function: &FunctionRepr{Name: aggregateInterfaceFunction},
		}}}
		require.ErrorIs(t, repr.RefreshInfo(), merr.ErrParameterInvalid)
	}
}

func aggregateInterfaceFrame(t *testing.T, pool memory.Allocator) *DataFrame {
	t.Helper()
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{2, 1}).SetIdentityLineage()
	defer builder.Release()
	for name, groups := range map[string][][]int64{"value": {{10, 40}, {20}}, "weight": {{1, 3}, {2}}, "unused": {{100, 100}, {100}}} {
		chunks := make([]arrow.Array, len(groups))
		for group, values := range groups {
			b := array.NewInt64Builder(pool)
			b.AppendValues(values, nil)
			chunks[group] = b.NewArray()
			b.Release()
		}
		require.NoError(t, builder.AddColumnFromChunks(name, chunks))
	}
	chunks := make([]arrow.Array, 2)
	for group, values := range [][]string{{"a", "b"}, {"a"}} {
		b := array.NewStringBuilder(pool)
		b.AppendValues(values, nil)
		chunks[group] = b.NewArray()
		b.Release()
	}
	require.NoError(t, builder.AddColumnFromChunks("doc_id", chunks))
	return builder.Build()
}

func TestAggregateInterfacePublicPlanFetchPruningAndExecution(t *testing.T) {
	installAggregateInterfaceFunction()
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := aggregateInterfaceFrame(t, pool)
	defer input.Release()
	plan := aggregateInterfacePlan(
		&schemapb.FunctionChainOp{Op: types.OpTypeUnionAll},
		aggregateInterfaceProto(),
		&schemapb.FunctionChainOp{Op: types.OpTypeSelect, Inputs: []string{"document", "total"}},
	)
	require.NoError(t, transform.ValidatePlan(plan))
	repr, err := ProtoChainToRepr(plan)
	require.NoError(t, err)
	for range 2 {
		require.NoError(t, repr.RefreshInfo())
		require.Equal(t, []string{"doc_id", "value", "weight"}, repr.Info.RequiredInputs)
		require.Equal(t, []string{"doc_id", "value", "value", "weight"}, repr.Operators[1].Inputs)
	}
	fetch, err := CompileDataFrameInputPlan(repr, &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 101, Name: "doc_id", DataType: schemapb.DataType_VarChar},
		{FieldID: 102, Name: "value", DataType: schemapb.DataType_Int64},
		{FieldID: 103, Name: "weight", DataType: schemapb.DataType_Int64},
		{FieldID: 104, Name: "unused", DataType: schemapb.DataType_Int64},
	}})
	require.NoError(t, err)
	require.Equal(t, []string{"doc_id", "value", "weight"}, fetch.PhysicalFieldNames())
	search := &types.SearchRuntimeInfo{MetricTypes: []string{"COSINE"}}
	fc, err := FuncChainFromReprWithContext(repr, pool, types.FunctionBuildContext{Search: search})
	require.NoError(t, err)
	aggregation := fc.operators[1].(*AggregateOp)
	fn := aggregation.Expression().(*aggregateInterfaceExpr)
	require.Same(t, search, fn.buildContext.Search)
	require.Equal(t, plan.Ops[1].Expr.Args, fn.args)
	output, err := fc.ExecuteWithOptions(context.Background(), ExecuteOptions{
		EnableColumnPruning: true,
		Downstream:          DownstreamSpec{RequiredColumns: []string{"document", "total"}},
	}, input)
	require.NoError(t, err)
	defer output.Release()
	require.Equal(t, 1, fn.calls)
	require.Equal(t, []string{"value", "value", "weight"}, []string{fn.inputSchema.Field(0).Name, fn.inputSchema.Field(1).Name, fn.inputSchema.Field(2).Name})
	require.Equal(t, []int64{2}, output.ChunkSizes())
	documents := output.Column("document").Chunk(0).(*array.String)
	require.Equal(t, []string{"a", "b"}, []string{documents.Value(0), documents.Value(1)})
	require.Equal(t, []int64{63, 83}, output.Column("total").Chunk(0).(*array.Int64).Int64Values())
	require.False(t, output.HasColumn("unused"))
	require.False(t, output.HasColumn("value"))
	require.Equal(t, transform.RowLineage{{Group: 0, Row: 0}, {Group: 1, Row: 0}}, output.Lineage()[0][0])
	require.Equal(t, transform.RowLineage{{Group: 0, Row: 1}}, output.Lineage()[0][1])
	batch, err := ToArrowBatch(output)
	require.NoError(t, err)
	defer ReleaseArrowBatch(batch)
	producer, exists := batch.Groups[0].Schema().Metadata().GetValue("producer")
	require.True(t, exists)
	require.Equal(t, "aggregate_interface", producer)

	stale := proto.Clone(plan).(*schemapb.FunctionChain)
	stale.Ops[2].Inputs = []string{"value"}
	_, err = ProtoChainToRepr(stale)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)

	bad := proto.Clone(plan).(*schemapb.FunctionChain)
	bad.Ops[1].Expr.Params = map[string]*schemapb.FunctionParamValue{"wrong_rows": boolParam(true)}
	badRepr, err := ProtoChainToRepr(bad)
	require.NoError(t, err)
	badChain, err := FuncChainFromRepr(badRepr, pool)
	require.NoError(t, err)
	result, err := badChain.ExecuteWithOptions(context.Background(), ExecuteOptions{EnableColumnPruning: true, Downstream: DownstreamSpec{RequiredColumns: []string{"document", "total"}}}, input)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	require.Nil(t, result)

	wrongType := proto.Clone(plan).(*schemapb.FunctionChain)
	wrongType.Ops[1].Params[types.InputDataTypesParam] = arrayParam(intParam(int64(schemapb.DataType_None)), intParam(int64(schemapb.DataType_VarChar)), intParam(int64(schemapb.DataType_Int64)), intParam(int64(schemapb.DataType_Int64)))
	typedChain, err := ParseFuncChainProto(wrongType, pool)
	require.NoError(t, err)
	typedFn := typedChain.operators[1].(*AggregateOp).Expression().(*aggregateInterfaceExpr)
	result, err = typedChain.ExecuteWithOptions(context.Background(), ExecuteOptions{EnableColumnPruning: true, Downstream: DownstreamSpec{RequiredColumns: []string{"document", "total"}}}, input)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	require.Nil(t, result)
	require.Zero(t, typedFn.calls, "a bad positional type binding must fail before executing user code")
}

func TestAggregateInterfaceStageAllowlist(t *testing.T) {
	stages := []string{types.StageIngestion, types.StagePreProcess, types.StagePostProcess, types.StageL0Rerank, types.StageL1Rerank, types.StageL2Rerank}
	for _, stage := range stages {
		t.Run(stage, func(t *testing.T) {
			fn := &aggregateInterfaceExpr{operatorTransformExpr: operatorTransformExpr{name: aggregateInterfaceFunction, stages: stages}}
			aggregation := &AggregateOp{BaseOp: BaseOp{inputs: []string{"value"}, outputs: []string{"total"}}, function: fn}
			for _, table := range []bool{false, true} {
				for _, op := range []Operator{aggregation, NewUnionAllOp()} {
					err := NewFuncChainWithAllocator(nil).SetStage(stage).SetTableResult(table).Add(op).Validate()
					if table && stage == types.StagePostProcess {
						require.NoError(t, err)
					} else {
						require.ErrorIs(t, err, merr.ErrParameterInvalid)
					}
				}
			}
		})
	}
}
