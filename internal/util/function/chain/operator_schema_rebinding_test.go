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
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestSchemaAwareMapCanRemoveSamePhysicalLogicalSourceType(t *testing.T) {
	for _, logical := range []schemapb.DataType{schemapb.DataType_JSON, schemapb.DataType_Float16Vector} {
		t.Run(logical.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			params := []*commonpb.KeyValuePair(nil)
			payload := `["eyJ4IjoyfQ==",null,"eyJ4IjoxfQ=="]`
			if logical == schemapb.DataType_Float16Vector {
				params = []*commonpb.KeyValuePair{{Key: "dim", Value: "1"}}
				payload = `["AgA=",null,"AQA="]`
			}
			field, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "v", DataType: logical, Nullable: true, TypeParams: params})
			require.NoError(t, err)
			data, _, err := array.FromJSON(pool, field.Type, strings.NewReader(payload))
			require.NoError(t, err)
			input := aggregateSchemaInput(t, pool, field, data)
			defer input.Release()
			fn := &reduceSchemaExpr{operatorTransformExpr: operatorTransformExpr{name: "plain_bytes", stages: []string{types.StagePostProcess}}, executeWithSchema: func(ctx *types.FuncContext, schema *arrow.Schema, columns []*arrow.Chunked) (*arrow.Schema, []*arrow.Chunked, error) {
				columns[0].Retain()
				// The complete returned schema deliberately declares plain bytes.
				return arrow.NewSchema([]arrow.Field{{Name: "plain", Type: columns[0].DataType(), Nullable: true}}, nil), []*arrow.Chunked{columns[0]}, nil
			}}
			output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
				Map(fn, []string{"v"}, []string{"v"}).Sort("v", false, "").Execute(input)
			require.NoError(t, err, "plain byte output must not inherit JSON/vector ordering restrictions")
			defer output.Release()
			actual, err := functionInputSchema(output, []string{"v"})
			require.NoError(t, err)
			_, tagged := actual.Field(0).Metadata.GetValue(types.MetadataKeyMilvusDataType)
			require.False(t, tagged)
			_, tagged = actual.Field(0).Metadata.GetValue(types.MetadataKeyMilvusFieldSchema)
			require.False(t, tagged)
			_, hasLogical := output.FieldType("v")
			require.False(t, hasLogical)
			require.Equal(t, []transform.RowLineage{{{Group: 0, Row: 2}}, {{Group: 0, Row: 0}}, {{Group: 0, Row: 1}}}, output.Lineage()[0])
		})
	}
}

func TestSchemaAwareMapRejectsReturnedSchemaPhysicalTypeMismatch(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := reduceTestFrame(t, pool, []int64{1}, nil)
	defer input.Release()
	fn := &reduceSchemaExpr{operatorTransformExpr: operatorTransformExpr{name: "wrong_physical", stages: []string{types.StagePostProcess}}, executeWithSchema: func(ctx *types.FuncContext, schema *arrow.Schema, columns []*arrow.Chunked) (*arrow.Schema, []*arrow.Chunked, error) {
		columns[0].Retain()
		return arrow.NewSchema([]arrow.Field{{Name: "x", Type: arrow.PrimitiveTypes.Float64, Nullable: true}}, nil), []*arrow.Chunked{columns[0]}, nil
	}}
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).Map(fn, []string{"key"}, []string{"x"}).Execute(input)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	require.Nil(t, output)
}

func TestLegacyMapNullableOutputNormalizesPreservedLogicalSchema(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	field, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "v", DataType: schemapb.DataType_Int64, Nullable: false})
	require.NoError(t, err)
	data, _, err := array.FromJSON(pool, field.Type, strings.NewReader("[2,1]"))
	require.NoError(t, err)
	table := operatorTransformTable(t, []arrow.Field{field}, []arrow.Array{data}, 2)
	defer table.Release()
	input, err := FromArrowBatch(transform.Batch{Groups: []arrow.Table{table}}, pool)
	require.NoError(t, err)
	defer input.Release()
	fn := &operatorTransformExpr{name: "legacy_nullable", stages: []string{types.StagePostProcess}, outputs: []arrow.DataType{arrow.PrimitiveTypes.Int64}, execute: func(ctx *types.FuncContext, columns []*arrow.Chunked) ([]*arrow.Chunked, error) {
		b := array.NewInt64Builder(ctx.Pool())
		b.AppendNull()
		b.Append(1)
		values := b.NewArray()
		b.Release()
		output := arrow.NewChunked(arrow.PrimitiveTypes.Int64, []arrow.Array{values})
		values.Release()
		return []*arrow.Chunked{output}, nil
	}}
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).Map(fn, []string{"v"}, []string{"v"}).Execute(input)
	require.NoError(t, err)
	defer output.Release()
	actual, err := functionInputSchema(output, []string{"v"})
	require.NoError(t, err)
	require.True(t, actual.Field(0).Nullable)
	logical, err := types.MilvusSchemaFromArrowField(actual.Field(0))
	require.NoError(t, err)
	require.True(t, logical.GetNullable())
	require.Equal(t, schemapb.DataType_Int64, logical.GetDataType())
	require.True(t, output.Column("v").Chunk(0).IsNull(0))
}

func TestSortKeepsTypeHintsFromDuplicateInputOccurrences(t *testing.T) {
	for _, hints := range [][]schemapb.DataType{
		{schemapb.DataType_None, schemapb.DataType_VarChar},
		{schemapb.DataType_Int64, schemapb.DataType_VarChar},
	} {
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		input := reduceTestFrame(t, pool, []int64{1, 2})
		plan, err := transform.PostTable("duplicate_hint", &schemapb.FunctionChainOp{Op: "aggregate", Outputs: []string{"n"}, Params: map[string]*schemapb.FunctionParamValue{
			"keys": arrayParam(), "aggregates": arrayParam(objectParam(map[string]*schemapb.FunctionParamValue{"op": stringParam("count")})),
		}}, &schemapb.FunctionChainOp{Op: "sort", Inputs: []string{"n", "n"}, Params: map[string]*schemapb.FunctionParamValue{
			"$input_data_types": arrayParam(intParam(int64(hints[0])), intParam(int64(hints[1]))),
		}})
		require.NoError(t, err)
		chain, err := ParseFuncChainProto(plan, pool)
		if err == nil {
			output, executeErr := chain.Execute(input)
			if output != nil {
				output.Release()
			}
			err = executeErr
		}
		require.ErrorIs(t, err, merr.ErrParameterInvalid, "normalization must retain every requested non-None binding")
		input.Release()
		pool.AssertSize(t, 0)
	}
}
