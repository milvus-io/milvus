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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type schemaAwareTestExpr struct {
	operatorTransformExpr
	schema *arrow.Schema
}

func (e *schemaAwareTestExpr) ExecuteWithSchema(ctx *types.FuncContext, inputSchema *arrow.Schema, inputs []*arrow.Chunked) (*arrow.Schema, []*arrow.Chunked, error) {
	outputs, err := e.execute(ctx, inputs)
	return e.schema, outputs, err
}

func schemaTestFrame(t *testing.T, pool memory.Allocator, field arrow.Field, count int, null bool) *DataFrame {
	t.Helper()
	values := array.NewBuilder(pool, field.Type)
	defer values.Release()
	for range count {
		if null {
			values.AppendNull()
		} else {
			values.AppendEmptyValue()
		}
	}
	b := NewDataFrameBuilder()
	defer b.Release()
	b.SetChunkSizes([]int64{int64(count)}).SetIdentityLineage()
	require.NoError(t, b.AddColumnFromChunks(field.Name, []arrow.Array{values.NewArray()}))
	b.SetFieldNullable(field.Name, field.Nullable).SetArrowFieldMetadata(field.Name, field.Metadata)
	if _, annotated := field.Metadata.GetValue(types.MetadataKeyMilvusDataType); annotated {
		logical, err := types.MilvusSchemaFromArrowField(field)
		require.NoError(t, err)
		b.SetFieldType(field.Name, logical.GetDataType())
	}
	return b.Build()
}

func TestRuntimeSchemaRejectsInputBeforeCallingFunction(t *testing.T) {
	for _, count := range []int{0, 2} {
		t.Run(string(rune('0'+count)), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			input := schemaTestFrame(t, pool, arrow.Field{Name: "a", Type: arrow.BinaryTypes.String}, count, false)
			defer input.Release()
			calls := 0
			fn := &operatorTransformExpr{name: "input_guard", stages: []string{types.StagePostProcess}, execute: func(_ *types.FuncContext, _ []*arrow.Chunked) ([]*arrow.Chunked, error) {
				calls++
				return nil, nil
			}}
			for _, name := range []string{"a", "absent"} {
				op, err := NewMapOp(fn, []string{name}, []string{"result"})
				require.NoError(t, err)
				op.SetInputDataTypes([]schemapb.DataType{schemapb.DataType_Int64})
				fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).Add(op)
				result, err := fc.Execute(input)
				require.Error(t, err)
				require.Nil(t, result)
				require.Zero(t, calls)
			}
		})
	}
}

func TestRuntimeSchemaRejectsIncorrectDeclaredOutputType(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := schemaTestFrame(t, pool, arrow.Field{Name: "a", Type: arrow.BinaryTypes.String}, 2, false)
	defer input.Release()
	fn := &operatorTransformExpr{name: "wrong_type", stages: []string{types.StagePostProcess}, outputs: []arrow.DataType{arrow.PrimitiveTypes.Int64}, execute: func(_ *types.FuncContext, inputs []*arrow.Chunked) ([]*arrow.Chunked, error) {
		inputs[0].Retain()
		return []*arrow.Chunked{inputs[0]}, nil
	}}
	fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).Map(fn, []string{"a"}, []string{"result"})
	result, err := fc.Execute(input)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	require.Nil(t, result)
	require.EqualValues(t, 2, input.Column("a").Len())
}

func TestRuntimeSchemaPreservesLogicalVectorTypeAcrossMapSelectIPC(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	field, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "vector", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}})
	require.NoError(t, err)
	input := schemaTestFrame(t, pool, field, 2, false)
	defer input.Release()
	returned, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "udf_result", DataType: schemapb.DataType_BFloat16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}})
	require.NoError(t, err)
	metadata := arrow.MetadataFrom(map[string]string{"model_version": "test-v1"})
	fn := &schemaAwareTestExpr{operatorTransformExpr: operatorTransformExpr{name: "change_logical_type", stages: []string{types.StagePostProcess}, execute: func(_ *types.FuncContext, values []*arrow.Chunked) ([]*arrow.Chunked, error) {
		values[0].Retain()
		return []*arrow.Chunked{values[0]}, nil
	}}, schema: arrow.NewSchema([]arrow.Field{returned}, &metadata)}
	fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).Map(fn, []string{"vector"}, []string{"mapped"}).SelectAs([]string{"mapped"}, []string{"embedding"})
	result, err := fc.ExecuteWithContext(context.Background(), input)
	require.NoError(t, err)
	defer result.Release()
	batch, err := ToArrowBatch(result)
	require.NoError(t, err)
	defer ReleaseArrowBatch(batch)
	restored, err := types.MilvusSchemaFromArrowField(batch.Groups[0].Schema().Field(0))
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_BFloat16Vector, restored.GetDataType())
	require.Equal(t, "embedding", restored.GetName())
	require.EqualValues(t, 2, batch.Groups[0].NumRows())
	require.Equal(t, input.Lineage(), result.Lineage())
	require.True(t, metadata.Equal(batch.Groups[0].Schema().Metadata()))
}

func TestRuntimeSchemaRejectsNonnullableFunctionOutput(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	field := arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int64, Nullable: true}
	input := schemaTestFrame(t, pool, field, 1, true)
	defer input.Release()
	fn := &schemaAwareTestExpr{operatorTransformExpr: operatorTransformExpr{name: "wrong_nullability", stages: []string{types.StagePostProcess}, execute: func(_ *types.FuncContext, values []*arrow.Chunked) ([]*arrow.Chunked, error) {
		values[0].Retain()
		return []*arrow.Chunked{values[0]}, nil
	}}, schema: arrow.NewSchema([]arrow.Field{{Name: "out", Type: field.Type, Nullable: false}}, nil)}
	fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).Map(fn, []string{"a"}, []string{"result"})
	result, err := fc.Execute(input)
	require.ErrorIs(t, err, merr.ErrFunctionFailed)
	require.Nil(t, result)
}

func TestRuntimeSchemaExplodePreservesEncodedVectorElementType(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	field, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{
		Name: "embeddings", DataType: schemapb.DataType_ArrayOfVector,
		ElementType: schemapb.DataType_BFloat16Vector, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}},
	})
	require.NoError(t, err)
	b := array.NewListBuilderWithField(pool, field.Type.(arrow.ListLikeType).ElemField())
	defer b.Release()
	b.Append(true)
	b.ValueBuilder().(*array.FixedSizeBinaryBuilder).Append([]byte{1, 2, 3, 4})
	b.ValueBuilder().(*array.FixedSizeBinaryBuilder).Append([]byte{5, 6, 7, 8})
	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes([]int64{1}).SetIdentityLineage().SetArrowFieldMetadata("embeddings", field.Metadata)
	require.NoError(t, builder.AddColumnFromChunks("embeddings", []arrow.Array{b.NewArray()}))
	input := builder.Build()
	defer input.Release()
	fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
		Explode("embeddings", "embedding").Select("embedding")
	result, err := fc.Execute(input)
	require.NoError(t, err)
	defer result.Release()
	batch, err := ToArrowBatch(result)
	require.NoError(t, err)
	defer ReleaseArrowBatch(batch)
	logical, err := types.MilvusSchemaFromArrowField(batch.Groups[0].Schema().Field(0))
	require.NoError(t, err)
	require.Equal(t, schemapb.DataType_BFloat16Vector, logical.GetDataType())
	require.EqualValues(t, 2, batch.Groups[0].NumRows())
	require.Equal(t, input.Lineage()[0][0], result.Lineage()[0][1])
}

func TestRuntimeSchemaScalarHintsRetainActualLogicalType(t *testing.T) {
	for _, test := range []struct{ actual, hint schemapb.DataType }{
		{schemapb.DataType_Text, schemapb.DataType_VarChar},
		{schemapb.DataType_VarChar, schemapb.DataType_String},
		{schemapb.DataType_Timestamptz, schemapb.DataType_Int64},
	} {
		t.Run(test.actual.String(), func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			field, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "a", DataType: test.actual})
			require.NoError(t, err)
			input := schemaTestFrame(t, pool, field, 1, false)
			defer input.Release()
			calls := 0
			fn := &schemaAwareTestExpr{operatorTransformExpr: operatorTransformExpr{name: "compatible_hint", stages: []string{types.StagePostProcess}, execute: func(_ *types.FuncContext, columns []*arrow.Chunked) ([]*arrow.Chunked, error) {
				calls++
				columns[0].Retain()
				return []*arrow.Chunked{columns[0]}, nil
			}}, schema: arrow.NewSchema([]arrow.Field{field}, nil)}
			op, err := NewMapOp(fn, []string{"a"}, []string{"result"})
			require.NoError(t, err)
			op.SetInputDataTypes([]schemapb.DataType{test.hint})
			fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).Add(op)
			output, err := fc.Execute(input)
			require.NoError(t, err)
			defer output.Release()
			actual, ok := output.FieldType("result")
			require.True(t, ok)
			require.Equal(t, test.actual, actual)
			require.Equal(t, 1, calls)
		})
	}
}
