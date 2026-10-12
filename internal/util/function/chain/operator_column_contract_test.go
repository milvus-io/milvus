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
	"strconv"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/transform"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

type operatorContractColumn struct {
	name     string
	dataType arrow.DataType
	json     string
}

func operatorContractFrame(t *testing.T, pool memory.Allocator, rows int64, columns ...operatorContractColumn) *DataFrame {
	t.Helper()
	builder := NewDataFrameBuilder()
	defer builder.Release()
	builder.SetChunkSizes([]int64{rows}).SetIdentityLineage()
	for _, column := range columns {
		values, _, err := array.FromJSON(pool, column.dataType, strings.NewReader(column.json))
		require.NoError(t, err, column.name)
		require.EqualValues(t, rows, values.Len())
		nullable := values.NullN() > 0
		require.NoError(t, builder.AddColumnFromChunks(column.name, []arrow.Array{values}))
		builder.SetFieldNullable(column.name, nullable)
	}
	return builder.Build()
}

func TestSortResolvedStructKeysAndTieBreak(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	structure := arrow.StructOf(arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	input := operatorContractFrame(t, pool, 3,
		operatorContractColumn{"summary", structure, `[{"count":2},{"count":1},null]`},
		operatorContractColumn{"rank", arrow.PrimitiveTypes.Int64, `[1,1,1]`},
		operatorContractColumn{"$id", arrow.PrimitiveTypes.Int64, `[10,20,30]`})
	defer input.Release()
	for _, operation := range []*SortOp{
		newSortOp("summary.count", false, ""),
		newSortOp("rank", true, `summary["count"]`),
	} {
		output, err := operation.Execute(types.NewFuncContextFull(t.Context(), pool, types.StagePostProcess).WithTableResult(true), input)
		require.NoError(t, err)
		ids := output.Column("$id").Chunk(0).(*array.Int64).Int64Values()
		if operation.Column() == "rank" {
			// Keep the existing ascending tie-break null order, without silently
			// discarding a path that refers to a nullable Struct member.
			require.Equal(t, []int64{30, 20, 10}, ids)
		} else {
			require.Equal(t, []int64{20, 10, 30}, ids)
		}
		output.Release()
	}
}

func TestSortRejectsUnavailableOrNonComparableKeys(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := operatorContractFrame(t, pool, 2,
		operatorContractColumn{"rank", arrow.PrimitiveTypes.Int64, `[1,1]`},
		operatorContractColumn{"payload", arrow.ListOf(arrow.PrimitiveTypes.Int64), `[[2],[1]]`})
	defer input.Release()
	ctx := types.NewFuncContextFull(t.Context(), pool, types.StagePostProcess).WithTableResult(true)
	for _, operation := range []*SortOp{
		newSortOp("missing", false, ""),
		newSortOp("rank", false, "missing"),
		newSortOp("payload", false, ""),
		newSortOp("rank", false, "payload"),
	} {
		output, err := operation.Execute(ctx, input)
		require.Error(t, err, operation.String())
		require.Nil(t, output)
	}
	for _, logicalType := range []schemapb.DataType{schemapb.DataType_JSON, schemapb.DataType_Geometry, schemapb.DataType_BinaryVector, schemapb.DataType_SparseFloatVector} {
		field := arrow.Field{
			Name: "binary_payload", Type: arrow.BinaryTypes.Binary,
			Metadata: arrow.MetadataFrom(map[string]string{"milvus.data_type": strconv.Itoa(int(logicalType))}),
		}
		require.ErrorIs(t, validateComparableArrowField(field, "sort_op"), merr.ErrParameterInvalid)
	}
}

func TestSortTemporalAndBinaryScalarKeys(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	cases := []operatorContractColumn{
		{"key", arrow.FixedWidthTypes.Boolean, `[true,false,true]`},
		{"key", arrow.BinaryTypes.LargeString, `["b","a","c"]`},
		{"key", arrow.BinaryTypes.Binary, `["Yg==","YQ==","Yw=="]`},
		{"key", arrow.BinaryTypes.LargeBinary, `["Yg==","YQ==","Yw=="]`},
		{"key", &arrow.FixedSizeBinaryType{ByteWidth: 1}, `["Yg==","YQ==","Yw=="]`},
		{"key", &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, `[2,1,3]`},
		{"key", arrow.FixedWidthTypes.Date32, `[2,1,3]`},
		{"key", arrow.FixedWidthTypes.Date64, `[2,1,3]`},
	}
	for _, key := range cases {
		t.Run(key.dataType.String(), func(t *testing.T) {
			input := operatorContractFrame(t, pool, 3, key, operatorContractColumn{"position", arrow.PrimitiveTypes.Int64, `[0,1,2]`})
			defer input.Release()
			output, err := newSortOp("key", false, "").Execute(types.NewFuncContextFull(t.Context(), pool, types.StagePostProcess).WithTableResult(true), input)
			require.NoError(t, err)
			defer output.Release()
			require.Equal(t, []int64{1, 0, 2}, output.Column("position").Chunk(0).(*array.Int64).Int64Values())
		})
	}
}

func TestTableNestedCollectExplodeSortSelect(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	chunks := arrow.ListOf(arrow.StructOf(arrow.Field{Name: "rank", Type: arrow.PrimitiveTypes.Int64}))
	input := operatorContractFrame(t, pool, 2,
		operatorContractColumn{"chunks", chunks, `[[{"rank":3},{"rank":1}],[{"rank":2}]]`})
	defer input.Release()
	fc := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
		Aggregate(nil, []string{"chunks"}, AggregateSpec{Op: "collect_list", Input: "chunks"}).
		Explode("chunks", "batch").Explode("batch", "chunk").
		Sort("chunk.rank", false, "").SelectAs([]string{"chunk.rank"}, []string{"rank"})
	output, err := fc.Execute(input)
	require.NoError(t, err)
	defer output.Release()
	require.Equal(t, []int64{1, 2, 3}, output.Column("rank").Chunk(0).(*array.Int64).Int64Values())
	for _, sources := range output.Lineage()[0] {
		require.Equal(t, transform.RowLineage{{Group: 0, Row: 0}, {Group: 0, Row: 1}}, sources)
	}
}

func TestGroupByResolvedScalarPathAndUnsupportedKey(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	structure := arrow.StructOf(arrow.Field{Name: "category", Type: arrow.BinaryTypes.LargeString})
	input := operatorContractFrame(t, pool, 3,
		operatorContractColumn{"features", structure, `[{"category":"A"},{"category":"B"},{"category":"A"}]`},
		operatorContractColumn{"$id", arrow.PrimitiveTypes.Int64, `[1,2,3]`},
		operatorContractColumn{"$score", arrow.PrimitiveTypes.Float32, `[0.9,0.8,0.7]`})
	defer input.Release()
	ctx := types.NewFuncContextFull(t.Context(), pool, types.StageL2Rerank)
	output, err := NewGroupByOp("features.category", 1, 2, 0).Execute(ctx, input)
	require.NoError(t, err)
	defer output.Release()
	require.Equal(t, []int64{1, 2}, output.Column("$id").Chunk(0).(*array.Int64).Int64Values())
	_, err = NewGroupByOp("features", 1, 2, 0).Execute(ctx, input)
	require.ErrorIs(t, err, merr.ErrParameterInvalid)
	_, err = NewGroupByOp("features.missing", 1, 2, 0).Execute(ctx, input)
	require.Error(t, err)
}

func TestMergePreservesNestedPayloadsAndEmptySchema(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	cases := []operatorContractColumn{
		{"payload", arrow.BinaryTypes.Binary, `["Yg==","YQ=="]`},
		{"payload", arrow.BinaryTypes.LargeString, `["b","a"]`},
		{"payload", &arrow.FixedSizeBinaryType{ByteWidth: 1}, `["Yg==","YQ=="]`},
		{"payload", arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32), `[[2,3],[1,4]]`},
		{"payload", arrow.ListOf(arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32)), `[[[2,3]],[[1,4]]]`},
		{"payload", arrow.ListOf(arrow.StructOf(arrow.Field{Name: "text", Type: arrow.BinaryTypes.String})), `[[{"text":"b"}],[{"text":"a"}]]`},
	}
	for _, payload := range cases {
		t.Run(payload.dataType.String(), func(t *testing.T) {
			input := operatorContractFrame(t, pool, 2, payload,
				operatorContractColumn{"$id", arrow.PrimitiveTypes.Int64, `[2,1]`},
				operatorContractColumn{"$score", arrow.PrimitiveTypes.Float32, `[0.5,0.9]`})
			defer input.Release()
			ctx := types.NewFuncContextFull(t.Context(), pool, types.StageL2Rerank)
			output, err := NewMergeOp(MergeStrategyMax).Execute(ctx, input)
			require.NoError(t, err)
			defer output.Release()
			expected, err := takeArrayWithNulls(pool, input.Column("payload").Chunk(0), []int{1, 0})
			require.NoError(t, err)
			defer expected.Release()
			require.True(t, array.Equal(expected, output.Column("payload").Chunk(0)))
			empty := operatorContractFrame(t, pool, 0,
				operatorContractColumn{"payload", payload.dataType, `[]`},
				operatorContractColumn{"$id", arrow.BinaryTypes.String, `[]`},
				operatorContractColumn{"$score", arrow.PrimitiveTypes.Float32, `[]`})
			defer empty.Release()
			emptyOutput, err := NewMergeOp(MergeStrategyMax).Execute(ctx, empty)
			require.NoError(t, err)
			defer emptyOutput.Release()
			require.True(t, arrow.TypeEqual(payload.dataType, emptyOutput.Column("payload").DataType()))
			require.Equal(t, arrow.STRING, emptyOutput.Column("$id").DataType().ID())
		})
	}
}

func TestMergeRejectsIncompatibleNestedSchemaEvenWhenEmpty(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	for _, rows := range []int64{0, 1} {
		inputs := make([]*DataFrame, 0, 2)
		for _, dimension := range []int32{2, 3} {
			json, ids, scores := `[]`, `[]`, `[]`
			if rows != 0 {
				json, ids, scores = `[[1,2]]`, `[1]`, `[0.8]`
				if dimension == 3 {
					json = `[[1,2,3]]`
				}
			}
			input := operatorContractFrame(t, pool, rows,
				operatorContractColumn{"vector", arrow.FixedSizeListOf(dimension, arrow.PrimitiveTypes.Float32), json},
				operatorContractColumn{"$id", arrow.PrimitiveTypes.Int64, ids},
				operatorContractColumn{"$score", arrow.PrimitiveTypes.Float32, scores})
			inputs = append(inputs, input)
		}
		output, err := NewMergeOp(MergeStrategyRRF).ExecuteMulti(types.NewFuncContextFull(t.Context(), pool, types.StageL2Rerank), inputs)
		require.ErrorIs(t, err, merr.ErrFunctionFailed)
		require.Nil(t, output)
		require.ErrorContains(t, err, "does not match")
		for _, input := range inputs {
			input.Release()
		}
	}
}

func TestMergeRejectsConflictingLogicalAndChildMetadata(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	for _, nested := range []bool{false, true} {
		inputs := make([]*DataFrame, 0, 2)
		for _, logicalType := range []schemapb.DataType{schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector} {
			metadata := arrow.MetadataFrom(map[string]string{
				types.MetadataKeyMilvusDataType: strconv.Itoa(int(logicalType)), "dim": "2",
			})
			var dataType arrow.DataType = &arrow.FixedSizeBinaryType{ByteWidth: 4}
			if nested {
				dataType = arrow.StructOf(arrow.Field{Name: "vector", Type: dataType, Metadata: metadata})
			}
			input := operatorContractFrame(t, pool, 0,
				operatorContractColumn{"payload", dataType, `[]`},
				operatorContractColumn{"$id", arrow.PrimitiveTypes.Int64, `[]`})
			if !nested {
				builder := NewDataFrameBuilder()
				builder.SetChunkSizes(input.ChunkSizes())
				for _, name := range input.ColumnNames() {
					require.NoError(t, builder.AddColumnFrom(input, name))
				}
				builder.SetFieldType("payload", logicalType).SetArrowFieldMetadata("payload", metadata)
				tagged := builder.Build()
				input.Release()
				input = tagged
			}
			inputs = append(inputs, input)
		}
		output, err := NewMergeOp(MergeStrategyRRF).ExecuteMulti(types.NewFuncContextFull(t.Context(), pool, types.StageL2Rerank), inputs)
		require.ErrorIs(t, err, merr.ErrFunctionFailed)
		require.Nil(t, output)
		for _, input := range inputs {
			input.Release()
		}
	}
}
