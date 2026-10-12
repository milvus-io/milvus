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
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math"
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

type aggregateSchemaCase struct {
	name    string
	field   arrow.Field
	values  string
	ordered bool
}

func aggregateRuntimeSchemaCases(t *testing.T) []aggregateSchemaCase {
	t.Helper()
	var cases []aggregateSchemaCase
	for _, spec := range []struct {
		typeName schemapb.DataType
		values   string
		ordered  bool
	}{
		{schemapb.DataType_Bool, "[true,null,false]", true},
		{schemapb.DataType_Int8, "[2,null,1]", true},
		{schemapb.DataType_Int16, "[2,null,1]", true},
		{schemapb.DataType_Int32, "[2,null,1]", true},
		{schemapb.DataType_Int64, "[2,null,1]", true},
		{schemapb.DataType_Float, "[2,null,1]", true},
		{schemapb.DataType_Double, "[2,null,1]", true},
		{schemapb.DataType_String, `["b",null,"a"]`, true},
		{schemapb.DataType_VarChar, `["b",null,"a"]`, true},
		{schemapb.DataType_Text, `["b",null,"a"]`, true},
		{schemapb.DataType_Date, "[2,null,1]", true},
		{schemapb.DataType_Time, "[2,null,1]", true},
		{schemapb.DataType_Timestamptz, "[2,null,1]", true},
		{schemapb.DataType_JSON, `["e30=",null,"eyJ2IjoxfQ=="]`, false},
		{schemapb.DataType_Geometry, `["AQEAAAAAAAAAAADwPwAAAAAAAABA",null,"AQEAAAAAAAAAAADwPwAAAAAAAPA/"]`, false},
		{schemapb.DataType_Mol, `["Ag==",null,"AQ=="]`, false},
		{schemapb.DataType_SparseFloatVector, `["AQAAAAAAAEA=",null,"AQAAAAAAgD8="]`, false},
	} {
		field, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "v", DataType: spec.typeName, Nullable: true})
		require.NoError(t, err)
		cases = append(cases, aggregateSchemaCase{spec.typeName.String(), field, spec.values, spec.ordered})
	}
	for _, typeName := range []schemapb.DataType{schemapb.DataType_UUID, schemapb.DataType_Decimal, schemapb.DataType_BinaryVector, schemapb.DataType_Float16Vector, schemapb.DataType_BFloat16Vector, schemapb.DataType_Int8Vector} {
		params := []*commonpb.KeyValuePair(nil)
		if typeName == schemapb.DataType_Decimal {
			params = []*commonpb.KeyValuePair{{Key: "precision", Value: "18"}, {Key: "scale", Value: "2"}}
		} else if typeName != schemapb.DataType_UUID {
			params = []*commonpb.KeyValuePair{{Key: "dim", Value: "8"}}
		}
		field, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "v", DataType: typeName, Nullable: true, TypeParams: params})
		require.NoError(t, err)
		width := field.Type.(*arrow.FixedSizeBinaryType).ByteWidth
		encoded, err := json.Marshal([]any{base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{2}, width)), nil, base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{1}, width))})
		require.NoError(t, err)
		cases = append(cases, aggregateSchemaCase{typeName.String(), field, string(encoded), typeName == schemapb.DataType_UUID})
	}
	for _, spec := range []struct {
		schema *schemapb.FieldSchema
		values string
	}{
		{&schemapb.FieldSchema{Name: "v", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int32, Nullable: true}, "[[2],null,[1]]"},
		{&schemapb.FieldSchema{Name: "v", DataType: schemapb.DataType_FloatVector, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}, "[[2,3],null,[1,2]]"},
		{&schemapb.FieldSchema{Name: "v", DataType: schemapb.DataType_Struct, Nullable: true}, `[{"text":"b"},null,{"text":"a"}]`},
		{&schemapb.FieldSchema{Name: "v", DataType: schemapb.DataType_ArrayOfStruct, Nullable: true}, `[[{"text":"b"}],null,[{"text":"a"}]]`},
		{&schemapb.FieldSchema{Name: "v", DataType: schemapb.DataType_ArrayOfVector, ElementType: schemapb.DataType_FloatVector, Nullable: true, TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}, "[[[2,3]],null,[[1,2]]]"},
	} {
		field, err := types.ArrowFieldFromMilvusSchema(spec.schema, arrow.Field{Name: "text", Type: arrow.BinaryTypes.String, Nullable: true})
		require.NoError(t, err)
		cases = append(cases, aggregateSchemaCase{spec.schema.GetDataType().String(), field, spec.values, false})
	}
	for _, spec := range []struct {
		dt      arrow.DataType
		values  string
		ordered bool
	}{
		{arrow.BinaryTypes.LargeString, `["b",null,"a"]`, true},
		{arrow.BinaryTypes.Binary, `["Yg==",null,"YQ=="]`, true},
		{arrow.BinaryTypes.LargeBinary, `["Yg==",null,"YQ=="]`, true},
		{&arrow.FixedSizeBinaryType{ByteWidth: 2}, `["YmI=",null,"YWE="]`, true},
		{arrow.PrimitiveTypes.Uint64, "[18446744073709551615,null,9223372036854775808]", true},
		{arrow.FixedWidthTypes.Date32, `["1970-01-03",null,"1970-01-02"]`, true},
		{arrow.FixedWidthTypes.Date64, `["1970-01-03",null,"1970-01-02"]`, true},
		{&arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}, "[2,null,1]", true},
		{&arrow.Time32Type{Unit: arrow.Second}, "[2,null,1]", true},
		{&arrow.Time64Type{Unit: arrow.Microsecond}, "[2,null,1]", true},
		{&arrow.DurationType{Unit: arrow.Microsecond}, "[2,null,1]", true},
		{&arrow.Decimal128Type{Precision: 8, Scale: 2}, `["2.50",null,"1.25"]`, false},
		{arrow.ListOf(arrow.PrimitiveTypes.Int32), "[[2],null,[1]]", false},
		{arrow.LargeListOf(arrow.PrimitiveTypes.Int32), "[[2],null,[1]]", false},
		{arrow.FixedSizeListOf(2, arrow.PrimitiveTypes.Float32), "[[2,3],null,[1,2]]", false},
		{arrow.StructOf(arrow.Field{Name: "text", Type: arrow.BinaryTypes.LargeString, Nullable: true}), `[{"text":"b"},null,{"text":"a"}]`, false},
		{arrow.ListOf(arrow.StructOf(arrow.Field{Name: "text", Type: arrow.BinaryTypes.LargeString, Nullable: true})), `[[{"text":"b"}],null,[{"text":"a"}]]`, false},
	} {
		cases = append(cases, aggregateSchemaCase{spec.dt.String(), arrow.Field{Name: "v", Type: spec.dt, Nullable: true}, spec.values, spec.ordered})
	}
	return cases
}

func aggregateSchemaInput(t *testing.T, alloc memory.Allocator, field arrow.Field, data arrow.Array) *DataFrame {
	t.Helper()
	groups := []arrow.Table{
		operatorTransformTable(t, []arrow.Field{field}, []arrow.Array{data}, int64(data.Len())),
		operatorTransformTable(t, []arrow.Field{field}, []arrow.Array{array.MakeArrayOfNull(alloc, field.Type, 0)}, 0),
		operatorTransformTable(t, []arrow.Field{field}, []arrow.Array{array.MakeArrayOfNull(alloc, field.Type, 2)}, 2),
	}
	batch := transform.Batch{Groups: groups}
	defer ReleaseArrowBatch(batch)
	input, err := FromArrowBatch(batch, alloc)
	require.NoError(t, err)
	return input
}

func TestAggregatePreservesEveryRuntimeTypeAndLogicalMetadata(t *testing.T) {
	for _, spec := range aggregateRuntimeSchemaCases(t) {
		t.Run(spec.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			data, _, err := array.FromJSON(pool, spec.field.Type, strings.NewReader(spec.values))
			require.NoError(t, err)
			input := aggregateSchemaInput(t, pool, spec.field, data)
			defer input.Release()
			output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
				Aggregate(nil, []string{"n", "first", "last", "values"}, AggregateSpec{Op: "count", Input: "v"}, AggregateSpec{Op: "first", Input: "v"}, AggregateSpec{Op: "last", Input: "v"}, AggregateSpec{Op: "collect_list", Input: "v"}).Execute(input)
			require.NoError(t, err)
			defer output.Release()
			require.Equal(t, []int64{1, 1, 1}, output.ChunkSizes())
			for group, count := range []int64{2, 0, 0} {
				require.Equal(t, count, output.Column("n").Chunk(group).(*array.Int64).Value(0))
				collected := output.Column("values").Chunk(group).(*array.List)
				require.True(t, array.Equal(input.Column("v").Chunk(group), collected.ListValues()))
				for _, name := range []string{"first", "last"} {
					require.True(t, arrow.TypeEqual(spec.field.Type, output.Column(name).DataType()), name)
					if group != 0 {
						require.True(t, output.Column(name).Chunk(group).IsNull(0))
					}
				}
			}
			first, last := array.NewSlice(input.Column("v").Chunk(0), 0, 1), array.NewSlice(input.Column("v").Chunk(0), 2, 3)
			defer first.Release()
			defer last.Release()
			require.True(t, array.Equal(first, output.Column("first").Chunk(0)))
			require.True(t, array.Equal(last, output.Column("last").Chunk(0)))
			if logicalType, ok := spec.field.Metadata.GetValue(types.MetadataKeyMilvusDataType); ok {
				for _, name := range []string{"first", "last"} {
					field, err := output.resolveArrowField(name)
					require.NoError(t, err)
					actual, present := field.Metadata.GetValue(types.MetadataKeyMilvusDataType)
					require.True(t, present)
					require.Equal(t, logicalType, actual)
				}
				element := output.Column("values").DataType().(*arrow.ListType).ElemField()
				actual, present := element.Metadata.GetValue(types.MetadataKeyMilvusDataType)
				require.True(t, present)
				require.Equal(t, logicalType, actual)
			}
			batch, err := ToArrowBatch(output)
			require.NoError(t, err)
			defer ReleaseArrowBatch(batch)
			for _, table := range batch.Groups {
				for index, field := range table.Schema().Fields() {
					require.NoError(t, types.ValidateArrowArrayAgainstField(table.Column(index).Data().Chunk(0), field))
				}
			}
		})
	}
}

func TestAggregateMinMaxUsesComparableRuntimeTypeAndNullSemantics(t *testing.T) {
	for _, spec := range aggregateRuntimeSchemaCases(t) {
		t.Run(spec.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			data, _, err := array.FromJSON(pool, spec.field.Type, strings.NewReader(spec.values))
			require.NoError(t, err)
			input := aggregateSchemaInput(t, pool, spec.field, data)
			defer input.Release()
			for _, op := range []string{"min", "max"} {
				output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).Aggregate(nil, []string{"out"}, AggregateSpec{Op: op, Input: "v"}).Execute(input)
				if !spec.ordered {
					require.ErrorIs(t, err, merr.ErrParameterInvalid)
					require.Nil(t, output)
					continue
				}
				require.NoError(t, err)
				index := int64(0)
				if op == "min" {
					index = 2
				}
				expected := array.NewSlice(input.Column("v").Chunk(0), index, index+1)
				require.True(t, array.Equal(expected, output.Column("out").Chunk(0)))
				expected.Release()
				require.True(t, output.Column("out").Chunk(1).IsNull(0))
				require.True(t, output.Column("out").Chunk(2).IsNull(0))
				output.Release()
			}
		})
	}
}

func TestAggregateNumericRejectsLogicalDatesStoredAsIntegers(t *testing.T) {
	for _, logicalType := range []schemapb.DataType{schemapb.DataType_Date, schemapb.DataType_Time, schemapb.DataType_Timestamptz} {
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		field, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "v", DataType: logicalType, Nullable: true})
		require.NoError(t, err)
		data, _, err := array.FromJSON(pool, field.Type, strings.NewReader("[2,null,1]"))
		require.NoError(t, err)
		input := aggregateSchemaInput(t, pool, field, data)
		for _, op := range []string{"sum", "avg"} {
			output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).Aggregate(nil, []string{"out"}, AggregateSpec{Op: op, Input: "v"}).Execute(input)
			require.ErrorIs(t, err, merr.ErrParameterInvalid, logicalType.String()+op)
			require.Nil(t, output)
		}
		input.Release()
		pool.AssertSize(t, 0)
	}
}

func TestAggregateMinMaxRetainsExistingNaNBehavior(t *testing.T) {
	for _, nanFirst := range []bool{false, true} {
		pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
		values := []float64{2, math.NaN(), 1}
		if nanFirst {
			values = []float64{math.NaN(), 2, 1}
		}
		builder := array.NewFloat64Builder(pool)
		builder.AppendValues(values, nil)
		data := builder.NewArray()
		builder.Release()
		input := aggregateSchemaInput(t, pool, arrow.Field{Name: "v", Type: arrow.PrimitiveTypes.Float64, Nullable: true}, data)
		for _, op := range []string{"min", "max"} {
			output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).Aggregate(nil, []string{"out"}, AggregateSpec{Op: op, Input: "v"}).Execute(input)
			require.NoError(t, err)
			actual := output.Column("out").Chunk(0).(*array.Float64).Value(0)
			if nanFirst {
				require.True(t, math.IsNaN(actual))
			} else if op == "min" {
				require.EqualValues(t, 1, actual)
			} else {
				require.EqualValues(t, 2, actual)
			}
			output.Release()
		}
		input.Release()
		pool.AssertSize(t, 0)
	}
}

func TestAggregateGroupingUsesLogicalComparableTypesEvenForEmptyGroups(t *testing.T) {
	for _, spec := range aggregateRuntimeSchemaCases(t) {
		t.Run(spec.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			data, _, err := array.FromJSON(pool, spec.field.Type, strings.NewReader(spec.values))
			require.NoError(t, err)
			input := aggregateSchemaInput(t, pool, spec.field, data)
			defer input.Release()
			output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
				Aggregate([]string{"v"}, []string{"key", "n"}, AggregateSpec{Op: "count"}).Execute(input)
			if !spec.ordered {
				require.ErrorIs(t, err, merr.ErrParameterInvalid)
				require.Nil(t, output)
				return
			}
			require.NoError(t, err)
			defer output.Release()
			require.Equal(t, []int64{3, 0, 1}, output.ChunkSizes())
			require.True(t, array.Equal(input.Column("v").Chunk(0), output.Column("key").Chunk(0)))
			require.True(t, output.Column("key").Chunk(2).IsNull(0))
			require.EqualValues(t, 2, output.Column("n").Chunk(2).(*array.Int64).Value(0))
			require.Equal(t, transform.RowLineage{{Group: 2, Row: 0}, {Group: 2, Row: 1}}, output.Lineage()[2][0])
		})
	}
}

func aggregateNestedSchemaInput(t *testing.T, pool memory.Allocator) *DataFrame {
	t.Helper()
	tenant, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "tenant", DataType: schemapb.DataType_VarChar})
	require.NoError(t, err)
	metric, err := types.ArrowFieldFromMilvusSchema(&schemapb.FieldSchema{Name: "metric", DataType: schemapb.DataType_Int64})
	require.NoError(t, err)
	payloadType := arrow.StructOf(tenant, metric)
	data, _, err := array.FromJSON(pool, payloadType, strings.NewReader(`[{"tenant":"a","metric":10},null,{"tenant":"b","metric":20},{"tenant":"a","metric":30}]`))
	require.NoError(t, err)
	return aggregateSchemaInput(t, pool, arrow.Field{Name: "payload", Type: payloadType, Nullable: true}, data)
}

func TestAggregateNestedPathsMaskNullParentsAndPreserveType(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := aggregateNestedSchemaInput(t, pool)
	defer input.Release()
	output, err := NewFuncChainWithAllocator(pool).SetStage(types.StagePostProcess).SetTableResult(true).
		Aggregate([]string{`payload["tenant"]`}, []string{"tenant", "sum", "n", "values"}, AggregateSpec{Op: "sum", Input: "payload.metric"}, AggregateSpec{Op: "count", Input: `payload['metric']`}, AggregateSpec{Op: "collect_list", Input: "payload.metric"}).Execute(input)
	require.NoError(t, err)
	defer output.Release()
	require.Equal(t, []int64{3, 0, 1}, output.ChunkSizes())
	require.EqualValues(t, 40, output.Column("sum").Chunk(0).(*array.Int64).Value(0))
	require.EqualValues(t, 2, output.Column("n").Chunk(0).(*array.Int64).Value(0))
	require.True(t, output.Column("tenant").Chunk(0).IsNull(1))
	require.True(t, output.Column("sum").Chunk(0).IsNull(1))
	require.Zero(t, output.Column("n").Chunk(0).(*array.Int64).Value(1))
	require.True(t, output.Column("values").Chunk(0).(*array.List).ListValues().IsNull(2))
	require.Equal(t, transform.RowLineage{{Group: 0, Row: 0}, {Group: 0, Row: 3}}, output.Lineage()[0][0])
	field, err := output.resolveArrowField("tenant")
	require.NoError(t, err)
	logicalType, ok := field.Metadata.GetValue(types.MetadataKeyMilvusDataType)
	require.True(t, ok)
	require.Equal(t, "21", logicalType)
}
