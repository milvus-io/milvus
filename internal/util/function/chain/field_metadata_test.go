/*
 * # Licensed to the LF AI & Data foundation under one
 * # or more contributor license agreements. See the NOTICE file
 * # distributed with this work for additional information
 * # regarding copyright ownership. The ASF licenses this file
 * # to you under the Apache License, Version 2.0 (the
 * # "License"); you may not use this file except in compliance
 * # with the License. You may obtain a copy of the License at
 * #
 * #     http://www.apache.org/licenses/LICENSE-2.0
 * #
 * # Unless required by applicable law or agreed to in writing, software
 * # distributed under the License is distributed on an "AS IS" BASIS,
 * # WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * # See the License for the specific language governing permissions and
 * # limitations under the License.
 */

package chain

import (
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestFieldDefinitionsSurviveSelectionAndPruning(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{2, 1})
	defer builder.Release()
	add := func(name string, dtype arrow.DataType, inputs ...string) {
		chunks := make([]arrow.Array, 0, len(inputs))
		for _, input := range inputs {
			values, _, err := array.FromJSON(pool, dtype, strings.NewReader(input))
			require.NoError(t, err)
			chunks = append(chunks, values)
		}
		require.NoError(t, builder.AddColumnFromChunks(name, chunks))
	}
	add("$id", arrow.PrimitiveTypes.Int64, `[10,20]`, `[30]`)
	add("$score", arrow.PrimitiveTypes.Float32, `[1,2]`, `[3]`)
	add("$timestamp", arrow.PrimitiveTypes.Int64, `[100,200]`, `[300]`)
	add("text", arrow.BinaryTypes.String, `["a","b"]`, `["c"]`)
	add("computed", arrow.PrimitiveTypes.Float32, `[0.1,0.2]`, `[0.3]`)
	add("drop", arrow.PrimitiveTypes.Int64, `[1,2]`, `[3]`)
	dtype := arrow.StructOf(arrow.Field{Name: "label", Type: arrow.ListOf(arrow.BinaryTypes.String)})
	add("items", dtype, `[{"label":["first"]},{"label":[]}]`, `[{"label":["last"]}]`)
	textSchema := &schemapb.FieldSchema{Name: "text", FieldID: 101, DataType: schemapb.DataType_Text, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "1024"}}}
	structSchema := &schemapb.StructArrayFieldSchema{Name: "items", FieldID: 200, Fields: []*schemapb.FieldSchema{
		{Name: "label", FieldID: 201, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Text},
	}}
	builder.SetFieldSchema("text", textSchema).SetStructArraySchema("items", structSchema)
	original := builder.Build()
	// Mutating the caller's schemas cannot alter the DataFrame definitions.
	textSchema.FieldID = 999
	textSchema.TypeParams[0].Value = "changed"
	structSchema.Fields[0].FieldID = 999
	selected, err := ExecuteOperator(types.NewFuncContext(pool), original, NewLimitOp(1, 0))
	require.NoError(t, err)
	original.Release()
	pruned, err := PruneDataFrame(selected, ColumnSet{"text": {}, "computed": {}, "items": {}}, SystemColumnPolicy{KeepAllSystemColumns: true})
	require.NoError(t, err)
	selected.Release()
	defer pruned.Release()
	require.Equal(t, int64(101), pruned.FieldSchema("text").FieldID)
	require.Equal(t, "1024", pruned.FieldSchema("text").TypeParams[0].Value)
	require.Equal(t, int64(201), pruned.StructArraySchema("items").Fields[0].FieldID)
	require.False(t, pruned.IsComputedColumn("text"))
	require.True(t, pruned.IsComputedColumn("computed"))
	detached := pruned.FieldSchema("text")
	detached.TypeParams[0].Value = "changed again"
	require.Equal(t, "1024", pruned.FieldSchema("text").TypeParams[0].Value)
	query, err := ToRetrieveResults(pruned)
	require.NoError(t, err)
	search, err := ToSearchResultDataWithOptions(pruned, &ExportOptions{SkipColumns: []string{"$timestamp"}})
	require.NoError(t, err)
	require.Equal(t, []int64{10, 30}, query.Ids.GetIntId().Data)
	require.Equal(t, []string{"a", "c"}, query.FieldsData[0].GetScalars().GetStringData().Data)
	require.Equal(t, []bool{true, true}, typeutil.GetFieldDataValidData(query.FieldsData[0]))
	require.Nil(t, typeutil.GetFieldDataValidData(search.FieldsData[0])) // Response-only compatibility policy.
	require.Equal(t, schemapb.DataType_Text, search.FieldsData[0].Type)
	byName := make(map[string]*schemapb.FieldData)
	for _, field := range search.FieldsData {
		byName[field.FieldName] = field
	}
	require.True(t, proto.Equal(byName["items"], query.FieldsData[1]))
	require.True(t, proto.Equal(byName["computed"], query.ComputedResults[0]))
}

func TestFieldDefinitionCopyOnWriteAndReplacement(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{1})
	defer builder.Release()
	values, _, err := array.FromJSON(pool, arrow.BinaryTypes.String, strings.NewReader(`["stored"]`))
	require.NoError(t, err)
	require.NoError(t, builder.AddColumnFromChunks("value", []arrow.Array{values}))
	builder.SetFieldSchema("value", &schemapb.FieldSchema{Name: "value", FieldID: 100, DataType: schemapb.DataType_Text})
	input := builder.Build()
	defer input.Release()
	copyBuilder := NewDataFrameBuilder().SetChunkSizes(input.ChunkSizes())
	defer copyBuilder.Release()
	require.NoError(t, copyBuilder.AddColumnFrom(input, "value"))
	copyBuilder.SetFieldID("value", 101).SetFieldNullable("value", true)
	copied := copyBuilder.Build()
	defer copied.Release()
	require.Equal(t, int64(100), input.FieldSchema("value").FieldID)
	require.False(t, input.FieldSchema("value").Nullable)
	require.Equal(t, int64(101), copied.FieldSchema("value").FieldID)
	require.True(t, copied.FieldSchema("value").Nullable)
	replacement := selectionTestColumn(t, pool, arrow.PrimitiveTypes.Int64, `[7]`)
	result := &OperatorResult{Columns: []NamedColumn{{Field: arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64}, Data: replacement}}}
	output, err := assembleOperatorResult(types.NewFuncContext(pool), input, []string{"value"}, result)
	result.Release()
	require.NoError(t, err)
	defer output.Release()
	require.True(t, output.IsComputedColumn("value"))
	require.Nil(t, output.FieldSchema("value"))
	exported, err := exportFieldData(output, "value")
	require.NoError(t, err)
	require.Zero(t, exported.FieldId)
	require.Equal(t, schemapb.DataType_Int64, exported.Type)
	require.Equal(t, []int64{7}, exported.GetScalars().GetLongData().Data)
}

func TestNullableComputedScalarUsesSameExport(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{2})
	defer builder.Release()
	for _, col := range []struct {
		name  string
		typ   arrow.DataType
		input string
	}{
		{"$id", arrow.PrimitiveTypes.Int64, `[1,2]`},
		{"$score", arrow.PrimitiveTypes.Float32, `[0.1,0.2]`},
		{"$timestamp", arrow.PrimitiveTypes.Int64, `[10,20]`},
		{"computed", arrow.PrimitiveTypes.Float32, `[1,null]`},
	} {
		values, _, err := array.FromJSON(pool, col.typ, strings.NewReader(col.input))
		require.NoError(t, err)
		require.NoError(t, builder.AddColumnFromChunks(col.name, []arrow.Array{values}))
	}
	builder.SetFieldNullable("computed", true)
	frame := builder.Build()
	defer frame.Release()
	query, err := ToRetrieveResults(frame)
	require.NoError(t, err)
	search, err := ToSearchResultDataWithOptions(frame, &ExportOptions{SkipColumns: []string{"$timestamp"}})
	require.NoError(t, err)
	require.True(t, proto.Equal(query.ComputedResults[0], search.FieldsData[0]))
	require.Equal(t, []bool{true, false}, typeutil.GetFieldDataValidData(query.ComputedResults[0]))
}

func TestMergePreservesFieldDefinitions(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	makeFrame := func(id int64, text string) *DataFrame {
		builder := NewDataFrameBuilder().SetChunkSizes([]int64{1})
		defer builder.Release()
		for _, name := range []string{"$id", "$timestamp", "computed"} {
			b := array.NewInt64Builder(pool)
			b.Append(id)
			require.NoError(t, builder.AddColumnFromChunks(name, []arrow.Array{b.NewArray()}))
			b.Release()
		}
		score := array.NewFloat32Builder(pool)
		score.Append(float32(id))
		require.NoError(t, builder.AddColumnFromChunks("$score", []arrow.Array{score.NewArray()}))
		score.Release()
		label := array.NewStringBuilder(pool)
		label.Append(text)
		require.NoError(t, builder.AddColumnFromChunks("label", []arrow.Array{label.NewArray()}))
		label.Release()
		builder.SetFieldSchema("label", &schemapb.FieldSchema{Name: "label", FieldID: 101, DataType: schemapb.DataType_Text,
			TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "128"}}})
		return builder.Build()
	}
	first, second := makeFrame(1, "first"), makeFrame(2, "second")
	output, err := NewMergeOp(MergeStrategyMax).ExecuteMulti(types.NewFuncContext(pool), []*DataFrame{first, second})
	first.Release()
	second.Release()
	require.NoError(t, err)
	defer output.Release()
	require.False(t, output.IsComputedColumn("label"))
	require.True(t, output.IsComputedColumn("computed"))
	require.Equal(t, "128", output.FieldSchema("label").TypeParams[0].Value)
	result, err := ToRetrieveResults(output)
	require.NoError(t, err)
	require.Len(t, result.ComputedResults, 1)
	require.Equal(t, []int64{2, 1}, result.Ids.GetIntId().Data)
	var ordinary *schemapb.FieldData
	for _, field := range result.FieldsData {
		if field.FieldName == "label" {
			ordinary = field
		}
	}
	require.NotNil(t, ordinary)
	require.Equal(t, int64(101), ordinary.FieldId)
	require.Equal(t, schemapb.DataType_Text, ordinary.Type)
	require.Equal(t, []string{"second", "first"}, ordinary.GetScalars().GetStringData().Data)
}

func TestSetFieldTypeRejectsDeclaredTypeChanges(t *testing.T) {
	for _, structArray := range []bool{false, true} {
		t.Run(map[bool]string{false: "scalar", true: "struct"}[structArray], func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			b := NewDataFrameBuilder().SetChunkSizes([]int64{1})
			defer b.Release()
			var dtype arrow.DataType = arrow.BinaryTypes.String
			input := `["value"]`
			logical := schemapb.DataType_Text
			if structArray {
				dtype = arrow.StructOf(arrow.Field{Name: "label", Type: arrow.ListOf(arrow.BinaryTypes.String)})
				input = `[{"label":["value"]}]`
				logical = schemapb.DataType_ArrayOfStruct
				b.SetStructArraySchema("field", &schemapb.StructArrayFieldSchema{Name: "field", FieldID: 100, Nullable: true, Fields: []*schemapb.FieldSchema{
					{Name: "label", FieldID: 101, DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Text},
				}})
			} else {
				b.SetFieldSchema("field", &schemapb.FieldSchema{Name: "field", FieldID: 100, DataType: logical, Nullable: true})
			}
			require.NoError(t, b.SetFieldType("field", logical))
			err := b.SetFieldType("field", schemapb.DataType_Int64)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			values, _, err := array.FromJSON(pool, dtype, strings.NewReader(input))
			require.NoError(t, err)
			require.NoError(t, b.AddColumnFromChunks("field", []arrow.Array{values}))
			df := b.Build()
			defer df.Release()
			got, ok := df.FieldType("field")
			require.True(t, ok)
			require.Equal(t, logical, got)
			require.False(t, df.IsComputedColumn("field"))
			require.True(t, df.FieldNullable("field"))
			output, err := exportFieldData(df, "field")
			require.NoError(t, err)
			require.Equal(t, int64(100), output.FieldId)
			require.Equal(t, logical, output.Type)
			if structArray {
				require.Equal(t, int64(101), df.StructArraySchema("field").Fields[0].FieldID)
				require.Equal(t, []string{"value"}, output.GetStructArrays().Fields[0].GetScalars().GetArrayData().Data[0].GetStringData().Data)
			} else {
				require.Equal(t, logical, df.FieldSchema("field").DataType)
			}
		})
	}
}

func TestFieldDefinitionReplacementIsExplicit(t *testing.T) {
	b := NewDataFrameBuilder()
	defer b.Release()
	b.SetStructArraySchema("field", &schemapb.StructArrayFieldSchema{Name: "field", FieldID: 100})
	b.SetFieldSchema("field", &schemapb.FieldSchema{Name: "field", FieldID: 200, DataType: schemapb.DataType_Int64})
	require.NoError(t, b.SetFieldType("field", schemapb.DataType_Int64))
	df := b.Build()
	defer df.Release()
	require.Nil(t, df.StructArraySchema("field"))
	require.Equal(t, int64(200), df.FieldSchema("field").FieldID)
	require.False(t, df.IsComputedColumn("field"))
}

func TestImportRejectsTypeConflictBeforeAllocating(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	b := NewDataFrameBuilder()
	defer b.Release()
	b.SetFieldSchema(types.ScoreFieldName, &schemapb.FieldSchema{DataType: schemapb.DataType_Text, FieldID: 100})
	err := importScores(b, []float32{1}, []int64{0, 1}, pool)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
}

func TestCopyColumnMetadataReplacesAbsentValues(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	srcBuilder := NewDataFrameBuilder().SetChunkSizes([]int64{1})
	defer srcBuilder.Release()
	values, _, err := array.FromJSON(pool, arrow.PrimitiveTypes.Float64, strings.NewReader(`[1.5]`))
	require.NoError(t, err)
	column := arrow.NewChunked(values.DataType(), []arrow.Array{values})
	values.Release()
	require.NoError(t, srcBuilder.AddColumns([]string{"value"}, []*arrow.Chunked{column}))
	source := srcBuilder.Build()
	defer source.Release()
	dst := NewDataFrameBuilder().SetChunkSizes(source.ChunkSizes())
	defer dst.Release()
	dst.SetFieldSchema("value", &schemapb.FieldSchema{Name: "value", FieldID: 100, DataType: schemapb.DataType_Text, Nullable: true})
	require.NoError(t, dst.AddColumnFrom(source, "value"))
	out := dst.Build()
	defer out.Release()
	_, hasType := out.FieldType("value")
	_, hasID := out.FieldID("value")
	require.False(t, hasType)
	require.False(t, hasID)
	require.False(t, out.FieldNullable("value"))
	require.Nil(t, out.FieldSchema("value"))
	require.True(t, out.IsComputedColumn("value"))
	exported, err := exportFieldData(out, "value")
	require.NoError(t, err)
	require.Zero(t, exported.FieldId)
	require.Equal(t, schemapb.DataType_Double, exported.Type)
	require.Equal(t, []float64{1.5}, exported.GetScalars().GetDoubleData().Data)
}

func TestMergeRejectsConflictingColumnDefinitions(t *testing.T) {
	for _, conflict := range []string{"origin", "id", "type", "params"} {
		t.Run(conflict, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			makeFrame := func(id int64, changed bool) *DataFrame {
				b := NewDataFrameBuilder().SetChunkSizes([]int64{1})
				defer b.Release()
				ids := array.NewInt64Builder(pool)
				ids.Append(id)
				require.NoError(t, b.AddColumnFromChunks("$id", []arrow.Array{ids.NewArray()}))
				ids.Release()
				scores := array.NewFloat32Builder(pool)
				scores.Append(float32(id))
				require.NoError(t, b.AddColumnFromChunks("$score", []arrow.Array{scores.NewArray()}))
				scores.Release()
				v := array.NewStringBuilder(pool)
				v.Append("value")
				require.NoError(t, b.AddColumnFromChunks("value", []arrow.Array{v.NewArray()}))
				v.Release()
				definition := &schemapb.FieldSchema{Name: "value", FieldID: 100, DataType: schemapb.DataType_Text,
					TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "128"}}}
				if changed {
					switch conflict {
					case "origin":
						return b.Build()
					case "id":
						definition.FieldID = 101
					case "type":
						definition.DataType = schemapb.DataType_VarChar
					case "params":
						definition.TypeParams[0].Value = "256"
					}
				}
				b.SetFieldSchema("value", definition)
				return b.Build()
			}
			a, b := makeFrame(1, false), makeFrame(2, true)
			defer a.Release()
			defer b.Release()
			for _, inputs := range [][]*DataFrame{{a, b}, {b, a}} {
				out, err := NewMergeOp(MergeStrategyMax).ExecuteMulti(types.NewFuncContext(pool), inputs)
				require.Nil(t, out)
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			}
		})
	}
}

func TestMergeJoinsNullableMetadata(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	makeFrame := func(id int64, nullable bool) *DataFrame {
		b := NewDataFrameBuilder().SetChunkSizes([]int64{1})
		defer b.Release()
		ib := array.NewInt64Builder(pool)
		ib.Append(id)
		require.NoError(t, b.AddColumnFromChunks("$id", []arrow.Array{ib.NewArray()}))
		ib.Release()
		sb := array.NewFloat32Builder(pool)
		sb.Append(float32(id))
		require.NoError(t, b.AddColumnFromChunks("$score", []arrow.Array{sb.NewArray()}))
		sb.Release()
		vb := array.NewStringBuilder(pool)
		if nullable {
			vb.AppendNull()
		} else {
			vb.Append("first")
		}
		require.NoError(t, b.AddColumnFromChunks("value", []arrow.Array{vb.NewArray()}))
		vb.Release()
		params := []*commonpb.KeyValuePair{{Key: "max_length", Value: "128"}, {Key: "enable_match", Value: "false"}}
		if nullable {
			params[0], params[1] = params[1], params[0]
		}
		b.SetFieldSchema("value", &schemapb.FieldSchema{Name: "value", FieldID: 100, DataType: schemapb.DataType_Text, Nullable: nullable, TypeParams: params})
		return b.Build()
	}
	a, b := makeFrame(1, false), makeFrame(2, true)
	defer a.Release()
	defer b.Release()
	for _, inputs := range [][]*DataFrame{{a, b}, {b, a}} {
		out, err := NewMergeOp(MergeStrategyMax).ExecuteMulti(types.NewFuncContext(pool), inputs)
		require.NoError(t, err)
		require.True(t, out.FieldNullable("value"))
		require.True(t, out.FieldSchema("value").Nullable)
		require.False(t, a.FieldNullable("value"))
		field, err := exportFieldData(out, "value")
		require.NoError(t, err)
		require.Equal(t, []bool{false, true}, typeutil.GetFieldDataValidData(field))
		require.Equal(t, []string{"", "first"}, field.GetScalars().GetStringData().Data)
		out.Release()
	}
}

func TestMergeMissingColumnWidensNullability(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	makeFrame := func(id int64, withValue bool) *DataFrame {
		b := NewDataFrameBuilder().SetChunkSizes([]int64{1})
		defer b.Release()
		ids := array.NewInt64Builder(pool)
		ids.Append(id)
		require.NoError(t, b.AddColumnFromChunks("$id", []arrow.Array{ids.NewArray()}))
		ids.Release()
		scores := array.NewFloat32Builder(pool)
		scores.Append(float32(id))
		require.NoError(t, b.AddColumnFromChunks("$score", []arrow.Array{scores.NewArray()}))
		scores.Release()
		if withValue {
			values := array.NewStringBuilder(pool)
			values.Append("value")
			require.NoError(t, b.AddColumnFromChunks("value", []arrow.Array{values.NewArray()}))
			values.Release()
		}
		return b.Build()
	}
	a, b := makeFrame(1, true), makeFrame(2, false)
	defer a.Release()
	defer b.Release()
	out, err := NewMergeOp(MergeStrategyMax).ExecuteMulti(types.NewFuncContext(pool), []*DataFrame{a, b})
	require.NoError(t, err)
	defer out.Release()
	require.True(t, out.FieldNullable("value"))
	result, err := ToSearchResultData(out)
	require.NoError(t, err)
	require.Equal(t, []bool{false, true}, typeutil.GetFieldDataValidData(result.FieldsData[0]))
	require.Equal(t, []string{"", "value"}, result.FieldsData[0].GetScalars().GetStringData().Data)
}
