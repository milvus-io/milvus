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
	textSchema := &schemapb.FieldSchema{
		Name: "text", FieldID: 101, DataType: schemapb.DataType_Text, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "1024"}},
	}
	builder.SetFieldSchema("text", textSchema)
	original := builder.Build()
	// Mutating the caller's schemas cannot alter the DataFrame definitions.
	textSchema.FieldID = 999
	textSchema.TypeParams[0].Value = "changed"
	selected, err := ExecuteOperator(types.NewFuncContext(pool), original, NewLimitOp(1, 0))
	require.NoError(t, err)
	original.Release()
	pruned, err := PruneDataFrame(selected, ColumnSet{"text": {}, "computed": {}}, SystemColumnPolicy{KeepAllSystemColumns: true})
	require.NoError(t, err)
	selected.Release()
	defer pruned.Release()
	require.Equal(t, int64(101), pruned.fieldMetadata["text"].field.FieldID)
	require.Equal(t, "1024", pruned.fieldMetadata["text"].field.TypeParams[0].Value)
	require.False(t, pruned.IsComputedColumn("text"))
	require.True(t, pruned.IsComputedColumn("computed"))
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
	require.Equal(t, int64(100), input.fieldMetadata["value"].field.FieldID)
	require.False(t, input.fieldMetadata["value"].field.Nullable)
	require.Equal(t, int64(101), copied.fieldMetadata["value"].field.FieldID)
	require.True(t, copied.fieldMetadata["value"].field.Nullable)
	replacement := selectionTestColumn(t, pool, arrow.PrimitiveTypes.Int64, `[7]`)
	result := &OperatorResult{Columns: []NamedColumn{{Field: arrow.Field{Name: "value", Type: arrow.PrimitiveTypes.Int64}, Data: replacement}}}
	output, err := assembleOperatorResult(types.NewFuncContext(pool), input, []string{"value"}, result)
	result.Release()
	require.NoError(t, err)
	defer output.Release()
	require.True(t, output.IsComputedColumn("value"))
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
		builder.SetFieldSchema("label", &schemapb.FieldSchema{
			Name: "label", FieldID: 101, DataType: schemapb.DataType_Text,
			TypeParams: []*commonpb.KeyValuePair{{Key: "max_length", Value: "128"}},
		})
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
	require.Equal(t, "128", output.fieldMetadata["label"].field.TypeParams[0].Value)
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
	b := NewDataFrameBuilder()
	defer b.Release()
	b.SetFieldSchema("field", &schemapb.FieldSchema{Name: "field", FieldID: 100, DataType: schemapb.DataType_Text, Nullable: true})
	require.NoError(t, b.SetFieldType("field", schemapb.DataType_Text))
	err := b.SetFieldType("field", schemapb.DataType_Int64)
	require.ErrorIs(t, err, merr.ErrServiceInternal)
	require.Equal(t, merr.SystemError, merr.GetErrorType(err))
	b.SetFieldSchema("field", &schemapb.FieldSchema{Name: "field", FieldID: 200, DataType: schemapb.DataType_Int64})
	require.NoError(t, b.SetFieldType("field", schemapb.DataType_Int64))
	df := b.Build()
	defer df.Release()
	require.Equal(t, int64(200), df.fieldMetadata["field"].field.FieldID)
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
	require.True(t, out.IsComputedColumn("value"))
	require.True(t, out.IsComputedColumn("value"))
	exported, err := exportFieldData(out, "value")
	require.NoError(t, err)
	require.Zero(t, exported.FieldId)
	require.Equal(t, schemapb.DataType_Double, exported.Type)
	require.Equal(t, []float64{1.5}, exported.GetScalars().GetDoubleData().Data)
}

func TestComputedJSONDeclarationSurvivesSelectionAndPruning(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	inputBuilder := NewDataFrameBuilder().SetChunkSizes([]int64{4})
	drop := array.NewInt64Builder(pool)
	drop.AppendValues([]int64{1, 2, 3, 4}, nil)
	require.NoError(t, inputBuilder.AddColumnFromChunks("drop", []arrow.Array{drop.NewArray()}))
	drop.Release()
	input := inputBuilder.Build()
	inputBuilder.Release()
	defer input.Release()
	b := array.NewBinaryBuilder(pool, arrow.BinaryTypes.Binary)
	b.Append([]byte(`[{"content":"a","starts":[0],"ends":[1]}]`))
	b.AppendNull()
	b.Append([]byte(`[]`))
	b.Append([]byte(`[{"content":"b","starts":[0],"ends":[1]}]`))
	values := b.NewArray()
	b.Release()
	column := arrow.NewChunked(values.DataType(), []arrow.Array{values})
	values.Release()
	field := grepOutputField("matches")
	field.Nullable = true
	result := &OperatorResult{Columns: []NamedColumn{{Field: field, Data: column}}}
	frame, err := assembleOperatorResult(types.NewFuncContext(pool), input, []string{"matches"}, result)
	result.Release()
	require.NoError(t, err)
	selected, err := assembleOperatorResult(types.NewFuncContext(pool), frame, nil, &OperatorResult{
		Selection: &RowSelection{Chunks: []ChunkSelection{RowIndices([]int{3, 1, 0, 3})}},
	})
	frame.Release()
	require.NoError(t, err)
	pruned, err := PruneDataFrame(selected, ColumnSet{"matches": {}}, SystemColumnPolicy{})
	selected.Release()
	require.NoError(t, err)
	defer pruned.Release()
	logical, ok := pruned.FieldType("matches")
	require.True(t, ok)
	require.Equal(t, schemapb.DataType_JSON, logical)
	require.True(t, pruned.IsComputedColumn("matches"))
	_, hasID := pruned.FieldID("matches")
	require.False(t, hasID)
	raw, ok := pruned.Schema().Field(0).Metadata.GetValue(ArrowDataTypeMetadataKey)
	require.True(t, ok)
	require.Equal(t, field.Metadata.Values()[0], raw)
	exported, err := exportFieldData(pruned, "matches")
	require.NoError(t, err)
	require.Equal(t, []bool{true, false, true, true}, typeutil.GetFieldDataValidData(exported))
	payloads := exported.GetScalars().GetJsonData().Data
	require.JSONEq(t, `[{"content":"b","starts":[0],"ends":[1]}]`, string(payloads[0]))
	require.JSONEq(t, `[{"content":"a","starts":[0],"ends":[1]}]`, string(payloads[2]))
	require.Equal(t, payloads[0], payloads[3])
}

func TestComputedLogicalTypeMetadataValidation(t *testing.T) {
	for name, field := range map[string]arrow.Field{
		"malformed": {Name: "x", Type: arrow.BinaryTypes.Binary, Metadata: arrow.NewMetadata([]string{ArrowDataTypeMetadataKey}, []string{"JSON"})},
		"unknown":   {Name: "x", Type: arrow.BinaryTypes.Binary, Metadata: arrow.NewMetadata([]string{ArrowDataTypeMetadataKey}, []string{"999999"})},
		"mismatch":  {Name: "x", Type: arrow.BinaryTypes.String, Metadata: grepOutputField("x").Metadata},
	} {
		t.Run(name, func(t *testing.T) {
			builder := NewDataFrameBuilder()
			defer builder.Release()
			err := builder.SetComputedField(field)
			require.ErrorIs(t, err, merr.ErrServiceInternal)
			require.Equal(t, merr.SystemError, merr.GetErrorType(err))
		})
	}
	// Binary alone has no unambiguous Milvus logical type.
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{0})
	defer builder.Release()
	require.NoError(t, builder.SetComputedField(arrow.Field{Name: "x", Type: arrow.BinaryTypes.Binary}))
	b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	require.NoError(t, builder.AddColumnFromChunks("x", []arrow.Array{b.NewArray()}))
	b.Release()
	frame := builder.Build()
	defer frame.Release()
	_, err := exportFieldData(frame, "x")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}
