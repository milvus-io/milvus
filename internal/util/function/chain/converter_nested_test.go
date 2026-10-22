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

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/function/chain/types"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestNestedExportPreservesSelectedRowsAndScalarMetadata(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{2, 1})
	defer builder.Release()
	add := func(name string, typ arrow.DataType, inputs ...string) {
		var chunks []arrow.Array
		for _, input := range inputs {
			values, _, err := array.FromJSON(pool, typ, strings.NewReader(input))
			require.NoError(t, err)
			chunks = append(chunks, values)
		}
		require.NoError(t, builder.AddColumnFromChunks(name, chunks))
	}
	add("$id", arrow.BinaryTypes.String, `["first","skip"]`, `["last"]`)
	add("$score", arrow.PrimitiveTypes.Float32, `[1,2]`, `[3]`)
	add("text", arrow.BinaryTypes.String, `["hello","skip"]`, `[null]`)
	builder.SetFieldType("text", schemapb.DataType_Text).SetFieldID("text", 101).SetFieldNullable("text", true)
	add("time", arrow.PrimitiveTypes.Int64, `[10,20]`, `[30]`)
	builder.SetFieldType("time", schemapb.DataType_Timestamptz).SetFieldID("time", 102)
	nested := arrow.ListOf(arrow.StructOf(
		arrow.Field{Name: "label", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "values", Type: arrow.ListOf(arrow.PrimitiveTypes.Int64)},
	))
	add("computed", nested, `[[{"label":"a","values":[1,2]}],[]]`, `[[{"label":"b","values":[3]}]]`)
	input := builder.Build()
	selected, err := ExecuteOperator(types.NewFuncContext(pool), input, NewLimitOp(1, 0))
	input.Release()
	require.NoError(t, err)
	output, err := ToSearchResultData(selected)
	selected.Release()
	require.NoError(t, err)
	// All Arrow owners have been released before inspecting the protobuf payload.
	require.Equal(t, []string{"first", "last"}, output.Ids.GetStrId().Data)
	fields := make(map[string]*schemapb.FieldData)
	for _, field := range output.FieldsData {
		fields[field.FieldName] = field
	}
	require.Equal(t, int64(101), fields["text"].FieldId)
	require.Equal(t, schemapb.DataType_Text, fields["text"].Type)
	require.Equal(t, []string{"hello", ""}, fields["text"].GetScalars().GetStringData().Data)
	require.Equal(t, []bool{true, false}, typeutil.GetFieldDataValidData(fields["text"]))
	require.Equal(t, int64(102), fields["time"].FieldId)
	require.Equal(t, []int64{10, 30}, fields["time"].GetScalars().GetTimestamptzData().Data)
	require.Zero(t, fields["computed"].FieldId)
	children := fields["computed"].GetStructArrays().Fields
	require.Equal(t, []string{"a"}, children[0].GetScalars().GetArrayData().Data[0].GetStringData().Data)
	require.Equal(t, []string{"b"}, children[0].GetScalars().GetArrayData().Data[1].GetStringData().Data)
	require.Equal(t, []int64{3}, children[1].GetScalars().GetArrayData().Data[1].GetArrayData().Data[0].GetLongData().Data)
}

func TestNestedExportEmptyAndInvalidValues(t *testing.T) {
	for _, tc := range []struct {
		name, input string
		valid       bool
	}{
		{"empty", `[]`, true},
		{"null list", `[null]`, false},
		{"null element", `[[null]]`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			values, _, err := array.FromJSON(pool, arrow.ListOf(arrow.PrimitiveTypes.Int64), strings.NewReader(tc.input))
			require.NoError(t, err)
			builder := NewDataFrameBuilder().SetChunkSizes([]int64{int64(values.Len())})
			defer builder.Release()
			require.NoError(t, builder.AddColumnFromChunks("computed", []arrow.Array{values}))
			frame := builder.Build()
			defer frame.Release()
			field, err := exportFieldData(frame, "computed")
			if tc.valid {
				require.NoError(t, err)
				require.Equal(t, schemapb.DataType_Array, field.Type)
				require.Equal(t, schemapb.DataType_Int64, field.GetScalars().GetArrayData().ElementType)
				require.Empty(t, field.GetScalars().GetArrayData().Data)
			} else {
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.Equal(t, merr.SystemError, merr.GetErrorType(err))
			}
		})
	}
}
