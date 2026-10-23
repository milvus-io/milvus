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
	"fmt"
	"runtime"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/util/arrowconv"
	"github.com/milvus-io/milvus/internal/util/function/grep/native"
	"github.com/milvus-io/milvus/pkg/v3/proto/segcorepb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

func TestQueryScanDetailRowValues(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := `[{"content":[],"starts":[],"ends":[]},{"content":["超时 timeout"],"starts":[[7]],"ends":[[14]]},{"content":["x",""],"starts":[[0],[]],"ends":[[1],[]]}]`
	details, _, err := array.FromJSON(pool, native.GrepDetailsType(), strings.NewReader(input))
	require.NoError(t, err)
	out := emptyQueryScanComputed(&ChainRepr{Name: "scan", OutputColumns: []string{"matches"}})
	// Deliberately reorder and duplicate document rows.
	for _, row := range []int{2, 0, 1, 2} {
		require.NoError(t, appendGrepComputedRow(out[0], details.(*array.Struct), row))
	}
	details.Release() // Returned strings and arrays must outlive Arrow.
	fields := out[0].GetStructArrays().Fields
	content := fields[0].GetScalars().GetArrayData().Data
	require.Equal(t, []string{"x", ""}, content[0].GetStringData().Data)
	require.Empty(t, content[1].GetStringData().Data)
	require.Equal(t, []string{"超时 timeout"}, content[2].GetStringData().Data)
	starts := fields[1].GetScalars().GetArrayData().Data
	ends := fields[2].GetScalars().GetArrayData().Data
	require.Equal(t, []int64{7}, starts[2].GetArrayData().Data[0].GetLongData().Data)
	require.Equal(t, []int64{14}, ends[2].GetArrayData().Data[0].GetLongData().Data)
	require.Empty(t, starts[0].GetArrayData().Data[1].GetLongData().Data)

	// The materialized segcore result must retain computed rows on the wire,
	// even after the source Arrow storage has been released.
	result := &segcorepb.RetrieveResults{
		Ids: &schemapb.IDs{IdField: &schemapb.IDs_IntId{
			IntId: &schemapb.LongArray{Data: []int64{30, 10, 20, 30}},
		}},
		ComputedResults: out,
	}
	wire, err := proto.Marshal(result)
	require.NoError(t, err)
	decoded := &segcorepb.RetrieveResults{}
	require.NoError(t, proto.Unmarshal(wire, decoded))
	require.True(t, proto.Equal(result, decoded))
}

func TestQueryScanComputedSlicedArrow(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	input := `[{"content":[],"starts":[],"ends":[]},{"content":["hello"],"starts":[[1]],"ends":[[3]]},{"content":[],"starts":[],"ends":[]}]`
	details, _, err := array.FromJSON(pool, native.GrepDetailsType(), strings.NewReader(input))
	require.NoError(t, err)
	sliced := array.NewSlice(details, 1, 3).(*array.Struct)
	output := emptyQueryScanComputed(&ChainRepr{Name: "scan", OutputColumns: []string{"matches"}})
	require.NoError(t, appendGrepComputedRow(output[0], sliced, 0))
	require.NoError(t, appendGrepComputedRow(output[0], sliced, 1))
	sliced.Release()
	details.Release()
	contents := output[0].GetStructArrays().Fields[0].GetScalars().GetArrayData().Data
	require.Equal(t, []string{"hello"}, contents[0].GetStringData().Data)
	require.Empty(t, contents[1].GetStringData().Data)
}

func BenchmarkMaterializeQueryScanDetails(b *testing.B) {
	for _, lines := range []int{1, 100, 1000} {
		b.Run(fmt.Sprintf("lines_%d", lines), func(b *testing.B) {
			repeat := func(value string) string { return strings.TrimSuffix(strings.Repeat(value+",", lines), ",") }
			input := `[{"content":[` + repeat(`"ERROR timeout"`) + `],"starts":[` + repeat(`[0,6]`) + `],"ends":[` + repeat(`[5,13]`) + `]}]`
			details, _, err := array.FromJSON(memory.DefaultAllocator, native.GrepDetailsType(), strings.NewReader(input))
			if err != nil {
				b.Fatal(err)
			}
			defer details.Release()
			column := arrow.NewChunked(details.DataType(), []arrow.Array{details})
			defer column.Release()
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				if _, err := arrowconv.ArrowChunkedStructColumnToFieldData("matches", column, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// Exercise many wrapper entries, then retain only a later document. The
// protobuf graph must own its storage after Arrow and the original output die.
func TestQueryScanComputedBatchOwnership(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	contentValues, startValues, endValues := make([]string, 128), make([]string, 128), make([]string, 128)
	for i := range contentValues {
		contentValues[i] = fmt.Sprintf(`"%s"`, strings.Repeat("x", 256))
		startValues[i] = fmt.Sprintf(`[%d]`, i)
		endValues[i] = fmt.Sprintf(`[%d]`, i+1)
	}
	input := `[{"content":[],"starts":[],"ends":[]},{"content":[` + strings.Join(contentValues, ",") + `],"starts":[` + strings.Join(startValues, ",") + `],"ends":[` + strings.Join(endValues, ",") + `]}]`
	details, _, err := array.FromJSON(pool, native.GrepDetailsType(), strings.NewReader(input))
	require.NoError(t, err)
	output := emptyQueryScanComputed(&ChainRepr{Name: "scan", OutputColumns: []string{"matches"}})
	for _, row := range []int{1, 0, 1} {
		require.NoError(t, appendGrepComputedRow(output[0], details.(*array.Struct), row))
	}
	details.Release()
	fields := output[0].GetStructArrays().Fields
	starts := fields[1].GetScalars().GetArrayData().Data
	ends := fields[2].GetScalars().GetArrayData().Data
	require.Empty(t, starts[1].GetArrayData().Data)
	for _, doc := range []int{0, 2} {
		require.Len(t, starts[doc].GetArrayData().Data, 128)
		for i := 0; i < 128; i++ {
			require.Equal(t, []int64{int64(i)}, starts[doc].GetArrayData().Data[i].GetLongData().Data)
			require.Equal(t, []int64{int64(i + 1)}, ends[doc].GetArrayData().Data[i].GetLongData().Data)
		}
	}
	require.NotSame(t, starts[0].GetArrayData().Data[0], starts[2].GetArrayData().Data[0])
	retained := starts[2]
	output = nil
	fields = nil
	starts = nil
	ends = nil
	runtime.GC()
	data, err := proto.Marshal(retained)
	require.NoError(t, err)
	decoded := &schemapb.ScalarField{}
	require.NoError(t, proto.Unmarshal(data, decoded))
	require.True(t, proto.Equal(retained, decoded))
	for i, entry := range decoded.GetArrayData().Data {
		require.Equal(t, []int64{int64(i)}, entry.GetLongData().Data)
	}
}

// Final projection is independent of operator position and physical column order.
func TestQueryScanFinalOutputProjection(t *testing.T) {
	for _, tc := range []struct {
		name      string
		names     []string
		wantError bool
	}{
		{"reordered", []string{"second", "first"}, false},
		{"subset", []string{"second"}, false},
		{"filter_only", nil, false},
		{"missing_after_retained_column", []string{"first", "missing"}, true},
		{"scalar_projection", []string{"ordinary"}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer pool.AssertSize(t, 0)
			builder := NewDataFrameBuilder().SetChunkSizes([]int64{2})
			defer builder.Release()
			for _, name := range []string{"first", "second"} {
				input := fmt.Sprintf(`[{"content":["%s0"],"starts":[[]],"ends":[[]]},{"content":["%s1"],"starts":[[]],"ends":[[]]}]`, name, name)
				values, _, err := array.FromJSON(pool, native.GrepDetailsType(), strings.NewReader(input))
				require.NoError(t, err)
				require.NoError(t, builder.AddColumnFromChunks(name, []arrow.Array{values}))
			}
			ordinary, _, err := array.FromJSON(pool, arrow.PrimitiveTypes.Int64, strings.NewReader(`[0,1]`))
			require.NoError(t, err)
			require.NoError(t, builder.AddColumnFromChunks("ordinary", []arrow.Array{ordinary}))
			ordinary.Retain()
			require.NoError(t, builder.AddColumnFromChunks("$id", []arrow.Array{ordinary}))
			frame := builder.Build()
			// Projection happens before export; the converter exports all final columns.
			projected := NewDataFrameBuilder().SetChunkSizes(frame.ChunkSizes())
			defer projected.Release()
			err = projected.AddColumnFrom(frame, "$id")
			require.NoError(t, err)
			ts := array.NewInt64Builder(pool)
			ts.AppendValues([]int64{1, 1}, nil)
			require.NoError(t, projected.AddColumnFromChunks("$timestamp", []arrow.Array{ts.NewArray()}))
			ts.Release()
			for _, name := range tc.names {
				if err = projected.AddColumnFrom(frame, name); err != nil {
					break
				}
			}
			var result *segcorepb.RetrieveResults
			if err == nil {
				output := projected.Build()
				defer output.Release()
				result, err = ToRetrieveResults(output)
			}
			frame.Release()
			if tc.wantError {
				require.ErrorIs(t, err, merr.ErrServiceInternal)
				require.Equal(t, merr.SystemError, merr.GetErrorType(err))
				require.Nil(t, result)
				return
			}
			require.NoError(t, err)
			require.Len(t, result.ComputedResults, len(tc.names))
			if tc.name == "scalar_projection" {
				require.Equal(t, []int64{0, 1}, result.ComputedResults[0].GetScalars().GetLongData().Data)
				return
			}
			for i, name := range tc.names {
				column := result.ComputedResults[i]
				require.Equal(t, name, column.FieldName)
				contents := column.GetStructArrays().Fields[0].GetScalars().GetArrayData().Data
				require.Equal(t, []string{name + "0"}, contents[0].GetStringData().Data)
				require.Equal(t, []string{name + "1"}, contents[1].GetStringData().Data)
			}
		})
	}
}

// Integration helpers keep wire assertions in tests, not in chain production code.
func emptyQueryScanComputed(repr *ChainRepr) []*schemapb.FieldData {
	if len(repr.OutputColumns) == 0 {
		return nil
	}
	var output []*schemapb.FieldData
	for _, name := range repr.OutputColumns {
		column := arrow.NewChunked(native.GrepDetailsType(), nil)
		field, err := arrowconv.ArrowChunkedStructColumnToFieldData(name, column, nil)
		column.Release()
		if err != nil {
			panic(err)
		}
		output = append(output, field)
	}
	return output
}
func appendGrepComputedRow(output *schemapb.FieldData, rows *array.Struct, row int) error {
	slice := array.NewSlice(rows, int64(row), int64(row+1))
	column := arrow.NewChunked(rows.DataType(), []arrow.Array{slice})
	slice.Release()
	defer column.Release()
	result, err := arrowconv.ArrowChunkedStructColumnToFieldData(output.FieldName, column, nil)
	if err != nil {
		return err
	}
	for i, field := range result.GetStructArrays().Fields {
		dst := output.GetStructArrays().Fields[i].GetScalars().GetArrayData()
		dst.Data = append(dst.Data, field.GetScalars().GetArrayData().Data...)
	}
	return nil
}

func TestComputedColumnsFromDataFrame(t *testing.T) {
	pool := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer pool.AssertSize(t, 0)
	builder := NewDataFrameBuilder().SetChunkSizes([]int64{2})
	defer builder.Release()
	values, _, err := array.FromJSON(pool, native.GrepDetailsType(), strings.NewReader(`[{"content":["a"],"starts":[[0]],"ends":[[1]]},{"content":["b"],"starts":[[0]],"ends":[[1]]}]`))
	require.NoError(t, err)
	require.NoError(t, builder.AddColumnFromChunks("matches", []arrow.Array{values}))
	intermediate, _, err := array.FromJSON(pool, arrow.PrimitiveTypes.Int64, strings.NewReader(`[10,20]`))
	require.NoError(t, err)
	require.NoError(t, builder.AddColumnFromChunks("internal", []arrow.Array{intermediate}))
	frame := builder.Build()
	defer frame.Release()
	result, err := exportFieldData(frame, "matches")
	require.NoError(t, err)
	content := result.GetStructArrays().Fields[0].GetScalars().GetArrayData().Data
	require.Equal(t, []string{"a"}, content[0].GetStringData().Data)
	require.Equal(t, []string{"b"}, content[1].GetStringData().Data)
	_, err = exportFieldData(frame, "missing")
	require.ErrorIs(t, err, merr.ErrServiceInternal)
}
