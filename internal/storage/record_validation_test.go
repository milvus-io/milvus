// Licensed to the LF AI & Data foundation under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package storage

import (
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
)

// A Record can violate its logical row contract even when each Arrow array is
// individually well-formed (e.g. independently decoded column groups).
type validationRecord struct {
	rows    int
	columns map[FieldID]arrow.Array
}

func (r *validationRecord) Len() int                      { return r.rows }
func (r *validationRecord) Column(id FieldID) arrow.Array { return r.columns[id] }
func (r *validationRecord) Retain()                       {}
func (r *validationRecord) Release()                      {}

func requireIntegrityFailure(t *testing.T, err error) {
	t.Helper()
	require.ErrorIs(t, err, merr.ErrDataIntegrity)
	status := merr.Status(err)
	require.EqualValues(t, 1009, status.GetCode())
	require.False(t, status.GetRetriable())
	require.NotEqual(t, "true", status.GetExtraInfo()["is_input_error"])
}

func TestToWriterRecordRejectsMalformedRecord(t *testing.T) {
	field := &schemapb.FieldSchema{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector,
		TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	physical, err := ConvertToArrowSchema(schema, false)
	require.NoError(t, err)
	valid := array.NewFixedSizeBinaryBuilder(memory.DefaultAllocator, &arrow.FixedSizeBinaryType{ByteWidth: 8})
	valid.Append(make([]byte, 8))
	column := valid.NewArray()
	valid.Release()
	defer column.Release()
	nulls := array.MakeArrayOfNull(memory.DefaultAllocator, physical.Field(0).Type, 1)
	defer nulls.Release()
	wrong := array.MakeArrayOfNull(memory.DefaultAllocator, arrow.PrimitiveTypes.Int64, 1)
	defer wrong.Release()
	for name, input := range map[string]Record{
		"nil record":       nil,
		"missing column":   &validationRecord{rows: 1},
		"short column":     &validationRecord{rows: 2, columns: map[FieldID]arrow.Array{101: column}},
		"wrong type":       &validationRecord{rows: 1, columns: map[FieldID]arrow.Array{101: wrong}},
		"nonnullable null": NewSimpleArrowRecord(array.NewRecord(physical, []arrow.Array{nulls}, 1), map[FieldID]int{101: 0}),
	} {
		t.Run(name, func(t *testing.T) {
			if input != nil {
				defer input.Release()
			}
			require.NotPanics(t, func() {
				record, release, err := toWriterRecord(input, schema, physical)
				if release != nil {
					defer release()
				}
				requireIntegrityFailure(t, err)
				require.Nil(t, record)
			})
		})
	}
}

func TestToWriterRecordSlicesLogicalRowsBeforeFFI(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64}}}
	physical, err := ConvertToArrowSchema(schema, false)
	require.NoError(t, err)
	b := array.NewInt64Builder(memory.DefaultAllocator)
	b.AppendValues([]int64{10, 20, 0}, []bool{true, true, false})
	column := b.NewArray()
	b.Release()
	defer column.Release()
	input := NewSimpleArrowRecord(array.NewRecord(physical, []arrow.Array{column}, 2), map[FieldID]int{100: 0})
	defer input.Release()
	out, release, err := toWriterRecord(input, schema, physical)
	require.NoError(t, err)
	defer release()
	require.EqualValues(t, 2, out.NumRows())
	require.Equal(t, 2, out.Column(0).Len(), "V2 FFI derives the row count from the first array")
	require.Zero(t, out.Column(0).NullN(), "nulls beyond the logical record are not input rows")
	require.Same(t, column.Data().Buffers()[1], out.Column(0).Data().Buffers()[1], "slicing must not copy the payload")
}

func TestSortRejectsMissingFieldBeforePredicate(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "missing", DataType: schemapb.DataType_Int64},
	}}
	input := mergeSortTestRec(t, map[FieldID][]int64{100: {1}}, nil)
	defer input.Release()
	for _, merge := range []bool{false, true} {
		calls := 0
		predicate := func(r Record, _, _ int) bool { calls++; r.Column(101); return true }
		require.NotPanics(t, func() {
			var err error
			readers := []RecordReader{&oneShotRecordReader{rec: input}}
			if merge {
				_, err = MergeSort(1024, schema, readers, nil, predicate, []int64{100})
			} else {
				_, _, err = Sort(1024, schema, readers, nil, predicate, []int64{100})
			}
			requireIntegrityFailure(t, err)
		})
		require.Zero(t, calls)
	}
}

func TestToWriterRecordValidatesVisibleVectorElements(t *testing.T) {
	for _, elementNullable := range []bool{false, true} {
		field := &schemapb.FieldSchema{FieldID: 101, Name: "vectors", DataType: schemapb.DataType_ArrayOfVector,
			ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: elementNullable,
			TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
		physical, err := ConvertToArrowSchema(schema, false)
		require.NoError(t, err)
		for _, scenario := range []string{"visible null", "null parent", "sliced parent", "wrong width"} {
			if scenario == "wrong width" && !elementNullable {
				continue
			}
			t.Run(scenario+"/"+physical.Field(0).Type.String(), func(t *testing.T) {
				b := array.NewBuilder(memory.DefaultAllocator, physical.Field(0).Type).(*array.ListBuilder)
				b.Append(scenario != "null parent")
				child := b.ValueBuilder()
				if scenario == "wrong width" {
					child.(*array.BinaryBuilder).Append([]byte{1})
				} else {
					child.AppendNull()
				}
				b.Append(true)
				switch values := child.(type) {
				case *array.BinaryBuilder:
					values.Append(make([]byte, 8))
				case *array.FixedSizeBinaryBuilder:
					values.Append(make([]byte, 8))
				}
				column := b.NewArray()
				b.Release()
				defer column.Release()
				if scenario == "sliced parent" {
					column = array.NewSlice(column, 1, 2)
					defer column.Release()
				}
				input := NewSimpleArrowRecord(array.NewRecord(physical, []arrow.Array{column}, int64(column.Len())), map[FieldID]int{101: 0})
				defer input.Release()
				out, release, err := toWriterRecord(input, schema, physical)
				if release != nil {
					defer release()
				}
				if scenario == "wrong width" || (scenario == "visible null" && !elementNullable) {
					requireIntegrityFailure(t, err)
					require.Nil(t, out)
				} else {
					require.NoError(t, err)
				}
			})
		}
	}
}

func TestToWriterRecordRejectsNullableVectorWrongWidth(t *testing.T) {
	field := &schemapb.FieldSchema{FieldID: 101, Name: "vector", DataType: schemapb.DataType_FloatVector, Nullable: true,
		TypeParams: []*commonpb.KeyValuePair{{Key: "dim", Value: "2"}}}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	physical, err := ConvertToArrowSchema(schema, false)
	require.NoError(t, err)
	b := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	b.Append([]byte{1, 2})
	b.AppendNull()
	column := b.NewArray()
	b.Release()
	defer column.Release()
	input := NewSimpleArrowRecord(array.NewRecord(physical, []arrow.Array{column}, 2), map[FieldID]int{101: 0})
	defer input.Release()
	out, release, err := toWriterRecord(input, schema, physical)
	if release != nil {
		defer release()
	}
	requireIntegrityFailure(t, err)
	require.Nil(t, out)
}

func TestBinlogWritersRejectMalformedRecordBeforeInitialization(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, Name: "ts", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	physical, err := ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	for _, kind := range []string{"v2", "v3", "text"} {
		t.Run(kind, func(t *testing.T) {
			// No allocator, stats collectors, or native handles: malformed input
			// must fail before any of those stateful resources is accessed.
			base := packedBinlogRecordWriterBase{schema: schema, arrowSchema: physical}
			var writer BinlogRecordWriter
			switch kind {
			case "v2":
				writer = &PackedBinlogRecordWriter{packedBinlogRecordWriterBase: base}
			case "v3":
				writer = &PackedManifestRecordWriter{packedBinlogRecordWriterBase: base}
			case "text":
				writer = &PackedTextManifestRecordWriter{packedBinlogRecordWriterBase: base}
			}
			require.NotPanics(t, func() {
				requireIntegrityFailure(t, writer.Write(&validationRecord{rows: 1}))
			})
			require.Zero(t, writer.GetRowNum())
			require.Zero(t, writer.GetWrittenUncompressed())
		})
	}
}

func TestManifestWriterRejectsBadBatchWithoutCommittingEarlierBatch(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 1, Name: "ts", DataType: schemapb.DataType_Int64},
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}
	physical, err := ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	for _, textWriter := range []bool{false, true} {
		t.Run(map[bool]string{false: "packed", true: "text"}[textWriter], func(t *testing.T) {
			// Native IO is the only substituted boundary. Validation, statistics,
			// outer Write, lower Write, Abort and outer Close remain real.
			writes := 0
			write := func(_ arrow.Record) error { writes++; return nil }
			p1 := mockey.Mock((*packed.FFIPackedWriter).WriteRecordBatch).To(func(_ *packed.FFIPackedWriter, r arrow.Record) error { return write(r) }).Build()
			defer p1.UnPatch()
			p2 := mockey.Mock((*packed.FFISegmentWriter).Write).To(func(_ *packed.FFISegmentWriter, r arrow.Record) error { return write(r) }).Build()
			defer p2.UnPatch()
			base := packedBinlogRecordWriterBase{schema: schema, arrowSchema: physical,
				pkCollector: &PkStatsCollector{}, bm25Collector: &Bm25StatsCollector{}}
			var writer BinlogRecordWriter
			var state *packedBinlogRecordWriterBase
			if textWriter {
				w := &PackedTextManifestRecordWriter{packedBinlogRecordWriterBase: base,
					writer: &packedTextBatchWriter{schema: schema, arrowSchema: physical, writer: &packed.FFISegmentWriter{}}}
				writer, state = w, &w.packedBinlogRecordWriterBase
			} else {
				w := &PackedManifestRecordWriter{packedBinlogRecordWriterBase: base,
					writer: &packedRecordBatchWriter{schema: schema, arrowSchema: physical, writer: &packed.FFIPackedWriter{}}}
				writer, state = w, &w.packedBinlogRecordWriterBase
			}
			good := mergeSortTestRec(t, map[FieldID][]int64{1: {10}, 100: {1}}, nil)
			defer good.Release()
			require.NoError(t, writer.Write(good))
			bytesBefore := writer.GetWrittenUncompressed()
			bad := mergeSortTestRec(t, map[FieldID][]int64{1: {99}}, nil)
			defer bad.Release()
			requireIntegrityFailure(t, writer.Write(bad))
			require.EqualValues(t, 10, state.tsTo, "bad batch must not mutate statistics")
			require.Equal(t, bytesBefore, writer.GetWrittenUncompressed())
			require.Equal(t, 1, writes, "bad batch must not reach native IO")
			require.True(t, state.aborted)
			require.NoError(t, writer.Close(), "cleanup after failure must not finalize native output")
			logs, stats, _, manifest, _ := writer.GetLogs()
			require.Empty(t, logs)
			require.Nil(t, stats)
			require.Empty(t, manifest)
		})
	}
}
