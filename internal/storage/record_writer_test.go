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
	"math"
	"strings"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/internal/storagev2"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestPackedRecordBatchWriterFieldLogicalSizes(t *testing.T) {
	// The target writes only name and TEXT. The reader also carries an anchor
	// and has a different column order. Both target fields share a column group.
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, DataType: schemapb.DataType_VarChar},
		{FieldID: 101, DataType: schemapb.DataType_Text, Nullable: true},
	}}
	target, err := ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	target = overrideTextFieldsToBinary(schema, target)
	reader := arrow.NewSchema([]arrow.Field{{Name: "anchor", Type: arrow.PrimitiveTypes.Int64}, target.Field(1), target.Field(0)}, nil)
	input := func(ref []byte) Record {
		b := array.NewRecordBuilder(memory.DefaultAllocator, reader)
		defer b.Release()
		b.Field(0).AppendEmptyValue()
		b.Field(1).(*array.BinaryBuilder).Append(ref)
		b.Field(2).(*array.StringBuilder).Append("猫")
		return NewSimpleArrowRecord(b.NewRecord(), map[FieldID]int{0: 0, 101: 1, 100: 2})
	}
	inline, external := input([]byte{0, 'x'}), input(append([]byte{1}, make([]byte, 23)...))
	defer inline.Release()
	defer external.Release()
	w := &packedRecordBatchWriter{
		v3BatchStats: v3BatchStats{columnGroupUncompressed: make(map[int64]uint64)},
		writer:       &packed.FFIPackedWriter{}, schema: schema, arrowSchema: target,
		columnGroups: []storagecommon.ColumnGroup{{GroupID: 7, Columns: []int{0, 1}, Fields: []int64{100, 101}}},
	}
	writes := 0
	write := mockey.Mock((*packed.FFIPackedWriter).WriteRecordBatch).To(func(_ *packed.FFIPackedWriter, rec arrow.Record) error {
		require.True(t, target.Equal(rec.Schema()))
		writes++
		if writes == 2 {
			return merr.ErrServiceUnavailable
		}
		return nil
	}).Build()
	defer write.UnPatch()
	require.Nil(t, w.GetFieldLogicalSizes())
	require.NoError(t, w.Write(inline))
	first := w.GetFieldLogicalSizes()
	require.EqualValues(t, 1, first.Version)
	require.EqualValues(t, 1, first.RowCount)
	require.Len(t, first.Fields, 2)
	requireLogicalSize(t, first.Fields[100], 3, 0)
	requireLogicalSize(t, first.Fields[101], 1, 1)
	first.Fields[101].PayloadBytes = 999
	delete(first.Fields, 100)

	require.ErrorIs(t, w.Write(external), merr.ErrServiceUnavailable)
	afterFailure := w.GetFieldLogicalSizes()
	require.EqualValues(t, 1, afterFailure.RowCount)
	requireLogicalSize(t, afterFailure.Fields[100], 3, 0)
	requireLogicalSize(t, afterFailure.Fields[101], 1, 1)
	require.NoError(t, w.Write(external))
	require.NoError(t, w.Write(inline))
	last := w.GetFieldLogicalSizes()
	require.EqualValues(t, 3, last.RowCount)
	requireLogicalSize(t, last.Fields[100], 9, 0)
	require.NotContains(t, last.Fields, int64(101), "later inline rows cannot repair an unknown prefix")
	require.Equal(t, w.GetWrittenUncompressed(), w.GetColumnGroupWrittenUncompressed(7))

	manifest := &PackedManifestRecordWriter{writer: w}
	require.Nil(t, manifest.GetFieldLogicalSizes(), "written files without a manifest cannot publish statistics")
	manifest.manifest = "committed"
	manifest.GetFieldLogicalSizes().Fields[100].PayloadBytes = 1
	requireLogicalSize(t, manifest.GetFieldLogicalSizes().Fields[100], 9, 0)

	w.rowNum = math.MaxInt64
	require.ErrorIs(t, w.Write(inline), merr.ErrServiceInternal)
	require.Equal(t, 4, writes, "row overflow must be rejected before the native write")
}

func TestPackedRecordBatchWriterFieldLogicalSizesAcrossGroups(t *testing.T) {
	const pk, text int64 = 100, 101
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: pk, DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
		{FieldID: text, DataType: schemapb.DataType_VarChar, Nullable: true},
	}}
	target, err := ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	b := array.NewRecordBuilder(memory.DefaultAllocator, target)
	defer b.Release()
	b.Field(0).(*array.StringBuilder).AppendValues([]string{"A", "猫", "B"}, nil)
	b.Field(1).(*array.StringBuilder).AppendValues([]string{"", "", strings.Repeat("v", 4096)}, []bool{true, false, true})
	data := b.NewRecord()
	defer data.Release()
	mapping := map[FieldID]int{pk: 0, text: 1}
	for _, tc := range []struct {
		name        string
		compression string
		groups      []storagecommon.ColumnGroup
		ends        []int64
	}{
		{
			name:        "one gzip group and batch",
			compression: "gzip",
			groups:      []storagecommon.ColumnGroup{{GroupID: 7, Columns: []int{0, 1}, Fields: []int64{pk, text}}},
			ends:        []int64{3},
		},
		{
			name:        "two compressed groups and batches",
			compression: "zstd",
			groups: []storagecommon.ColumnGroup{
				{GroupID: 7, Columns: []int{0}, Fields: []int64{pk}},
				{GroupID: 8, Columns: []int{1}, Fields: []int64{text}},
			},
			ends: []int64{1, 3},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
			native, err := packed.NewFFIPackedWriter(cfg.RootPath, target, tc.groups, cfg, nil, map[string]string{
				packed.PropertyWriterFormat: "parquet",
				"writer.compression":        tc.compression,
			})
			require.NoError(t, err)
			w := &packedRecordBatchWriter{
				v3BatchStats: v3BatchStats{columnGroupUncompressed: make(map[int64]uint64)},
				writer:       native,
				schema:       schema,
				arrowSchema:  target,
				columnGroups: tc.groups,
			}
			defer w.Abort()
			var start int64
			for _, end := range tc.ends {
				rec := NewSimpleArrowRecord(data.NewSlice(start, end), mapping)
				err := w.Write(rec)
				rec.Release()
				require.NoError(t, err)
				start = end
			}
			out, err := w.Close()
			require.NoError(t, err)
			defer out.Destroy()
			sizes := w.GetFieldLogicalSizes()
			require.EqualValues(t, 3, sizes.GetRowCount())
			require.Len(t, sizes.Fields, 2)
			requireLogicalSize(t, sizes.Fields[pk], 5, 0)
			requireLogicalSize(t, sizes.Fields[text], 4096, 3)

			groups, err := out.(*packed.ColumnGroups).ColumnGroupEntries()
			require.NoError(t, err)
			require.Len(t, groups, len(tc.groups))
			for _, group := range groups {
				require.Len(t, group.Files, 1)
				r, err := file.OpenParquetFile(group.Files[0].Path, false)
				require.NoError(t, err)
				t.Cleanup(func() { _ = r.Close() })
				column, err := r.MetaData().RowGroup(0).ColumnChunk(0)
				require.NoError(t, err)
				require.Equal(t, tc.compression, strings.ToLower(column.Compression().String()))
			}
		})
	}
}

func TestPackedRecordBatchWriterCopiedTextFieldLogicalSizes(t *testing.T) {
	const inline, external int64 = 100, 101
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: inline, DataType: schemapb.DataType_Text, Nullable: true},
		{FieldID: external, DataType: schemapb.DataType_Text, Nullable: true},
	}}
	target, err := ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, overrideTextFieldsToBinary(schema, target))
	defer builder.Release()
	for i, first := range [][]byte{append([]byte{0}, strings.Repeat("v", 23)...), append([]byte{1}, make([]byte, 23)...)} {
		col := builder.Field(i).(*array.BinaryBuilder)
		col.Append(first)
		col.Append([]byte{0})
		col.AppendNull()
	}
	rec := NewSimpleArrowRecord(builder.NewRecord(), map[FieldID]int{inline: 0, external: 1})
	defer rec.Release()

	// The reference points at a LOB that does not exist anywhere in this root.
	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: t.TempDir()}
	groups := []storagecommon.ColumnGroup{{GroupID: 7, Columns: []int{0, 1}, Fields: []int64{inline, external}}}
	w, err := NewPartialPackedRecordBatchWriterWithTextRefsAsBinary(cfg.RootPath, schema, 0, 0, groups, cfg, nil, "parquet", nil)
	require.NoError(t, err)
	defer w.Abort()
	before, err := storagev2.GetFilesystemMetricsWithConfig(cfg)
	require.NoError(t, err)
	require.Contains(t, before, "origin")
	require.NoError(t, w.Write(rec))
	out, err := w.Close()
	require.NoError(t, err)
	defer out.Destroy()
	sizes := w.GetFieldLogicalSizes()
	require.EqualValues(t, 3, sizes.GetRowCount())
	requireLogicalSize(t, sizes.Fields[inline], 23, 3)
	require.NotContains(t, sizes.Fields, external)
	after, err := storagev2.GetFilesystemMetricsWithConfig(cfg)
	require.NoError(t, err)
	require.Contains(t, after, "origin")
	require.Equal(t, before["origin"].ReadCount, after["origin"].ReadCount)
	require.Equal(t, before["origin"].ReadBytes, after["origin"].ReadBytes)
}

func TestPartialPackedBatchWriterFieldLogicalSizes(t *testing.T) {
	ages := &schemapb.FieldSchema{FieldID: 201, Name: "age", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int16, ElementNullable: true, Nullable: true}
	names := &schemapb.FieldSchema{FieldID: 202, Name: "name", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_VarChar, ElementNullable: true, Nullable: true}
	scalar := &schemapb.FieldSchema{FieldID: 100, DataType: schemapb.DataType_Int64, Nullable: true}
	target := &schemapb.CollectionSchema{
		Fields:            []*schemapb.FieldSchema{scalar, {FieldID: 101, DataType: schemapb.DataType_VarChar}},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{FieldID: 200, Nullable: true, Fields: []*schemapb.FieldSchema{ages, names}}},
	}
	// The partial writer receives only physical FieldSchemas (not their
	// parents), interleaved with a top-level field in native group order.
	projected := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{names, scalar, ages}}
	arrowSchema, err := ConvertToArrowSchema(projected, true)
	require.NoError(t, err)
	w := &packedRecordBatchWriter{writer: &packed.FFIPackedWriter{}, schema: projected, arrowSchema: arrowSchema}
	require.NoError(t, w.SetFieldLogicalSizeSchema(target))
	b := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer b.Release()
	nameList := b.Field(0).(*array.ListBuilder)
	nameList.Append(true)
	nameList.ValueBuilder().(*array.StringBuilder).AppendValues([]string{"猫", ""}, nil)
	nameList.AppendNull()
	b.Field(1).AppendEmptyValues(2)
	ageList := b.Field(2).(*array.ListBuilder)
	ageList.Append(true)
	ageList.ValueBuilder().(*array.Int16Builder).AppendValues([]int16{7, 0}, []bool{true, false})
	ageList.AppendNull()
	rec := NewSimpleArrowRecord(b.NewRecord(), map[FieldID]int{202: 0, 100: 1, 201: 2})
	defer rec.Release()
	write := mockey.Mock((*packed.FFIPackedWriter).WriteRecordBatch).Return(nil).Build()
	defer write.UnPatch()
	require.NoError(t, w.Write(rec))
	require.ErrorIs(t, w.SetFieldLogicalSizeSchema(target), merr.ErrServiceInternal)
	snapshot := w.GetFieldLogicalSizes()
	require.Len(t, snapshot.Fields, 4)
	requireLogicalSize(t, snapshot.Fields[200], 0, 2) // one shared row mask
	requireLogicalSize(t, snapshot.Fields[201], 2, 2) // only element validity
	requireLogicalSize(t, snapshot.Fields[202], 3, 2)
	requireLogicalSize(t, snapshot.Fields[100], 16, 2)
	require.NotContains(t, snapshot.Fields, int64(101))
}

func TestPackedTextBatchWriterNativeFieldLogicalSizes(t *testing.T) {
	const rewritten, missingTotal, copiedExternal, copiedInline, pk int64 = 100, 101, 102, 103, 104
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: rewritten, DataType: schemapb.DataType_Text, Nullable: true},
		{FieldID: missingTotal, DataType: schemapb.DataType_Text, Nullable: true},
		{FieldID: copiedExternal, DataType: schemapb.DataType_Text},
		{FieldID: copiedInline, DataType: schemapb.DataType_Text, Nullable: true},
		{FieldID: pk, DataType: schemapb.DataType_Int64},
	}}
	arrowSchema, err := ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, overrideTextFieldsToBinary(schema, arrowSchema))
	defer builder.Release()
	for _, i := range []int{0, 1, 3} {
		col := builder.Field(i).(*array.BinaryBuilder)
		col.Append(append([]byte{0}, "猫"...))
		col.AppendNull()
	}
	external := builder.Field(2).(*array.BinaryBuilder)
	external.Append(append([]byte{1}, make([]byte, 23)...))
	external.Append([]byte{0})
	builder.Field(4).AppendEmptyValues(2)
	rec := builder.NewRecord()
	defer rec.Release()

	w := &packedTextBatchWriter{v3BatchStats: v3BatchStats{
		logicalLayout: newLogicalSizeLayout(schema, schema, typeutil.NewUniqueSet(rewritten, missingTotal)),
	}}
	require.NoError(t, w.writeBatch(rec, schema, nil, func(uint64) error { return nil }))
	before := w.GetFieldLogicalSizes()
	require.NotContains(t, before.Fields, rewritten, "Go must skip even inline values when native owns the payload")
	require.NotContains(t, before.Fields, missingTotal)
	requireLogicalSize(t, before.Fields[copiedInline], 3, 2)

	w.finishTextSizes(2, map[int64]uint64{rewritten: 65537, copiedInline: 99, copiedExternal: 99, pk: 99})
	s := w.GetFieldLogicalSizes()
	requireLogicalSize(t, s.Fields[rewritten], 65537, 2)
	require.NotContains(t, s.Fields, missingTotal)
	require.NotContains(t, s.Fields, copiedExternal)
	requireLogicalSize(t, s.Fields[copiedInline], 3, 2)
	requireLogicalSize(t, s.Fields[pk], 16, 0)
	w.finishTextSizes(3, nil)
	require.Nil(t, w.GetFieldLogicalSizes(), "native row count doesn't match the accepted prefix")
}

// TestToWriterRecordProjectsOverWideRecord pins the schema-bump additive
// regression: the reader hands the writer a record carrying the row anchor in
// addition to the appended field, while the writer's schema is only the
// appended field. toWriterRecord must narrow the record to the writer's column
// set instead of passing the over-wide record straight to the FFI writer
// (which crashed loon_writer_write on the column-count mismatch).
func TestToWriterRecordProjectsOverWideRecord(t *testing.T) {
	// Writer schema: only the appended field 103.
	writerSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 103, Name: "added", DataType: schemapb.DataType_VarChar},
	}}
	writerArrow, err := ConvertToArrowSchema(writerSchema, false)
	require.NoError(t, err)

	// Over-wide reader record: anchor field 0 (int64) + appended field 103.
	anchorB := array.NewInt64Builder(memory.DefaultAllocator)
	anchorB.AppendValues([]int64{10, 11, 12}, nil)
	anchor := anchorB.NewArray()
	anchorB.Release()
	defer anchor.Release()

	addedB := array.NewStringBuilder(memory.DefaultAllocator)
	addedB.AppendValues([]string{"a", "b", "c"}, nil)
	added := addedB.NewArray()
	addedB.Release()
	defer added.Release()

	wideArrow := arrow.NewSchema([]arrow.Field{
		{Name: "0", Type: arrow.PrimitiveTypes.Int64},
		{Name: "103", Type: arrow.BinaryTypes.String},
	}, nil)
	wideRec := array.NewRecord(wideArrow, []arrow.Array{anchor, added}, 3)
	defer wideRec.Release()
	overWide := NewSimpleArrowRecord(wideRec, map[FieldID]int{0: 0, 103: 1})

	rec, release, err := toWriterRecord(overWide, writerSchema, writerArrow)
	require.NoError(t, err)
	defer release()

	require.EqualValues(t, 1, rec.NumCols(), "record must be narrowed to the writer's single column")
	require.Equal(t, "added", rec.Schema().Field(0).Name)
	col, ok := rec.Column(0).(*array.String)
	require.True(t, ok)
	require.Equal(t, 3, col.Len())
	require.Equal(t, []string{"a", "b", "c"}, []string{col.Value(0), col.Value(1), col.Value(2)})
}

// TestToWriterRecordFastPathOnMatchingSchema verifies the optimization is kept:
// a simpleArrowRecord whose schema already equals the writer's is returned
// as-is with a no-op release.
func TestToWriterRecordFastPathOnMatchingSchema(t *testing.T) {
	writerSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 103, Name: "added", DataType: schemapb.DataType_VarChar},
	}}
	writerArrow, err := ConvertToArrowSchema(writerSchema, false)
	require.NoError(t, err)

	b := array.NewStringBuilder(memory.DefaultAllocator)
	b.AppendValues([]string{"x", "y"}, nil)
	col := b.NewArray()
	b.Release()
	defer col.Release()
	matchRec := array.NewRecord(writerArrow, []arrow.Array{col}, 2)
	defer matchRec.Release()
	sar := NewSimpleArrowRecord(matchRec, map[FieldID]int{103: 0})

	rec, release, err := toWriterRecord(sar, writerSchema, writerArrow)
	require.NoError(t, err)
	defer release()

	require.Same(t, matchRec, rec, "matching schema must take the fast path and return the backing record")
}

func TestToWriterRecordV3BuilderFastPath(t *testing.T) {
	field := &schemapb.FieldSchema{FieldID: 103, Name: "added", ExternalField: "source_added", DataType: schemapb.DataType_VarChar}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	inputSchema, err := ConvertToArrowSchema(schema, false)
	require.NoError(t, err)
	inputBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	inputBuilder.Append("value")
	column := inputBuilder.NewArray()
	inputBuilder.Release()
	input := NewSimpleArrowRecord(array.NewRecord(inputSchema, []arrow.Array{column}, 1), map[FieldID]int{103: 0})
	column.Release()
	builder := NewRecordBuilderWithFieldID(schema)
	defer builder.Release()
	require.NoError(t, builder.Append(input, 0, 1))
	input.Release()
	batch := builder.Build()
	defer batch.Release()
	writerSchema, err := ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	converted, release, err := toWriterRecord(batch, schema, writerSchema)
	require.NoError(t, err)
	defer release()
	require.Same(t, batch.(*simpleArrowRecord).r, converted)
}

func TestToWriterRecordV3BuilderNormalizesNullText(t *testing.T) {
	field := &schemapb.FieldSchema{FieldID: 103, Name: "text", DataType: schemapb.DataType_Text, Nullable: true}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	column := array.MakeArrayOfNull(memory.DefaultAllocator, arrow.BinaryTypes.Binary, 1)
	input := NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema([]arrow.Field{
		{Name: field.Name, Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil), []arrow.Array{column}, 1), map[FieldID]int{103: 0})
	column.Release()
	builder := NewRecordBuilderWithFieldID(schema)
	defer builder.Release()
	require.NoError(t, builder.Append(input, 0, 1))
	input.Release()
	batch := builder.Build()
	defer batch.Release()
	writerSchema, err := ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	converted, release, err := toWriterRecord(batch, schema, writerSchema)
	require.NoError(t, err)
	defer release()
	require.NotSame(t, batch.(*simpleArrowRecord).r, converted)
	require.True(t, writerSchema.Equal(converted.Schema()))
	require.Equal(t, 1, converted.Column(0).NullN())
}

func TestToWriterRecordNormalizesNullText(t *testing.T) {
	field := &schemapb.FieldSchema{FieldID: 100, Name: "text", DataType: schemapb.DataType_Text, Nullable: true}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	for _, target := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.Binary} {
		t.Run(target.Name(), func(t *testing.T) {
			source := arrow.BinaryTypes.Binary
			if target.ID() == arrow.BINARY {
				source = arrow.BinaryTypes.String
			}
			column := array.MakeArrayOfNull(memory.DefaultAllocator, source, 3)
			defer column.Release()
			record := NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema([]arrow.Field{
				{Name: field.Name, Type: source, Nullable: true},
			}, nil), []arrow.Array{column}, 3), map[FieldID]int{100: 0})
			defer record.Release()
			writerSchema := arrow.NewSchema([]arrow.Field{{Name: field.Name, Type: target, Nullable: true}}, nil)
			require.NotPanics(t, func() {
				converted, release, err := toWriterRecord(record, schema, writerSchema)
				require.NoError(t, err)
				defer release()
				require.True(t, writerSchema.Equal(converted.Schema()))
				require.Equal(t, target, converted.Column(0).DataType())
				require.Equal(t, 3, converted.Column(0).NullN())
			})
		})
	}
}

func TestPackedTextWriterUsesWriterSchemaAcrossBatches(t *testing.T) {
	field := &schemapb.FieldSchema{FieldID: 100, Name: "text", DataType: schemapb.DataType_Text, Nullable: true}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	for _, target := range []arrow.DataType{arrow.BinaryTypes.String, arrow.BinaryTypes.Binary} {
		t.Run(target.Name(), func(t *testing.T) {
			writerSchema := arrow.NewSchema([]arrow.Field{{Name: field.Name, Type: target, Nullable: true}}, nil)
			writer := &packedTextBatchWriter{schema: schema, arrowSchema: writerSchema, writer: &packed.FFISegmentWriter{}}
			writes := 0
			patch := mockey.Mock((*packed.FFISegmentWriter).Write).To(func(_ *packed.FFISegmentWriter, record arrow.Record) error {
				require.True(t, writerSchema.Equal(record.Schema()))
				require.Equal(t, target, record.Column(0).DataType())
				if writes != 1 {
					require.Equal(t, 1, record.Column(0).NullN())
				} else {
					switch column := record.Column(0).(type) {
					case *array.String:
						require.Equal(t, "decoded text", column.Value(0))
					case *array.Binary:
						require.Equal(t, []byte("encoded ref"), column.Value(0))
					}
				}
				writes++
				return nil
			}).Build()
			defer patch.UnPatch()
			rb := NewRecordBuilder(schema)
			defer rb.Release()
			for batch := range 3 {
				var sourceType arrow.DataType = arrow.BinaryTypes.Binary
				if target.ID() == arrow.BINARY {
					sourceType = arrow.BinaryTypes.String
				}
				if batch == 1 {
					sourceType = target
				}
				input := array.NewBuilder(memory.DefaultAllocator, sourceType)
				if batch == 1 {
					switch input := input.(type) {
					case *array.StringBuilder:
						input.Append("decoded text")
					case *array.BinaryBuilder:
						input.Append([]byte("encoded ref"))
					}
				} else {
					input.AppendNull()
				}
				column := input.NewArray()
				input.Release()
				record := NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema([]arrow.Field{
					{Name: field.Name, Type: sourceType, Nullable: true},
				}, nil), []arrow.Array{column}, 1), map[FieldID]int{100: 0})
				column.Release()
				require.NoError(t, rb.Append(record, 0, 1))
				record.Release()
				out := rb.Build()
				err := writer.Write(out)
				out.Release()
				require.NoError(t, err)
			}
			require.Equal(t, 3, writes)
		})
	}
}

func TestToWriterRecordRejectsNonNullTextTypeMismatch(t *testing.T) {
	field := &schemapb.FieldSchema{FieldID: 100, Name: "text", DataType: schemapb.DataType_Text, Nullable: true}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{field}}
	builder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	builder.Append([]byte("encoded LOB reference"))
	column := builder.NewArray()
	builder.Release()
	defer column.Release()
	record := NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema([]arrow.Field{
		{Name: field.Name, Type: arrow.BinaryTypes.Binary, Nullable: true},
	}, nil), []arrow.Array{column}, 1), map[FieldID]int{100: 0})
	defer record.Release()
	writerSchema := arrow.NewSchema([]arrow.Field{{Name: field.Name, Type: arrow.BinaryTypes.String, Nullable: true}}, nil)
	require.NotPanics(t, func() {
		converted, release, err := toWriterRecord(record, schema, writerSchema)
		require.ErrorIs(t, err, merr.ErrServiceInternal)
		require.Nil(t, converted)
		require.Nil(t, release)
	})
}
