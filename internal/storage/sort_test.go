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
	"bytes"
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"strings"
	"testing"
	"unsafe"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/merr"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

type oneShotRecordReader struct {
	rec  Record
	done bool
}

func (r *oneShotRecordReader) Next() (Record, error) {
	if r.done {
		return nil, io.EOF
	}
	r.done = true
	return r.rec, nil
}

func (r *oneShotRecordReader) Close() error {
	return nil
}

// mergeSortTestRec builds a record with one column per given field. int64Cols
// and strCols are keyed by FieldID; all columns must have the same length.
func mergeSortTestRec(t *testing.T, int64Cols map[FieldID][]int64, strCols map[FieldID][]string) Record {
	t.Helper()
	fids := make([]FieldID, 0, len(int64Cols)+len(strCols))
	for fid := range int64Cols {
		fids = append(fids, fid)
	}
	for fid := range strCols {
		fids = append(fids, fid)
	}
	slices.Sort(fids)

	fields := make([]arrow.Field, 0, len(fids))
	arrs := make([]arrow.Array, 0, len(fids))
	f2c := make(map[FieldID]int, len(fids))
	n := 0
	for _, fid := range fids {
		if vals, ok := int64Cols[fid]; ok {
			b := array.NewInt64Builder(memory.DefaultAllocator)
			b.AppendValues(vals, nil)
			arrs = append(arrs, b.NewArray())
			b.Release()
			fields = append(fields, arrow.Field{Name: strconv.FormatInt(fid, 10), Type: arrow.PrimitiveTypes.Int64})
			n = len(vals)
		} else {
			vals := strCols[fid]
			b := array.NewStringBuilder(memory.DefaultAllocator)
			b.AppendValues(vals, nil)
			arrs = append(arrs, b.NewArray())
			b.Release()
			fields = append(fields, arrow.Field{Name: strconv.FormatInt(fid, 10), Type: arrow.BinaryTypes.String})
			n = len(vals)
		}
		f2c[fid] = len(arrs) - 1
	}
	return NewSimpleArrowRecord(array.NewRecord(arrow.NewSchema(fields, nil), arrs, int64(n)), f2c)
}

// sliceRecordReader yields the given records in order.
type sliceRecordReader struct {
	recs []Record
	pos  int
}

func (r *sliceRecordReader) Next() (Record, error) {
	if r.pos >= len(r.recs) {
		return nil, io.EOF
	}
	rec := r.recs[r.pos]
	r.pos++
	return rec, nil
}

func (r *sliceRecordReader) Close() error { return nil }

// nonForwardableTestRecord hides the concrete Arrow record type so tests and
// benchmarks can exercise the prepared rebuild path without a production
// fast-path toggle.
type nonForwardableTestRecord struct {
	inner Record
}

var _ Record = nonForwardableTestRecord{}

func (r nonForwardableTestRecord) Column(fieldID FieldID) arrow.Array { return r.inner.Column(fieldID) }
func (r nonForwardableTestRecord) Len() int                           { return r.inner.Len() }
func (r nonForwardableTestRecord) Release()                           { r.inner.Release() }
func (r nonForwardableTestRecord) Retain()                            { r.inner.Retain() }

func writerCompatibleTestRecord(t *testing.T, schema *schemapb.CollectionSchema, input Record) Record {
	t.Helper()
	builder := NewRecordBuilder(schema)
	defer builder.Release()
	require.NoError(t, builder.Append(input, 0, input.Len()))
	return builder.Build()
}

func writerCompatibleMergeSortTestRec(t *testing.T, schema *schemapb.CollectionSchema,
	int64Cols map[FieldID][]int64, strCols map[FieldID][]string,
) Record {
	t.Helper()
	input := mergeSortTestRec(t, int64Cols, strCols)
	defer input.Release()
	return writerCompatibleTestRecord(t, schema, input)
}

func writerArrowSchema(schema *schemapb.CollectionSchema) *arrow.Schema {
	builder := NewRecordBuilder(schema)
	defer builder.Release()
	return builder.arrowSchema
}

func arrowValueBufferAddress(t *testing.T, a arrow.Array) uintptr {
	t.Helper()
	for i := len(a.Data().Buffers()) - 1; i >= 0; i-- {
		buffer := a.Data().Buffers()[i]
		if buffer != nil && len(buffer.Bytes()) > 0 {
			return uintptr(unsafe.Pointer(&buffer.Bytes()[0]))
		}
	}
	t.Fatal("array has no non-empty data buffer")
	return 0
}

func TestRadixSortByInt64(t *testing.T) {
	t.Run("edge values across records", func(t *testing.T) {
		// Keys laid out across 3 records, mixing negatives, zero, duplicates and
		// the int64 bounds to exercise the sign-bit flip and every byte position.
		keys := [][]int64{
			{5, math.MaxInt64, -1},
			{0, math.MinInt64, -1},
			{42, 5},
		}
		var indices []rowIndex
		for ri := range keys {
			for i := range keys[ri] {
				indices = append(indices, rowIndex{int32(ri), int32(i)})
			}
		}

		radixSortByInt64(indices, keys)

		got := make([]int64, len(indices))
		for k, idx := range indices {
			got[k] = keys[idx.ri][idx.i]
		}
		assert.Equal(t, []int64{math.MinInt64, -1, -1, 0, 5, 5, 42, math.MaxInt64}, got)
	})

	t.Run("stable for equal keys", func(t *testing.T) {
		// Three rows share key 7 ({0,0},{1,0},{1,1} in input order); a stable sort
		// must keep that relative order among the duplicates.
		keys := [][]int64{
			{7, 3},
			{7, 7, 1},
		}
		indices := []rowIndex{{0, 0}, {0, 1}, {1, 0}, {1, 1}, {1, 2}}

		radixSortByInt64(indices, keys)

		assert.Equal(t, []rowIndex{{1, 2}, {0, 1}, {0, 0}, {1, 0}, {1, 1}}, indices)
	})

	t.Run("small inputs are no-ops", func(t *testing.T) {
		single := []rowIndex{{0, 0}}
		radixSortByInt64(single, [][]int64{{99}})
		assert.Equal(t, []rowIndex{{0, 0}}, single)

		assert.NotPanics(t, func() { radixSortByInt64(nil, nil) })
	})
}

func TestSort(t *testing.T) {
	const batchSize = 64 * 1024 * 1024

	getReaders := func() []RecordReader {
		blobs, err := generateTestDataWithSeed(10, 3)
		assert.NoError(t, err)
		reader10 := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))
		blobs, err = generateTestDataWithSeed(20, 3)
		assert.NoError(t, err)
		reader20 := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))
		rr := []RecordReader{reader20, reader10}
		return rr
	}

	lastPK := int64(-1)
	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			pk := r.Column(common.RowIDField).(*array.Int64).Value(0)
			assert.Greater(t, pk, lastPK)
			lastPK = pk
			return nil
		},

		closefn: func() error {
			lastPK = int64(-1)
			return nil
		},
	}

	t.Run("sort", func(t *testing.T) {
		gotNumRows, timings, err := Sort(batchSize, generateTestSchema(), getReaders(), rw, func(r Record, ri, i int) bool {
			return true
		}, []int64{common.RowIDField})
		assert.NoError(t, err)
		assert.Equal(t, 6, gotNumRows)
		assert.NotNil(t, timings)
		assert.Equal(t, 6, timings.NumRows)
		assert.Greater(t, timings.NumBatches, 0)
		assert.GreaterOrEqual(t, timings.ReadCost.Nanoseconds(), int64(0))
		assert.GreaterOrEqual(t, timings.SortCost.Nanoseconds(), int64(0))
		assert.GreaterOrEqual(t, timings.WriteCost.Nanoseconds(), int64(0))
		err = rw.Close()
		assert.NoError(t, err)
	})

	t.Run("sort with predicate", func(t *testing.T) {
		gotNumRows, timings, err := Sort(batchSize, generateTestSchema(), getReaders(), rw, func(r Record, ri, i int) bool {
			pk := r.Column(common.RowIDField).(*array.Int64).Value(i)
			return pk >= 20
		}, []int64{common.RowIDField})
		assert.NoError(t, err)
		assert.Equal(t, 3, gotNumRows)
		assert.NotNil(t, timings)
		assert.Equal(t, 3, timings.NumRows)
		err = rw.Close()
		assert.NoError(t, err)
	})

	t.Run("sort empty readers", func(t *testing.T) {
		gotNumRows, timings, err := Sort(batchSize, generateTestSchema(), []RecordReader{}, rw, func(r Record, ri, i int) bool {
			return true
		}, []int64{common.RowIDField})
		assert.NoError(t, err)
		assert.Equal(t, 0, gotNumRows)
		assert.NotNil(t, timings)
		assert.GreaterOrEqual(t, timings.ReadCost.Nanoseconds(), int64(0))
	})

	t.Run("sort with reader error", func(t *testing.T) {
		mockNext := mockey.Mock((*IterativeRecordReader).Next).Return(nil, fmt.Errorf("read error")).Build()
		defer mockNext.UnPatch()
		errReader := &IterativeRecordReader{}
		gotNumRows, timings, err := Sort(batchSize, generateTestSchema(), []RecordReader{errReader}, rw, func(r Record, ri, i int) bool {
			return true
		}, []int64{common.RowIDField})
		assert.Error(t, err)
		assert.Equal(t, 0, gotNumRows)
		assert.Nil(t, timings)
	})

	t.Run("sort with batch write error", func(t *testing.T) {
		errWriter := &MockRecordWriter{
			writefn: func(r Record) error {
				return fmt.Errorf("write error")
			},
			closefn: func() error {
				return nil
			},
		}
		// Use small batchSize to trigger mid-loop batch write error (line 157)
		gotNumRows, timings, err := Sort(1, generateTestSchema(), getReaders(), errWriter, func(r Record, ri, i int) bool {
			return true
		}, []int64{common.RowIDField})
		assert.Error(t, err)
		assert.Equal(t, 0, gotNumRows)
		assert.Nil(t, timings)
	})

	t.Run("sort with final write error", func(t *testing.T) {
		errWriter := &MockRecordWriter{
			writefn: func(r Record) error {
				// Fail on the first write (which is the final batch write when batchSize is large)
				return fmt.Errorf("write error")
			},
			closefn: func() error {
				return nil
			},
		}
		// Use large batchSize so data doesn't trigger mid-loop write, only the final batch write (line 164)
		gotNumRows, timings, err := Sort(batchSize, generateTestSchema(), getReaders(), errWriter, func(r Record, ri, i int) bool {
			return true
		}, []int64{common.RowIDField})
		assert.Error(t, err)
		assert.Equal(t, 0, gotNumRows)
		assert.Nil(t, timings)
	})
}

func TestSortCachesNullableGeometryDefaultWKB(t *testing.T) {
	const defaultWKT = "POINT (1 2)"
	defaultWKB, err := common.ConvertWKTToWKB(defaultWKT)
	require.NoError(t, err)

	convertCalls := 0
	patch := mockey.Mock(common.ConvertWKTToWKB).To(func(wkt string) ([]byte, error) {
		convertCalls++
		require.Equal(t, defaultWKT, wkt)
		return defaultWKB, nil
	}).Build()
	defer patch.UnPatch()

	pkField := &schemapb.FieldSchema{
		FieldID:      100,
		Name:         "pk",
		DataType:     schemapb.DataType_Int64,
		IsPrimaryKey: true,
	}
	geomField := &schemapb.FieldSchema{
		FieldID:  101,
		Name:     "geom",
		DataType: schemapb.DataType_Geometry,
		Nullable: true,
		DefaultValue: &schemapb.ValueField{
			Data: &schemapb.ValueField_StringData{StringData: defaultWKT},
		},
	}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{pkField, geomField}}

	pkBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	defer pkBuilder.Release()
	pkBuilder.AppendValues([]int64{3, 1, 2}, nil)
	pkColumn := pkBuilder.NewArray()
	defer pkColumn.Release()

	geomBuilder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	defer geomBuilder.Release()
	geomBuilder.AppendNulls(3)
	geomColumn := geomBuilder.NewArray()
	defer geomColumn.Release()

	rec := NewSimpleArrowRecord(array.NewRecord(
		arrow.NewSchema([]arrow.Field{
			{Name: pkField.Name, Type: arrow.PrimitiveTypes.Int64},
			{Name: geomField.Name, Type: arrow.BinaryTypes.Binary, Nullable: true},
		}, nil),
		[]arrow.Array{pkColumn, geomColumn},
		3,
	), map[FieldID]int{pkField.FieldID: 0, geomField.FieldID: 1})
	defer rec.Release()

	writer := &MockRecordWriter{
		writefn: func(r Record) error {
			out := r.Column(geomField.FieldID).(*array.Binary)
			require.Equal(t, 3, out.Len())
			for i := 0; i < out.Len(); i++ {
				require.True(t, out.IsValid(i))
				require.Equal(t, defaultWKB, out.Value(i))
			}
			return nil
		},
		closefn: func() error {
			return nil
		},
	}

	gotNumRows, timings, err := Sort(64*1024*1024, schema, []RecordReader{&oneShotRecordReader{rec: rec}}, writer, func(r Record, ri, i int) bool {
		return true
	}, []int64{pkField.FieldID})
	require.NoError(t, err)
	require.Equal(t, 3, gotNumRows)
	require.NotNil(t, timings)
	require.Equal(t, 1, convertCalls)
}

func TestMergeSort(t *testing.T) {
	getReaders := func() []RecordReader {
		blobs, err := generateTestDataWithSeed(1000, 5000)
		assert.NoError(t, err)
		reader10 := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))
		blobs, err = generateTestDataWithSeed(4000, 5000)
		assert.NoError(t, err)
		reader20 := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))
		rr := []RecordReader{reader20, reader10}
		return rr
	}

	lastPK := int64(-1)
	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			// check every row, not just the first of each batch. The two
			// readers overlap on pk, so the merged order is non-decreasing
			// rather than strictly increasing.
			col := r.Column(common.RowIDField).(*array.Int64)
			for i := 0; i < col.Len(); i++ {
				pk := col.Value(i)
				assert.GreaterOrEqual(t, pk, lastPK)
				lastPK = pk
			}
			return nil
		},

		closefn: func() error {
			lastPK = int64(-1)
			return nil
		},
	}

	// small enough to force multiple output batches
	const batchSize = 4096

	t.Run("merge sort", func(t *testing.T) {
		gotNumRows, err := MergeSort(batchSize, generateTestSchema(), getReaders(), rw, func(r Record, ri, i int) bool {
			return true
		}, []int64{common.RowIDField})
		assert.NoError(t, err)
		assert.Equal(t, 10000, gotNumRows)
		err = rw.Close()
		assert.NoError(t, err)
	})

	t.Run("merge sort with predicate", func(t *testing.T) {
		gotNumRows, err := MergeSort(batchSize, generateTestSchema(), getReaders(), rw, func(r Record, ri, i int) bool {
			pk := r.Column(common.RowIDField).(*array.Int64).Value(i)
			// cover a single record (1024 rows) that is deleted, or the last data in the record is deleted
			// index 1023 is deleted. records (1024-2048) and (5000-6023) are all deleted
			return pk < 2000 || (pk >= 3050 && pk < 5000) || pk >= 7000
		}, []int64{common.RowIDField})
		assert.NoError(t, err)
		assert.Equal(t, 5950, gotNumRows)
		err = rw.Close()
		assert.NoError(t, err)
	})
}

func TestMergeSortReturnsRecordBuilderAppendError(t *testing.T) {
	textBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	textBuilder.Append("not-a-lob-ref")
	textColumn := textBuilder.NewArray()
	defer textColumn.Release()
	textBuilder.Release()

	pkBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	pkBuilder.Append(1)
	pkColumn := pkBuilder.NewArray()
	defer pkColumn.Release()
	pkBuilder.Release()

	rec := NewSimpleArrowRecord(array.NewRecord(
		arrow.NewSchema([]arrow.Field{
			{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
			{Name: "text", Type: arrow.BinaryTypes.String},
		}, nil),
		[]arrow.Array{pkColumn, textColumn},
		1,
	), map[FieldID]int{100: 0, 101: 1})
	defer rec.Release()

	reader := &oneShotRecordReader{rec: rec}
	writer := &MockRecordWriter{
		writefn: func(r Record) error {
			return nil
		},
		closefn: func() error {
			return nil
		},
	}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: 101, Name: "text", DataType: schemapb.DataType_Text},
	}}

	_, err := MergeSort(1024, schema, []RecordReader{reader}, writer, func(r Record, ri, i int) bool {
		return true
	}, []int64{100})
	assert.ErrorContains(t, err, "failed to append value")
}

func TestMergeSortPreparedOutputMatchesGenericRecordBuilder(t *testing.T) {
	const keyField = FieldID(100)
	const nullableField = FieldID(101)
	const defaultField = FieldID(102)
	const textField = FieldID(103)
	const geometryField = FieldID(104)
	const vectorArrayField = FieldID(105)
	const structChildField = FieldID(106)
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: keyField, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: nullableField, Name: "nullable", DataType: schemapb.DataType_Int64, Nullable: true},
			{
				FieldID: defaultField, Name: "with_default", DataType: schemapb.DataType_Int64, Nullable: true,
				DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}},
			},
			{FieldID: textField, Name: "text", DataType: schemapb.DataType_Text, Nullable: true},
			{FieldID: geometryField, Name: "geometry", DataType: schemapb.DataType_Geometry, Nullable: true},
			{
				FieldID: vectorArrayField, Name: "vector_array", DataType: schemapb.DataType_ArrayOfVector,
				ElementType: schemapb.DataType_FloatVector,
				TypeParams:  []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}},
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			Name: "struct", Fields: []*schemapb.FieldSchema{{
				FieldID: structChildField, Name: "child", DataType: schemapb.DataType_VarChar, Nullable: true,
			}},
		}},
	}

	newValidity := func(valid []bool) []bool { return append([]bool(nil), valid...) }
	keyBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	keyBuilder.AppendValues([]int64{0, 1, 2}, nil)
	keyArray := keyBuilder.NewArray()
	keyBuilder.Release()

	nullableBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	nullableBuilder.AppendValues([]int64{10, 0, 30}, newValidity([]bool{true, false, true}))
	nullableArray := nullableBuilder.NewArray()
	nullableBuilder.Release()

	defaultBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	defaultBuilder.AppendValues([]int64{1, 0, 3}, newValidity([]bool{true, false, true}))
	defaultArray := defaultBuilder.NewArray()
	defaultBuilder.Release()

	textBuilder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	textBuilder.AppendValues([][]byte{[]byte("lob-0"), nil, []byte("lob-2")}, newValidity([]bool{true, false, true}))
	textArray := textBuilder.NewArray()
	textBuilder.Release()

	geometryBuilder := array.NewBinaryBuilder(memory.DefaultAllocator, arrow.BinaryTypes.Binary)
	geometryBuilder.AppendValues([][]byte{{1, 2}, nil, {3, 4}}, newValidity([]bool{true, false, true}))
	geometryArray := geometryBuilder.NewArray()
	geometryBuilder.Release()

	vectorType := &arrow.FixedSizeBinaryType{ByteWidth: 8}
	vectorListBuilder := array.NewListBuilder(memory.DefaultAllocator, vectorType)
	vectorValues := vectorListBuilder.ValueBuilder().(*array.FixedSizeBinaryBuilder)
	vectorListBuilder.Append(true)
	vectorValues.Append([]byte{0, 1, 2, 3, 4, 5, 6, 7})
	vectorListBuilder.Append(true)
	vectorValues.Append([]byte{8, 9, 10, 11, 12, 13, 14, 15})
	vectorValues.Append([]byte{16, 17, 18, 19, 20, 21, 22, 23})
	vectorListBuilder.Append(true)
	vectorArray := vectorListBuilder.NewArray()
	vectorListBuilder.Release()

	structChildBuilder := array.NewStringBuilder(memory.DefaultAllocator)
	structChildBuilder.AppendValues([]string{"a", "", "c"}, newValidity([]bool{true, false, true}))
	structChildArray := structChildBuilder.NewArray()
	structChildBuilder.Release()

	arrays := []arrow.Array{keyArray, nullableArray, defaultArray, textArray, geometryArray, vectorArray, structChildArray}
	defer func() {
		for _, a := range arrays {
			a.Release()
		}
	}()
	arrowSchema, err := ConvertToArrowSchema(schema, false)
	require.NoError(t, err)
	arrowFields := arrowSchema.Fields()
	arrowFields[3].Type = arrow.BinaryTypes.Binary
	rec := NewSimpleArrowRecord(array.NewRecord(
		arrow.NewSchema(arrowFields, nil), arrays, 3),
		map[FieldID]int{
			keyField: 0, nullableField: 1, defaultField: 2, textField: 3, geometryField: 4,
			vectorArrayField: 5, structChildField: 6,
		})
	defer rec.Release()

	referenceBuilder := NewRecordBuilder(schema)
	require.NoError(t, referenceBuilder.Append(rec, 0, rec.Len()))
	referenceSize := referenceBuilder.GetSize()
	reference := referenceBuilder.Build()
	referenceBuilder.Release()
	defer reference.Release()
	var forwardedSize uint64
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		size, ok := recordBuilderValueSize(reference.Column(field.GetFieldID()), 0, reference.Len())
		require.True(t, ok, field.GetName())
		forwardedSize += size
	}
	require.Equal(t, referenceSize, forwardedSize)

	var output Record
	rw := &MockRecordWriter{writefn: func(out Record) error {
		out.Retain()
		output = out
		return nil
	}, closefn: func() error { return nil }}
	n, err := MergeSort(64*1024*1024, schema, []RecordReader{&oneShotRecordReader{rec: rec}}, rw,
		func(Record, int, int) bool { return true }, []int64{keyField})
	require.NoError(t, err)
	require.Equal(t, rec.Len(), n)
	require.NotNil(t, output)
	defer output.Release()
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		require.True(t, array.Equal(reference.Column(field.GetFieldID()), output.Column(field.GetFieldID())), field.GetName())
	}
}

func TestMergeSortPreparedOutputMatchesGenericForStorageFieldFamilies(t *testing.T) {
	schema := generateTestSchema()
	blobs, err := generateTestDataWithSeed(3, 3)
	require.NoError(t, err)
	reader := newIterativeCompositeBinlogRecordReader(schema, nil, MakeBlobsReader(blobs))
	defer reader.Close()
	record, err := reader.Next()
	require.NoError(t, err)

	referenceBuilder := NewRecordBuilder(schema)
	require.NoError(t, referenceBuilder.Append(record, 0, record.Len()))
	referenceSize := referenceBuilder.GetSize()
	reference := referenceBuilder.Build()
	referenceBuilder.Release()
	defer reference.Release()
	var forwardedSize uint64
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		size, ok := recordBuilderValueSize(reference.Column(field.GetFieldID()), 0, reference.Len())
		require.True(t, ok, "unsupported size accounting for %s (%s)", field.GetName(), field.GetDataType())
		forwardedSize += size
	}
	require.Equal(t, referenceSize, forwardedSize)

	var output Record
	writer := &MockRecordWriter{
		writefn: func(record Record) error {
			record.Retain()
			output = record
			return nil
		},
		closefn: func() error { return nil },
	}
	rows, err := MergeSort(64*1024*1024, schema, []RecordReader{&oneShotRecordReader{rec: record}}, writer,
		func(Record, int, int) bool { return true }, []int64{common.RowIDField})
	require.NoError(t, err)
	require.Equal(t, record.Len(), rows)
	require.NotNil(t, output)
	defer output.Release()

	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		require.True(t, array.Equal(reference.Column(field.GetFieldID()), output.Column(field.GetFieldID())),
			"prepared output mismatch for %s (%s)", field.GetName(), field.GetDataType())
	}
}

func TestRecordBuilderValueSizeIgnoresNullVariablePayload(t *testing.T) {
	const field = FieldID(100)
	cases := []struct {
		name       string
		dataType   schemapb.DataType
		arrowType  arrow.DataType
		buildArray func(arrow.ArrayData) arrow.Array
	}{
		{
			name: "string", dataType: schemapb.DataType_VarChar, arrowType: arrow.BinaryTypes.String,
			buildArray: func(data arrow.ArrayData) arrow.Array { return array.NewStringData(data) },
		},
		{
			name: "binary", dataType: schemapb.DataType_JSON, arrowType: arrow.BinaryTypes.Binary,
			buildArray: func(data arrow.ArrayData) arrow.Array { return array.NewBinaryData(data) },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
				FieldID: field, Name: "value", DataType: tc.dataType, Nullable: true,
			}}}
			data := array.NewData(tc.arrowType, 3, []*memory.Buffer{
				memory.NewBufferBytes([]byte{0b00000101}),
				memory.NewBufferBytes(arrow.Int32Traits.CastToBytes([]int32{0, 1, 4, 5})),
				memory.NewBufferBytes([]byte("abcde")),
			}, nil, 1, 0)
			values := tc.buildArray(data)
			data.Release()
			record := NewSimpleArrowRecord(array.NewRecord(
				writerArrowSchema(schema), []arrow.Array{values}, 3), map[FieldID]int{field: 0})
			values.Release()
			defer record.Release()

			builder := NewRecordBuilder(schema)
			defer builder.Release()
			var prepared preparedRecordAppender
			require.NoError(t, builder.prepareRecord(record, &prepared))
			for row := 0; row < record.Len(); row++ {
				require.NoError(t, builder.appendPreparedRow(&prepared, row))
			}

			size, ok := recordBuilderValueSize(record.Column(field), 0, record.Len())
			require.True(t, ok)
			require.Equal(t, builder.GetSize(), size)
		})
	}
}

func TestMergeSortDirectForwardMatchesRebuiltIPCBytes(t *testing.T) {
	const rows = 256
	const keyField = FieldID(100)
	const payloadField = FieldID(101)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: keyField, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: payloadField, Name: "payload", DataType: schemapb.DataType_VarChar},
	}}
	keys := make([]int64, rows)
	payloads := make([]string, rows)
	for row := range rows {
		keys[row] = int64(row)
		payloads[row] = fmt.Sprintf("payload-%03d", row)
	}
	record := writerCompatibleMergeSortTestRec(t, schema,
		map[FieldID][]int64{keyField: keys}, map[FieldID][]string{payloadField: payloads})
	defer record.Release()
	sourceBuffer := arrowValueBufferAddress(t, record.Column(keyField))

	run := func(rebuild bool) []byte {
		input := record
		if rebuild {
			input = nonForwardableTestRecord{inner: record}
		}
		var encoded bytes.Buffer
		writer := &MockRecordWriter{writefn: func(out Record) error {
			if rebuild {
				require.NotEqual(t, sourceBuffer, arrowValueBufferAddress(t, out.Column(keyField)))
			} else {
				require.Equal(t, sourceBuffer, arrowValueBufferAddress(t, out.Column(keyField)))
			}
			arrowRecord := out.(*simpleArrowRecord).r
			ipcWriter := ipc.NewWriter(&encoded, ipc.WithSchema(arrowRecord.Schema()))
			require.NoError(t, ipcWriter.Write(arrowRecord))
			require.NoError(t, ipcWriter.Close())
			return nil
		}, closefn: func() error { return nil }}
		mergedRows, err := MergeSort(64*1024*1024, schema,
			[]RecordReader{&oneShotRecordReader{rec: input}}, writer,
			func(Record, int, int) bool { return true }, []int64{keyField})
		require.NoError(t, err)
		require.Equal(t, rows, mergedRows)
		return encoded.Bytes()
	}

	require.Equal(t, run(true), run(false))
}

// Benchmark sort
func BenchmarkSort(b *testing.B) {
	batch := 500000
	blobs, err := generateTestDataWithSeed(batch, batch)
	assert.NoError(b, err)
	reader10 := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))
	blobs, err = generateTestDataWithSeed(batch*2+1, batch)
	assert.NoError(b, err)
	reader20 := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))
	rr := []RecordReader{reader20, reader10}

	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			return nil
		},

		closefn: func() error {
			return nil
		},
	}

	const batchSize = 64 * 1024 * 1024
	b.ResetTimer()

	b.Run("sort", func(b *testing.B) {
		for i := 0; i < b.N; i++ {
			Sort(batchSize, generateTestSchema(), rr, rw, func(r Record, ri, i int) bool {
				return true
			}, []int64{common.RowIDField})
		}
	})
}

// Benchmark merge sort
func BenchmarkMergeSort(b *testing.B) {
	batch := 100000
	const batchSize = 64 * 1024 * 1024

	rw := &MockRecordWriter{
		writefn: func(r Record) error { return nil },
		closefn: func() error { return nil },
	}

	// Generate the payload once: it dwarfs the merge itself, and ReportAllocs
	// counts allocations made inside StopTimer too.
	blobs10, err := generateTestDataWithSeed(batch, batch)
	assert.NoError(b, err)
	blobs20, err := generateTestDataWithSeed(batch*2+1, batch)
	assert.NoError(b, err)
	schema := generateTestSchema()

	b.Run("merge_sort", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			// readers are single-use, so only they are rebuilt per iteration
			reader10 := newIterativeCompositeBinlogRecordReader(schema, nil, MakeBlobsReader(blobs10))
			reader20 := newIterativeCompositeBinlogRecordReader(schema, nil, MakeBlobsReader(blobs20))

			_, err := MergeSort(batchSize, schema, []RecordReader{reader20, reader10}, rw,
				func(r Record, ri, i int) bool { return true }, []int64{common.RowIDField})
			assert.NoError(b, err)
		}
	})
}

// Benchmark merge sort on a varchar key, which takes the string comparison path
// and the reusable key buffer in the ordering check.
func BenchmarkMergeSortVarcharKey(b *testing.B) {
	const strField = FieldID(16)
	const rowsPerRec = 4096
	const recsPerReader = 8
	const batchSize = 64 * 1024 * 1024

	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: strField, Name: "vc", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
	}}

	// two interleaved ascending key spaces, 32-byte keys
	build := func(t *testing.B, offset int) []Record {
		recs := make([]Record, recsPerReader)
		k := offset
		for j := range recs {
			vals := make([]string, rowsPerRec)
			for i := range vals {
				vals[i] = fmt.Sprintf("%024d%08d", k, k)
				k += 2
			}
			bld := array.NewStringBuilder(memory.DefaultAllocator)
			bld.AppendValues(vals, nil)
			arr := bld.NewArray()
			bld.Release()
			recs[j] = NewSimpleArrowRecord(
				array.NewRecord(arrow.NewSchema([]arrow.Field{{Name: "16", Type: arrow.BinaryTypes.String}}, nil),
					[]arrow.Array{arr}, int64(rowsPerRec)),
				map[FieldID]int{strField: 0})
		}
		return recs
	}

	rw := &MockRecordWriter{
		writefn: func(r Record) error { return nil },
		closefn: func() error { return nil },
	}

	// records are read-only here, so build them once and only rewrap per iteration
	recs0, recs1 := build(b, 0), build(b, 1)

	b.Run("merge_sort_varchar", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			r0 := &sliceRecordReader{recs: recs0}
			r1 := &sliceRecordReader{recs: recs1}

			_, err := MergeSort(batchSize, schema, []RecordReader{r0, r1}, rw,
				func(r Record, ri, i int) bool { return true }, []int64{strField})
			assert.NoError(b, err)
		}
	})
}

func BenchmarkMergeSortDisjoint(b *testing.B) {
	const rows = 32768
	const batchSize = 64 * 1024 * 1024
	const pkField = FieldID(100)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: pkField, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
	arrowSchema := arrow.NewSchema([]arrow.Field{{
		Name: "pk", Type: arrow.PrimitiveTypes.Int64,
		Metadata: arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"100"}),
	}}, nil)
	build := func(start int64) Record {
		values := make([]int64, rows)
		for i := range values {
			values[i] = start + int64(i)
		}
		builder := array.NewInt64Builder(memory.DefaultAllocator)
		builder.AppendValues(values, nil)
		arr := builder.NewArray()
		builder.Release()
		rec := NewSimpleArrowRecord(array.NewRecord(arrowSchema, []arrow.Array{arr}, rows), map[FieldID]int{pkField: 0})
		arr.Release()
		return rec
	}
	recs0 := []Record{build(0)}
	recs1 := []Record{build(rows)}
	defer recs0[0].Release()
	defer recs1[0].Release()
	rebuild0 := []Record{nonForwardableTestRecord{inner: recs0[0]}}
	rebuild1 := []Record{nonForwardableTestRecord{inner: recs1[0]}}
	rw := &MockRecordWriter{writefn: func(Record) error { return nil }, closefn: func() error { return nil }}
	run := func(b *testing.B, records0, records1 []Record) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, err := MergeSort(batchSize, schema,
				[]RecordReader{&sliceRecordReader{recs: records0}, &sliceRecordReader{recs: records1}}, rw,
				func(Record, int, int) bool { return true }, []int64{pkField})
			require.NoError(b, err)
		}
	}
	b.Run("rebuild", func(b *testing.B) { run(b, rebuild0, rebuild1) })
	b.Run("forward", func(b *testing.B) { run(b, recs0, recs1) })
}

// BenchmarkMergeSortRunSelectionPhase measures only heap/run ownership
// decisions over prepared int64 key columns. It deliberately performs no
// Arrow output construction or writer work; those have separate phase gates.
func BenchmarkMergeSortRunSelectionPhase(b *testing.B) {
	const (
		readers       = 30
		rowsPerReader = 512
	)
	keys := make([][]int64, readers)
	for reader := range readers {
		keys[reader] = make([]int64, rowsPerReader)
		for row := range rowsPerReader {
			keys[reader][row] = int64(reader*rowsPerReader + row)
		}
	}
	run := func(b *testing.B, coalesce bool) {
		positions := make([]int, readers)
		h := rowHeap{less: func(left, right rowIndex) bool {
			leftKey, rightKey := keys[left.ri][left.i], keys[right.ri][right.i]
			if leftKey != rightKey {
				return leftKey < rightKey
			}
			return left.ri < right.ri
		}}
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			clear(positions)
			h.items = h.items[:0]
			for reader := range readers {
				h.push(rowIndex{ri: int32(reader)})
			}
			selected := 0
			for h.len() > 0 {
				current := h.pop()
				reader := int(current.ri)
				end := positions[reader] + 1
				if coalesce {
					for end < rowsPerReader {
						candidate := rowIndex{ri: current.ri, i: int32(end)}
						if h.len() > 0 && !h.less(candidate, h.items[0]) {
							break
						}
						end++
					}
				}
				selected += end - positions[reader]
				positions[reader] = end
				if end < rowsPerReader {
					h.push(rowIndex{ri: current.ri, i: int32(end)})
				}
			}
			if selected != readers*rowsPerReader {
				b.Fatalf("selected rows=%d", selected)
			}
		}
	}
	b.Run("per_row_baseline", func(b *testing.B) { run(b, false) })
	b.Run("coalesced_runs", func(b *testing.B) { run(b, true) })
}

// BenchmarkMergeSortPreparedOutputConstructionPhase measures the prepared
// Arrow builder path alone, without reader decode, merge selection, or writer
// serialization.
func BenchmarkMergeSortPreparedOutputConstructionPhase(b *testing.B) {
	const rows = 8192
	schema := generateTestSchema()
	blobs, err := generateTestDataWithSeed(1, rows)
	require.NoError(b, err)
	reader := newIterativeCompositeBinlogRecordReader(schema, nil, MakeBlobsReader(blobs))
	record, err := reader.Next()
	require.NoError(b, err)
	defer reader.Close()

	b.ReportAllocs()
	b.ResetTimer()
	for range b.N {
		builder := NewRecordBuilder(schema)
		var prepared preparedRecordAppender
		require.NoError(b, builder.prepareRecord(record, &prepared))
		builder.reservePrepared(record.Len())
		for row := 0; row < record.Len(); row++ {
			require.NoError(b, builder.appendPreparedRow(&prepared, row))
		}
		output := builder.Build()
		if output.Len() != record.Len() {
			b.Fatalf("output rows=%d expected=%d", output.Len(), record.Len())
		}
		output.Release()
		builder.Release()
	}
}

func TestSortByMoreThanOneField(t *testing.T) {
	const batchSize = 10000
	sortByFieldIDs := []int64{common.RowIDField, common.TimeStampField}

	blobs, err := generateTestDataWithSeed(10, batchSize)
	assert.NoError(t, err)
	reader10 := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))
	blobs, err = generateTestDataWithSeed(20, batchSize)
	assert.NoError(t, err)
	reader20 := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))
	rr := []RecordReader{reader20, reader10}

	lastPK := int64(-1)
	lastTS := int64(-1)
	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			pk := r.Column(common.RowIDField).(*array.Int64).Value(0)
			ts := r.Column(common.TimeStampField).(*array.Int64).Value(0)
			assert.True(t, pk > lastPK || (pk == lastPK && ts > lastTS))
			lastPK = pk
			return nil
		},

		closefn: func() error {
			lastPK = int64(-1)
			return nil
		},
	}
	gotNumRows, _, err := Sort(batchSize, generateTestSchema(), rr, rw, func(r Record, ri, i int) bool {
		return true
	}, sortByFieldIDs)
	assert.NoError(t, err)
	assert.Equal(t, batchSize*2, gotNumRows)
	assert.NoError(t, rw.Close())
}

func TestMergeSortVarcharKey(t *testing.T) {
	const strField = FieldID(16)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: strField, Name: "vc", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
	}}

	r0 := &sliceRecordReader{recs: []Record{
		mergeSortTestRec(t, nil, map[FieldID][]string{strField: {"a", "c", "e"}}),
		mergeSortTestRec(t, nil, map[FieldID][]string{strField: {"g", "i"}}),
	}}
	r1 := &sliceRecordReader{recs: []Record{
		mergeSortTestRec(t, nil, map[FieldID][]string{strField: {"b", "d", "f", "h"}}),
	}}

	var got []string
	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			col := r.Column(strField).(*array.String)
			for i := 0; i < col.Len(); i++ {
				got = append(got, col.Value(i))
			}
			return nil
		},
		closefn: func() error { return nil },
	}

	n, err := MergeSort(16, schema, []RecordReader{r0, r1}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{strField})
	assert.NoError(t, err)
	assert.Equal(t, 9, n)
	assert.NoError(t, rw.Close())
	assert.Equal(t, []string{"a", "b", "c", "d", "e", "f", "g", "h", "i"}, got)
}

func TestMergeSortByMoreThanOneField(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "rowid", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: common.TimeStampField, Name: "ts", DataType: schemapb.DataType_Int64},
	}}

	// each record is ascending by (rowid, ts)
	r0 := &sliceRecordReader{recs: []Record{
		mergeSortTestRec(t, map[FieldID][]int64{
			common.RowIDField:     {1, 1, 3},
			common.TimeStampField: {10, 20, 10},
		}, nil),
	}}
	r1 := &sliceRecordReader{recs: []Record{
		mergeSortTestRec(t, map[FieldID][]int64{
			common.RowIDField:     {1, 2, 3},
			common.TimeStampField: {15, 5, 5},
		}, nil),
	}}

	type pair struct{ pk, ts int64 }
	var got []pair
	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			pk := r.Column(common.RowIDField).(*array.Int64)
			ts := r.Column(common.TimeStampField).(*array.Int64)
			for i := 0; i < pk.Len(); i++ {
				got = append(got, pair{pk.Value(i), ts.Value(i)})
			}
			return nil
		},
		closefn: func() error { return nil },
	}

	n, err := MergeSort(16, schema, []RecordReader{r0, r1}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{common.RowIDField, common.TimeStampField})
	assert.NoError(t, err)
	assert.Equal(t, 6, n)
	assert.NoError(t, rw.Close())
	assert.Equal(t, []pair{{1, 10}, {1, 15}, {1, 20}, {2, 5}, {3, 5}, {3, 10}}, got)
}

// The predicate carries a side effect in production (segmentTotalRows[ri]++ in
// merge_sort.go), so every row must be evaluated exactly once.
func TestMergeSortPredicateCalledOncePerRow(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "rowid", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}

	r0 := &sliceRecordReader{recs: []Record{
		mergeSortTestRec(t, map[FieldID][]int64{common.RowIDField: {1, 3, 5}}, nil),
		mergeSortTestRec(t, map[FieldID][]int64{common.RowIDField: {7, 9}}, nil),
	}}
	r1 := &sliceRecordReader{recs: []Record{
		mergeSortTestRec(t, map[FieldID][]int64{common.RowIDField: {2, 4, 6, 8}}, nil),
	}}

	// pk is unique across all records here, so it identifies a row globally.
	counts := map[int64]int{}
	rw := &MockRecordWriter{
		writefn: func(r Record) error { return nil },
		closefn: func() error { return nil },
	}

	_, err := MergeSort(16, schema, []RecordReader{r0, r1}, rw, func(r Record, ri, i int) bool {
		pk := r.Column(common.RowIDField).(*array.Int64).Value(i)
		counts[pk]++
		return pk%3 != 0 // also exercise skipping filtered rows
	}, []int64{common.RowIDField})
	assert.NoError(t, err)
	assert.NoError(t, rw.Close())

	assert.Equal(t, 9, len(counts), "every row must be visited exactly once")
	for pk, n := range counts {
		assert.Equalf(t, 1, n, "predicate called %d times for pk %d", n, pk)
	}
}

func TestMergeSortDirectForwardsContiguousRecord(t *testing.T) {
	const rows = 256
	const field = FieldID(100)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: field, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(i)
	}
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	builder.Release()
	rec := NewSimpleArrowRecord(array.NewRecord(
		writerArrowSchema(schema),
		[]arrow.Array{arr}, rows), map[FieldID]int{field: 0})
	arr.Release()
	defer rec.Release()

	writes := 0
	sourceBuffer := arrowValueBufferAddress(t, rec.Column(field))
	var outputBuffer uintptr
	rw := &MockRecordWriter{writefn: func(out Record) error {
		writes++
		outputBuffer = arrowValueBufferAddress(t, out.Column(field))
		require.Equal(t, rows, out.Len())
		return nil
	}, closefn: func() error { return nil }}
	reader := &oneShotRecordReader{rec: rec}
	calls := 0
	n, err := MergeSort(64*1024*1024, schema, []RecordReader{reader}, rw, func(Record, int, int) bool { calls++; return true }, []int64{field})
	require.NoError(t, err)
	require.Equal(t, rows, n)
	require.Equal(t, rows, calls)
	require.Equal(t, 1, writes)
	require.Equal(t, sourceBuffer, outputBuffer, "direct forwarding must reuse the source value buffer")
}

func TestMergeSortDirectForwardsPrefixSlice(t *testing.T) {
	const field = FieldID(100)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: field, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
	leftValues := make([]int64, 300)
	for i := range leftValues {
		leftValues[i] = int64(i)
	}
	left := writerCompatibleMergeSortTestRec(t, schema, map[FieldID][]int64{field: leftValues}, nil)
	right := writerCompatibleMergeSortTestRec(t, schema, map[FieldID][]int64{field: {128, 512}}, nil)
	defer left.Release()
	defer right.Release()

	sourceBuffer := arrowValueBufferAddress(t, left.Column(field))
	forwardedPrefix := false
	rw := &MockRecordWriter{writefn: func(out Record) error {
		if out.Len() >= directForwardMinRows && arrowValueBufferAddress(t, out.Column(field)) == sourceBuffer {
			values := out.Column(field).(*array.Int64)
			require.Equal(t, int64(0), values.Value(0))
			require.LessOrEqual(t, values.Value(out.Len()-1), int64(128))
			forwardedPrefix = true
		}
		return nil
	}, closefn: func() error { return nil }}
	n, err := MergeSort(64*1024*1024, schema,
		[]RecordReader{&oneShotRecordReader{rec: left}, &oneShotRecordReader{rec: right}}, rw,
		func(Record, int, int) bool { return true }, []int64{field})
	require.NoError(t, err)
	require.Equal(t, 302, n)
	require.True(t, forwardedPrefix, "eligible record prefix must be forwarded without copying")
}

func TestMergeSortTruncatedForwardedPrefixRequeuesBeforeCompetitor(t *testing.T) {
	const field = FieldID(100)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: field, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
	leftValues := make([]int64, 300)
	for i := range leftValues {
		leftValues[i] = int64(i)
	}
	left := writerCompatibleMergeSortTestRec(t, schema, map[FieldID][]int64{field: leftValues}, nil)
	right := writerCompatibleMergeSortTestRec(t, schema, map[FieldID][]int64{field: {250, 400}}, nil)
	defer left.Release()
	defer right.Release()

	got := make([]int64, 0, len(leftValues)+2)
	rw := &MockRecordWriter{writefn: func(out Record) error {
		values := out.Column(field).(*array.Int64)
		for i := 0; i < values.Len(); i++ {
			got = append(got, values.Value(i))
		}
		return nil
	}, closefn: func() error { return nil }}
	n, err := MergeSort(directForwardMinRows*8, schema,
		[]RecordReader{&oneShotRecordReader{rec: left}, &oneShotRecordReader{rec: right}}, rw,
		func(Record, int, int) bool { return true }, []int64{field})
	require.NoError(t, err)
	require.Equal(t, len(leftValues)+2, n)
	require.Len(t, got, len(leftValues)+2)
	for i := 1; i < len(got); i++ {
		require.LessOrEqualf(t, got[i-1], got[i], "output key decreased at row %d", i)
	}
	require.Equal(t, []int64{249, 250, 250, 251}, got[249:253],
		"reader-index stability must be preserved at the competing key")
}

func TestMergeSortDirectForwardsSuffixSlice(t *testing.T) {
	const field = FieldID(100)
	const payloadField = FieldID(101)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: field, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: payloadField, Name: "payload", DataType: schemapb.DataType_VarChar},
	}}
	leftValues := make([]int64, 300)
	leftPayloads := make([]string, len(leftValues))
	leftValues[0] = 0
	for i := 1; i < len(leftValues); i++ {
		leftValues[i] = int64(i + 1)
	}
	left := writerCompatibleMergeSortTestRec(t, schema, map[FieldID][]int64{field: leftValues}, map[FieldID][]string{payloadField: leftPayloads})
	right := writerCompatibleMergeSortTestRec(t, schema, map[FieldID][]int64{field: {1}}, map[FieldID][]string{payloadField: {strings.Repeat("x", 6000)}})
	defer left.Release()
	defer right.Release()

	sourceBuffer := arrowValueBufferAddress(t, left.Column(field))
	forwardedSuffix := false
	rw := &MockRecordWriter{writefn: func(out Record) error {
		if out.Len() == len(leftValues)-1 && arrowValueBufferAddress(t, out.Column(field)) == sourceBuffer {
			values := out.Column(field).(*array.Int64)
			require.Equal(t, int64(2), values.Value(0))
			forwardedSuffix = true
		}
		return nil
	}, closefn: func() error { return nil }}
	n, err := MergeSort(5000, schema,
		[]RecordReader{&oneShotRecordReader{rec: left}, &oneShotRecordReader{rec: right}}, rw,
		func(Record, int, int) bool { return true }, []int64{field})
	require.NoError(t, err)
	require.Equal(t, 301, n)
	require.True(t, forwardedSuffix, "eligible record suffix must be forwarded without copying")
}

func TestMergeSortDirectForwardsMiddleSlice(t *testing.T) {
	const field = FieldID(100)
	const payloadField = FieldID(101)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: field, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: payloadField, Name: "payload", DataType: schemapb.DataType_VarChar},
	}}
	leftValues := make([]int64, 400)
	leftPayloads := make([]string, len(leftValues))
	leftValues[0] = 0
	for i := 1; i < len(leftValues); i++ {
		leftValues[i] = int64(i + 1)
	}
	left := writerCompatibleMergeSortTestRec(t, schema, map[FieldID][]int64{field: leftValues}, map[FieldID][]string{payloadField: leftPayloads})
	right := writerCompatibleMergeSortTestRec(t, schema, map[FieldID][]int64{field: {1, 300}}, map[FieldID][]string{payloadField: {strings.Repeat("x", 6000), ""}})
	defer left.Release()
	defer right.Release()

	sourceBuffer := arrowValueBufferAddress(t, left.Column(field))
	forwardedMiddle := false
	rw := &MockRecordWriter{writefn: func(out Record) error {
		if out.Len() >= directForwardMinRows && out.Len() < len(leftValues)-1 &&
			arrowValueBufferAddress(t, out.Column(field)) == sourceBuffer {
			values := out.Column(field).(*array.Int64)
			require.Equal(t, int64(2), values.Value(0))
			require.LessOrEqual(t, values.Value(out.Len()-1), int64(300))
			forwardedMiddle = true
		}
		return nil
	}, closefn: func() error { return nil }}
	n, err := MergeSort(5000, schema,
		[]RecordReader{&oneShotRecordReader{rec: left}, &oneShotRecordReader{rec: right}}, rw,
		func(Record, int, int) bool { return true }, []int64{field})
	require.NoError(t, err)
	require.Equal(t, 402, n)
	require.True(t, forwardedMiddle, "eligible record middle must be forwarded without copying")
}

func TestMergeSortDirectForwardFallsBackForSmallInterval(t *testing.T) {
	const field = FieldID(100)
	values := make([]int64, directForwardMinRows-1)
	for i := range values {
		values[i] = int64(i)
	}
	rec := mergeSortTestRec(t, map[FieldID][]int64{field: values}, nil)
	defer rec.Release()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: field, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
	sourceBuffer := arrowValueBufferAddress(t, rec.Column(field))
	rw := &MockRecordWriter{writefn: func(out Record) error {
		require.NotEqual(t, sourceBuffer, arrowValueBufferAddress(t, out.Column(field)))
		return nil
	}, closefn: func() error { return nil }}
	n, err := MergeSort(64*1024*1024, schema, []RecordReader{&oneShotRecordReader{rec: rec}}, rw,
		func(Record, int, int) bool { return true }, []int64{field})
	require.NoError(t, err)
	require.Equal(t, len(values), n)
}

func TestMergeSortDirectForwardKeepsPendingRebuiltBatchCombined(t *testing.T) {
	const field = FieldID(100)
	leftValues := make([]int64, 300)
	leftValues[0] = 0
	for i := 1; i < len(leftValues); i++ {
		leftValues[i] = int64(i + 1)
	}
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: field, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
	left := writerCompatibleMergeSortTestRec(t, schema, map[FieldID][]int64{field: leftValues}, nil)
	right := writerCompatibleMergeSortTestRec(t, schema, map[FieldID][]int64{field: {1}}, nil)
	defer left.Release()
	defer right.Release()
	sourceBuffer := arrowValueBufferAddress(t, left.Column(field))
	writes := 0
	rw := &MockRecordWriter{writefn: func(out Record) error {
		writes++
		require.Equal(t, len(leftValues)+1, out.Len())
		require.NotEqual(t, sourceBuffer, arrowValueBufferAddress(t, out.Column(field)),
			"a forwarded suffix must not split an existing rebuilt batch")
		return nil
	}, closefn: func() error { return nil }}
	n, err := MergeSort(64*1024*1024, schema,
		[]RecordReader{&oneShotRecordReader{rec: left}, &oneShotRecordReader{rec: right}}, rw,
		func(Record, int, int) bool { return true }, []int64{field})
	require.NoError(t, err)
	require.Equal(t, len(leftValues)+1, n)
	require.Equal(t, 1, writes)
}

func TestMergeSortDirectForwardFallsBackWhenLaterRowIsFiltered(t *testing.T) {
	const rows = 256
	const field = FieldID(100)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: field, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(i)
	}
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	builder.Release()
	rec := NewSimpleArrowRecord(array.NewRecord(
		writerArrowSchema(schema),
		[]arrow.Array{arr}, rows), map[FieldID]int{field: 0})
	arr.Release()
	defer rec.Release()

	emittedRows := 0
	sourceBuffer := arrowValueBufferAddress(t, rec.Column(field))
	var outputBuffers []uintptr
	rw := &MockRecordWriter{writefn: func(out Record) error {
		outputBuffers = append(outputBuffers, arrowValueBufferAddress(t, out.Column(field)))
		values := out.Column(field).(*array.Int64)
		for i := 0; i < out.Len(); i++ {
			require.NotEqual(t, int64(128), values.Value(i))
		}
		emittedRows += out.Len()
		return nil
	}, closefn: func() error { return nil }}
	n, err := MergeSort(64*1024*1024, schema, []RecordReader{&oneShotRecordReader{rec: rec}}, rw,
		func(r Record, _, i int) bool { return i != 128 }, []int64{field})
	require.NoError(t, err)
	require.Equal(t, rows-1, n)
	require.Equal(t, rows-1, emittedRows)
	for _, outputBuffer := range outputBuffers {
		require.NotEqual(t, sourceBuffer, outputBuffer, "a filtered source record must be rebuilt")
	}
}

func TestMergeSortFilteredRecordDoesNotLeakDiscardedForwardSlice(t *testing.T) {
	const rows = 300
	const field = FieldID(100)
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewInt64Builder(alloc)
	for i := range rows {
		builder.Append(int64(i))
	}
	arr := builder.NewArray()
	builder.Release()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: field, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
	rec := NewSimpleArrowRecord(array.NewRecord(
		writerArrowSchema(schema), []arrow.Array{arr}, rows), map[FieldID]int{field: 0})
	arr.Release()
	rw := &MockRecordWriter{writefn: func(Record) error { return nil }, closefn: func() error { return nil }}
	n, err := MergeSort(64*1024*1024, schema, []RecordReader{&oneShotRecordReader{rec: rec}}, rw,
		func(_ Record, _, i int) bool { return i != 0 }, []int64{field})
	require.NoError(t, err)
	require.Equal(t, rows-1, n)
	rec.Release()
	alloc.AssertSize(t, 0)
}

func TestMergeSortDirectForwardFallsBackForNullableDefault(t *testing.T) {
	const rows = 256
	const keyField = FieldID(100)
	const defaultField = FieldID(101)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: keyField, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{
			FieldID: defaultField, Name: "with_default", DataType: schemapb.DataType_Int64, Nullable: true,
			DefaultValue: &schemapb.ValueField{Data: &schemapb.ValueField_LongData{LongData: 42}},
		},
	}}
	keyBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	defaultBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	for i := range rows {
		keyBuilder.Append(int64(i))
		defaultBuilder.AppendNull()
	}
	keyArray, defaultArray := keyBuilder.NewArray(), defaultBuilder.NewArray()
	keyBuilder.Release()
	defaultBuilder.Release()
	rec := NewSimpleArrowRecord(array.NewRecord(
		writerArrowSchema(schema),
		[]arrow.Array{keyArray, defaultArray}, rows), map[FieldID]int{keyField: 0, defaultField: 1})
	keyArray.Release()
	defaultArray.Release()
	defer rec.Release()

	sourceBuffer := arrowValueBufferAddress(t, rec.Column(keyField))
	var outputBuffer uintptr
	rw := &MockRecordWriter{writefn: func(out Record) error {
		outputBuffer = arrowValueBufferAddress(t, out.Column(keyField))
		values := out.Column(defaultField).(*array.Int64)
		for i := range out.Len() {
			require.True(t, values.IsValid(i))
			require.Equal(t, int64(42), values.Value(i))
		}
		return nil
	}, closefn: func() error { return nil }}
	n, err := MergeSort(64*1024*1024, schema, []RecordReader{&oneShotRecordReader{rec: rec}}, rw,
		func(Record, int, int) bool { return true }, []int64{keyField})
	require.NoError(t, err)
	require.Equal(t, rows, n)
	require.NotEqual(t, sourceBuffer, outputBuffer, "default replacement requires rebuilding")
}

func TestMergeSortDirectForwardKeepsBorrowedBuffersAliveDuringWrite(t *testing.T) {
	const rows = 256
	const field = FieldID(100)
	alloc := memory.NewCheckedAllocator(memory.NewGoAllocator())
	builder := array.NewInt64Builder(alloc)
	for i := range rows {
		builder.Append(int64(i))
	}
	arr := builder.NewArray()
	builder.Release()
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: field, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
	rec := NewSimpleArrowRecord(array.NewRecord(
		writerArrowSchema(schema), []arrow.Array{arr}, rows), map[FieldID]int{field: 0})
	arr.Release()
	reader := &oneShotRecordReader{rec: rec}
	rw := &MockRecordWriter{writefn: func(out Record) error {
		// The source owner may release its record as soon as the synchronous
		// writer call begins. The forwarded slice must retain the shared buffer.
		rec.Release()
		rec = nil
		values := out.Column(field).(*array.Int64)
		require.Equal(t, int64(rows-1), values.Value(rows-1))
		require.Greater(t, alloc.CurrentAlloc(), 0)
		return nil
	}, closefn: func() error { return nil }}
	n, err := MergeSort(64*1024*1024, schema, []RecordReader{reader}, rw,
		func(Record, int, int) bool { return true }, []int64{field})
	require.NoError(t, err)
	require.Equal(t, rows, n)
	require.Nil(t, rec)
	alloc.AssertSize(t, 0)
}

func TestMergeSortDirectForwardPreservesEqualKeyReaderOrder(t *testing.T) {
	const rows = 256
	const keyField = FieldID(100)
	const sourceField = FieldID(101)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: keyField, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: sourceField, Name: "source", DataType: schemapb.DataType_Int64},
	}}
	build := func(key, source int64) Record {
		arrowSchema := writerArrowSchema(schema)
		keyBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		sourceBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		for range rows {
			keyBuilder.Append(key)
			sourceBuilder.Append(source)
		}
		keyArray, sourceArray := keyBuilder.NewArray(), sourceBuilder.NewArray()
		keyBuilder.Release()
		sourceBuilder.Release()
		rec := NewSimpleArrowRecord(array.NewRecord(arrowSchema, []arrow.Array{keyArray, sourceArray}, rows), map[FieldID]int{keyField: 0, sourceField: 1})
		keyArray.Release()
		sourceArray.Release()
		return rec
	}
	r0, r1 := build(7, 0), build(7, 1)
	defer r0.Release()
	defer r1.Release()
	var sources []int64
	rw := &MockRecordWriter{writefn: func(out Record) error {
		col := out.Column(sourceField).(*array.Int64)
		for i := 0; i < out.Len(); i++ {
			sources = append(sources, col.Value(i))
		}
		return nil
	}, closefn: func() error { return nil }}
	n, err := MergeSort(64*1024*1024, schema, []RecordReader{&oneShotRecordReader{rec: r0}, &oneShotRecordReader{rec: r1}}, rw,
		func(Record, int, int) bool { return true }, []int64{keyField})
	require.NoError(t, err)
	require.Equal(t, rows*2, n)
	require.Equal(t, slices.Repeat([]int64{0}, rows), sources[:rows])
	require.Equal(t, slices.Repeat([]int64{1}, rows), sources[rows:])
}

func TestMergeSortDirectForwardPreservesEqualKeyOrderAcrossRecords(t *testing.T) {
	const rows = 128
	const keyField = FieldID(100)
	const sourceField = FieldID(101)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: keyField, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
		{FieldID: sourceField, Name: "source", DataType: schemapb.DataType_Int64},
	}}
	build := func(key, source int64) Record {
		arrowSchema := writerArrowSchema(schema)
		keyBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		sourceBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		for range rows {
			keyBuilder.Append(key)
			sourceBuilder.Append(source)
		}
		keyArray, sourceArray := keyBuilder.NewArray(), sourceBuilder.NewArray()
		keyBuilder.Release()
		sourceBuilder.Release()
		rec := NewSimpleArrowRecord(array.NewRecord(arrowSchema, []arrow.Array{keyArray, sourceArray}, rows), map[FieldID]int{keyField: 0, sourceField: 1})
		keyArray.Release()
		sourceArray.Release()
		return rec
	}
	r00, r01, r1 := build(7, 0), build(7, 1), build(7, 2)
	defer r00.Release()
	defer r01.Release()
	defer r1.Release()
	var sources []int64
	rw := &MockRecordWriter{writefn: func(out Record) error {
		col := out.Column(sourceField).(*array.Int64)
		for i := range out.Len() {
			sources = append(sources, col.Value(i))
		}
		return nil
	}, closefn: func() error { return nil }}
	n, err := MergeSort(64*1024*1024, schema,
		[]RecordReader{&sliceRecordReader{recs: []Record{r00, r01}}, &oneShotRecordReader{rec: r1}}, rw,
		func(Record, int, int) bool { return true }, []int64{keyField})
	require.NoError(t, err)
	require.Equal(t, rows*3, n)
	require.Equal(t, slices.Repeat([]int64{0}, rows), sources[:rows])
	require.Equal(t, slices.Repeat([]int64{1}, rows), sources[rows:rows*2])
	require.Equal(t, slices.Repeat([]int64{2}, rows), sources[rows*2:])
}

func TestMergeSortDirectForwardRebuildsIncompatibleRecord(t *testing.T) {
	const rows = 256
	const field = FieldID(100)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: field, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
	values := make([]int64, rows)
	for i := range values {
		values[i] = int64(i)
	}
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	builder.AppendValues(values, nil)
	arr := builder.NewArray()
	builder.Release()
	rec := NewSimpleArrowRecord(array.NewRecord(
		arrow.NewSchema([]arrow.Field{{Name: "extra", Type: arrow.PrimitiveTypes.Int64}, {Name: "pk", Type: arrow.PrimitiveTypes.Int64}}, nil),
		[]arrow.Array{arr, arr}, rows), map[FieldID]int{field: 1})
	arr.Release()
	defer rec.Release()
	sourceBuffer := arrowValueBufferAddress(t, rec.Column(field))
	writes := 0
	rw := &MockRecordWriter{writefn: func(out Record) error {
		writes++
		require.Equal(t, 1, out.(*simpleArrowRecord).r.Schema().NumFields())
		require.NotEqual(t, sourceBuffer, arrowValueBufferAddress(t, out.Column(field)))
		return nil
	}, closefn: func() error { return nil }}
	_, err := MergeSort(64*1024*1024, schema, []RecordReader{&oneShotRecordReader{rec: rec}}, rw,
		func(Record, int, int) bool { return true }, []int64{field})
	require.NoError(t, err)
	require.Equal(t, 1, writes)
}

func TestMergeSortDirectForwardRequiresExactWriterSchema(t *testing.T) {
	const (
		rows  = 256
		field = FieldID(100)
	)
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{{
		FieldID: field, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true,
	}}}
	wantSchema := writerArrowSchema(schema)
	wantField := wantSchema.Field(0)
	cases := map[string]*arrow.Schema{}
	nameMismatch := wantField
	nameMismatch.Name = "other"
	cases["field_name"] = arrow.NewSchema([]arrow.Field{nameMismatch}, nil)
	nullableMismatch := wantField
	nullableMismatch.Nullable = !nullableMismatch.Nullable
	cases["nullability"] = arrow.NewSchema([]arrow.Field{nullableMismatch}, nil)
	metadataMismatch := wantField
	metadataMismatch.Metadata = arrow.NewMetadata([]string{"PARQUET:field_id"}, []string{"999"})
	cases["field_metadata"] = arrow.NewSchema([]arrow.Field{metadataMismatch}, nil)
	withSchemaMetadata := arrow.NewSchema([]arrow.Field{wantField}, func() *arrow.Metadata {
		metadata := arrow.NewMetadata([]string{"source"}, []string{"incompatible"})
		return &metadata
	}())
	cases["schema_metadata"] = withSchemaMetadata

	for name, sourceSchema := range cases {
		t.Run(name, func(t *testing.T) {
			builder := array.NewInt64Builder(memory.DefaultAllocator)
			for row := range rows {
				builder.Append(int64(row))
			}
			values := builder.NewArray()
			builder.Release()
			record := NewSimpleArrowRecord(array.NewRecord(sourceSchema, []arrow.Array{values}, rows), map[FieldID]int{field: 0})
			values.Release()
			defer record.Release()
			sourceBuffer := arrowValueBufferAddress(t, record.Column(field))
			writer := &MockRecordWriter{writefn: func(output Record) error {
				require.NotEqual(t, sourceBuffer, arrowValueBufferAddress(t, output.Column(field)))
				require.True(t, output.(*simpleArrowRecord).r.Schema().Equal(wantSchema))
				return nil
			}, closefn: func() error { return nil }}
			_, err := MergeSort(64*1024*1024, schema, []RecordReader{&oneShotRecordReader{rec: record}}, writer,
				func(Record, int, int) bool { return true }, []int64{field})
			require.NoError(t, err)
		})
	}
}

func TestRowHeap(t *testing.T) {
	h := &rowHeap{less: func(x, y rowIndex) bool {
		if x.ri != y.ri {
			return x.ri < y.ri
		}
		return x.i < y.i
	}}
	assert.Equal(t, 0, h.len())

	in := []rowIndex{{3, 1}, {1, 2}, {2, 0}, {1, 0}, {3, 0}, {2, 1}}
	for _, v := range in {
		h.push(v)
	}
	assert.Equal(t, len(in), h.len())

	var got []rowIndex
	for h.len() > 0 {
		got = append(got, h.pop())
	}
	assert.Equal(t, []rowIndex{{1, 0}, {1, 2}, {2, 0}, {2, 1}, {3, 0}, {3, 1}}, got)
}

func TestRowHeapSingleElement(t *testing.T) {
	h := &rowHeap{less: func(x, y rowIndex) bool { return x.i < y.i }}
	h.push(rowIndex{0, 7})
	assert.Equal(t, 1, h.len())
	assert.Equal(t, rowIndex{0, 7}, h.pop())
	assert.Equal(t, 0, h.len())
}

func TestRowHeapPushAfterRoot(t *testing.T) {
	h := &rowHeap{less: func(x, y rowIndex) bool { return x.i < y.i }}
	for _, value := range []int32{0, 100, 200, 300, 400, 500, 600} {
		h.push(rowIndex{i: value})
	}

	// 1 is known to belong after root 0, but must still bubble through the
	// lower heap levels from its initial insertion position.
	h.pushAfterRoot(rowIndex{i: 1})

	got := make([]int32, 0, h.len())
	for h.len() > 0 {
		got = append(got, h.pop().i)
	}
	assert.Equal(t, []int32{0, 1, 100, 200, 300, 400, 500, 600}, got)
}

// A k-way merge relies on each input record being sorted by the merge key.
// When that does not hold, fail explicitly rather than emitting rows out of
// order. This is the shape reported in #48322: the last row of a record carries
// the smallest key, and the next record is shorter.
func TestMergeSortUnsortedInputReturnsError(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "rowid", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}

	r0 := &sliceRecordReader{recs: []Record{
		mergeSortTestRec(t, map[FieldID][]int64{common.RowIDField: {50, 60, 1}}, nil),
		mergeSortTestRec(t, map[FieldID][]int64{common.RowIDField: {70}}, nil),
	}}

	rw := &MockRecordWriter{
		writefn: func(r Record) error { return nil },
		closefn: func() error { return nil },
	}

	_, err := MergeSort(1024, schema, []RecordReader{r0}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{common.RowIDField})
	assert.ErrorContains(t, err, "not sorted by the merge key")
	assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	assert.ErrorContains(t, err, "reader 0 record 0 row 2 out of order")
}

// The disorder in TestMergeSortUnsortedInputReturnsError falls in the reader's
// first record, so a reported record number of 0 does not prove that number
// was actually computed rather than hardcoded. This variant keeps the first
// record in order and puts the offending row in the second record, so the
// reported record number must be non-zero to be correct.
func TestMergeSortUnsortedInputReportsLaterRecord(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "rowid", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}

	r0 := &sliceRecordReader{recs: []Record{
		mergeSortTestRec(t, map[FieldID][]int64{common.RowIDField: {10, 20, 30}}, nil),
		mergeSortTestRec(t, map[FieldID][]int64{common.RowIDField: {5, 40}}, nil),
	}}

	rw := &MockRecordWriter{
		writefn: func(r Record) error { return nil },
		closefn: func() error { return nil },
	}

	_, err := MergeSort(1024, schema, []RecordReader{r0}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{common.RowIDField})
	assert.ErrorContains(t, err, "not sorted by the merge key")
	assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	assert.ErrorContains(t, err, "reader 0 record 1 row 0 out of order")
}

// Both preceding tests drive a single reader, so idx.ri is always 0 in the
// error -- a hardcoded 0 in place of idx.ri would pass them too. This variant
// uses two readers, keeps reader 0 in order throughout, and puts the disorder
// in reader 1's second record, so the reported reader index and per-reader
// record number are both load-bearing.
func TestMergeSortUnsortedInputReportsOffendingReader(t *testing.T) {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.RowIDField, Name: "rowid", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
	}}

	r0 := &sliceRecordReader{recs: []Record{
		mergeSortTestRec(t, map[FieldID][]int64{common.RowIDField: {10, 20, 30}}, nil),
		mergeSortTestRec(t, map[FieldID][]int64{common.RowIDField: {40}}, nil),
	}}
	r1 := &sliceRecordReader{recs: []Record{
		mergeSortTestRec(t, map[FieldID][]int64{common.RowIDField: {15, 25}}, nil),
		mergeSortTestRec(t, map[FieldID][]int64{common.RowIDField: {35, 5}}, nil),
	}}

	rw := &MockRecordWriter{
		writefn: func(r Record) error { return nil },
		closefn: func() error { return nil },
	}

	_, err := MergeSort(1024, schema, []RecordReader{r0, r1}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{common.RowIDField})
	assert.ErrorContains(t, err, "not sorted by the merge key")
	assert.ErrorIs(t, err, merr.ErrDataIntegrity)
	assert.ErrorContains(t, err, "reader 1 record 1 row 1 out of order")
}
