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
	"fmt"
	"io"
	"math"
	"slices"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
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
