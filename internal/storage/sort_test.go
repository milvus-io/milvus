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
	"sort"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
)

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

	const batchSize = 64 * 1024 * 1024

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

// buildInt64Array creates an arrow Int64 array from a slice.
func buildInt64Array(values []int64) *array.Int64 {
	builder := array.NewInt64Builder(memory.DefaultAllocator)
	defer builder.Release()
	builder.AppendValues(values, nil)
	return builder.NewInt64Array()
}

// makeRecord creates a Record with two Int64 fields (fieldA=100, fieldB=101).
func makeRecord(fieldA, fieldB []int64) Record {
	arrA := buildInt64Array(fieldA)
	arrB := buildInt64Array(fieldB)
	nRows := int64(len(fieldA))
	fields := []arrow.Field{
		{Name: "fieldA", Type: arrow.PrimitiveTypes.Int64},
		{Name: "fieldB", Type: arrow.PrimitiveTypes.Int64},
	}
	rec := array.NewRecord(arrow.NewSchema(fields, nil), []arrow.Array{arrA, arrB}, nRows)
	return NewSimpleArrowRecord(rec, map[FieldID]int{100: 0, 101: 1})
}

// sliceRecordReader is a RecordReader that returns pre-built records in order.
type sliceRecordReader struct {
	records []Record
	pos     int
}

func (r *sliceRecordReader) Next() (Record, error) {
	if r.pos >= len(r.records) {
		return nil, io.EOF
	}
	rec := r.records[r.pos]
	r.pos++
	return rec, nil
}

func (r *sliceRecordReader) Close() error { return nil }

// TestMergeSortBatchNotSortedBySortKey_NoPanic verifies that MergeSort does
// not panic when the last row of a batch (by index) is dequeued before other
// rows from the same batch. This is the root cause of issue #48322: the old
// endPositions mechanism assumed the row at the highest valid index would be
// the last dequeued; when sorting by a different key than the data order, this
// assumption is violated, causing premature advanceRecord and stale PQ entries
// that access a replaced (shorter) batch → out-of-range panic.
//
// This test constructs the exact triggering scenario:
//   - Batch 1 has 5 rows with fieldB=[50,10,40,20,1]. The last row (index 4)
//     has the SMALLEST sort key, so it's dequeued FIRST.
//   - Batch 2 has only 2 rows, shorter than batch 1.
//   - With the old endPositions code, dequeuing index 4 triggers advanceRecord,
//     replacing recs[0] with the 2-row batch. Stale entries at index 3 then
//     access batch 2 at index 3 → panic: index out of range [3] with length 2.
func TestMergeSortBatchNotSortedBySortKey_NoPanic(t *testing.T) {
	const fieldA FieldID = 100
	const fieldB FieldID = 101
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: fieldA, Name: "fieldA", DataType: schemapb.DataType_Int64},
			{FieldID: fieldB, Name: "fieldB", DataType: schemapb.DataType_Int64},
		},
	}

	// Reader 0: batch 1 has the last row (index 4) with the smallest fieldB.
	// Batch 2 is shorter (2 rows) — stale index 3 from batch 1 would be OOB.
	reader0 := &sliceRecordReader{records: []Record{
		makeRecord([]int64{1, 2, 3, 4, 5}, []int64{50, 10, 40, 20, 1}),
		makeRecord([]int64{6, 7}, []int64{60, 70}),
	}}

	// Reader 1: interleaves with reader 0 to exercise cross-reader comparisons.
	reader1 := &sliceRecordReader{records: []Record{
		makeRecord([]int64{10, 11, 12}, []int64{5, 25, 55}),
	}}

	var outputRows []int64
	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			col := r.Column(fieldB).(*array.Int64)
			for i := 0; i < col.Len(); i++ {
				outputRows = append(outputRows, col.Value(i))
			}
			return nil
		},
		closefn: func() error { return nil },
	}

	numRows, err := MergeSort(64*1024*1024, schema, []RecordReader{reader0, reader1}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{fieldB})

	assert.NoError(t, err)
	assert.Equal(t, 10, numRows) // 5 + 2 + 3 = 10 total rows

	// Verify all values are present (no data loss). Sort order may not be
	// globally correct since the input batches aren't sorted by the sort key,
	// but no rows should be missing or duplicated.
	sort.Slice(outputRows, func(i, j int) bool { return outputRows[i] < outputRows[j] })
	expected := []int64{1, 5, 10, 20, 25, 40, 50, 55, 60, 70}
	assert.Equal(t, expected, outputRows)
}

// TestMergeSortBatchNotSortedWithPredicate verifies the fix works when some
// rows are filtered out by the predicate, ensuring remainingCounts correctly
// tracks only valid (non-filtered) rows.
func TestMergeSortBatchNotSortedWithPredicate(t *testing.T) {
	const fieldA FieldID = 100
	const fieldB FieldID = 101
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: fieldA, Name: "fieldA", DataType: schemapb.DataType_Int64},
			{FieldID: fieldB, Name: "fieldB", DataType: schemapb.DataType_Int64},
		},
	}

	reader0 := &sliceRecordReader{records: []Record{
		makeRecord([]int64{1, 2, 3, 4, 5}, []int64{50, 10, 40, 20, 1}),
		makeRecord([]int64{6, 7}, []int64{60, 70}),
	}}

	reader1 := &sliceRecordReader{records: []Record{
		makeRecord([]int64{10, 11, 12}, []int64{5, 25, 55}),
	}}

	var outputRows []int64
	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			col := r.Column(fieldB).(*array.Int64)
			for i := 0; i < col.Len(); i++ {
				outputRows = append(outputRows, col.Value(i))
			}
			return nil
		},
		closefn: func() error { return nil },
	}

	// Predicate: exclude fieldB >= 50
	numRows, err := MergeSort(64*1024*1024, schema, []RecordReader{reader0, reader1}, rw, func(r Record, ri, i int) bool {
		val := r.Column(fieldB).(*array.Int64).Value(i)
		return val < 50
	}, []int64{fieldB})

	assert.NoError(t, err)
	// Filtered out: 50, 55, 60, 70. Remaining: 1,10,40,20 + 5,25 = 6
	assert.Equal(t, 6, numRows)

	sort.Slice(outputRows, func(i, j int) bool { return outputRows[i] < outputRows[j] })
	expected := []int64{1, 5, 10, 20, 25, 40}
	assert.Equal(t, expected, outputRows)
}
