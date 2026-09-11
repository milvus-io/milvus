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
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
)

type mockRecordReader struct {
	mock.Mock
}

func (m *mockRecordReader) Next() (Record, error) {
	args := m.Called()
	if rec := args.Get(0); rec != nil {
		return rec.(Record), args.Error(1)
	}
	return nil, args.Error(1)
}

func (m *mockRecordReader) Close() error {
	args := m.Called()
	return args.Error(0)
}

type mockRecordWriter struct {
	mock.Mock
}

func (m *mockRecordWriter) GetWrittenUncompressed() uint64 {
	panic("implement me")
}

func (m *mockRecordWriter) Close() error {
	panic("implement me")
}

func (m *mockRecordWriter) Write(rec Record) error {
	args := m.Called(rec)
	return args.Error(0)
}

type mockRecord struct {
	mock.Mock
	*simpleArrowRecord // embed to satisfy interface if needed
}

func (m *mockRecord) Column(fieldID int64) arrow.Array {
	args := m.Called(fieldID)
	return args.Get(0).(arrow.Array)
}

func (m *mockRecord) Len() int {
	args := m.Called()
	return args.Int(0)
}

func (m *mockRecord) Retain() {
	m.Called()
}

func (m *mockRecord) Release() {
	m.Called()
}

func testSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Int64},
		},
	}
}

func TestSort_ErrorCases(t *testing.T) {
	paramtable.Get().Init(paramtable.NewBaseTable())
	origPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	defer func() {
		paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, origPath)
	}()
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, t.TempDir())

	schema := testSchema()
	batchSize := uint64(100)
	sortBy := []int64{100}

	predicate := func(r Record, ri, i int) bool { return true }

	t.Run("MkdirTemp fails", func(t *testing.T) {
		badPath := "/nonexistent/invalid/path/that/should/fail"
		paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, badPath)

		rr := []RecordReader{&mockRecordReader{}}
		rw := &mockRecordWriter{}

		_, _, err := Sort(batchSize, schema, rr, rw, predicate, sortBy)
		assert.Error(t, err)
		assert.Contains(t, err.Error(), "failed to create temp dir")
	})

	t.Run("flushRun - unsupported sort type", func(t *testing.T) {
		tmpRoot := t.TempDir()
		paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, tmpRoot)
		builder := array.NewBooleanBuilder(memory.DefaultAllocator)
		builder.AppendValues([]bool{true, false, true}, nil)
		arr := builder.NewBooleanArray()

		mockRec := &mockRecord{}
		mockRec.On("Retain").Return()
		mockRec.On("Len").Return(1)
		// Return a type not handled (e.g. Boolean)
		mockRec.On("Column", mock.Anything).Return(arr)
		mockRec.On("Release").Return()

		mrr := &mockRecordReader{}
		mrr.On("Next").Return(mockRec, nil).Once()
		mrr.On("Next").Return(nil, io.EOF).Once()

		rr := []RecordReader{mrr}
		rw := &mockRecordWriter{}

		_, _, err := Sort(batchSize, schema, rr, rw, predicate, sortBy)
		assert.Error(t, err)
	})

	t.Run("Next() returns non-EOF error", func(t *testing.T) {
		tmpRoot := t.TempDir()
		paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, tmpRoot)

		expectedErr := errors.New("reader error")
		mrr := &mockRecordReader{}
		mrr.On("Next").Return(nil, expectedErr)

		rr := []RecordReader{mrr}
		rw := &mockRecordWriter{}

		_, _, err := Sort(batchSize, schema, rr, rw, predicate, sortBy)
		assert.Error(t, err)
		assert.Equal(t, expectedErr, err)
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

	t.Run("sort with disk merge", func(t *testing.T) {
		oldRowLimit := runRowLimit
		defer func() {
			runRowLimit = oldRowLimit
		}()
		runRowLimit = 3
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

	t.Run("sort by string with disk merge", func(t *testing.T) {
		oldRowLimit := runRowLimit
		defer func() {
			runRowLimit = oldRowLimit
		}()
		runRowLimit = 3
		gotNumRows, timings, err := Sort(batchSize, generateTestSchema(), getReaders(), rw, func(r Record, ri, i int) bool {
			return true
		}, []int64{17})
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

func TestSort_AllRowsFiltered(t *testing.T) {
	// All records are read (totalRecords > 0), but predicate rejects every row.
	// This exercises the path where runRows stays 0 after reading, then the
	// totalRecords == 0 check is NOT hit (records were read) but no tmp files
	// are produced, so we go through MergeSort with no readers.
	origPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	defer func() {
		paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, origPath)
	}()
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, t.TempDir())

	const batchSize = 64 * 1024 * 1024

	blobs, err := generateTestDataWithSeed(10, 3)
	assert.NoError(t, err)
	reader := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))

	rw := &MockRecordWriter{
		writefn: func(r Record) error { return nil },
		closefn: func() error { return nil },
	}

	gotNumRows, timings, err := Sort(batchSize, generateTestSchema(), []RecordReader{reader}, rw, func(r Record, ri, i int) bool {
		return false // reject all rows
	}, []int64{common.RowIDField})
	assert.NoError(t, err)
	assert.Equal(t, 0, gotNumRows)
	assert.NotNil(t, timings)
	assert.GreaterOrEqual(t, timings.ReadCost.Nanoseconds(), int64(0))
}

func TestSort_NoSortFields(t *testing.T) {
	// When sortByFieldIDs is empty, no comparators are created, so the rows
	// are written in insertion order (no sorting).
	origPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	defer func() {
		paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, origPath)
	}()
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, t.TempDir())

	const batchSize = 64 * 1024 * 1024

	blobs, err := generateTestDataWithSeed(10, 3)
	assert.NoError(t, err)
	reader := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))

	var writtenRows int
	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			writtenRows += r.Len()
			return nil
		},
		closefn: func() error { return nil },
	}

	gotNumRows, timings, err := Sort(batchSize, generateTestSchema(), []RecordReader{reader}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{}) // empty sort fields
	assert.NoError(t, err)
	assert.Equal(t, 3, gotNumRows)
	assert.NotNil(t, timings)
	assert.Equal(t, 3, timings.NumRows)
	assert.Equal(t, 3, writtenRows)
}

func TestSort_SmallBatchSizeInFlushRun(t *testing.T) {
	// Use a very small batchSize (1 byte) so that flushRun hits the inner
	// "flush to file if batchSize reached" path on every row, exercising
	// the mid-flush batch write within flushRun.
	origPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	defer func() {
		paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, origPath)
	}()
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, t.TempDir())

	const batchSize = 1 // very small to trigger mid-flush writes

	blobs, err := generateTestDataWithSeed(10, 5)
	assert.NoError(t, err)
	reader := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))

	var writtenRows int
	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			writtenRows += r.Len()
			return nil
		},
		closefn: func() error { return nil },
	}

	gotNumRows, timings, err := Sort(batchSize, generateTestSchema(), []RecordReader{reader}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{common.RowIDField})
	assert.NoError(t, err)
	assert.Equal(t, 5, gotNumRows)
	assert.NotNil(t, timings)
	assert.Equal(t, 5, timings.NumRows)
	assert.Equal(t, 5, writtenRows)
}

func TestSort_PartialPredicateWithFlush(t *testing.T) {
	// Predicate accepts only some rows. Also use a small runRowLimit to force
	// the flush path with partial acceptance.
	origPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	defer func() {
		paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, origPath)
	}()
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, t.TempDir())

	const batchSize = 64 * 1024 * 1024
	oldRowLimit := runRowLimit
	defer func() { runRowLimit = oldRowLimit }()
	runRowLimit = 2 // force flush after 2 accepted rows

	blobs, err := generateTestDataWithSeed(10, 5)
	assert.NoError(t, err)
	reader := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))

	var writtenRows int
	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			writtenRows += r.Len()
			return nil
		},
		closefn: func() error { return nil },
	}

	// Accept only even-indexed PKs (10, 12, 14) from seed=10 num=5 → PKs are 10,11,12,13,14
	gotNumRows, timings, err := Sort(batchSize, generateTestSchema(), []RecordReader{reader}, rw, func(r Record, ri, i int) bool {
		pk := r.Column(common.RowIDField).(*array.Int64).Value(i)
		return pk%2 == 0
	}, []int64{common.RowIDField})
	assert.NoError(t, err)
	assert.Equal(t, 3, gotNumRows) // 10, 12, 14
	assert.NotNil(t, timings)
	assert.Equal(t, 3, timings.NumRows)
	assert.Equal(t, 3, writtenRows)
}

func TestMergeSort_NoReaders(t *testing.T) {
	rw := &MockRecordWriter{
		writefn: func(r Record) error { return nil },
		closefn: func() error { return nil },
	}
	numRows, err := MergeSort(1024, generateTestSchema(), []RecordReader{}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{common.RowIDField})
	assert.NoError(t, err)
	assert.Equal(t, 0, numRows)
}

func TestMergeSort_AllReadersEOF(t *testing.T) {
	// All readers return EOF immediately on the first call to Next().
	// With non-empty sortByFieldIDs, recs[0] is nil → panics trying to
	// determine comparator type. Test with empty sort fields to exercise
	// the "all-EOF, no enqueue" path gracefully.
	mrr1 := &mockRecordReader{}
	mrr1.On("Next").Return(nil, io.EOF)
	mrr1.On("Close").Return(nil)

	mrr2 := &mockRecordReader{}
	mrr2.On("Next").Return(nil, io.EOF)
	mrr2.On("Close").Return(nil)

	rw := &MockRecordWriter{
		writefn: func(r Record) error { return nil },
		closefn: func() error { return nil },
	}

	numRows, err := MergeSort(1024, generateTestSchema(), []RecordReader{mrr1, mrr2}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{}) // empty sort fields avoids nil-deref on recs[0]
	assert.NoError(t, err)
	assert.Equal(t, 0, numRows)
}

func TestMergeSort_AllReadersEOF_WithSortField_Panics(t *testing.T) {
	// Demonstrates that if all readers return EOF and sortByFieldIDs is non-empty,
	// the code panics because recs[0] is nil. This documents the current behavior.
	mrr1 := &mockRecordReader{}
	mrr1.On("Next").Return(nil, io.EOF)
	mrr1.On("Close").Return(nil)

	rw := &MockRecordWriter{
		writefn: func(r Record) error { return nil },
		closefn: func() error { return nil },
	}

	assert.Panics(t, func() {
		MergeSort(1024, generateTestSchema(), []RecordReader{mrr1}, rw, func(r Record, ri, i int) bool {
			return true
		}, []int64{common.RowIDField})
	})
}

func TestMergeSort_InitialAdvanceRecordError(t *testing.T) {
	// First reader succeeds, second reader returns a non-EOF error.
	blobs, err := generateTestDataWithSeed(10, 3)
	assert.NoError(t, err)
	goodReader := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))

	expectedErr := errors.New("reader connection error")
	badReader := &mockRecordReader{}
	badReader.On("Next").Return(nil, expectedErr)
	badReader.On("Close").Return(nil)

	rw := &MockRecordWriter{
		writefn: func(r Record) error { return nil },
		closefn: func() error { return nil },
	}

	numRows, err := MergeSort(1024, generateTestSchema(), []RecordReader{goodReader, badReader}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{common.RowIDField})
	assert.Error(t, err)
	assert.Equal(t, expectedErr, err)
	assert.Equal(t, 0, numRows)
}

func TestMergeSort_UnsupportedSortType(t *testing.T) {
	// Create a reader that returns a record with a boolean column as the sort key.
	alloc := memory.DefaultAllocator
	boolBuilder := array.NewBooleanBuilder(alloc)
	boolBuilder.AppendValues([]bool{true, false}, nil)
	boolArr := boolBuilder.NewBooleanArray()
	defer boolArr.Release()

	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "100", Type: arrow.FixedWidthTypes.Boolean},
	}, nil)
	arrowRec := array.NewRecord(arrowSchema, []arrow.Array{boolArr}, 2)
	defer arrowRec.Release()

	field2Col := map[FieldID]int{100: 0}
	rec := NewSimpleArrowRecord(arrowRec, field2Col)

	mrr := &mockRecordReader{}
	mrr.On("Next").Return(rec, nil).Once()
	mrr.On("Next").Return(nil, io.EOF)
	mrr.On("Close").Return(nil)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, DataType: schemapb.DataType_Bool},
		},
	}

	rw := &MockRecordWriter{
		writefn: func(r Record) error { return nil },
		closefn: func() error { return nil },
	}

	numRows, err := MergeSort(1024, schema, []RecordReader{mrr}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{100})
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "unsupported type for sorting key")
	assert.Equal(t, 0, numRows)
}

func TestMergeSort_StringSort(t *testing.T) {
	// Sort by a string field (field 17 in generateTestSchema is a string).
	blobs, err := generateTestDataWithSeed(10, 5)
	assert.NoError(t, err)
	reader1 := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))

	blobs, err = generateTestDataWithSeed(20, 5)
	assert.NoError(t, err)
	reader2 := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))

	var writtenRows int
	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			writtenRows += r.Len()
			return nil
		},
		closefn: func() error { return nil },
	}

	numRows, err := MergeSort(64*1024*1024, generateTestSchema(), []RecordReader{reader1, reader2}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{17}) // field 17 is string type
	assert.NoError(t, err)
	assert.Equal(t, 10, numRows)
	assert.Equal(t, 10, writtenRows)
}

func TestMergeSort_WriteError(t *testing.T) {
	// MergeSort where rw.Write fails.
	blobs, err := generateTestDataWithSeed(10, 3)
	assert.NoError(t, err)
	reader := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))

	writeErr := errors.New("disk full")
	rw := &MockRecordWriter{
		writefn: func(r Record) error { return writeErr },
		closefn: func() error { return nil },
	}

	numRows, err := MergeSort(64*1024*1024, generateTestSchema(), []RecordReader{reader}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{common.RowIDField})
	assert.Error(t, err)
	assert.Equal(t, writeErr, err)
	assert.Equal(t, 0, numRows)
}

func TestMergeSort_SmallBatchSizeWriteError(t *testing.T) {
	// MergeSort with small batchSize so the mid-loop write is triggered, then fails.
	blobs, err := generateTestDataWithSeed(10, 5)
	assert.NoError(t, err)
	reader := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))

	writeErr := errors.New("write failed")
	rw := &MockRecordWriter{
		writefn: func(r Record) error { return writeErr },
		closefn: func() error { return nil },
	}

	numRows, err := MergeSort(1, generateTestSchema(), []RecordReader{reader}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{common.RowIDField})
	assert.Error(t, err)
	assert.Equal(t, writeErr, err)
	assert.Equal(t, 0, numRows)
}

func TestPriorityQueue(t *testing.T) {
	t.Run("basic operations", func(t *testing.T) {
		pq := NewPriorityQueue(func(x, y *int) bool {
			return *x < *y
		})
		assert.Equal(t, 0, pq.Len())

		vals := []int{5, 1, 3, 2, 4}
		for i := range vals {
			pq.Enqueue(&vals[i])
		}
		assert.Equal(t, 5, pq.Len())

		// Dequeue should return in sorted order
		prev := -1
		for pq.Len() > 0 {
			v := pq.Dequeue()
			assert.Greater(t, *v, prev)
			prev = *v
		}
		assert.Equal(t, 0, pq.Len())
	})

	t.Run("single element", func(t *testing.T) {
		pq := NewPriorityQueue(func(x, y *int) bool {
			return *x < *y
		})
		v := 42
		pq.Enqueue(&v)
		assert.Equal(t, 1, pq.Len())
		result := pq.Dequeue()
		assert.Equal(t, 42, *result)
		assert.Equal(t, 0, pq.Len())
	})

	t.Run("string priority", func(t *testing.T) {
		pq := NewPriorityQueue(func(x, y *string) bool {
			return *x < *y
		})
		strs := []string{"banana", "apple", "cherry"}
		for i := range strs {
			pq.Enqueue(&strs[i])
		}
		first := pq.Dequeue()
		assert.Equal(t, "apple", *first)
		second := pq.Dequeue()
		assert.Equal(t, "banana", *second)
		third := pq.Dequeue()
		assert.Equal(t, "cherry", *third)
	})
}

func TestMergeSort_PredicateSkipsEntireRecord(t *testing.T) {
	// Exercise the enqueueAll recursive path: the first record batch has all
	// rows rejected by predicate, forcing advanceRecord to be called again
	// within enqueueAll.
	blobs, err := generateTestDataWithSeed(10, 3)
	assert.NoError(t, err)
	reader1 := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))

	blobs, err = generateTestDataWithSeed(20, 3)
	assert.NoError(t, err)
	reader2 := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))

	var writtenRows int
	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			writtenRows += r.Len()
			return nil
		},
		closefn: func() error { return nil },
	}

	// Accept only PKs >= 20 → the first reader (PKs 10-12) will be fully skipped
	numRows, err := MergeSort(64*1024*1024, generateTestSchema(), []RecordReader{reader1, reader2}, rw, func(r Record, ri, i int) bool {
		pk := r.Column(common.RowIDField).(*array.Int64).Value(i)
		return pk >= 20
	}, []int64{common.RowIDField})
	assert.NoError(t, err)
	assert.Equal(t, 3, numRows)
	assert.Equal(t, 3, writtenRows)
}

func TestSort_MultipleRunsMerge(t *testing.T) {
	// Force multiple runs by setting a very small runRowLimit, then verify
	// the final merge produces correct sorted output.
	origPath := paramtable.Get().LocalStorageCfg.Path.GetValue()
	defer func() {
		paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, origPath)
	}()
	paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, t.TempDir())

	const batchSize = 64 * 1024 * 1024
	oldRowLimit := runRowLimit
	defer func() { runRowLimit = oldRowLimit }()
	runRowLimit = 1 // force a flush after every single row

	blobs, err := generateTestDataWithSeed(100, 5)
	assert.NoError(t, err)
	reader := newIterativeCompositeBinlogRecordReader(generateTestSchema(), nil, MakeBlobsReader(blobs))

	lastPK := int64(-1)
	rw := &MockRecordWriter{
		writefn: func(r Record) error {
			for i := 0; i < r.Len(); i++ {
				pk := r.Column(common.RowIDField).(*array.Int64).Value(i)
				assert.Greater(t, pk, lastPK)
				lastPK = pk
			}
			return nil
		},
		closefn: func() error {
			lastPK = int64(-1)
			return nil
		},
	}

	gotNumRows, timings, err := Sort(batchSize, generateTestSchema(), []RecordReader{reader}, rw, func(r Record, ri, i int) bool {
		return true
	}, []int64{common.RowIDField})
	assert.NoError(t, err)
	assert.Equal(t, 5, gotNumRows)
	assert.NotNil(t, timings)
	assert.Equal(t, 5, timings.NumRows)
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
