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
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/bytedance/mockey"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

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

func makeInt64Array(vals []int64) *array.Int64 {
	b := array.NewInt64Builder(memory.DefaultAllocator)
	defer b.Release()

	b.AppendValues(vals, nil)
	return b.NewInt64Array()
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

	t.Run("flushRun - CreateTemp fails", func(t *testing.T) {
		tmpRoot := t.TempDir()
		paramtable.Get().Save(paramtable.Get().LocalStorageCfg.Path.Key, tmpRoot)
		require.NoError(t, os.Chmod(tmpRoot, 0o555))

		// Create a reader that returns one record to trigger flush
		mockRec := &mockRecord{}
		mockRec.On("Retain").Return()
		mockRec.On("Len").Return(1)
		mockRec.On("Column", mock.Anything).Return(makeInt64Array([]int64{1, 2, 3}))
		mockRec.On("Release").Return()

		mrr := &mockRecordReader{}
		mrr.On("Next").Return(mockRec, nil).Once()
		mrr.On("Next").Return(nil, io.EOF).Once()
		mrr.On("Close").Return(nil)

		rr := []RecordReader{mrr}
		rw := &mockRecordWriter{}

		_, _, err := Sort(batchSize, schema, rr, rw, predicate, sortBy)
		assert.Error(t, err)
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
