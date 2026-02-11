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
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/bytedance/mockey"
	"github.com/stretchr/testify/assert"

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
