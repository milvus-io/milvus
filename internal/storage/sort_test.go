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

	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/stretchr/testify/assert"
)

func TestSort(t *testing.T) {
	getReaders := func() []RecordReader {
		blobs, err := generateTestDataWithSeed(10, 3)
		assert.NoError(t, err)
		reader10, err := NewCompositeBinlogRecordReader(blobs)
		assert.NoError(t, err)
		blobs, err = generateTestDataWithSeed(20, 3)
		assert.NoError(t, err)
		reader20, err := NewCompositeBinlogRecordReader(blobs)
		assert.NoError(t, err)
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
		gotNumRows, err := Sort(getReaders(), common.RowIDField, rw, func(r Record, i int) bool {
			return true
		})
		assert.NoError(t, err)
		assert.Equal(t, 6, gotNumRows)
		err = rw.Close()
		assert.NoError(t, err)
	})

	t.Run("sort with predicate", func(t *testing.T) {
		gotNumRows, err := Sort(getReaders(), common.RowIDField, rw, func(r Record, i int) bool {
			pk := r.Column(common.RowIDField).(*array.Int64).Value(i)
			return pk >= 20
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, gotNumRows)
		err = rw.Close()
		assert.NoError(t, err)
	})
}

func TestMergeSort(t *testing.T) {
	getReaders := func() []RecordReader {
		blobs, err := generateTestDataWithSeed(10, 3)
		assert.NoError(t, err)
		reader10, err := NewCompositeBinlogRecordReader(blobs)
		assert.NoError(t, err)
		blobs, err = generateTestDataWithSeed(20, 3)
		assert.NoError(t, err)
		reader20, err := NewCompositeBinlogRecordReader(blobs)
		assert.NoError(t, err)
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

	t.Run("merge sort", func(t *testing.T) {
		gotNumRows, err := MergeSort(getReaders(), common.RowIDField, rw, func(r Record, i int) bool {
			return true
		})
		assert.NoError(t, err)
		assert.Equal(t, 6, gotNumRows)
		err = rw.Close()
		assert.NoError(t, err)
	})

	t.Run("Sort with predicate", func(t *testing.T) {
		gotNumRows, err := MergeSort(getReaders(), common.RowIDField, rw, func(r Record, i int) bool {
			pk := r.Column(common.RowIDField).(*array.Int64).Value(i)
			return pk >= 20
		})
		assert.NoError(t, err)
		assert.Equal(t, 3, gotNumRows)
		err = rw.Close()
		assert.NoError(t, err)
	})
}
