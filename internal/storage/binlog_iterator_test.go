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

	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/internal/proto/schemapb"
	"github.com/milvus-io/milvus/internal/rootcoord"

	"github.com/stretchr/testify/assert"
)

func generateTestData(t *testing.T, num int) []*Blob {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: rootcoord.TimeStampField, Name: "ts", DataType: schemapb.DataType_Int64},
		{FieldID: rootcoord.RowIDField, Name: "rowid", DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "int32", DataType: schemapb.DataType_Int32},
		{FieldID: 102, Name: "floatVector", DataType: schemapb.DataType_FloatVector},
		{FieldID: 103, Name: "binaryVector", DataType: schemapb.DataType_BinaryVector},
	}}
	insertCodec := NewInsertCodec(&etcdpb.CollectionMeta{ID: 1, Schema: schema})

	var (
		field0   []int64
		field1   []int64
		field101 []int32
		field102 []float32
		field103 []byte
	)

	for i := 1; i <= num; i++ {
		field0 = append(field0, int64(i))
		field1 = append(field1, int64(i))
		field101 = append(field101, int32(i))

		f102 := make([]float32, 8)
		for j := range f102 {
			f102[j] = float32(i)
		}

		field102 = append(field102, f102...)
		field103 = append(field103, byte(i))
	}

	data := &InsertData{Data: map[FieldID]FieldData{
		rootcoord.RowIDField:     &Int64FieldData{Data: field0},
		rootcoord.TimeStampField: &Int64FieldData{Data: field1},
		101:                      &Int32FieldData{Data: field101},
		102: &FloatVectorFieldData{
			NumRows: []int64{int64(num)},
			Data:    field102,
			Dim:     8,
		},
		103: &BinaryVectorFieldData{
			NumRows: []int64{int64(num)},
			Data:    field103,
			Dim:     8,
		},
	}}

	blobs, _, err := insertCodec.Serialize(1, 1, data)
	assert.Nil(t, err)
	return blobs
}

func TestInsertlogIterator(t *testing.T) {
	t.Run("empty iterator", func(t *testing.T) {
		itr := &InsertBinlogIterator{
			data: &InsertData{},
		}
		assert.False(t, itr.HasNext())
		_, err := itr.Next()
		assert.Equal(t, ErrNoMoreRecord, err)
	})

	t.Run("test dispose", func(t *testing.T) {
		blobs := generateTestData(t, 1)
		itr, err := NewInsertBinlogIterator(blobs, rootcoord.RowIDField, schemapb.DataType_Int64)
		assert.Nil(t, err)

		itr.Dispose()
		assert.False(t, itr.HasNext())
		_, err = itr.Next()
		assert.Equal(t, ErrDisposed, err)
	})

	t.Run("not empty iterator", func(t *testing.T) {
		blobs := generateTestData(t, 3)
		itr, err := NewInsertBinlogIterator(blobs, rootcoord.RowIDField, schemapb.DataType_Int64)
		assert.Nil(t, err)

		for i := 1; i <= 3; i++ {
			assert.True(t, itr.HasNext())
			v, err := itr.Next()
			assert.Nil(t, err)
			value := v.(*Value)

			f102 := make([]float32, 8)
			for j := range f102 {
				f102[j] = float32(i)
			}

			pk := &Int64PrimaryKey{
				Value: int64(i),
			}
			expected := &Value{
				int64(i),
				pk,
				int64(i),
				false,
				map[FieldID]interface{}{
					rootcoord.TimeStampField: int64(i),
					rootcoord.RowIDField:     int64(i),
					101:                      int32(i),
					102:                      f102,
					103:                      []byte{byte(i)},
				},
			}
			assert.EqualValues(t, expected, value)
		}

		assert.False(t, itr.HasNext())
		_, err = itr.Next()
		assert.Equal(t, ErrNoMoreRecord, err)
	})
}

func TestMergeIterator(t *testing.T) {
	t.Run("empty iterators", func(t *testing.T) {
		iterators := make([]Iterator, 0)
		for i := 0; i < 3; i++ {
			iterators = append(iterators, &InsertBinlogIterator{
				data: &InsertData{},
			})
		}
		itr := NewMergeIterator(iterators)
		assert.False(t, itr.HasNext())
		_, err := itr.Next()
		assert.Equal(t, ErrNoMoreRecord, err)
	})

	t.Run("empty and non-empty iterators", func(t *testing.T) {
		blobs := generateTestData(t, 3)
		insertItr, err := NewInsertBinlogIterator(blobs, rootcoord.RowIDField, schemapb.DataType_Int64)
		assert.Nil(t, err)
		iterators := []Iterator{
			&InsertBinlogIterator{data: &InsertData{}},
			insertItr,
		}

		itr := NewMergeIterator(iterators)

		for i := 1; i <= 3; i++ {
			assert.True(t, itr.HasNext())
			v, err := itr.Next()
			assert.Nil(t, err)
			value := v.(*Value)
			f102 := make([]float32, 8)
			for j := range f102 {
				f102[j] = float32(i)
			}

			pk := &Int64PrimaryKey{
				Value: int64(i),
			}
			expected := &Value{
				int64(i),
				pk,
				int64(i),
				false,
				map[FieldID]interface{}{
					rootcoord.TimeStampField: int64(i),
					rootcoord.RowIDField:     int64(i),
					101:                      int32(i),
					102:                      f102,
					103:                      []byte{byte(i)},
				},
			}
			assert.EqualValues(t, expected, value)
		}
		assert.False(t, itr.HasNext())
		_, err = itr.Next()
		assert.Equal(t, ErrNoMoreRecord, err)
	})

	t.Run("non-empty iterators", func(t *testing.T) {
		blobs := generateTestData(t, 3)
		itr1, err := NewInsertBinlogIterator(blobs, rootcoord.RowIDField, schemapb.DataType_Int64)
		assert.Nil(t, err)
		itr2, err := NewInsertBinlogIterator(blobs, rootcoord.RowIDField, schemapb.DataType_Int64)
		assert.Nil(t, err)
		iterators := []Iterator{itr1, itr2}
		itr := NewMergeIterator(iterators)

		for i := 1; i <= 3; i++ {
			f102 := make([]float32, 8)
			for j := range f102 {
				f102[j] = float32(i)
			}

			pk := &Int64PrimaryKey{
				Value: int64(i),
			}
			expected := &Value{
				int64(i),
				pk,
				int64(i),
				false,
				map[FieldID]interface{}{
					rootcoord.TimeStampField: int64(i),
					rootcoord.RowIDField:     int64(i),
					101:                      int32(i),
					102:                      f102,
					103:                      []byte{byte(i)},
				},
			}
			for j := 0; j < 2; j++ {
				assert.True(t, itr.HasNext())
				v, err := itr.Next()
				assert.Nil(t, err)
				value := v.(*Value)
				assert.EqualValues(t, expected, value)
			}
		}

		assert.False(t, itr.HasNext())
		_, err = itr.Next()
		assert.Equal(t, ErrNoMoreRecord, err)
	})

	t.Run("test dispose", func(t *testing.T) {
		blobs := generateTestData(t, 3)
		itr1, err := NewInsertBinlogIterator(blobs, rootcoord.RowIDField, schemapb.DataType_Int64)
		assert.Nil(t, err)
		itr := NewMergeIterator([]Iterator{itr1})

		itr.Dispose()
		assert.False(t, itr.HasNext())
		_, err = itr.Next()
		assert.Equal(t, ErrDisposed, err)
	})
}
