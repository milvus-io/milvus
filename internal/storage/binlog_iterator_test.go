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

	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/internal/proto/etcdpb"
	"github.com/milvus-io/milvus/pkg/common"
	"github.com/milvus-io/milvus/pkg/util/typeutil"
)

func generateTestSchema() *schemapb.CollectionSchema {
	schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: common.TimeStampField, Name: "ts", DataType: schemapb.DataType_Int64},
		{FieldID: common.RowIDField, Name: "rowid", DataType: schemapb.DataType_Int64},
		{FieldID: 10, Name: "bool", DataType: schemapb.DataType_Bool},
		{FieldID: 11, Name: "int8", DataType: schemapb.DataType_Int8},
		{FieldID: 12, Name: "int16", DataType: schemapb.DataType_Int16},
		{FieldID: 13, Name: "int64", DataType: schemapb.DataType_Int64},
		{FieldID: 14, Name: "float", DataType: schemapb.DataType_Float},
		{FieldID: 15, Name: "double", DataType: schemapb.DataType_Double},
		{FieldID: 16, Name: "varchar", DataType: schemapb.DataType_VarChar, IsPrimaryKey: true},
		{FieldID: 17, Name: "string", DataType: schemapb.DataType_String},
		{FieldID: 18, Name: "array", DataType: schemapb.DataType_Array},
		{FieldID: 19, Name: "string", DataType: schemapb.DataType_JSON},
		{FieldID: 101, Name: "int32", DataType: schemapb.DataType_Int32},
		{FieldID: 102, Name: "floatVector", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		}},
		{FieldID: 103, Name: "binaryVector", DataType: schemapb.DataType_BinaryVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		}},
		{FieldID: 104, Name: "float16Vector", DataType: schemapb.DataType_Float16Vector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		}},
		{FieldID: 105, Name: "bf16Vector", DataType: schemapb.DataType_BFloat16Vector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "8"},
		}},
		{FieldID: 106, Name: "sparseFloatVector", DataType: schemapb.DataType_SparseFloatVector, TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "28433"},
		}},
	}}

	return schema
}

func generateTestData(num int) ([]*Blob, error) {
	insertCodec := NewInsertCodecWithSchema(&etcdpb.CollectionMeta{ID: 1, Schema: generateTestSchema()})

	var (
		field0 []int64
		field1 []int64

		field10 []bool
		field11 []int8
		field12 []int16
		field13 []int64
		field14 []float32
		field15 []float64
		field16 []string
		field17 []string
		field18 []*schemapb.ScalarField
		field19 [][]byte

		field101 []int32
		field102 []float32
		field103 []byte

		field104 []byte
		field105 []byte
		field106 [][]byte
	)

	for i := 1; i <= num; i++ {
		field0 = append(field0, int64(i))
		field1 = append(field1, int64(i))
		field10 = append(field10, true)
		field11 = append(field11, int8(i))
		field12 = append(field12, int16(i))
		field13 = append(field13, int64(i))
		field14 = append(field14, float32(i))
		field15 = append(field15, float64(i))
		field16 = append(field16, fmt.Sprint(i))
		field17 = append(field17, fmt.Sprint(i))

		arr := &schemapb.ScalarField{
			Data: &schemapb.ScalarField_IntData{
				IntData: &schemapb.IntArray{Data: []int32{int32(i), int32(i), int32(i)}},
			},
		}
		field18 = append(field18, arr)

		field19 = append(field19, []byte{byte(i)})
		field101 = append(field101, int32(i))

		f102 := make([]float32, 8)
		for j := range f102 {
			f102[j] = float32(i)
		}

		field102 = append(field102, f102...)
		field103 = append(field103, 0xff)

		f104 := make([]byte, 16)
		for j := range f104 {
			f104[j] = byte(i)
		}
		field104 = append(field104, f104...)
		field105 = append(field105, f104...)

		field106 = append(field106, typeutil.CreateSparseFloatRow([]uint32{0, uint32(18 * i), uint32(284 * i)}, []float32{1.1, 0.3, 2.4}))
	}

	data := &InsertData{Data: map[FieldID]FieldData{
		common.RowIDField:     &Int64FieldData{Data: field0},
		common.TimeStampField: &Int64FieldData{Data: field1},

		10:  &BoolFieldData{Data: field10},
		11:  &Int8FieldData{Data: field11},
		12:  &Int16FieldData{Data: field12},
		13:  &Int64FieldData{Data: field13},
		14:  &FloatFieldData{Data: field14},
		15:  &DoubleFieldData{Data: field15},
		16:  &StringFieldData{Data: field16},
		17:  &StringFieldData{Data: field17},
		18:  &ArrayFieldData{Data: field18},
		19:  &JSONFieldData{Data: field19},
		101: &Int32FieldData{Data: field101},
		102: &FloatVectorFieldData{
			Data: field102,
			Dim:  8,
		},
		103: &BinaryVectorFieldData{
			Data: field103,
			Dim:  8,
		},
		104: &Float16VectorFieldData{
			Data: field104,
			Dim:  8,
		},
		105: &BFloat16VectorFieldData{
			Data: field105,
			Dim:  8,
		},
		106: &SparseFloatVectorFieldData{
			SparseFloatArray: schemapb.SparseFloatArray{
				Dim:      28433,
				Contents: field106,
			},
		},
	}}

	blobs, err := insertCodec.Serialize(1, 1, data)
	return blobs, err
}

// Verify value of index i (1-based numbering) in data generated by generateTestData
func assertTestData(t *testing.T, i int, value *Value) {
	f102 := make([]float32, 8)
	for j := range f102 {
		f102[j] = float32(i)
	}

	f104 := make([]byte, 16)
	for j := range f104 {
		f104[j] = byte(i)
	}

	f106 := typeutil.CreateSparseFloatRow([]uint32{0, uint32(18 * i), uint32(284 * i)}, []float32{1.1, 0.3, 2.4})

	assert.EqualExportedValues(t, &Value{
		int64(i),
		&Int64PrimaryKey{Value: int64(i)},
		int64(i),
		false,
		map[FieldID]interface{}{
			common.TimeStampField: int64(i),
			common.RowIDField:     int64(i),

			10:  true,
			11:  int8(i),
			12:  int16(i),
			13:  int64(i),
			14:  float32(i),
			15:  float64(i),
			16:  fmt.Sprint(i),
			17:  fmt.Sprint(i),
			18:  &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: []int32{int32(i), int32(i), int32(i)}}}},
			19:  []byte{byte(i)},
			101: int32(i),
			102: f102,
			103: []byte{0xff},
			104: f104,
			105: f104,
			106: f106,
		},
	}, value)
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
		blobs, err := generateTestData(1)
		assert.NoError(t, err)
		itr, err := NewInsertBinlogIterator(blobs, common.RowIDField, schemapb.DataType_Int64)
		assert.NoError(t, err)

		itr.Dispose()
		assert.False(t, itr.HasNext())
		_, err = itr.Next()
		assert.Equal(t, ErrDisposed, err)
	})

	t.Run("not empty iterator", func(t *testing.T) {
		blobs, err := generateTestData(3)
		assert.NoError(t, err)
		itr, err := NewInsertBinlogIterator(blobs, common.RowIDField, schemapb.DataType_Int64)
		assert.NoError(t, err)

		for i := 1; i <= 3; i++ {
			assert.True(t, itr.HasNext())
			v, err := itr.Next()
			assert.NoError(t, err)
			value := v.(*Value)
			assertTestData(t, i, value)
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
		blobs, err := generateTestData(3)
		assert.NoError(t, err)
		insertItr, err := NewInsertBinlogIterator(blobs, common.RowIDField, schemapb.DataType_Int64)
		assert.NoError(t, err)
		iterators := []Iterator{
			&InsertBinlogIterator{data: &InsertData{}},
			insertItr,
		}

		itr := NewMergeIterator(iterators)

		for i := 1; i <= 3; i++ {
			assert.True(t, itr.HasNext())
			v, err := itr.Next()
			assert.NoError(t, err)
			value := v.(*Value)
			assertTestData(t, i, value)
		}
		assert.False(t, itr.HasNext())
		_, err = itr.Next()
		assert.Equal(t, ErrNoMoreRecord, err)
	})

	t.Run("non-empty iterators", func(t *testing.T) {
		blobs, err := generateTestData(3)
		assert.NoError(t, err)
		itr1, err := NewInsertBinlogIterator(blobs, common.RowIDField, schemapb.DataType_Int64)
		assert.NoError(t, err)
		itr2, err := NewInsertBinlogIterator(blobs, common.RowIDField, schemapb.DataType_Int64)
		assert.NoError(t, err)
		iterators := []Iterator{itr1, itr2}
		itr := NewMergeIterator(iterators)

		for i := 1; i <= 3; i++ {
			for j := 0; j < 2; j++ {
				assert.True(t, itr.HasNext())
				v, err := itr.Next()
				assert.NoError(t, err)
				value := v.(*Value)
				assertTestData(t, i, value)
			}
		}

		assert.False(t, itr.HasNext())
		_, err = itr.Next()
		assert.Equal(t, ErrNoMoreRecord, err)
	})

	t.Run("test dispose", func(t *testing.T) {
		blobs, err := generateTestData(3)
		assert.NoError(t, err)
		itr1, err := NewInsertBinlogIterator(blobs, common.RowIDField, schemapb.DataType_Int64)
		assert.NoError(t, err)
		itr := NewMergeIterator([]Iterator{itr1})

		itr.Dispose()
		assert.False(t, itr.HasNext())
		_, err = itr.Next()
		assert.Equal(t, ErrDisposed, err)
	})
}
