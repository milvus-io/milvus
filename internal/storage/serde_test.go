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
	"io"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/common"
)

func TestBinlogDeserializeReader(t *testing.T) {
	t.Run("test empty data", func(t *testing.T) {
		reader, err := NewBinlogDeserializeReader(nil, common.RowIDField)
		assert.NoError(t, err)
		defer reader.Close()
		err = reader.Next()
		assert.Equal(t, io.EOF, err)

		// blobs := generateTestData(t, 0)
		// reader, err = NewBinlogDeserializeReader(blobs, common.RowIDField)
		// assert.NoError(t, err)
		// err = reader.Next()
		// assert.Equal(t, io.EOF, err)
	})

	t.Run("test deserialize", func(t *testing.T) {
		len := 3
		blobs := generateTestData(t, len)
		reader, err := NewBinlogDeserializeReader(blobs, common.RowIDField)
		assert.NoError(t, err)
		defer reader.Close()

		for i := 1; i <= len; i++ {
			err = reader.Next()
			assert.NoError(t, err)

			value := reader.Value()

			f102 := make([]float32, 8)
			for j := range f102 {
				f102[j] = float32(i)
			}
			assertTestData(t, i, value)
		}

		err = reader.Next()
		assert.Equal(t, io.EOF, err)
	})
}

func Test_deserializeCell(t *testing.T) {
	onelinerArray := func(dtype arrow.DataType, payload interface{}) arrow.Array {
		mem := memory.DefaultAllocator

		switch dtype.ID() {
		case arrow.BOOL:
			builder := array.NewBooleanBuilder(mem)
			builder.Append(payload.(bool))
			return builder.NewBooleanArray()
		case arrow.INT8:
			builder := array.NewInt8Builder(mem)
			builder.Append(payload.(int8))
			return builder.NewInt8Array()
		case arrow.INT16:
			builder := array.NewInt16Builder(mem)
			builder.Append(payload.(int16))
			return builder.NewInt16Array()
		case arrow.INT32:
			builder := array.NewInt32Builder(mem)
			builder.Append(payload.(int32))
			return builder.NewInt32Array()
		case arrow.INT64:
			builder := array.NewInt64Builder(mem)
			builder.Append(payload.(int64))
			return builder.NewInt64Array()
		case arrow.FLOAT32:
			builder := array.NewFloat32Builder(mem)
			builder.Append(payload.(float32))
			return builder.NewFloat32Array()
		case arrow.FLOAT64:
			builder := array.NewFloat64Builder(mem)
			builder.Append(payload.(float64))
			return builder.NewFloat64Array()
		case arrow.STRING:
			builder := array.NewStringBuilder(mem)
			builder.Append(payload.(string))
			return builder.NewStringArray()
		case arrow.BINARY:
			builder := array.NewBinaryBuilder(mem, arrow.BinaryTypes.Binary)
			builder.Append(payload.([]byte))
			return builder.NewBinaryArray()
		case arrow.FIXED_SIZE_BINARY:
			typ := dtype.(*arrow.FixedSizeBinaryType)
			builder := array.NewFixedSizeBinaryBuilder(mem, typ)
			builder.Append(payload.([]byte))
			return builder.NewFixedSizeBinaryArray()
		}

		return nil
	}

	type args struct {
		col      arrow.Array
		dataType schemapb.DataType
		i        int
	}
	tests := []struct {
		name  string
		args  args
		want  interface{}
		want1 bool
	}{
		{"test bool", args{col: onelinerArray(arrow.FixedWidthTypes.Boolean, true), dataType: schemapb.DataType_Bool, i: 0}, true, true},
		{"test bool negative", args{col: onelinerArray(arrow.Null, nil), dataType: schemapb.DataType_Bool, i: 0}, nil, false},
		{"test int8", args{col: onelinerArray(arrow.PrimitiveTypes.Int8, int8(1)), dataType: schemapb.DataType_Int8, i: 0}, int8(1), true},
		{"test int8 negative", args{col: onelinerArray(arrow.Null, nil), dataType: schemapb.DataType_Int8, i: 0}, nil, false},
		{"test int16", args{col: onelinerArray(arrow.PrimitiveTypes.Int16, int16(1)), dataType: schemapb.DataType_Int16, i: 0}, int16(1), true},
		{"test int16 negative", args{col: onelinerArray(arrow.Null, nil), dataType: schemapb.DataType_Int16, i: 0}, nil, false},
		{"test int32", args{col: onelinerArray(arrow.PrimitiveTypes.Int32, int32(1)), dataType: schemapb.DataType_Int32, i: 0}, int32(1), true},
		{"test int32 negative", args{col: onelinerArray(arrow.Null, nil), dataType: schemapb.DataType_Int32, i: 0}, nil, false},
		{"test int64", args{col: onelinerArray(arrow.PrimitiveTypes.Int64, int64(1)), dataType: schemapb.DataType_Int64, i: 0}, int64(1), true},
		{"test int64 negative", args{col: onelinerArray(arrow.Null, nil), dataType: schemapb.DataType_Int64, i: 0}, nil, false},
		{"test float32", args{col: onelinerArray(arrow.PrimitiveTypes.Float32, float32(1)), dataType: schemapb.DataType_Float, i: 0}, float32(1), true},
		{"test float32 negative", args{col: onelinerArray(arrow.Null, nil), dataType: schemapb.DataType_Float, i: 0}, nil, false},
		{"test float64", args{col: onelinerArray(arrow.PrimitiveTypes.Float64, float64(1)), dataType: schemapb.DataType_Double, i: 0}, float64(1), true},
		{"test float64 negative", args{col: onelinerArray(arrow.Null, nil), dataType: schemapb.DataType_Double, i: 0}, nil, false},
		{"test string", args{col: onelinerArray(arrow.BinaryTypes.String, "test"), dataType: schemapb.DataType_String, i: 0}, "test", true},
		{"test string negative", args{col: onelinerArray(arrow.Null, nil), dataType: schemapb.DataType_String, i: 0}, nil, false},
		{"test varchar", args{col: onelinerArray(arrow.BinaryTypes.String, "test"), dataType: schemapb.DataType_VarChar, i: 0}, "test", true},
		{"test varchar negative", args{col: onelinerArray(arrow.Null, nil), dataType: schemapb.DataType_VarChar, i: 0}, nil, false},
		{"test array negative", args{col: onelinerArray(arrow.BinaryTypes.Binary, []byte("{}")), dataType: schemapb.DataType_Array, i: 0}, nil, false},
		{"test array negative null", args{col: onelinerArray(arrow.Null, nil), dataType: schemapb.DataType_Array, i: 0}, nil, false},
		{"test json", args{col: onelinerArray(arrow.BinaryTypes.Binary, []byte("{}")), dataType: schemapb.DataType_JSON, i: 0}, []byte("{}"), true},
		{"test json negative", args{col: onelinerArray(arrow.Null, nil), dataType: schemapb.DataType_JSON, i: 0}, nil, false},
		{"test float vector", args{col: onelinerArray(&arrow.FixedSizeBinaryType{ByteWidth: 4}, []byte{0, 0, 0, 0}), dataType: schemapb.DataType_FloatVector, i: 0}, []float32{0.0}, true},
		{"test float vector negative", args{col: onelinerArray(arrow.Null, nil), dataType: schemapb.DataType_FloatVector, i: 0}, nil, false},
		{"test bool vector", args{col: onelinerArray(&arrow.FixedSizeBinaryType{ByteWidth: 4}, []byte("test")), dataType: schemapb.DataType_BinaryVector, i: 0}, []byte("test"), true},
		{"test float16 vector", args{col: onelinerArray(&arrow.FixedSizeBinaryType{ByteWidth: 4}, []byte("test")), dataType: schemapb.DataType_Float16Vector, i: 0}, []byte("test"), true},
		{"test bfloat16 vector", args{col: onelinerArray(&arrow.FixedSizeBinaryType{ByteWidth: 4}, []byte("test")), dataType: schemapb.DataType_BFloat16Vector, i: 0}, []byte("test"), true},
		{"test bfloat16 vector negative", args{col: onelinerArray(arrow.Null, nil), dataType: schemapb.DataType_BFloat16Vector, i: 0}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := deserializeCell(tt.args.col, tt.args.dataType, tt.args.i)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("deserializeCell() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("deserializeCell() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
