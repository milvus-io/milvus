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
	"context"
	"io"
	"reflect"
	"testing"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/apache/arrow/go/v12/parquet/file"
	"github.com/apache/arrow/go/v12/parquet/pqarrow"
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
		size := 3
		blobs, err := generateTestData(size)
		assert.NoError(t, err)
		reader, err := NewBinlogDeserializeReader(blobs, common.RowIDField)
		assert.NoError(t, err)
		defer reader.Close()

		for i := 1; i <= size; i++ {
			err = reader.Next()
			assert.NoError(t, err)

			value := reader.Value()
			assertTestData(t, i, value)
		}

		err = reader.Next()
		assert.Equal(t, io.EOF, err)
	})
}

func TestBinlogStreamWriter(t *testing.T) {
	t.Run("test write", func(t *testing.T) {
		size := 3

		field := arrow.Field{Name: "bool", Type: arrow.FixedWidthTypes.Boolean}
		var w bytes.Buffer
		rw, err := newSingleFieldRecordWriter(1, field, &w)
		assert.NoError(t, err)

		builder := array.NewBooleanBuilder(memory.DefaultAllocator)
		builder.AppendValues([]bool{true, false, true}, nil)
		arr := builder.NewArray()
		defer arr.Release()
		ar := array.NewRecord(
			arrow.NewSchema(
				[]arrow.Field{field},
				nil,
			),
			[]arrow.Array{arr},
			int64(size),
		)
		r := newSimpleArrowRecord(ar, map[FieldID]schemapb.DataType{1: schemapb.DataType_Bool}, map[FieldID]int{1: 0})
		defer r.Release()
		err = rw.Write(r)
		assert.NoError(t, err)
		rw.Close()

		reader, err := file.NewParquetReader(bytes.NewReader(w.Bytes()))
		assert.NoError(t, err)
		arrowReader, err := pqarrow.NewFileReader(reader, pqarrow.ArrowReadProperties{BatchSize: 1024}, memory.DefaultAllocator)
		assert.NoError(t, err)
		rr, err := arrowReader.GetRecordReader(context.Background(), nil, nil)
		assert.NoError(t, err)
		defer rr.Release()
		ok := rr.Next()
		assert.True(t, ok)
		rec := rr.Record()
		defer rec.Release()
		assert.Equal(t, int64(size), rec.NumRows())
		ok = rr.Next()
		assert.False(t, ok)
	})
}

func TestBinlogSerializeWriter(t *testing.T) {
	t.Run("test empty data", func(t *testing.T) {
		reader, err := NewBinlogDeserializeReader(nil, common.RowIDField)
		assert.NoError(t, err)
		defer reader.Close()
		err = reader.Next()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("test serialize", func(t *testing.T) {
		size := 16
		blobs, err := generateTestData(size)
		assert.NoError(t, err)
		reader, err := NewBinlogDeserializeReader(blobs, common.RowIDField)
		assert.NoError(t, err)
		defer reader.Close()

		schema := generateTestSchema()
		// Copy write the generated data
		writers := NewBinlogStreamWriters(0, 0, 0, schema.Fields)
		writer, err := NewBinlogSerializeWriter(schema, 0, 0, writers, 7)
		assert.NoError(t, err)

		for i := 1; i <= size; i++ {
			err = reader.Next()
			assert.NoError(t, err)

			value := reader.Value()
			assertTestData(t, i, value)
			err := writer.Write(value)
			assert.NoError(t, err)
		}

		err = reader.Next()
		assert.Equal(t, io.EOF, err)
		err = writer.Close()
		assert.NoError(t, err)
		assert.True(t, writer.WrittenMemorySize() >= 429)

		// Read from the written data
		newblobs := make([]*Blob, len(writers))
		i := 0
		for _, w := range writers {
			blob, err := w.Finalize()
			assert.NoError(t, err)
			assert.NotNil(t, blob)
			assert.True(t, blob.MemorySize > 0)
			newblobs[i] = blob
			i++
		}
		// assert.Equal(t, blobs[0].Value, newblobs[0].Value)
		reader, err = NewBinlogDeserializeReader(blobs, common.RowIDField)
		assert.NoError(t, err)
		defer reader.Close()
		for i := 1; i <= size; i++ {
			err = reader.Next()
			assert.NoError(t, err, i)

			value := reader.Value()
			assertTestData(t, i, value)
		}
	})
}

func TestNull(t *testing.T) {
	t.Run("test null", func(t *testing.T) {
		schema := generateTestSchema()
		// Copy write the generated data
		writers := NewBinlogStreamWriters(0, 0, 0, schema.Fields)
		writer, err := NewBinlogSerializeWriter(schema, 0, 0, writers, 1024)
		assert.NoError(t, err)

		m := make(map[FieldID]any)
		m[common.RowIDField] = int64(0)
		m[common.TimeStampField] = int64(0)
		m[10] = nil
		m[11] = nil
		m[12] = nil
		m[13] = nil
		m[14] = nil
		m[15] = nil
		m[16] = nil
		m[17] = nil
		m[18] = nil
		m[19] = nil
		m[101] = nil
		m[102] = nil
		m[103] = nil
		m[104] = nil
		m[105] = nil
		m[106] = nil
		pk, err := GenPrimaryKeyByRawData(m[common.RowIDField], schemapb.DataType_Int64)
		assert.NoError(t, err)

		value := &Value{
			ID:        0,
			PK:        pk,
			Timestamp: 0,
			IsDeleted: false,
			Value:     m,
		}
		writer.Write(value)
		err = writer.Close()
		assert.NoError(t, err)

		// Read from the written data
		blobs := make([]*Blob, len(writers))
		i := 0
		for _, w := range writers {
			blob, err := w.Finalize()
			assert.NoError(t, err)
			assert.NotNil(t, blob)
			blobs[i] = blob
			i++
		}
		reader, err := NewBinlogDeserializeReader(blobs, common.RowIDField)
		assert.NoError(t, err)
		defer reader.Close()
		err = reader.Next()
		assert.NoError(t, err)

		readValue := reader.Value()
		assert.Equal(t, value, readValue)
	})
}

func TestSerDe(t *testing.T) {
	type args struct {
		dt schemapb.DataType
		v  any
	}
	tests := []struct {
		name  string
		args  args
		want  interface{}
		want1 bool
	}{
		{"test bool", args{dt: schemapb.DataType_Bool, v: true}, true, true},
		{"test bool null", args{dt: schemapb.DataType_Bool, v: nil}, nil, true},
		{"test bool negative", args{dt: schemapb.DataType_Bool, v: -1}, nil, false},
		{"test int8", args{dt: schemapb.DataType_Int8, v: int8(1)}, int8(1), true},
		{"test int8 null", args{dt: schemapb.DataType_Int8, v: nil}, nil, true},
		{"test int8 negative", args{dt: schemapb.DataType_Int8, v: true}, nil, false},
		{"test int16", args{dt: schemapb.DataType_Int16, v: int16(1)}, int16(1), true},
		{"test int16 null", args{dt: schemapb.DataType_Int16, v: nil}, nil, true},
		{"test int16 negative", args{dt: schemapb.DataType_Int16, v: true}, nil, false},
		{"test int32", args{dt: schemapb.DataType_Int32, v: int32(1)}, int32(1), true},
		{"test int32 null", args{dt: schemapb.DataType_Int32, v: nil}, nil, true},
		{"test int32 negative", args{dt: schemapb.DataType_Int32, v: true}, nil, false},
		{"test int64", args{dt: schemapb.DataType_Int64, v: int64(1)}, int64(1), true},
		{"test int64 null", args{dt: schemapb.DataType_Int64, v: nil}, nil, true},
		{"test int64 negative", args{dt: schemapb.DataType_Int64, v: true}, nil, false},
		{"test float32", args{dt: schemapb.DataType_Float, v: float32(1)}, float32(1), true},
		{"test float32 null", args{dt: schemapb.DataType_Float, v: nil}, nil, true},
		{"test float32 negative", args{dt: schemapb.DataType_Float, v: -1}, nil, false},
		{"test float64", args{dt: schemapb.DataType_Double, v: float64(1)}, float64(1), true},
		{"test float64 null", args{dt: schemapb.DataType_Double, v: nil}, nil, true},
		{"test float64 negative", args{dt: schemapb.DataType_Double, v: -1}, nil, false},
		{"test string", args{dt: schemapb.DataType_String, v: "test"}, "test", true},
		{"test string null", args{dt: schemapb.DataType_String, v: nil}, nil, true},
		{"test string negative", args{dt: schemapb.DataType_String, v: -1}, nil, false},
		{"test varchar", args{dt: schemapb.DataType_VarChar, v: "test"}, "test", true},
		{"test varchar null", args{dt: schemapb.DataType_VarChar, v: nil}, nil, true},
		{"test varchar negative", args{dt: schemapb.DataType_VarChar, v: -1}, nil, false},
		{"test array negative", args{dt: schemapb.DataType_Array, v: "{}"}, nil, false},
		{"test array null", args{dt: schemapb.DataType_Array, v: nil}, nil, true},
		{"test json", args{dt: schemapb.DataType_JSON, v: []byte("{}")}, []byte("{}"), true},
		{"test json null", args{dt: schemapb.DataType_JSON, v: nil}, nil, true},
		{"test json negative", args{dt: schemapb.DataType_JSON, v: -1}, nil, false},
		{"test float vector", args{dt: schemapb.DataType_FloatVector, v: []float32{1.0}}, []float32{1.0}, true},
		{"test float vector null", args{dt: schemapb.DataType_FloatVector, v: nil}, nil, true},
		{"test float vector negative", args{dt: schemapb.DataType_FloatVector, v: []int{1}}, nil, false},
		{"test bool vector", args{dt: schemapb.DataType_BinaryVector, v: []byte{0xff}}, []byte{0xff}, true},
		{"test float16 vector", args{dt: schemapb.DataType_Float16Vector, v: []byte{0xff, 0xff}}, []byte{0xff, 0xff}, true},
		{"test bfloat16 vector", args{dt: schemapb.DataType_BFloat16Vector, v: []byte{0xff, 0xff}}, []byte{0xff, 0xff}, true},
		{"test bfloat16 vector null", args{dt: schemapb.DataType_BFloat16Vector, v: nil}, nil, true},
		{"test bfloat16 vector negative", args{dt: schemapb.DataType_BFloat16Vector, v: -1}, nil, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			dt := tt.args.dt
			v := tt.args.v
			builder := array.NewBuilder(memory.DefaultAllocator, serdeMap[dt].arrowType(1))
			serdeMap[dt].serialize(builder, v)
			// assert.True(t, ok)
			a := builder.NewArray()
			got, got1 := serdeMap[dt].deserialize(a, 0)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("deserialize() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("deserialize() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func BenchmarkDeserializeReader(b *testing.B) {
	len := 1000000
	blobs, err := generateTestData(len)
	assert.NoError(b, err)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		reader, err := NewBinlogDeserializeReader(blobs, common.RowIDField)
		assert.NoError(b, err)
		defer reader.Close()
		for i := 0; i < len; i++ {
			err = reader.Next()
			_ = reader.Value()
			assert.NoError(b, err)
		}
		err = reader.Next()
		assert.Equal(b, io.EOF, err)
	}
}

func BenchmarkBinlogIterator(b *testing.B) {
	len := 1000000
	blobs, err := generateTestData(len)
	assert.NoError(b, err)
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		itr, err := NewInsertBinlogIterator(blobs, common.RowIDField, schemapb.DataType_Int64)
		assert.NoError(b, err)
		defer itr.Dispose()
		for i := 0; i < len; i++ {
			assert.True(b, itr.HasNext())
			_, err = itr.Next()
			assert.NoError(b, err)
		}
		assert.False(b, itr.HasNext())
	}
}
