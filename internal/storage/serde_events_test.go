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

func generateTestDeltalogData(size int) (*Blob, error) {
	codec := NewDeleteCodec()
	pks := make([]int64, size)
	tss := make([]uint64, size)
	for i := 0; i < size; i++ {
		pks[i] = int64(i)
		tss[i] = uint64(i + 1)
	}
	data := &DeleteData{}
	for i := range pks {
		data.Append(NewInt64PrimaryKey(pks[i]), tss[i])
	}
	return codec.Serialize(0, 0, 0, data)
}

func assertTestDeltalogData(t *testing.T, i int, value *DeleteLog) {
	assert.Equal(t, &Int64PrimaryKey{int64(i)}, value.Pk)
	assert.Equal(t, uint64(i+1), value.Ts)
}

func TestDeltalogDeserializeReader(t *testing.T) {
	t.Run("test empty data", func(t *testing.T) {
		reader, err := NewDeltalogDeserializeReader(nil)
		assert.NoError(t, err)
		defer reader.Close()
		err = reader.Next()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("test deserialize", func(t *testing.T) {
		size := 3
		blob, err := generateTestDeltalogData(size)
		assert.NoError(t, err)
		reader, err := NewDeltalogDeserializeReader([]*Blob{blob})
		assert.NoError(t, err)
		defer reader.Close()

		for i := 0; i < size; i++ {
			err = reader.Next()
			assert.NoError(t, err)

			value := reader.Value()
			assertTestDeltalogData(t, i, value)
		}

		err = reader.Next()
		assert.Equal(t, io.EOF, err)
	})
}

func TestDeltalogSerializeWriter(t *testing.T) {
	t.Run("test empty data", func(t *testing.T) {
		reader, err := NewDeltalogDeserializeReader(nil)
		assert.NoError(t, err)
		defer reader.Close()
		err = reader.Next()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("test serialize", func(t *testing.T) {
		size := 16
		blob, err := generateTestDeltalogData(size)
		assert.NoError(t, err)
		reader, err := NewDeltalogDeserializeReader([]*Blob{blob})
		assert.NoError(t, err)
		defer reader.Close()

		// Copy write the generated data
		eventWriter := NewDeltalogStreamWriter(0, 0, 0)
		writer, err := NewDeltalogSerializeWriter(0, 0, eventWriter, 7)
		assert.NoError(t, err)

		for i := 0; i < size; i++ {
			err = reader.Next()
			assert.NoError(t, err)

			value := reader.Value()
			assertTestDeltalogData(t, i, value)
			err := writer.Write(value)
			assert.NoError(t, err)
		}

		err = reader.Next()
		assert.Equal(t, io.EOF, err)
		err = writer.Close()
		assert.NoError(t, err)

		// Read from the written data
		newblob, err := eventWriter.Finalize()
		assert.NoError(t, err)
		assert.NotNil(t, newblob)
		// assert.Equal(t, blobs[0].Value, newblobs[0].Value)
		reader, err = NewDeltalogDeserializeReader([]*Blob{newblob})
		assert.NoError(t, err)
		defer reader.Close()
		for i := 0; i < size; i++ {
			err = reader.Next()
			assert.NoError(t, err, i)

			value := reader.Value()
			assertTestDeltalogData(t, i, value)
		}
	})
}
