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
	"fmt"
	"io"
	"math/rand"
	"sort"
	"strconv"
	"testing"
	"time"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet/file"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/log"
)

func TestBinlogDeserializeReader(t *testing.T) {
	t.Run("test empty data", func(t *testing.T) {
		reader, err := NewBinlogDeserializeReader(nil, func() ([]*Blob, error) {
			return nil, io.EOF
		})
		assert.NoError(t, err)
		defer reader.Close()
		err = reader.Next()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("test deserialize", func(t *testing.T) {
		size := 3
		blobs, err := generateTestData(size)
		assert.NoError(t, err)
		reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs))
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

	t.Run("test deserialize with added field", func(t *testing.T) {
		size := 3
		blobs, err := generateTestData(size)
		assert.NoError(t, err)
		reader, err := NewBinlogDeserializeReader(generateTestAddedFieldSchema(), MakeBlobsReader(blobs))
		assert.NoError(t, err)
		defer reader.Close()

		for i := 1; i <= size; i++ {
			err = reader.Next()
			assert.NoError(t, err)

			value := reader.Value()
			assertTestAddedFieldData(t, i, value)
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
		r := NewSimpleArrowRecord(ar, map[FieldID]int{1: 0})
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
		reader, err := NewBinlogDeserializeReader(nil, func() ([]*Blob, error) {
			return nil, io.EOF
		})
		assert.NoError(t, err)
		defer reader.Close()
		err = reader.Next()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("test serialize", func(t *testing.T) {
		size := 16
		blobs, err := generateTestData(size)
		assert.NoError(t, err)
		reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs))
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

		for _, f := range schema.Fields {
			props := writers[f.FieldID].rw.writerProps
			assert.Equal(t, !f.IsPrimaryKey, props.DictionaryEnabled())
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
		// Both field pk and field 13 are with datatype int64 and auto id
		// in test data. Field pk uses delta byte array encoding, while
		// field 13 uses dict encoding.
		assert.Less(t, writers[0].buf.Len(), writers[13].buf.Len())

		// assert.Equal(t, blobs[0].Value, newblobs[0].Value)
		reader, err = NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(newblobs))
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

func BenchmarkSerializeWriter(b *testing.B) {
	const (
		dim     = 128
		numRows = 200000
	)

	var (
		rId = &schemapb.FieldSchema{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64}
		ts  = &schemapb.FieldSchema{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64}
		pk  = &schemapb.FieldSchema{FieldID: 100, Name: "pk", IsPrimaryKey: true, DataType: schemapb.DataType_VarChar, TypeParams: []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "100"}}}
		f   = &schemapb.FieldSchema{FieldID: 101, Name: "random", DataType: schemapb.DataType_Double}
		// fVec = &schemapb.FieldSchema{FieldID: 102, Name: "vec", DataType: schemapb.DataType_FloatVector, TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: strconv.Itoa(dim)}}}
	)
	schema := &schemapb.CollectionSchema{Name: "test-aaa", Fields: []*schemapb.FieldSchema{rId, ts, pk, f}}

	// prepare data values
	start := time.Now()
	vec := make([]float32, dim)
	for j := 0; j < dim; j++ {
		vec[j] = rand.Float32()
	}
	values := make([]*Value, numRows)
	for i := 0; i < numRows; i++ {
		value := &Value{}
		value.Value = make(map[int64]interface{}, len(schema.GetFields()))
		m := value.Value.(map[int64]interface{})
		for _, field := range schema.GetFields() {
			switch field.GetDataType() {
			case schemapb.DataType_Int64:
				m[field.GetFieldID()] = int64(i)
			case schemapb.DataType_VarChar:
				k := fmt.Sprintf("test_pk_%d", i)
				m[field.GetFieldID()] = k
				value.PK = &VarCharPrimaryKey{
					Value: k,
				}
			case schemapb.DataType_Double:
				m[field.GetFieldID()] = float64(i)
			case schemapb.DataType_FloatVector:
				m[field.GetFieldID()] = vec
			}
		}
		value.ID = int64(i)
		value.Timestamp = int64(0)
		value.IsDeleted = false
		value.Value = m
		values[i] = value
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].PK.LT(values[j].PK)
	})
	log.Info("prepare data done", zap.Int("len", len(values)), zap.Duration("dur", time.Since(start)))

	b.ResetTimer()

	sizes := []int{100, 1000, 10000, 100000}
	for _, s := range sizes {
		b.Run(fmt.Sprintf("batch size=%d", s), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				writers := NewBinlogStreamWriters(0, 0, 0, schema.Fields)
				writer, err := NewBinlogSerializeWriter(schema, 0, 0, writers, s)
				assert.NoError(b, err)
				for _, v := range values {
					_ = writer.Write(v)
					assert.NoError(b, err)
				}
				writer.Close()
			}
		})
	}
}

func TestNull(t *testing.T) {
	t.Run("test null", func(t *testing.T) {
		schema := generateTestSchema()
		// Copy write the generated data
		writers := NewBinlogStreamWriters(0, 0, 0, schema.Fields)
		writer, err := NewBinlogSerializeWriter(schema, 0, 0, writers, 1024)
		assert.NoError(t, err)

		m := make(map[FieldID]any)
		for _, fs := range schema.Fields {
			m[fs.FieldID] = nil
		}
		m[common.RowIDField] = int64(0)
		m[common.TimeStampField] = int64(0)
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
		reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs))
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
		reader, err := newDeltalogDeserializeReader(nil)
		assert.NoError(t, err)
		defer reader.Close()
		err = reader.Next()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("test deserialize", func(t *testing.T) {
		size := 3
		blob, err := generateTestDeltalogData(size)
		assert.NoError(t, err)
		reader, err := newDeltalogDeserializeReader([]*Blob{blob})
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
		reader, err := newDeltalogDeserializeReader(nil)
		assert.NoError(t, err)
		defer reader.Close()
		err = reader.Next()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("test serialize", func(t *testing.T) {
		size := 16
		blob, err := generateTestDeltalogData(size)
		assert.NoError(t, err)
		reader, err := newDeltalogDeserializeReader([]*Blob{blob})
		assert.NoError(t, err)
		defer reader.Close()

		// Copy write the generated data
		eventWriter := newDeltalogStreamWriter(0, 0, 0)
		writer, err := newDeltalogSerializeWriter(eventWriter, 7)
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
		reader, err = newDeltalogDeserializeReader([]*Blob{newblob})
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

func TestDeltalogPkTsSeparateFormat(t *testing.T) {
	t.Run("test empty data", func(t *testing.T) {
		eventWriter := newMultiFieldDeltalogStreamWriter(0, 0, 0, schemapb.DataType_Int64)
		writer, err := newDeltalogMultiFieldWriter(eventWriter, 7)
		assert.NoError(t, err)
		defer writer.Close()
		err = writer.Close()
		assert.NoError(t, err)
		blob, err := eventWriter.Finalize()
		assert.NoError(t, err)
		assert.NotNil(t, blob)
	})

	testCases := []struct {
		name     string
		pkType   schemapb.DataType
		assertPk func(t *testing.T, i int, value *DeleteLog)
	}{
		{
			name:   "test int64 pk",
			pkType: schemapb.DataType_Int64,
			assertPk: func(t *testing.T, i int, value *DeleteLog) {
				assert.Equal(t, NewInt64PrimaryKey(int64(i)), value.Pk)
				assert.Equal(t, uint64(i+1), value.Ts)
			},
		},
		{
			name:   "test varchar pk",
			pkType: schemapb.DataType_VarChar,
			assertPk: func(t *testing.T, i int, value *DeleteLog) {
				assert.Equal(t, NewVarCharPrimaryKey(strconv.Itoa(i)), value.Pk)
				assert.Equal(t, uint64(i+1), value.Ts)
			},
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// serialize data
			size := 10
			blob, err := writeDeltalogNewFormat(size, tc.pkType, 7)
			assert.NoError(t, err)

			// Deserialize data
			reader, err := newDeltalogDeserializeReader([]*Blob{blob})
			assert.NoError(t, err)
			defer reader.Close()
			for i := 0; i < size; i++ {
				err = reader.Next()
				assert.NoError(t, err)

				value := reader.Value()
				tc.assertPk(t, i, value)
			}
			err = reader.Next()
			assert.Equal(t, io.EOF, err)
		})
	}
}

func BenchmarkDeltalogReader(b *testing.B) {
	size := 1000000
	blob, err := generateTestDeltalogData(size)
	assert.NoError(b, err)

	b.Run("one string format reader", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			readDeltaLog(size, blob)
		}
	})

	blob, err = writeDeltalogNewFormat(size, schemapb.DataType_Int64, size)
	assert.NoError(b, err)

	b.Run("pk ts separate format reader", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			readDeltaLog(size, blob)
		}
	})
}

func BenchmarkDeltalogFormatWriter(b *testing.B) {
	size := 1000000
	b.Run("one string format writer", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			eventWriter := newDeltalogStreamWriter(0, 0, 0)
			writer, _ := newDeltalogSerializeWriter(eventWriter, size)
			var value *DeleteLog
			for j := 0; j < size; j++ {
				value = NewDeleteLog(NewInt64PrimaryKey(int64(j)), uint64(j+1))
				writer.Write(value)
			}
			writer.Close()
			eventWriter.Finalize()
		}
		b.ReportAllocs()
	})

	b.Run("pk and ts separate format writer", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writeDeltalogNewFormat(size, schemapb.DataType_Int64, size)
		}
		b.ReportAllocs()
	})
}

func writeDeltalogNewFormat(size int, pkType schemapb.DataType, batchSize int) (*Blob, error) {
	var err error
	eventWriter := newMultiFieldDeltalogStreamWriter(0, 0, 0, pkType)
	writer, err := newDeltalogMultiFieldWriter(eventWriter, batchSize)
	if err != nil {
		return nil, err
	}
	var value *DeleteLog
	for i := 0; i < size; i++ {
		switch pkType {
		case schemapb.DataType_Int64:
			value = NewDeleteLog(NewInt64PrimaryKey(int64(i)), uint64(i+1))
		case schemapb.DataType_VarChar:
			value = NewDeleteLog(NewVarCharPrimaryKey(strconv.Itoa(i)), uint64(i+1))
		}
		if err = writer.Write(value); err != nil {
			return nil, err
		}
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	blob, err := eventWriter.Finalize()
	if err != nil {
		return nil, err
	}
	return blob, nil
}

func readDeltaLog(size int, blob *Blob) error {
	reader, err := newDeltalogDeserializeReader([]*Blob{blob})
	if err != nil {
		return err
	}
	defer reader.Close()
	for j := 0; j < size; j++ {
		err = reader.Next()
		_ = reader.Value()
		if err != nil {
			return err
		}
	}
	return nil
}
