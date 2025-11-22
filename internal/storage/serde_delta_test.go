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
	"context"
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v2/schemapb"
	"github.com/milvus-io/milvus/pkg/v2/common"
	"github.com/milvus-io/milvus/pkg/v2/util/paramtable"
)

// createTestRecord creates a Record with pk and ts columns for testing
// The record uses column indices 0 for pk and 1 for ts
func createTestRecord(pkType schemapb.DataType, numRows int) Record {
	var pkArray arrow.Array

	allocator := memory.DefaultAllocator

	switch pkType {
	case schemapb.DataType_Int64:
		builder := array.NewInt64Builder(allocator)
		defer builder.Release()
		for i := 0; i < numRows; i++ {
			builder.Append(int64(i))
		}
		pkArray = builder.NewArray()
	case schemapb.DataType_VarChar:
		builder := array.NewStringBuilder(allocator)
		defer builder.Release()
		for i := 0; i < numRows; i++ {
			builder.Append("pk_" + string(rune(i+'0')))
		}
		pkArray = builder.NewArray()
	default:
		panic(fmt.Sprintf("unexpected pk type %v", pkType))
	}

	pkFieldType := serdeMap[pkType].arrowType(0, schemapb.DataType_None)
	pkArrowField := arrow.Field{Name: "pk", Type: pkFieldType, Nullable: false}
	tsField := arrow.Field{Name: "ts", Type: arrow.PrimitiveTypes.Int64, Nullable: false}
	tsBuilder := array.NewInt64Builder(allocator)
	defer tsBuilder.Release()
	for i := 0; i < numRows; i++ {
		tsBuilder.Append(int64(i + 1000))
	}
	tsArray := tsBuilder.NewArray()

	schema := arrow.NewSchema([]arrow.Field{pkArrowField, tsField}, nil)
	arrowRec := array.NewRecord(schema, []arrow.Array{pkArray, tsArray}, int64(numRows))

	// Map column indices to field IDs: index 0 -> field ID 0 (pk), index 1 -> field ID 1 (ts)
	field2Col := map[FieldID]int{
		0: 0, // pk column
		1: 1, // ts column
	}

	return NewSimpleArrowRecord(arrowRec, field2Col)
}

func TestLegacyDeltalogReaderWriter(t *testing.T) {
	const (
		testCollectionID = int64(1)
		testPartitionID  = int64(2)
		testSegmentID    = int64(3)
		pkFieldID        = int64(100)
	)

	tests := []struct {
		name    string
		format  string
		pkType  schemapb.DataType
		wantErr bool
	}{
		{
			name:    "Int64 PK - JSON format",
			format:  "json",
			pkType:  schemapb.DataType_Int64,
			wantErr: false,
		},
		{
			name:    "VarChar PK - JSON format",
			format:  "json",
			pkType:  schemapb.DataType_VarChar,
			wantErr: false,
		},
		{
			name:    "Int64 PK - Parquet format",
			format:  "parquet",
			pkType:  schemapb.DataType_Int64,
			wantErr: false,
		},
		{
			name:    "VarChar PK - Parquet format",
			format:  "parquet",
			pkType:  schemapb.DataType_VarChar,
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// Set deltalog format
			originalFormat := paramtable.Get().DataNodeCfg.DeltalogFormat.GetValue()
			paramtable.Get().Save(paramtable.Get().DataNodeCfg.DeltalogFormat.Key, tt.format)
			defer paramtable.Get().Save(paramtable.Get().DataNodeCfg.DeltalogFormat.Key, originalFormat)

			// Create test record with pk and ts columns
			testNumRows := 10
			record := createTestRecord(tt.pkType, testNumRows)
			defer record.Release()

			blob := &Blob{}
			path := "test.bin"
			writer, err := NewLegacyDeltalogWriter(testCollectionID, testPartitionID, testSegmentID, 0, tt.pkType,
				func(ctx context.Context, kvs map[string][]byte) error {
					blob.Value = kvs[path]
					blob.Key = path
					return nil
				}, path)
			require.NoError(t, err)

			err = writer.Write(record)
			require.NoError(t, err)

			err = writer.Close()
			require.NoError(t, err)

			// Test round trip
			reader, err := NewLegacyDeltalogReader(tt.pkType, func(ctx context.Context, paths []string) ([][]byte, error) {
				return [][]byte{blob.Value}, nil
			}, []string{blob.Key})
			require.NoError(t, err)
			require.NotNil(t, reader)

			// Read and verify contents
			rec, err := reader.Next()
			assert.NoError(t, err)
			require.NotNil(t, rec)

			// Verify the record has the expected number of rows
			assert.Equal(t, testNumRows, rec.Len())

			// The reader returns a record with field IDs: pkFieldID for pk, TimeStampField for ts
			// The original record has field IDs: 0 for pk, 1 for ts

			// Verify pk column
			pkCol := rec.Column(0)
			origPkCol := record.Column(0)
			require.NotNil(t, pkCol)
			require.NotNil(t, origPkCol)
			assert.Equal(t, origPkCol.Len(), pkCol.Len())

			if tt.pkType == schemapb.DataType_Int64 {
				origPkArr := origPkCol.(*array.Int64)
				pkArr := pkCol.(*array.Int64)
				for i := 0; i < testNumRows; i++ {
					assert.Equal(t, origPkArr.Value(i), pkArr.Value(i), "pk mismatch at index %d", i)
				}
			} else {
				origPkArr := origPkCol.(*array.String)
				pkArr := pkCol.(*array.String)
				for i := 0; i < testNumRows; i++ {
					assert.Equal(t, origPkArr.Value(i), pkArr.Value(i), "pk mismatch at index %d", i)
				}
			}

			// Verify ts column
			tsCol := rec.Column(common.TimeStampField).(*array.Int64)
			origTsCol := record.Column(1).(*array.Int64)
			for i := 0; i < testNumRows; i++ {
				assert.Equal(t, origTsCol.Value(i), tsCol.Value(i), "ts mismatch at index %d", i)
			}

			err = reader.Close()
			assert.NoError(t, err)
		})
	}
}

func TestDeltalogStreamWriter_NoRecordWriter(t *testing.T) {
	writer := newDeltalogStreamWriter(1, 2, 3)
	assert.NotNil(t, writer)

	// Finalize without getting record writer should return error
	blob, err := writer.Finalize()
	assert.Error(t, err)
	assert.Nil(t, blob)
}

// generateTestDeltalogData generates test deltalog data using the old codec
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

// writeDeltalogNewFormat writes deltalog data in the new multi-field format
func writeDeltalogNewFormat(size int, pkType schemapb.DataType) (*Blob, error) {
	blob := &Blob{}
	path := "test.bin"
	writer, err := NewLegacyDeltalogWriter(0, 0, 0, 0, pkType, func(ctx context.Context, kvs map[string][]byte) error {
		blob.Value = kvs[path]
		blob.Key = path
		return nil
	}, path)
	if err != nil {
		return nil, err
	}
	record := createTestRecord(pkType, size)
	if err = writer.Write(record); err != nil {
		return nil, err
	}
	if err = writer.Close(); err != nil {
		return nil, err
	}
	return blob, nil
}

// readDeltaLog reads deltalog data and returns any error
func readDeltaLog(size int, blob *Blob, pkType schemapb.DataType) error {
	reader, err := NewLegacyDeltalogReader(pkType, func(ctx context.Context, paths []string) ([][]byte, error) {
		return [][]byte{blob.Value}, nil
	}, []string{blob.Key})
	if err != nil {
		return err
	}
	defer reader.Close()
	for j := 0; j < size; j++ {
		_, err = reader.Next()
		if err != nil {
			return err
		}
	}
	return nil
}

// BenchmarkDeltalogReader benchmarks reading deltalog data in different formats
func BenchmarkDeltalogReader(b *testing.B) {
	size := 1000000
	blob, err := generateTestDeltalogData(size)
	assert.NoError(b, err)

	b.Run("one string format reader", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			readDeltaLog(size, blob, schemapb.DataType_Int64)
		}
	})

	blob, err = writeDeltalogNewFormat(size, schemapb.DataType_Int64)
	assert.NoError(b, err)

	b.Run("pk ts separate format reader", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			readDeltaLog(size, blob, schemapb.DataType_Int64)
		}
	})
}

// BenchmarkDeltalogFormatWriter benchmarks writing deltalog data in different formats
func BenchmarkDeltalogFormatWriter(b *testing.B) {
	size := 1000000
	b.Run("one string format writer", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writer, err := NewLegacyDeltalogWriter(0, 0, 0, 0, schemapb.DataType_Int64,
				func(ctx context.Context, kvs map[string][]byte) error {
					return nil
				}, "test.bin")
			if err != nil {
				b.Fatal(err)
			}
			if err = writer.Write(createTestRecord(schemapb.DataType_Int64, size)); err != nil {
				b.Fatal(err)
			}
			if err = writer.Close(); err != nil {
				b.Fatal(err)
			}
		}
		b.ReportAllocs()
	})

	b.Run("pk and ts separate format writer", func(b *testing.B) {
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			writeDeltalogNewFormat(size, schemapb.DataType_Int64)
		}
		b.ReportAllocs()
	})
}
