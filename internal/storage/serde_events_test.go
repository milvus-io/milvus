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
	"github.com/bytedance/mockey"
	"github.com/samber/lo"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/hook"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storagev2/packed"
	"github.com/milvus-io/milvus/internal/util/hookutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mlog"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexcgopb"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

func TestBinlogDeserializeReader(t *testing.T) {
	t.Run("test empty data", func(t *testing.T) {
		reader, err := NewBinlogDeserializeReader(generateTestSchema(), func() ([]*Blob, error) {
			return nil, io.EOF
		}, false)
		assert.NoError(t, err)
		defer reader.Close()
		_, err = reader.NextValue()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("test deserialize", func(t *testing.T) {
		size := 3
		blobs, err := generateTestData(size)
		assert.NoError(t, err)
		reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs), false)
		assert.NoError(t, err)
		defer reader.Close()

		for i := 1; i <= size; i++ {
			value, err := reader.NextValue()
			assert.NoError(t, err)

			assertTestData(t, i, *value)
		}

		_, err = reader.NextValue()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("test deserialize with added field", func(t *testing.T) {
		size := 3
		blobs, err := generateTestData(size)
		assert.NoError(t, err)
		reader, err := NewBinlogDeserializeReader(generateTestAddedFieldSchema(), MakeBlobsReader(blobs), false)
		assert.NoError(t, err)
		defer reader.Close()

		for i := 1; i <= size; i++ {
			value, err := reader.NextValue()
			assert.NoError(t, err)

			assertTestAddedFieldData(t, i, *value)
		}

		_, err = reader.NextValue()
		assert.Equal(t, io.EOF, err)
	})
}

func TestBinlogStreamWriter(t *testing.T) {
	t.Run("test write", func(t *testing.T) {
		size := 3

		field := &schemapb.FieldSchema{
			FieldID:  1,
			DataType: schemapb.DataType_Bool,
		}

		var w bytes.Buffer
		rw, err := newSingleFieldRecordWriter(field, &w)
		assert.NoError(t, err)

		builder := array.NewBooleanBuilder(memory.DefaultAllocator)
		builder.AppendValues([]bool{true, false, true}, nil)
		arr := builder.NewArray()
		defer arr.Release()
		ar := array.NewRecord(
			arrow.NewSchema(
				[]arrow.Field{{Name: "bool", Type: arrow.FixedWidthTypes.Boolean}},
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

func TestRecordToInsertData(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
			{FieldID: 101, Name: "sparse", DataType: schemapb.DataType_SparseFloatVector, IsFunctionOutput: true},
		},
	}
	arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "text_col", Type: arrow.BinaryTypes.String}}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.StringBuilder).AppendValues([]string{"a", "b"}, nil)
	arrowRecord := builder.NewRecord()

	record := NewSimpleArrowRecord(arrowRecord, map[FieldID]int{100: 0})
	defer record.Release()
	insertData, err := RecordToInsertData(record, schema, typeutil.NewSet[int64](100))
	require.NoError(t, err)
	require.Equal(t, 2, insertData.Data[100].RowNum())
	require.Equal(t, 0, insertData.Data[101].RowNum())
	require.Equal(t, []string{"a", "b"}, insertData.Data[100].(*StringFieldData).Data)

	arrowRecord.Retain()
	missingRecord := NewSimpleArrowRecord(arrowRecord, map[FieldID]int{999: 0})
	defer missingRecord.Release()
	_, err = RecordToInsertData(missingRecord, schema, typeutil.NewSet[int64](100))
	require.Error(t, err)
	require.Contains(t, err.Error(), "required field text")
}

type recordToInsertSparseRecord struct {
	arr arrow.Array
}

func (r *recordToInsertSparseRecord) Column(fieldID FieldID) arrow.Array {
	if fieldID == 100 {
		return r.arr
	}
	return nil
}

func (r *recordToInsertSparseRecord) Len() int {
	return r.arr.Len()
}

func (r *recordToInsertSparseRecord) Release() {
	r.arr.Release()
}

func (r *recordToInsertSparseRecord) Retain() {
	r.arr.Retain()
}

func TestRecordToInsertDataBranches(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
		},
	}

	t.Run("invalid schema", func(t *testing.T) {
		_, err := RecordToInsertData(nil, nil, nil)
		require.Error(t, err)
	})

	t.Run("nil and empty record", func(t *testing.T) {
		insertData, err := RecordToInsertData(nil, schema, typeutil.NewSet[int64](100))
		require.NoError(t, err)
		require.Equal(t, 0, insertData.Data[100].RowNum())

		arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String}}, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer builder.Release()
		arrowRecord := builder.NewRecord()
		record := NewSimpleArrowRecord(arrowRecord, map[FieldID]int{100: 0})
		defer record.Release()

		insertData, err = RecordToInsertData(record, schema, typeutil.NewSet[int64](100))
		require.NoError(t, err)
		require.Equal(t, 0, insertData.Data[100].RowNum())
	})

	t.Run("row count mismatch", func(t *testing.T) {
		builder := array.NewStringBuilder(memory.DefaultAllocator)
		builder.AppendValues([]string{"a", "b"}, nil)
		first := builder.NewArray()
		builder.Release()

		builder = array.NewStringBuilder(memory.DefaultAllocator)
		builder.Append("x")
		second := builder.NewArray()
		builder.Release()

		record := &compositeRecord{
			index: map[FieldID]int16{100: 1},
			recs:  []arrow.Array{first, second},
		}
		defer record.Release()

		_, err := RecordToInsertData(record, schema, typeutil.NewSet[int64](100))
		require.Error(t, err)
		require.Contains(t, err.Error(), "row count mismatch")
	})

	t.Run("vector and missing final required field", func(t *testing.T) {
		vectorSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:  100,
					Name:     "vector",
					DataType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "2"},
					},
				},
			},
		}
		builder := array.NewBuilder(memory.DefaultAllocator,
			serdeMap[schemapb.DataType_FloatVector].arrowType(2, schemapb.DataType_None))
		require.NoError(t, serdeMap[schemapb.DataType_FloatVector].serialize(
			builder, []float32{1, 2}, schemapb.DataType_None))
		arr := builder.NewArray()
		builder.Release()
		record := &compositeRecord{
			index: map[FieldID]int16{100: 0},
			recs:  []arrow.Array{arr},
		}
		defer record.Release()

		insertData, err := RecordToInsertData(record, vectorSchema, typeutil.NewSet[int64](100))
		require.NoError(t, err)
		require.Equal(t, []float32{1, 2}, insertData.Data[100].(*FloatVectorFieldData).Data)

		_, err = RecordToInsertData(record, vectorSchema, typeutil.NewSet[int64](100, 999))
		require.Error(t, err)
		require.Contains(t, err.Error(), "required field ID=999")
	})

	t.Run("array of vector", func(t *testing.T) {
		vectorSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{
					FieldID:     100,
					Name:        "vectors",
					DataType:    schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector,
					TypeParams: []*commonpb.KeyValuePair{
						{Key: common.DimKey, Value: "2"},
					},
				},
			},
		}
		entry := serdeMap[schemapb.DataType_ArrayOfVector]
		builder := array.NewBuilder(memory.DefaultAllocator,
			entry.arrowType(2, schemapb.DataType_FloatVector))
		require.NoError(t, entry.serialize(builder, &schemapb.VectorField{
			Dim: 2,
			Data: &schemapb.VectorField_FloatVector{
				FloatVector: &schemapb.FloatArray{Data: []float32{1, 2}},
			},
		}, schemapb.DataType_FloatVector))
		arr := builder.NewArray()
		builder.Release()
		record := &compositeRecord{
			index: map[FieldID]int16{100: 0},
			recs:  []arrow.Array{arr},
		}
		defer record.Release()

		insertData, err := RecordToInsertData(record, vectorSchema, typeutil.NewSet[int64](100))
		require.NoError(t, err)
		got := insertData.Data[100].(*VectorArrayFieldData).Data
		require.Len(t, got, 1)
		require.Equal(t, []float32{1, 2}, got[0].GetFloatVector().GetData())
	})

	t.Run("deserialize and append errors", func(t *testing.T) {
		intBuilder := array.NewInt64Builder(memory.DefaultAllocator)
		intBuilder.Append(1)
		intArr := intBuilder.NewArray()
		intBuilder.Release()
		badTypeRecord := &compositeRecord{
			index: map[FieldID]int16{100: 0},
			recs:  []arrow.Array{intArr},
		}
		defer badTypeRecord.Release()
		_, err := RecordToInsertData(badTypeRecord, schema, typeutil.NewSet[int64](100))
		require.Error(t, err)
		require.Contains(t, err.Error(), "deserialize field text")

		stringBuilder := array.NewStringBuilder(memory.DefaultAllocator)
		stringBuilder.AppendNull()
		nullArr := stringBuilder.NewArray()
		stringBuilder.Release()
		nullRecord := &compositeRecord{
			index: map[FieldID]int16{100: 0},
			recs:  []arrow.Array{nullArr},
		}
		defer nullRecord.Release()
		_, err = RecordToInsertData(nullRecord, schema, typeutil.NewSet[int64](100))
		require.Error(t, err)
		require.Contains(t, err.Error(), "append field text")
	})

	t.Run("generic record skips missing optional field", func(t *testing.T) {
		genericSchema := &schemapb.CollectionSchema{
			Fields: []*schemapb.FieldSchema{
				{FieldID: 100, Name: "text", DataType: schemapb.DataType_VarChar},
				{FieldID: 101, Name: "optional", DataType: schemapb.DataType_VarChar},
			},
		}
		builder := array.NewStringBuilder(memory.DefaultAllocator)
		builder.Append("a")
		record := &recordToInsertSparseRecord{arr: builder.NewArray()}
		builder.Release()
		defer record.Release()

		insertData, err := RecordToInsertData(record, genericSchema, typeutil.NewSet[int64](100))
		require.NoError(t, err)
		require.Equal(t, []string{"a"}, insertData.Data[100].(*StringFieldData).Data)
		require.Equal(t, 0, insertData.Data[101].RowNum())
	})

	t.Run("simple record invalid column", func(t *testing.T) {
		arrowSchema := arrow.NewSchema([]arrow.Field{{Name: "text", Type: arrow.BinaryTypes.String}}, nil)
		builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
		defer builder.Release()
		builder.Field(0).(*array.StringBuilder).Append("a")
		arrowRecord := builder.NewRecord()
		record := NewSimpleArrowRecord(arrowRecord, map[FieldID]int{100: 10})
		defer record.Release()

		insertData, err := RecordToInsertData(record, schema, nil)
		require.NoError(t, err)
		require.Equal(t, 0, insertData.Data[100].RowNum())
	})

	t.Run("insert data initializer misses field", func(t *testing.T) {
		mockNewInsertData := mockey.Mock(NewInsertDataWithFunctionOutputField).
			Return(&InsertData{Data: map[FieldID]FieldData{}}, nil).Build()
		defer mockNewInsertData.UnPatch()

		builder := array.NewStringBuilder(memory.DefaultAllocator)
		builder.Append("a")
		arr := builder.NewArray()
		builder.Release()
		record := &compositeRecord{
			index: map[FieldID]int16{100: 0},
			recs:  []arrow.Array{arr},
		}
		defer record.Release()

		_, err := RecordToInsertData(record, schema, typeutil.NewSet[int64](100))
		require.Error(t, err)
		require.Contains(t, err.Error(), "not initialized")
	})

	t.Run("serde entry missing", func(t *testing.T) {
		entry := serdeMap[schemapb.DataType_VarChar]
		delete(serdeMap, schemapb.DataType_VarChar)
		defer func() { serdeMap[schemapb.DataType_VarChar] = entry }()

		builder := array.NewStringBuilder(memory.DefaultAllocator)
		builder.Append("a")
		arr := builder.NewArray()
		builder.Release()
		record := &compositeRecord{
			index: map[FieldID]int16{100: 0},
			recs:  []arrow.Array{arr},
		}
		defer record.Release()

		_, err := RecordToInsertData(record, schema, typeutil.NewSet[int64](100))
		require.Error(t, err)
		require.Contains(t, err.Error(), "unsupported data type")
	})
}

type fakeManifestRecordReader struct{}

func (fakeManifestRecordReader) Next() (Record, error) {
	return nil, io.EOF
}

func (fakeManifestRecordReader) Close() error {
	return nil
}

func TestManifestReaderExternalContext(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name:           "external_collection",
		ExternalSource: "s3://bucket/source",
		ExternalSpec:   `{"format":"parquet"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_Text, ExternalField: "text_col"},
			{FieldID: 101, Name: "score", DataType: schemapb.DataType_Int64},
		},
	}
	internalSchema := &schemapb.CollectionSchema{
		Name: "internal_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_Text},
			{FieldID: 101, Name: "score", DataType: schemapb.DataType_Int64},
		},
	}
	storageConfig := &indexpb.StorageConfig{RootPath: "root"}
	externalReader := packed.ExternalReaderContext{
		CollectionID: 19530,
		Source:       schema.GetExternalSource(),
		Spec:         schema.GetExternalSpec(),
	}

	opts := DefaultReaderOptions()
	WithExternalReaderContext(externalReader)(opts)
	require.Equal(t, externalReader, opts.externalReader)

	var capturedManifest string
	var capturedColumns []string
	var capturedBufferSize int64
	var capturedSchema *arrow.Schema
	var capturedStorageConfig *indexpb.StorageConfig
	var capturedPluginContext *indexcgopb.StoragePluginContext
	var capturedExternalReader packed.ExternalReaderContext
	mock := mockey.Mock(packed.NewFFIPackedReader).To(
		func(manifestPath string,
			arrowSchema *arrow.Schema,
			neededColumns []string,
			bufferSize int64,
			cfg *indexpb.StorageConfig,
			pluginContext *indexcgopb.StoragePluginContext,
			ext packed.ExternalReaderContext,
			_ ...context.Context,
		) (*packed.FFIPackedReader, error) {
			capturedManifest = manifestPath
			capturedColumns = append([]string(nil), neededColumns...)
			capturedBufferSize = bufferSize
			capturedSchema = arrowSchema
			capturedStorageConfig = cfg
			capturedPluginContext = pluginContext
			capturedExternalReader = ext
			return &packed.FFIPackedReader{}, nil
		}).Build()
	defer mock.UnPatch()

	reader, err := NewManifestReader("manifest-json", schema, 4096, storageConfig, nil,
		WithExternalReaderContext(externalReader))
	require.NoError(t, err)
	require.Equal(t, "manifest-json", capturedManifest)
	require.Equal(t, []string{"text_col", "101"}, capturedColumns)
	require.Equal(t, int64(4096), capturedBufferSize)
	require.Same(t, storageConfig, capturedStorageConfig)
	require.Nil(t, capturedPluginContext)
	require.Equal(t, externalReader, capturedExternalReader)
	require.Equal(t, arrow.BinaryTypes.String, capturedSchema.Field(0).Type)
	require.Equal(t, map[FieldID]int{100: 0, 101: 1}, reader.field2Col)

	reader, err = NewManifestReader("manifest-json", internalSchema, 4096, storageConfig, nil)
	require.NoError(t, err)
	require.Equal(t, arrow.BinaryTypes.Binary, capturedSchema.Field(0).Type)
	require.Equal(t, []string{"100", "101"}, reader.neededColumns)

	reader, err = NewManifestReader("manifest-json", schema, 4096, storageConfig, nil)
	require.NoError(t, err)
	require.Equal(t, arrow.BinaryTypes.String, capturedSchema.Field(0).Type)
	require.Equal(t, []string{"text_col", "101"}, reader.neededColumns)
}

func TestDeltalogReaderExternalContext(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{RootPath: "root"}
	externalReader := packed.ExternalReaderContext{
		CollectionID: 19530,
		Source:       "s3://bucket/source",
		Spec:         `{"format":"milvus-table"}`,
	}
	sourcePath := "s3://bucket/source/_delta/1"

	var capturedPaths []string
	var capturedBufferSize int64
	var capturedStorageConfig *indexpb.StorageConfig
	var capturedPluginContext *indexcgopb.StoragePluginContext
	var capturedExternalReader packed.ExternalReaderContext
	mock := mockey.Mock(packed.NewPackedReaderWithExtfs).To(
		func(paths []string,
			arrowSchema *arrow.Schema,
			bufferSize int64,
			cfg *indexpb.StorageConfig,
			pluginContext *indexcgopb.StoragePluginContext,
			ext packed.ExternalReaderContext,
			_ ...context.Context,
		) (*packed.PackedReader, error) {
			require.NotNil(t, arrowSchema)
			capturedPaths = append([]string(nil), paths...)
			capturedBufferSize = bufferSize
			capturedStorageConfig = cfg
			capturedPluginContext = pluginContext
			capturedExternalReader = ext
			return &packed.PackedReader{}, nil
		}).Build()
	defer mock.UnPatch()

	reader, err := NewDeltalogReader(
		schemapb.DataType_Int64,
		[]string{sourcePath},
		WithVersion(StorageV3),
		WithStorageConfig(storageConfig),
		WithBufferSize(4096),
		WithExternalReaderContext(externalReader),
	)
	require.NoError(t, err)
	record, err := reader.Next()
	require.Nil(t, record)
	require.ErrorIs(t, err, io.EOF)
	require.Equal(t, []string{sourcePath}, capturedPaths)
	require.Equal(t, int64(4096), capturedBufferSize)
	require.Same(t, storageConfig, capturedStorageConfig)
	require.Nil(t, capturedPluginContext)
	require.Equal(t, externalReader, capturedExternalReader)
}

func TestManifestReaderExternalContextErrors(t *testing.T) {
	storageConfig := &indexpb.StorageConfig{RootPath: "root"}
	badSchema := &schemapb.CollectionSchema{
		Name: "bad_schema",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "bad", DataType: schemapb.DataType_None},
		},
	}
	_, err := NewManifestReader("manifest-json", badSchema, 4096, storageConfig, nil)
	require.Error(t, err)

	duplicateFieldSchema := &schemapb.CollectionSchema{
		Name: "duplicate_field_schema",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "dup", DataType: schemapb.DataType_Int64},
			{FieldID: 101, Name: "dup", DataType: schemapb.DataType_Int64},
		},
	}
	_, err = NewManifestReader("manifest-json", duplicateFieldSchema, 4096, storageConfig, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "duplicated fieldName")

	schema := &schemapb.CollectionSchema{
		Name: "external_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_Text},
		},
	}
	mock := mockey.Mock(packed.NewFFIPackedReader).Return(nil, fmt.Errorf("ffi open failed")).Build()
	defer mock.UnPatch()

	_, err = NewManifestReader("manifest-json", schema, 4096, storageConfig, nil)
	require.Error(t, err)
	require.Contains(t, err.Error(), "ffi open failed")
}

func TestNewManifestRecordReaderPassesExternalContext(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name:           "external_collection",
		ExternalSource: "s3://bucket/source",
		ExternalSpec:   `{"format":"parquet"}`,
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_Text},
		},
	}
	storageConfig := &indexpb.StorageConfig{RootPath: "root"}
	externalReader := packed.ExternalReaderContext{
		CollectionID: 19530,
		Source:       schema.GetExternalSource(),
		Spec:         schema.GetExternalSpec(),
	}

	var capturedOptions *rwOptions
	mock := mockey.Mock(NewRecordReaderFromManifest).To(
		func(manifest string,
			schema *schemapb.CollectionSchema,
			bufferSize int64,
			cfg *indexpb.StorageConfig,
			pluginContext *indexcgopb.StoragePluginContext,
			option ...RwOption,
		) (RecordReader, error) {
			capturedOptions = DefaultReaderOptions()
			for _, opt := range option {
				opt(capturedOptions)
			}
			require.Equal(t, "manifest-json", manifest)
			require.Equal(t, int64(4096), bufferSize)
			require.Same(t, storageConfig, cfg)
			require.Nil(t, pluginContext)
			return fakeManifestRecordReader{}, nil
		}).Build()
	defer mock.UnPatch()

	reader, err := NewManifestRecordReader(context.Background(), "manifest-json", schema,
		WithVersion(StorageV3),
		WithStorageConfig(storageConfig),
		WithBufferSize(4096),
		WithExternalReaderContext(externalReader),
	)
	require.NoError(t, err)
	require.NotNil(t, reader)
	require.NotNil(t, capturedOptions)
	require.Equal(t, StorageV3, capturedOptions.version)
	require.Equal(t, storageConfig, capturedOptions.storageConfig)
	require.Equal(t, externalReader, capturedOptions.externalReader)
}

type fakeUnsafeKeyCipher struct{}

func (fakeUnsafeKeyCipher) Init(params map[string]string) error {
	return nil
}

func (fakeUnsafeKeyCipher) GetEncryptor(ezID, collectionID int64) (hook.Encryptor, []byte, error) {
	return nil, nil, nil
}

func (fakeUnsafeKeyCipher) GetDecryptor(ezID, collectionID int64, safeKey []byte) (hook.Decryptor, error) {
	return nil, nil
}

func (fakeUnsafeKeyCipher) GetUnsafeKey(ezID, collectionID int64) []byte {
	return []byte("unsafe-key")
}

func TestNewManifestRecordReaderBranches(t *testing.T) {
	schema := &schemapb.CollectionSchema{
		Name: "external_collection",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "text", DataType: schemapb.DataType_Text},
		},
	}
	_, err := NewManifestRecordReader(context.Background(), "manifest-json", schema)
	require.Error(t, err)

	pluginContext := &indexcgopb.StoragePluginContext{
		EncryptionZoneId: 1,
		CollectionId:     2,
		EncryptionKey:    "key",
	}
	mockEncryption := mockey.Mock(hookutil.IsClusterEncryptionEnabled).Return(true).Build()
	defer mockEncryption.UnPatch()

	var capturedPluginContext *indexcgopb.StoragePluginContext
	mockReader := mockey.Mock(NewRecordReaderFromManifest).To(
		func(manifest string,
			schema *schemapb.CollectionSchema,
			bufferSize int64,
			cfg *indexpb.StorageConfig,
			pluginContext *indexcgopb.StoragePluginContext,
			option ...RwOption,
		) (RecordReader, error) {
			capturedPluginContext = pluginContext
			return fakeManifestRecordReader{}, nil
		}).Build()
	defer mockReader.UnPatch()

	reader, err := NewManifestRecordReader(context.Background(), "manifest-json", schema,
		WithVersion(StorageV3),
		WithStorageConfig(&indexpb.StorageConfig{RootPath: "root"}),
		WithPluginContext(pluginContext),
	)
	require.NoError(t, err)
	require.NotNil(t, reader)
	require.Same(t, pluginContext, capturedPluginContext)

	schemaWithEZ := proto.Clone(schema).(*schemapb.CollectionSchema)
	schemaWithEZ.Properties = []*commonpb.KeyValuePair{
		{Key: common.EncryptionEzIDKey, Value: "7"},
	}
	mockCipher := mockey.Mock(hookutil.GetCipher).Return(fakeUnsafeKeyCipher{}).Build()
	defer mockCipher.UnPatch()

	reader, err = NewManifestRecordReader(context.Background(), "manifest-json", schemaWithEZ,
		WithVersion(StorageV3),
		WithStorageConfig(&indexpb.StorageConfig{RootPath: "root"}),
		WithCollectionID(2),
	)
	require.NoError(t, err)
	require.NotNil(t, reader)
	require.NotNil(t, capturedPluginContext)
	require.Equal(t, int64(7), capturedPluginContext.GetEncryptionZoneId())
	require.Equal(t, int64(2), capturedPluginContext.GetCollectionId())
	require.Equal(t, "unsafe-key", capturedPluginContext.GetEncryptionKey())
}

func TestBinlogSerializeWriter(t *testing.T) {
	t.Run("test write value", func(t *testing.T) {
		size := 100
		blobs, err := generateTestData(size)
		assert.NoError(t, err)
		reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs), false)
		assert.NoError(t, err)
		defer reader.Close()

		schema := generateTestSchema()
		alloc := allocator.NewLocalAllocator(1, 92) // 90 for 18 fields * 5 chunks, 1 for 1 stats file
		chunkSize := uint64(64)                     // 64B
		rw, err := newCompositeBinlogRecordWriter(0, 0, 0, schema,
			func(b []*Blob) error {
				mlog.Debug(context.TODO(), "write blobs", mlog.Int("files", len(b)))
				return nil
			},
			alloc, chunkSize, "root", 10000)
		assert.NoError(t, err)
		writer := NewBinlogValueWriter(rw, 20)
		assert.NoError(t, err)

		for i := 1; i <= size; i++ {
			value, err := reader.NextValue()
			assert.NoError(t, err)

			assertTestData(t, i, *value)
			err = writer.WriteValue(*value)
			assert.NoError(t, err)
		}

		_, err = reader.NextValue()
		assert.Equal(t, io.EOF, err)
		err = writer.Close()
		assert.NoError(t, err)

		logs, _, _, _, _ := writer.GetLogs()
		assert.Equal(t, 18, len(logs))
		assert.Equal(t, 5, len(logs[0].Binlogs))
	})
}

func TestValueSerializer_NullArrayOfVectorRoundTrip(t *testing.T) {
	const (
		pkFieldID          FieldID = common.StartOfUserFieldID
		vectorArrayFieldID FieldID = common.StartOfUserFieldID + 1
	)

	schema := &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.RowIDField,
				Name:     common.RowIDFieldName,
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  common.TimeStampField,
				Name:     common.TimeStampFieldName,
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:      pkFieldID,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:     vectorArrayFieldID,
				Name:        "vec_array",
				DataType:    schemapb.DataType_ArrayOfVector,
				ElementType: schemapb.DataType_FloatVector,
				Nullable:    true,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: "dim", Value: "4"},
				},
			},
		},
	}

	values := []*Value{
		{
			Value: map[FieldID]any{
				common.RowIDField:     int64(11),
				common.TimeStampField: int64(101),
				pkFieldID:             int64(1),
				vectorArrayFieldID:    makeFloatVec(4, 1, 2, 3, 4),
			},
		},
		{
			Value: map[FieldID]any{
				common.RowIDField:     int64(12),
				common.TimeStampField: int64(102),
				pkFieldID:             int64(2),
				vectorArrayFieldID:    nil,
			},
		},
	}

	record, err := ValueSerializer(values, schema)
	require.NoError(t, err)
	defer record.Release()
	assert.True(t, record.Column(vectorArrayFieldID).IsNull(1))

	roundTripValues := make([]*Value, record.Len())
	err = ValueDeserializerWithSchema(record, roundTripValues, schema, true)
	require.NoError(t, err)
	assert.Nil(t, roundTripValues[1].Value.(map[FieldID]interface{})[vectorArrayFieldID])

	rewrittenRecord, err := ValueSerializer(roundTripValues, schema)
	require.NoError(t, err)
	defer rewrittenRecord.Release()
	assert.True(t, rewrittenRecord.Column(vectorArrayFieldID).IsNull(1))
}

func TestValueDeserializerNullableDenseVectorBinaryRecord(t *testing.T) {
	for _, tc := range nullableDenseVectorSerdeCases() {
		t.Run(tc.name, func(t *testing.T) {
			schema := nullableDenseVectorSerdeSchema(tc.dataType, tc.dim)
			insertData := nullableDenseVectorSerdeInsertData(t, tc, schema)

			arrowSchema, err := ConvertToArrowSchema(schema, false)
			require.NoError(t, err)
			builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
			defer builder.Release()
			require.NoError(t, BuildRecord(builder, insertData, schema))
			record := NewSimpleArrowRecord(builder.NewRecord(), map[FieldID]int{
				common.RowIDField:          0,
				common.TimeStampField:      1,
				nullableSerdePKFieldID:     2,
				nullableSerdeVectorFieldID: 3,
			})
			defer record.Release()

			require.IsType(t, &array.Binary{}, record.Column(nullableSerdeVectorFieldID))
			require.True(t, record.Column(nullableSerdeVectorFieldID).IsNull(1))

			values := make([]*Value, record.Len())
			err = ValueDeserializerWithSchema(record, values, schema, true)
			require.NoError(t, err)

			got := values[0].Value.(map[FieldID]interface{})[nullableSerdeVectorFieldID]
			assert.Equal(t, tc.row0, got)
			assert.Nil(t, values[1].Value.(map[FieldID]interface{})[nullableSerdeVectorFieldID])
			got = values[2].Value.(map[FieldID]interface{})[nullableSerdeVectorFieldID]
			assert.Equal(t, tc.row2, got)
		})
	}
}

func TestValueSerializerNullableDenseVectorUsesBinaryArrow(t *testing.T) {
	for _, tc := range nullableDenseVectorSerdeCases() {
		t.Run(tc.name, func(t *testing.T) {
			schema := nullableDenseVectorSerdeSchema(tc.dataType, tc.dim)
			values := []*Value{
				{Value: map[FieldID]any{
					common.RowIDField:          int64(11),
					common.TimeStampField:      int64(101),
					nullableSerdePKFieldID:     int64(1),
					nullableSerdeVectorFieldID: tc.row0,
				}},
				{Value: map[FieldID]any{
					common.RowIDField:          int64(12),
					common.TimeStampField:      int64(102),
					nullableSerdePKFieldID:     int64(2),
					nullableSerdeVectorFieldID: nil,
				}},
				{Value: map[FieldID]any{
					common.RowIDField:          int64(13),
					common.TimeStampField:      int64(103),
					nullableSerdePKFieldID:     int64(3),
					nullableSerdeVectorFieldID: tc.row2,
				}},
			}

			record, err := ValueSerializer(values, schema)
			require.NoError(t, err)
			defer record.Release()

			require.IsType(t, &array.Binary{}, record.Column(nullableSerdeVectorFieldID))
			require.True(t, record.Column(nullableSerdeVectorFieldID).IsNull(1))
			simpleRecord := record.(*simpleArrowRecord)
			dim, ok := simpleRecord.r.Schema().Field(3).Metadata.GetValue(common.DimKey)
			require.True(t, ok)
			assert.Equal(t, strconv.FormatInt(tc.dim, 10), dim)

			roundTrip := make([]*Value, record.Len())
			err = ValueDeserializerWithSchema(record, roundTrip, schema, true)
			require.NoError(t, err)
			assert.Equal(t, tc.row0, roundTrip[0].Value.(map[FieldID]interface{})[nullableSerdeVectorFieldID])
			assert.Nil(t, roundTrip[1].Value.(map[FieldID]interface{})[nullableSerdeVectorFieldID])
			assert.Equal(t, tc.row2, roundTrip[2].Value.(map[FieldID]interface{})[nullableSerdeVectorFieldID])
		})
	}
}

func TestValueDeserializerSerializerTextLobRefUsesBinaryArrow(t *testing.T) {
	const (
		pkFieldID   FieldID = 100
		textFieldID FieldID = 101
	)
	schema := &schemapb.CollectionSchema{
		Name: "text_lob_ref",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: "Timestamp", DataType: schemapb.DataType_Int64},
			{FieldID: pkFieldID, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: textFieldID, Name: "content", DataType: schemapb.DataType_Text},
		},
	}
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "row_id", Type: arrow.PrimitiveTypes.Int64},
		{Name: "Timestamp", Type: arrow.PrimitiveTypes.Int64},
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "content", Type: arrow.BinaryTypes.Binary},
	}, nil)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	builder.Field(0).(*array.Int64Builder).Append(11)
	builder.Field(1).(*array.Int64Builder).Append(101)
	builder.Field(2).(*array.Int64Builder).Append(1)
	builder.Field(3).(*array.BinaryBuilder).Append([]byte("lob-ref"))

	record := NewSimpleArrowRecord(builder.NewRecord(), map[FieldID]int{
		common.RowIDField:     0,
		common.TimeStampField: 1,
		pkFieldID:             2,
		textFieldID:           3,
	})
	defer record.Release()

	values := make([]*Value, record.Len())
	err := ValueDeserializerWithSchema(record, values, schema, true)
	require.NoError(t, err)
	textValue := values[0].Value.(map[FieldID]interface{})[textFieldID]
	require.IsType(t, TextLobRef{}, textValue)
	require.Equal(t, TextLobRef("lob-ref"), textValue)

	rewrittenRecord, err := ValueSerializer(values, schema)
	require.NoError(t, err)
	defer rewrittenRecord.Release()
	textColumn := rewrittenRecord.Column(textFieldID)
	require.IsType(t, &array.Binary{}, textColumn)
	require.Equal(t, []byte("lob-ref"), textColumn.(*array.Binary).Value(0))
}

func TestValueSerializerTextRejectsRawBytes(t *testing.T) {
	const (
		pkFieldID   FieldID = 100
		textFieldID FieldID = 101
	)
	schema := &schemapb.CollectionSchema{
		Name: "text_raw_bytes",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: "row_id", DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: "Timestamp", DataType: schemapb.DataType_Int64},
			{FieldID: pkFieldID, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: textFieldID, Name: "content", DataType: schemapb.DataType_Text},
		},
	}
	values := []*Value{
		{
			PK:        NewInt64PrimaryKey(1),
			Timestamp: 101,
			Value: map[FieldID]interface{}{
				common.RowIDField:     int64(11),
				common.TimeStampField: int64(101),
				pkFieldID:             int64(1),
				textFieldID:           []byte("raw-text-bytes"),
			},
		},
	}

	_, err := ValueSerializer(values, schema)
	require.Error(t, err)
	require.Contains(t, err.Error(), "expected string value")
}

type nullableDenseVectorSerdeCase struct {
	name     string
	dataType schemapb.DataType
	dim      int64
	row0     any
	row2     any
}

const (
	nullableSerdePKFieldID     FieldID = common.StartOfUserFieldID
	nullableSerdeVectorFieldID FieldID = common.StartOfUserFieldID + 1
)

func nullableDenseVectorSerdeCases() []nullableDenseVectorSerdeCase {
	return []nullableDenseVectorSerdeCase{
		{
			name:     "FloatVector",
			dataType: schemapb.DataType_FloatVector,
			dim:      2,
			row0:     []float32{1, 2},
			row2:     []float32{3, 4},
		},
		{
			name:     "BinaryVector",
			dataType: schemapb.DataType_BinaryVector,
			dim:      16,
			row0:     []byte{0x11, 0x12},
			row2:     []byte{0x21, 0x22},
		},
		{
			name:     "Float16Vector",
			dataType: schemapb.DataType_Float16Vector,
			dim:      2,
			row0:     []byte{1, 2, 3, 4},
			row2:     []byte{5, 6, 7, 8},
		},
		{
			name:     "BFloat16Vector",
			dataType: schemapb.DataType_BFloat16Vector,
			dim:      2,
			row0:     []byte{11, 12, 13, 14},
			row2:     []byte{15, 16, 17, 18},
		},
		{
			name:     "Int8Vector",
			dataType: schemapb.DataType_Int8Vector,
			dim:      2,
			row0:     []int8{1, 2},
			row2:     []int8{3, 4},
		},
	}
}

func nullableDenseVectorSerdeSchema(dataType schemapb.DataType, dim int64) *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Fields: []*schemapb.FieldSchema{
			{
				FieldID:  common.RowIDField,
				Name:     common.RowIDFieldName,
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:  common.TimeStampField,
				Name:     common.TimeStampFieldName,
				DataType: schemapb.DataType_Int64,
			},
			{
				FieldID:      nullableSerdePKFieldID,
				Name:         "pk",
				DataType:     schemapb.DataType_Int64,
				IsPrimaryKey: true,
			},
			{
				FieldID:  nullableSerdeVectorFieldID,
				Name:     "nullable_vector",
				DataType: dataType,
				Nullable: true,
				TypeParams: []*commonpb.KeyValuePair{
					{Key: common.DimKey, Value: strconv.FormatInt(dim, 10)},
				},
			},
		},
	}
}

func nullableDenseVectorSerdeInsertData(t *testing.T, tc nullableDenseVectorSerdeCase, schema *schemapb.CollectionSchema) *InsertData {
	t.Helper()

	vectorField := schema.GetFields()[3]
	vectorData, err := NewFieldData(tc.dataType, vectorField, 3)
	require.NoError(t, err)
	require.NoError(t, vectorData.AppendRow(tc.row0))
	require.NoError(t, vectorData.AppendRow(nil))
	require.NoError(t, vectorData.AppendRow(tc.row2))

	return &InsertData{
		Data: map[FieldID]FieldData{
			common.RowIDField:          &Int64FieldData{Data: []int64{11, 12, 13}},
			common.TimeStampField:      &Int64FieldData{Data: []int64{101, 102, 103}},
			nullableSerdePKFieldID:     &Int64FieldData{Data: []int64{1, 2, 3}},
			nullableSerdeVectorFieldID: vectorData,
		},
	}
}

func TestCompositeBinlogRecordWriter_TTLFieldCollection(t *testing.T) {
	ttlFieldID := FieldID(100)
	w := &CompositeBinlogRecordWriter{
		// avoid initWriters() side-effects in Write()
		rw: &MockRecordWriter{
			writefn: func(Record) error { return nil },
			closefn: func() error { return nil },
		},
		pkCollector:   &PkStatsCollector{pkstats: nil},
		bm25Collector: &Bm25StatsCollector{bm25Stats: map[int64]*BM25Stats{}},
		chunkSize:     1<<63 - 1,
		ttlFieldID:    ttlFieldID,
	}

	// Build a record with timestamp + nullable ttl field:
	// ttl values: [10, -1, 0, null, 20] -> only [10, 20] should be collected.
	rows := 5
	tsBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	tsBuilder.AppendValues([]int64{1, 2, 3, 4, 5}, nil)
	tsArr := tsBuilder.NewArray()
	tsBuilder.Release()
	defer tsArr.Release()

	ttlBuilder := array.NewInt64Builder(memory.DefaultAllocator)
	ttlBuilder.AppendValues([]int64{10, -1, 0, 30, 20}, []bool{true, true, true, false, true})
	ttlArr := ttlBuilder.NewArray()
	ttlBuilder.Release()
	defer ttlArr.Release()

	ar := array.NewRecord(
		arrow.NewSchema(
			[]arrow.Field{
				{Name: "ts", Type: arrow.PrimitiveTypes.Int64},
				{Name: "ttl", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			},
			nil,
		),
		[]arrow.Array{tsArr, ttlArr},
		int64(rows),
	)
	r := NewSimpleArrowRecord(ar, map[FieldID]int{
		common.TimeStampField: 0,
		ttlFieldID:            1,
	})
	defer r.Release()

	err := w.Write(r)
	assert.NoError(t, err)

	assert.Equal(t, int64(rows), w.rowNum)
	assert.ElementsMatch(t, []int64{10, 20}, w.ttlFieldValues)

	neverExpire := int64(^uint64(0) >> 1)
	got := w.GetExpirQuantiles()
	assert.Equal(t, []int64{10, 20, neverExpire, neverExpire, neverExpire}, got)
}

func TestBinlogValueWriter(t *testing.T) {
	t.Run("test empty data", func(t *testing.T) {
		reader, err := NewBinlogDeserializeReader(generateTestSchema(), func() ([]*Blob, error) {
			return nil, io.EOF
		}, false)
		assert.NoError(t, err)
		defer reader.Close()
		_, err = reader.NextValue()
		assert.Equal(t, io.EOF, err)
	})

	t.Run("test serialize", func(t *testing.T) {
		size := 16
		blobs, err := generateTestData(size)
		assert.NoError(t, err)
		reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs), false)
		assert.NoError(t, err)
		defer reader.Close()

		schema := generateTestSchema()
		// Copy write the generated data
		writers := NewBinlogStreamWriters(0, 0, 0, schema)
		writer, err := NewBinlogSerializeWriter(schema, 0, 0, writers, 7)
		assert.NoError(t, err)

		for i := 1; i <= size; i++ {
			value, err := reader.NextValue()
			assert.NoError(t, err)

			assertTestData(t, i, *value)
			err = writer.WriteValue(*value)
			assert.NoError(t, err)
		}

		for _, f := range schema.Fields {
			props := writers[f.FieldID].rw.writerProps
			assert.Equal(t, !f.IsPrimaryKey, props.DictionaryEnabled())
		}

		_, err = reader.NextValue()
		assert.Equal(t, io.EOF, err)
		err = writer.Close()
		assert.NoError(t, err)
		assert.True(t, writer.GetWrittenUncompressed() >= 429)

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
		reader, err = NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(newblobs), false)
		assert.NoError(t, err)
		defer reader.Close()
		for i := 1; i <= size; i++ {
			value, err := reader.NextValue()
			assert.NoError(t, err, i)

			assertTestData(t, i, *value)
		}
	})
}

func TestSize(t *testing.T) {
	t.Run("test array of int", func(t *testing.T) {
		size := 100
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{
				FieldID:     18,
				Name:        "array",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_Int32,
			},
		}}

		writers := NewBinlogStreamWriters(0, 0, 0, schema)
		writer, err := NewBinlogSerializeWriter(schema, 0, 0, writers, 7)
		assert.NoError(t, err)

		for i := 0; i < size; i++ {
			e := int32(i)
			d := []int32{e, e, e, e, e, e, e, e}
			value := &Value{
				Value: map[FieldID]any{
					18: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_IntData{
							IntData: &schemapb.IntArray{Data: d},
						},
					},
				},
			}
			err := writer.WriteValue(value)
			assert.NoError(t, err)
		}

		err = writer.Close()
		assert.NoError(t, err)
		memSize := writer.GetWrittenUncompressed()
		assert.Greater(t, memSize, uint64(8*4*size)) // written memory size should greater than data size
		t.Log("writtern memory size", memSize)
	})

	t.Run("test array of varchar", func(t *testing.T) {
		size := 100
		schema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
			{
				FieldID:     18,
				Name:        "array",
				DataType:    schemapb.DataType_Array,
				ElementType: schemapb.DataType_String,
			},
		}}

		writers := NewBinlogStreamWriters(0, 0, 0, schema)
		writer, err := NewBinlogSerializeWriter(schema, 0, 0, writers, 7)
		assert.NoError(t, err)

		for i := 0; i < size; i++ {
			e := fmt.Sprintf("%4d", i)
			d := []string{e, e, e, e, e, e, e, e}
			value := &Value{
				Value: map[FieldID]any{
					18: &schemapb.ScalarField{
						Data: &schemapb.ScalarField_StringData{
							StringData: &schemapb.StringArray{Data: d},
						},
					},
				},
			}
			err := writer.WriteValue(value)
			assert.NoError(t, err)
		}

		err = writer.Close()
		assert.NoError(t, err)
		memSize := writer.GetWrittenUncompressed()
		assert.Greater(t, memSize, uint64(8*4*size)) // written memory size should greater than data size
		t.Log("writtern memory size", memSize)
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
	mlog.Info(context.TODO(), "prepare data done", mlog.Int("len", len(values)), mlog.Duration("dur", time.Since(start)))

	b.ResetTimer()

	sizes := []int{100, 1000, 10000, 100000}
	for _, s := range sizes {
		b.Run(fmt.Sprintf("batch size=%d", s), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				writers := NewBinlogStreamWriters(0, 0, 0, schema)
				writer, err := NewBinlogSerializeWriter(schema, 0, 0, writers, s)
				assert.NoError(b, err)
				for _, v := range values {
					_ = writer.WriteValue(v)
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
		writers := NewBinlogStreamWriters(0, 0, 0, schema)
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
		writer.WriteValue(value)
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
		reader, err := NewBinlogDeserializeReader(generateTestSchema(), MakeBlobsReader(blobs), false)
		assert.NoError(t, err)
		defer reader.Close()
		v, err := reader.NextValue()
		assert.NoError(t, err)

		assert.Equal(t, value, *v)
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
		_, err = reader.NextValue()
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
			value, err := reader.NextValue()
			assert.NoError(t, err)

			assertTestDeltalogData(t, i, *value)
		}

		_, err = reader.NextValue()
		assert.Equal(t, io.EOF, err)
	})
}

func TestDeltalogSerializeWriter(t *testing.T) {
	t.Run("test empty data", func(t *testing.T) {
		reader, err := newDeltalogDeserializeReader(nil)
		assert.NoError(t, err)
		defer reader.Close()
		_, err = reader.NextValue()
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
			value, err := reader.NextValue()
			assert.NoError(t, err)

			assertTestDeltalogData(t, i, *value)
			err = writer.WriteValue(*value)
			assert.NoError(t, err)
		}

		_, err = reader.NextValue()
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
			value, err := reader.NextValue()
			assert.NoError(t, err, i)

			assertTestDeltalogData(t, i, *value)
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
				value, err := reader.NextValue()
				assert.NoError(t, err)

				tc.assertPk(t, i, *value)
			}
			_, err = reader.NextValue()
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
				writer.WriteValue(value)
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
		if err = writer.WriteValue(value); err != nil {
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
		_, err = reader.NextValue()
		if err != nil {
			return err
		}
	}
	return nil
}

func TestMakeBlobsReader(t *testing.T) {
	type args struct {
		blobs []string
	}
	tests := []struct {
		name string
		args args
		want [][]string
	}{
		{
			name: "test empty",
			args: args{
				blobs: nil,
			},
			want: nil,
		},
		{
			name: "test aligned",
			args: args{
				blobs: []string{
					"x/1/1/1/1/1",
					"x/1/1/1/2/2",
					"x/1/1/1/3/3",
					"x/1/1/1/1/4",
					"x/1/1/1/2/5",
					"x/1/1/1/3/6",
					"x/1/1/1/1/7",
					"x/1/1/1/2/8",
					"x/1/1/1/3/9",
				},
			},
			want: [][]string{
				{"x/1/1/1/1/1", "x/1/1/1/2/2", "x/1/1/1/3/3"},
				{"x/1/1/1/1/4", "x/1/1/1/2/5", "x/1/1/1/3/6"},
				{"x/1/1/1/1/7", "x/1/1/1/2/8", "x/1/1/1/3/9"},
			},
		},
		{
			name: "test added field",
			args: args{
				blobs: []string{
					"x/1/1/1/1/1",
					"x/1/1/1/2/2",
					"x/1/1/1/1/3",
					"x/1/1/1/2/4",
					"x/1/1/1/1/5",
					"x/1/1/1/2/6",
					"x/1/1/1/3/7",
				},
			},

			want: [][]string{
				{"x/1/1/1/1/1", "x/1/1/1/2/2"},
				{"x/1/1/1/1/3", "x/1/1/1/2/4"},
				{"x/1/1/1/1/5", "x/1/1/1/2/6", "x/1/1/1/3/7"},
			},
		},
		{
			name: "test if there is a hole",
			args: args{
				blobs: []string{
					"x/1/1/1/1/1",
					"x/1/1/1/2/2",
					"x/1/1/1/3/3",
					"x/1/1/1/1/4",
					"x/1/1/1/2/5",
					"x/1/1/1/1/6",
					"x/1/1/1/2/7",
					"x/1/1/1/3/8",
				},
			},

			want: [][]string{
				{"x/1/1/1/1/1", "x/1/1/1/2/2", "x/1/1/1/3/3"},
				{"x/1/1/1/1/4", "x/1/1/1/2/5"},
				{"x/1/1/1/1/6", "x/1/1/1/2/7", "x/1/1/1/3/8"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			blobs := lo.Map(tt.args.blobs, func(item string, index int) *Blob {
				return &Blob{
					Key: item,
				}
			})
			reader := MakeBlobsReader(blobs)
			got := make([][]string, 0)
			for {
				bs, err := reader()
				if err == io.EOF {
					break
				}
				if err != nil {
					assert.Fail(t, err.Error())
				}
				got = append(got, lo.Map(bs, func(item *Blob, index int) string {
					return item.Key
				}))
			}
			assert.ElementsMatch(t, tt.want, got)
		})
	}
}
