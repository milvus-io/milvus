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
	"io"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/msgpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/allocator"
	"github.com/milvus-io/milvus/internal/storagecommon"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/mq/msgstream"
	"github.com/milvus-io/milvus/pkg/v3/proto/indexpb"
	"github.com/milvus-io/milvus/pkg/v3/util/paramtable"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const (
	w6bPKFieldID       = int64(100)
	w6bLegacyFieldID   = int64(101)
	w6bStructFieldID   = int64(200)
	w6bNullableFieldID = int64(201)
	w6bStringFieldID   = int64(202)
	w6bIntFieldID      = int64(203)
	w6bVectorFieldID   = int64(204)
	w6bOldVectorID     = int64(205)
)

func w6bNestedType(leaf schemapb.DataType, innerNullable, leafNullable bool) *schemapb.TypeSchema {
	leafType := &schemapb.TypeSchema{Nullable: leafNullable, Kind: &schemapb.TypeSchema_LeafType{LeafType: leaf}}
	inner := &schemapb.TypeSchema{Nullable: innerNullable, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: leafType}}
	return &schemapb.TypeSchema{Nullable: true, Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: inner}}
}

func w6bPipelineSchema() *schemapb.CollectionSchema {
	dim := []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "2"}}
	return &schemapb.CollectionSchema{
		Name: "native_array_pipeline",
		Fields: []*schemapb.FieldSchema{
			{FieldID: common.RowIDField, Name: common.RowIDFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: common.TimeStampField, Name: common.TimeStampFieldName, DataType: schemapb.DataType_Int64},
			{FieldID: w6bPKFieldID, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{FieldID: w6bLegacyFieldID, Name: "legacy", DataType: schemapb.DataType_Array, ElementType: schemapb.DataType_Int64},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID: w6bStructFieldID, Name: "s", Nullable: true,
			Fields: []*schemapb.FieldSchema{
				{FieldID: w6bNullableFieldID, Name: "s[nullable_ints]", DataType: schemapb.DataType_Array,
					ElementType: schemapb.DataType_Int64, Nullable: true, ElementNullable: true},
				{FieldID: w6bStringFieldID, Name: "s[strings]", DataType: schemapb.DataType_Array,
					ElementType: schemapb.DataType_Array, Nullable: true, ElementNullable: true,
					TypeSchema: w6bNestedType(schemapb.DataType_VarChar, true, true)},
				{FieldID: w6bIntFieldID, Name: "s[ints]", DataType: schemapb.DataType_Array,
					ElementType: schemapb.DataType_Array, Nullable: true,
					TypeSchema: w6bNestedType(schemapb.DataType_Int32, false, false)},
				{FieldID: w6bVectorFieldID, Name: "s[nullable_vectors]", DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector, Nullable: true, ElementNullable: true, TypeParams: dim},
				{FieldID: w6bOldVectorID, Name: "s[vectors]", DataType: schemapb.DataType_ArrayOfVector,
					ElementType: schemapb.DataType_FloatVector, Nullable: true, TypeParams: dim},
			},
		}},
	}
}

func w6bScalarArrayFieldData(id int64, rows []*schemapb.ScalarField, valid []bool) *schemapb.FieldData {
	fd := &schemapb.FieldData{FieldId: id, Type: schemapb.DataType_Array,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{Data: rows}},
		}}}
	if valid != nil {
		typeutil.SetFieldDataValidData(fd, valid)
	}
	return fd
}

func w6bVectorArrayFieldData(id int64, rows []*schemapb.VectorField, valid []bool) *schemapb.FieldData {
	fd := &schemapb.FieldData{FieldId: id, Type: schemapb.DataType_ArrayOfVector,
		Field: &schemapb.FieldData_Vectors{Vectors: &schemapb.VectorField{
			Data: &schemapb.VectorField_VectorArray{VectorArray: &schemapb.VectorArray{
				Dim: 2, ElementType: schemapb.DataType_FloatVector, Data: rows,
			}},
		}}}
	typeutil.SetFieldDataValidData(fd, valid)
	return fd
}

func w6bPipelineInsertMsg(t *testing.T) *msgstream.InsertMsg {
	t.Helper()
	longRow := func(v ...int64) *schemapb.ScalarField {
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: v}}}
	}
	stringRow := func(v ...string) *schemapb.ScalarField {
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_StringData{StringData: &schemapb.StringArray{Data: v}}}
	}
	intRow := func(v ...int32) *schemapb.ScalarField {
		return &schemapb.ScalarField{Data: &schemapb.ScalarField_IntData{IntData: &schemapb.IntArray{Data: v}}}
	}
	vectorRow := func(v ...float32) *schemapb.VectorField {
		return &schemapb.VectorField{Dim: 2, Data: &schemapb.VectorField_FloatVector{FloatVector: &schemapb.FloatArray{Data: v}}}
	}
	nullVector, err := typeutil.NewEmptyArrayOfVectorRow(2, schemapb.DataType_FloatVector)
	require.NoError(t, err)
	validRows := []bool{true, false, true}
	nullableInt := longRow(11, 0, 13)
	nullableInt.ValidData = []bool{true, false, true}
	strings := stringRow("a", "", "c")
	strings.ValidData = []bool{true, false, true}
	nestedStrings := &schemapb.ScalarField{ValidData: []bool{true, false, true},
		Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
			ElementType: schemapb.DataType_VarChar,
			Data:        []*schemapb.ScalarField{strings, stringRow(), stringRow()},
		}}}
	nestedInts := &schemapb.ScalarField{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{
		ElementType: schemapb.DataType_Int32,
		Data:        []*schemapb.ScalarField{intRow(1, 2), intRow()},
	}}}
	nullableVectors := vectorRow(1, 2, 3, 4)
	nullableVectors.ValidData = []bool{true, false, true}
	pk := &schemapb.FieldData{FieldId: w6bPKFieldID, Type: schemapb.DataType_Int64,
		Field: &schemapb.FieldData_Scalars{Scalars: &schemapb.ScalarField{
			Data: &schemapb.ScalarField_LongData{LongData: &schemapb.LongArray{Data: []int64{1, 2, 3}}},
		}}}
	msg := &msgstream.InsertMsg{InsertRequest: &msgpb.InsertRequest{
		Base:    &commonpb.MsgBase{MsgType: commonpb.MsgType_Insert},
		Version: msgpb.InsertDataVersion_ColumnBased, NumRows: 3,
		RowIDs: []int64{11, 12, 13}, Timestamps: []uint64{101, 102, 103},
		FieldsData: []*schemapb.FieldData{
			pk,
			w6bScalarArrayFieldData(w6bLegacyFieldID, []*schemapb.ScalarField{longRow(7, 8), longRow(9), longRow()}, nil),
			w6bScalarArrayFieldData(w6bNullableFieldID, []*schemapb.ScalarField{nullableInt, nil, longRow()}, validRows),
			w6bScalarArrayFieldData(w6bStringFieldID, []*schemapb.ScalarField{nestedStrings, nil,
				{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_VarChar}}}}, validRows),
			w6bScalarArrayFieldData(w6bIntFieldID, []*schemapb.ScalarField{nestedInts, nil,
				{Data: &schemapb.ScalarField_ArrayData{ArrayData: &schemapb.ArrayArray{ElementType: schemapb.DataType_Int32}}}}, validRows),
			w6bVectorArrayFieldData(w6bVectorFieldID, []*schemapb.VectorField{nullableVectors, nullVector, vectorRow()}, validRows),
			w6bVectorArrayFieldData(w6bOldVectorID, []*schemapb.VectorField{vectorRow(5, 6, 7, 8), nullVector, vectorRow()}, validRows),
		},
	}}
	return msg
}

func w6bAssertPipelineRows(t *testing.T, want, got *InsertData, offset int) {
	t.Helper()
	require.Len(t, got.Data, len(want.Data))
	for id, wantField := range want.Data {
		gotField := got.Data[id]
		require.NotNil(t, gotField, "field %d", id)
		if wantField.GetNullable() {
			require.Equal(t, wantField.GetValidData()[offset:offset+gotField.RowNum()], gotField.GetValidData(), "field %d row validity", id)
		}
		for i := 0; i < gotField.RowNum(); i++ {
			wantRow := wantField.GetRow(offset + i)
			gotRow := gotField.GetRow(i)
			switch w := wantRow.(type) {
			case *schemapb.ScalarField:
				require.True(t, proto.Equal(w, gotRow.(*schemapb.ScalarField)), "field %d row %d", id, offset+i)
			case *schemapb.VectorField:
				require.True(t, proto.Equal(w, gotRow.(*schemapb.VectorField)), "field %d row %d", id, offset+i)
			default:
				require.Equal(t, wantRow, gotRow, "field %d row %d", id, offset+i)
			}
		}
	}
}

func TestNativeArrayInsertMsgPackedWriterReaderPipeline(t *testing.T) {
	paramtable.Init()
	pt := paramtable.Get()
	pt.Save(pt.CommonCfg.StorageType.Key, "local")
	pt.Save(pt.LocalStorageCfg.Path.Key, t.TempDir())
	t.Cleanup(func() { pt.Reset(pt.CommonCfg.StorageType.Key); pt.Reset(pt.LocalStorageCfg.Path.Key) })
	schema := w6bPipelineSchema()
	msg := w6bPipelineInsertMsg(t)
	insertData, err := ColumnBasedInsertMsgToInsertData(msg, schema)
	require.NoError(t, err)
	require.Equal(t, 3, insertData.GetRowNum())
	arrowSchema, err := ConvertToArrowSchema(schema, true)
	require.NoError(t, err)
	builder := array.NewRecordBuilder(memory.DefaultAllocator, arrowSchema)
	defer builder.Release()
	require.NoError(t, BuildRecord(builder, insertData, schema))
	fields := typeutil.GetAllFieldSchemas(schema)
	field2Col := make(map[FieldID]int, len(fields))
	for i, field := range fields {
		field2Col[field.GetFieldID()] = i
	}
	record := NewSimpleArrowRecord(builder.NewRecord(), field2Col)
	defer record.Release()
	require.Equal(t, 3, record.Len())

	for _, id := range []int64{w6bNullableFieldID, w6bStringFieldID, w6bIntFieldID, w6bVectorFieldID, w6bOldVectorID} {
		require.Equal(t, 1, record.Column(id).NullN(), "row null count for field %d", id)
		require.Positive(t, ActualSizeInBytes(record.Column(id).Data()))
		require.Less(t, ActualSizeInBytes(record.Column(id).Data()), uint64(4096), "three short rows cannot occupy 4 KiB of logical Arrow data")
	}
	require.Equal(t, 0, record.Column(w6bLegacyFieldID).NullN())
	require.Greater(t, insertData.GetMemorySize(), 0)
	require.Less(t, insertData.GetMemorySize(), 4096, "small proto fixture must not expand beyond 4 KiB")
	for _, id := range []int64{w6bNullableFieldID, w6bStringFieldID, w6bIntFieldID, w6bVectorFieldID} {
		require.Greater(t, insertData.Data[id].GetMemorySize(), 0)
	}
	for _, field := range schema.GetStructArrayFields()[0].GetFields() {
		writer, err := newSingleFieldRecordWriter(field, io.Discard)
		require.NoError(t, err)
		require.Equal(t, 1, writer.memoryExpansionRatio, "native list fields have no proto-width expansion")
	}

	counts := &packedBinlogRecordWriterBase{schema: schema}
	counts.collectNullCounts(record)
	counts.collectNullCounts(record)
	group := storagecommon.ColumnGroup{Fields: []int64{w6bLegacyFieldID, w6bNullableFieldID, w6bStringFieldID,
		w6bIntFieldID, w6bVectorFieldID, w6bOldVectorID}}
	gotCounts := counts.getFieldNullCountsForColumnGroup(group)
	require.Equal(t, int64(0), gotCounts[w6bLegacyFieldID])
	for _, id := range group.Fields[1:] {
		require.Equal(t, int64(2), gotCounts[id], "two batches, one null row each; nested nulls do not count")
	}

	cfg := &indexpb.StorageConfig{StorageType: "local", RootPath: pt.LocalStorageCfg.Path.GetValue()}
	for _, tc := range []struct {
		name    string
		version int64
		format  string
	}{
		{"v2-parquet", StorageV2, "parquet"},
		{"v3-parquet", StorageV3, "parquet"},
		{"v3-vortex", StorageV3, "vortex"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			writer, err := NewBinlogRecordWriter(context.Background(), 1, 2, UniqueID(10+tc.version), schema,
				allocator.NewLocalAllocator(1000, 10000), 1<<20, 3,
				WithVersion(tc.version), WithStorageConfig(cfg), WithWriterFormat(tc.format),
				WithUploader(func(context.Context, map[string][]byte) error { return nil }))
			require.NoError(t, err)
			require.NoError(t, writer.Write(record))
			require.NoError(t, writer.Close())
			logs, _, _, manifest, _ := writer.GetLogs()
			var reader RecordReader
			if tc.version == StorageV3 {
				require.NotEmpty(t, manifest)
				reader, err = NewManifestRecordReader(context.Background(), manifest, schema,
					WithVersion(StorageV3), WithStorageConfig(cfg))
			} else {
				require.Empty(t, manifest)
				reader, err = NewBinlogRecordReader(context.Background(), SortFieldBinlogs(logs), schema,
					WithVersion(StorageV2), WithStorageConfig(cfg))
			}
			require.NoError(t, err)
			defer reader.Close()
			readRows := 0
			for {
				readRecord, err := reader.Next()
				if err == io.EOF {
					break
				}
				require.NoError(t, err)
				got, err := RecordToInsertData(readRecord, schema, nil)
				require.NoError(t, err)
				w6bAssertPipelineRows(t, insertData, got, readRows)
				readRows += readRecord.Len()
			}
			require.Equal(t, 3, readRows)
			logNullCounts := make(map[int64]int64)
			for _, fieldBinlog := range logs {
				for _, binlog := range fieldBinlog.GetBinlogs() {
					for id, n := range binlog.GetFieldNullCounts() {
						logNullCounts[id] += n
					}
				}
			}
			require.Equal(t, int64(0), logNullCounts[w6bLegacyFieldID])
			for _, id := range []int64{w6bNullableFieldID, w6bStringFieldID, w6bIntFieldID, w6bVectorFieldID, w6bOldVectorID} {
				require.Equal(t, int64(1), logNullCounts[id], "on-disk row null count for field %d", id)
			}
		})
	}
}
