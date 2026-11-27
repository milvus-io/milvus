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

package testutil_test

import (
	"context"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/cockroachdb/errors"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/milvus-io/milvus-proto/go-api/v3/commonpb"
	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/json"
	"github.com/milvus-io/milvus/internal/storage"
	csvreader "github.com/milvus-io/milvus/internal/util/importutilv2/csv"
	jsonreader "github.com/milvus-io/milvus/internal/util/importutilv2/json"
	parquetreader "github.com/milvus-io/milvus/internal/util/importutilv2/parquet"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/common"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

// The generators produce element-nullable and nested Array struct sub-fields
// in the layout the import readers produce, and write them in the JSON, CSV and
// Parquet shapes the readers accept. Writing the generated data and reading it
// back must give the same rows.

func roundTripCapacity(capacity int) []*commonpb.KeyValuePair {
	return []*commonpb.KeyValuePair{{Key: common.MaxCapacityKey, Value: strconv.Itoa(capacity)}}
}

func roundTripScalar(id int64, name string, elementType schemapb.DataType, elementNullable bool) *schemapb.FieldSchema {
	params := roundTripCapacity(32)
	if typeutil.IsStringType(elementType) {
		params = append(params, &commonpb.KeyValuePair{Key: common.MaxLengthKey, Value: "64"})
	}
	return &schemapb.FieldSchema{
		FieldID:         id,
		Name:            typeutil.ConcatStructFieldName("s", name),
		DataType:        schemapb.DataType_Array,
		ElementType:     elementType,
		ElementNullable: elementNullable,
		Nullable:        true,
		TypeParams:      params,
	}
}

func roundTripNested(id int64, name string, leaf schemapb.DataType, elementNullable, leafNullable bool) *schemapb.FieldSchema {
	var leafParams []*commonpb.KeyValuePair
	if typeutil.IsStringType(leaf) {
		leafParams = []*commonpb.KeyValuePair{{Key: common.MaxLengthKey, Value: "64"}}
	}
	return &schemapb.FieldSchema{
		FieldID:         id,
		Name:            typeutil.ConcatStructFieldName("s", name),
		DataType:        schemapb.DataType_Array,
		ElementType:     schemapb.DataType_Array,
		ElementNullable: elementNullable,
		Nullable:        true,
		TypeSchema: &schemapb.TypeSchema{
			Nullable:   true,
			TypeParams: roundTripCapacity(32),
			Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
				Nullable:   elementNullable,
				TypeParams: roundTripCapacity(8),
				Kind: &schemapb.TypeSchema_ArrayElement{ArrayElement: &schemapb.TypeSchema{
					Nullable:   leafNullable,
					TypeParams: leafParams,
					Kind:       &schemapb.TypeSchema_LeafType{LeafType: leaf},
				}},
			}},
		},
	}
}

func roundTripVector(id int64, name string, elementNullable bool) *schemapb.FieldSchema {
	return &schemapb.FieldSchema{
		FieldID:         id,
		Name:            typeutil.ConcatStructFieldName("s", name),
		DataType:        schemapb.DataType_ArrayOfVector,
		ElementType:     schemapb.DataType_FloatVector,
		ElementNullable: elementNullable,
		Nullable:        true,
		TypeParams: []*commonpb.KeyValuePair{
			{Key: common.DimKey, Value: "4"},
			{Key: common.MaxCapacityKey, Value: "32"},
		},
	}
}

func roundTripSchema() *schemapb.CollectionSchema {
	return &schemapb.CollectionSchema{
		Name: "struct_round_trip",
		Fields: []*schemapb.FieldSchema{
			{FieldID: 100, Name: "pk", DataType: schemapb.DataType_Int64, IsPrimaryKey: true},
			{
				FieldID: 101, Name: "vec", DataType: schemapb.DataType_FloatVector,
				TypeParams: []*commonpb.KeyValuePair{{Key: common.DimKey, Value: "4"}},
			},
		},
		StructArrayFields: []*schemapb.StructArrayFieldSchema{{
			FieldID:  200,
			Name:     "s",
			Nullable: true,
			Fields: []*schemapb.FieldSchema{
				roundTripScalar(201, "id", schemapb.DataType_Int64, false),
				roundTripScalar(202, "name", schemapb.DataType_VarChar, true),
				roundTripScalar(203, "i8", schemapb.DataType_Int8, true),
				roundTripScalar(204, "i16", schemapb.DataType_Int16, true),
				roundTripScalar(205, "f", schemapb.DataType_Float, true),
				roundTripScalar(206, "d", schemapb.DataType_Double, true),
				roundTripScalar(207, "flag", schemapb.DataType_Bool, true),
				roundTripNested(208, "tags", schemapb.DataType_Int64, true, true),
				roundTripNested(209, "words", schemapb.DataType_VarChar, false, true),
				roundTripNested(210, "codes", schemapb.DataType_Int32, true, false),
				roundTripVector(211, "emb", true),
				roundTripVector(212, "emb2", false),
			},
		}},
	}
}

func roundTripChunkManager(t *testing.T) storage.ChunkManager {
	factory := storage.NewChunkManagerFactory("local", objectstorage.RootPath(t.TempDir()))
	cm, err := factory.NewPersistentStorageChunkManager(context.Background())
	require.NoError(t, err)
	return cm
}

type roundTripReader interface {
	Read() (*storage.InsertData, error)
	Close()
}

func roundTripReadAll(t *testing.T, reader roundTripReader) *storage.InsertData {
	defer reader.Close()
	var all *storage.InsertData
	for {
		data, err := reader.Read()
		if errors.Is(err, io.EOF) {
			break
		}
		require.NoError(t, err)
		if all == nil {
			all = data
			continue
		}
		require.NoError(t, storage.MergeInsertData(all, data))
	}
	require.NotNil(t, all)
	return all
}

func roundTripRequireSame(t *testing.T, schema *schemapb.CollectionSchema, want, got *storage.InsertData) {
	for _, field := range typeutil.GetAllFieldSchemas(schema) {
		w := want.Data[field.GetFieldID()]
		g, ok := got.Data[field.GetFieldID()]
		require.True(t, ok, "read data lacks field %s", field.GetName())
		require.Equal(t, w.RowNum(), g.RowNum(), "row count of field %s", field.GetName())
		for i := 0; i < w.RowNum(); i++ {
			wantRow, gotRow := w.GetRow(i), g.GetRow(i)
			switch wr := wantRow.(type) {
			case nil:
				require.Nil(t, gotRow, "field %s row %d should be null", field.GetName(), i)
			case proto.Message:
				gr, ok := gotRow.(proto.Message)
				require.True(t, ok, "field %s row %d: read row is %T", field.GetName(), i, gotRow)
				require.True(t, proto.Equal(wr, gr), "field %s row %d\nwant: %v\ngot:  %v", field.GetName(), i, wr, gr)
			default:
				require.Equal(t, wantRow, gotRow, "field %s row %d", field.GetName(), i)
			}
		}
	}
}

func TestStructSubFieldGeneratorsRoundTrip(t *testing.T) {
	ctx := context.Background()
	schema := roundTripSchema()
	const rows = 40
	want, err := testutil.CreateInsertData(schema, rows)
	require.NoError(t, err)

	// the generated data must carry nulls at every element-nullable level
	name := want.Data[202].(*storage.ArrayFieldData)
	sawNullElement := false
	for i := 0; i < rows; i++ {
		if row, ok := name.GetRow(i).(*schemapb.ScalarField); ok && row != nil {
			for _, valid := range typeutil.GetArrayElementValidData(row) {
				sawNullElement = sawNullElement || !valid
			}
		}
	}
	require.True(t, sawNullElement, "generator produced no null element")

	t.Run("json", func(t *testing.T) {
		jsonRows, err := testutil.CreateInsertDataRowsForJSON(schema, want)
		require.NoError(t, err)
		raw, err := json.Marshal(jsonRows)
		require.NoError(t, err)
		path := filepath.Join(t.TempDir(), "data.json")
		require.NoError(t, os.WriteFile(path, raw, 0o600))
		reader, err := jsonreader.NewReader(ctx, roundTripChunkManager(t), schema, path, 64*1024*1024)
		require.NoError(t, err)
		roundTripRequireSame(t, schema, want, roundTripReadAll(t, reader))
	})

	t.Run("csv", func(t *testing.T) {
		const nullKey = "NULL"
		records, err := testutil.CreateInsertDataForCSV(schema, want, nullKey)
		require.NoError(t, err)
		path := filepath.Join(t.TempDir(), "data.csv")
		f, err := os.Create(path)
		require.NoError(t, err)
		writer := csv.NewWriter(f)
		require.NoError(t, writer.WriteAll(records))
		require.NoError(t, f.Close())
		reader, err := csvreader.NewReader(ctx, roundTripChunkManager(t), schema, path, 64*1024*1024, ',', nullKey)
		require.NoError(t, err)
		roundTripRequireSame(t, schema, want, roundTripReadAll(t, reader))
	})

	t.Run("parquet", func(t *testing.T) {
		arrowSchema, err := parquetreader.ConvertToArrowSchemaForUT(schema, false)
		require.NoError(t, err)
		columns, err := testutil.BuildArrayData(schema, want, false)
		require.NoError(t, err)
		record := array.NewRecord(arrowSchema, columns, rows)
		defer record.Release()
		path := filepath.Join(t.TempDir(), "data.parquet")
		f, err := os.Create(path)
		require.NoError(t, err)
		fw, err := pqarrow.NewFileWriter(arrowSchema, f, parquet.NewWriterProperties(), pqarrow.DefaultWriterProps())
		require.NoError(t, err)
		require.NoError(t, fw.Write(record))
		require.NoError(t, fw.Close())
		reader, err := parquetreader.NewReader(ctx, roundTripChunkManager(t), schema, path, 64*1024*1024)
		require.NoError(t, err)
		roundTripRequireSame(t, schema, want, roundTripReadAll(t, reader))
	})
}
