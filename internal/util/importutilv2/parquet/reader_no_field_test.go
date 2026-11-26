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

package parquet

import (
	"context"
	"fmt"
	"math/rand"
	"os"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/storage"
	"github.com/milvus-io/milvus/internal/util/testutil"
	"github.com/milvus-io/milvus/pkg/v3/objectstorage"
)

// A parquet file whose columns match no field of the collection cannot produce
// a row. Before the check, NewReader succeeded with no column reader and Read
// looped forever; a vector field, never nullable and always required, used to
// rule this out. A file that matches at least one column is still accepted.
func TestParquetReaderRejectsFileWithNoCollectionField(t *testing.T) {
	ctx := context.Background()
	// the file holds a single int64 column named "other"
	fileSchema := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "other", IsPrimaryKey: true, DataType: schemapb.DataType_Int64},
	}}
	filePath := fmt.Sprintf("/tmp/test_%d_no_field.parquet", rand.Int())
	wf, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0o666)
	require.NoError(t, err)
	defer os.Remove(filePath)
	pqSchema, err := ConvertToArrowSchemaForUT(fileSchema, false)
	require.NoError(t, err)
	fw, err := pqarrow.NewFileWriter(pqSchema, wf, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(3)), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	insertData, err := testutil.CreateInsertData(fileSchema, 3, 0)
	require.NoError(t, err)
	columns, err := testutil.BuildArrayData(fileSchema, insertData, false)
	require.NoError(t, err)
	require.NoError(t, fw.Write(array.NewRecord(pqSchema, columns, 3)))
	require.NoError(t, fw.Close())

	f := storage.NewChunkManagerFactory("local", objectstorage.RootPath(testOutputPath))
	cm, err := f.NewPersistentStorageChunkManager(ctx)
	require.NoError(t, err)

	// every field of the collection is optional, so the schema check passes
	// and only the "no column" check can reject the file
	collection := &schemapb.CollectionSchema{Fields: []*schemapb.FieldSchema{
		{FieldID: 100, Name: "id", IsPrimaryKey: true, AutoID: true, DataType: schemapb.DataType_Int64},
		{FieldID: 101, Name: "age", DataType: schemapb.DataType_Int32, Nullable: true},
	}}
	_, err = NewReader(ctx, cm, collection, filePath, 64*1024*1024)
	require.Error(t, err)
	require.Contains(t, err.Error(), "provides no field of the collection")

	// the same file is accepted once a column matches
	collection.Fields[1].Name = "other"
	collection.Fields[1].DataType = schemapb.DataType_Int64
	reader, err := NewReader(ctx, cm, collection, filePath, 64*1024*1024)
	require.NoError(t, err)
	reader.Close()
}
