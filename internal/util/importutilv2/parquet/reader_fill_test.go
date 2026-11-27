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
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/apache/arrow/go/v17/parquet"
	"github.com/apache/arrow/go/v17/parquet/pqarrow"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
	"github.com/milvus-io/milvus/internal/mocks"
	"github.com/milvus-io/milvus/internal/storage"
	importcommon "github.com/milvus-io/milvus/internal/util/importutilv2/common"
	"github.com/milvus-io/milvus/pkg/v3/util/typeutil"
)

const fillElementsPerRow = 4

// fillTestSchema has one narrow sub-field the file carries (s[id]) and five
// element-nullable sub-fields it omits: three VarChar, a nested Array and a
// vector.
func fillTestSchema() *schemapb.CollectionSchema {
	return enStructSchema(
		enScalarSubField(201, "s[id]", schemapb.DataType_Int64, false),
		enScalarSubField(202, "s[a]", schemapb.DataType_VarChar, true),
		enScalarSubField(203, "s[b]", schemapb.DataType_VarChar, true),
		enScalarSubField(204, "s[c]", schemapb.DataType_VarChar, true),
		enNestedSubField(205, "s[tags]", schemapb.DataType_Int64, true, true),
		enVectorSubField(206, "s[emb]", schemapb.DataType_FloatVector, true),
	)
}

// fillTestParquet writes rows of pk and s = list<struct<id: int64>> with
// fillElementsPerRow elements each.
func fillTestParquet(t *testing.T, rows int) []byte {
	t.Helper()
	mem := memory.NewGoAllocator()
	structType := arrow.StructOf(arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
	arrowSchema := arrow.NewSchema([]arrow.Field{
		{Name: "pk", Type: arrow.PrimitiveTypes.Int64},
		{Name: "s", Type: arrow.ListOf(structType), Nullable: true},
	}, nil)
	pkBuilder := array.NewInt64Builder(mem)
	lb := array.NewListBuilder(mem, structType)
	sb := lb.ValueBuilder().(*array.StructBuilder)
	idBuilder := sb.FieldBuilder(0).(*array.Int64Builder)
	for i := 0; i < rows; i++ {
		pkBuilder.Append(int64(i))
		lb.Append(true)
		for j := 0; j < fillElementsPerRow; j++ {
			sb.Append(true)
			idBuilder.Append(int64(i*10 + j))
		}
	}
	pkArray := pkBuilder.NewArray()
	pkBuilder.Release()
	defer pkArray.Release()
	structArray := lb.NewArray()
	lb.Release()
	defer structArray.Release()
	record := array.NewRecord(arrowSchema, []arrow.Array{pkArray, structArray}, int64(rows))
	defer record.Release()

	var buf bytes.Buffer
	fw, err := pqarrow.NewFileWriter(arrowSchema, &buf, parquet.NewWriterProperties(parquet.WithMaxRowGroupLength(64)), pqarrow.DefaultWriterProps())
	require.NoError(t, err)
	require.NoError(t, fw.Write(record))
	require.NoError(t, fw.Close())
	return buf.Bytes()
}

func newFillTestReader(t *testing.T, data []byte, bufferSize int) *reader {
	t.Helper()
	cm := mocks.NewChunkManager(t)
	cm.EXPECT().Reader(mock.Anything, "fill.parquet").Return(importcommon.NewMockReader(string(data)), nil)
	r, err := NewReader(context.Background(), cm, fillTestSchema(), "fill.parquet", bufferSize)
	require.NoError(t, err)
	t.Cleanup(r.Close)
	return r
}

// TestParquetReaderFillsMissingSubFieldsWithinBuffer reads a file that omits
// several element-nullable sub-fields with a small buffer. The reader fills
// them chunk by chunk before its memory check, so every batch it hands out is
// complete and, as the reader reads whole chunks until the batch reaches
// bufferSize, stays below bufferSize plus one chunk. Filling after the read
// instead would let the narrow s[id] column alone decide the batch size and
// grow each batch far beyond that bound.
func TestParquetReaderFillsMissingSubFieldsWithinBuffer(t *testing.T) {
	const rows = 600
	const bufferSize = 16 * 1024
	data := fillTestParquet(t, rows)
	schema := fillTestSchema()
	subFields := schema.GetStructArrayFields()[0].GetFields()

	// The memory of one filled row, from a reader that reads one row per batch.
	one := newFillTestReader(t, data, 1)
	require.EqualValues(t, 1, one.count)
	first, err := one.Read()
	require.NoError(t, err)
	require.Equal(t, 1, first.GetRowNum())
	rowMemory := first.GetMemorySize()

	r := newFillTestReader(t, data, bufferSize)
	require.Greater(t, r.count, int64(1))
	chunkMemory := int(r.count) * rowMemory
	require.Less(t, chunkMemory, bufferSize, "the test needs batches of several chunks")

	batches := 0
	total := 0
	for {
		batch, err := r.Read()
		if err == io.EOF {
			break
		}
		require.NoError(t, err)
		batches++
		n := batch.Data[100].RowNum()
		total += n
		require.Less(t, batch.GetMemorySize(), bufferSize+chunkMemory, "batch %d with %d rows", batches, n)
		for _, subField := range subFields {
			fieldData := batch.Data[subField.GetFieldID()]
			require.Equal(t, n, fieldData.RowNum(), subField.GetName())
			if !subField.GetElementNullable() {
				continue
			}
			for i := 0; i < n; i++ {
				var valid []bool
				switch fd := fieldData.(type) {
				case *storage.ArrayFieldData:
					valid = typeutil.GetArrayElementValidData(fd.Data[i])
				case *storage.VectorArrayFieldData:
					valid = typeutil.GetVectorArrayElementValidData(fd.Data[i])
				}
				require.Equal(t, make([]bool, fillElementsPerRow), valid, "%s row %d", subField.GetName(), i)
			}
		}
	}
	require.Equal(t, rows, total)
	require.Greater(t, batches, 1)
}
