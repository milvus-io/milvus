/*
 * Licensed to the LF AI & Data foundation under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package tasks

import (
	"io"
	"strconv"
	"testing"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/milvus-io/milvus-proto/go-api/v3/schemapb"
)

func TestDataFrameFromArrowReader_PreservesFieldMetadataAndNullability(t *testing.T) {
	pool := memory.NewGoAllocator()

	gbBuilder := array.NewInt64Builder(pool)
	gbBuilder.AppendValues([]int64{0, 42}, []bool{false, true})
	gbArr := gbBuilder.NewArray()
	gbBuilder.Release()
	defer gbArr.Release()

	schema := arrow.NewSchema([]arrow.Field{
		{
			Name:     groupByCol,
			Type:     arrow.PrimitiveTypes.Int64,
			Nullable: false,
			Metadata: arrow.NewMetadata(
				[]string{arrowMetadataFieldIDKey, arrowMetadataDataTypeKey},
				[]string{"105", strconv.Itoa(int(schemapb.DataType_Timestamptz))},
			),
		},
	}, nil)

	rec := array.NewRecord(schema, []arrow.Array{gbArr}, int64(gbArr.Len()))
	defer rec.Release()
	reader := &singleRecordReader{rec: rec}
	df, err := dataFrameFromArrowReader(reader)
	require.NoError(t, err)
	defer df.Release()
	assert.True(t, reader.released, "reader Release should be called after import")

	fieldID, ok := df.FieldID(groupByCol)
	require.True(t, ok)
	assert.Equal(t, int64(105), fieldID)

	fieldType, ok := df.FieldType(groupByCol)
	require.True(t, ok)
	assert.Equal(t, schemapb.DataType_Timestamptz, fieldType)

	fields := df.Schema().Fields()
	require.Len(t, fields, 1)
	assert.True(t, fields[0].Nullable, "array nulls must mark the DataFrame field nullable")
}

type singleRecordReader struct {
	rec      arrow.Record
	read     bool
	released bool
}

func (r *singleRecordReader) Read() (arrow.Record, error) {
	if r.read {
		return nil, io.EOF
	}
	r.read = true
	return r.rec, nil
}

func (r *singleRecordReader) Release() {
	r.released = true
}
